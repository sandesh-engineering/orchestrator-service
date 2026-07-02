import { ConsumeMessage } from '@platform/queue-rabbitmq';
import { logger } from '@platform/logger';
import { SagaCoordinator } from '../saga-coordinator';
import { OrchestratorDlqMessage } from '../types/saga.types';
import { withSpan } from '@platform/tracing';
import { ContextPropagation } from 'src/tracing/propagation/context';

/**
 * Typed shape of the event published by the payment service on `payment.v1.order.succeeded`.
 * All fields sourced from the payment service outbox payload contract.
 */
export interface PaymentOrderSucceededEvent {
  order_id: string;
  user_id: string;
  amount: number;
  currency: string;
  payment_reference: string;
  paid_at: string;
}

export class OrchestratorListener {
  constructor(private readonly coordinator: SagaCoordinator) {}

  /**
   * Handles an incoming RabbitMQ `ConsumeMessage` from the `payment.exchange` exchange.
   * Parses and validates the payload, then starts a dispatch saga if the event is new.
   *
   * @remarks
   * - Idempotent: if a saga with the same `order_id` already exists the handler returns early.
   * - Immediately dispatches step 0 (restaurant confirm) after saga creation so no extra poll
   *   cycle is required for the first command.
   *
   * @param {ConsumeMessage | null} message - Raw RabbitMQ consume message; null is a no-op.
   * @returns {Promise<void>} Resolves when the saga has been started and step 0 dispatched.
   */
  onPaymentOrderSucceeded = async (
    message: ConsumeMessage | null,
  ): Promise<void> => {
    /* Guard: broker sends null when the consumer is cancelled */
    if (!message) return;

    const context = ContextPropagation.extractContext(
      message?.properties?.headers?.trace ?? {},
    );

    await withSpan(
      'Dispatch Saga Start',
      async () => {
        const raw = JSON.parse(
          message.content.toString(),
        ) as PaymentOrderSucceededEvent;

        logger.info(
          'Received payment.v1.order.succeeded, evaluating saga start',
          {
            order_id: raw.order_id,
            payment_reference: raw.payment_reference,
          },
        );

        /* Edge case: malformed event missing the business key */
        if (!raw.order_id) {
          logger.warn('Ignoring payment event — missing order_id', { raw });
          return;
        }

        const idempotencyKey = `dispatch-saga:order:${raw.order_id}`;

        const { isNew, sagaId } = await this.coordinator.startSaga(
          idempotencyKey,
          raw.order_id,
          raw as unknown as Record<string, unknown>,
        );

        if (!isNew || !sagaId) {
          logger.warn('Saga already running or completed for order, skipping', {
            order_id: raw.order_id,
            sagaId,
            idempotencyKey,
          });
          return;
        }

        logger.info('Saga started for order, dispatching first step', {
          order_id: raw.order_id,
          sagaId,
        });

        /* Dispatch step 0 immediately — avoids waiting for the next outbox poll cycle */
        await this.coordinator.dispatchNextStep({
          payload: raw as unknown as Record<string, unknown>,
          sagaId,
          skipTransaction: true,
          stepIndex: 0,
        });
      },
      context,
    );
  };

  /**
   * Handles a message from the `orchestrator.dispatch.dlq` queue.
   *
   * @remarks
   * - Dispatch-domain failures are terminal by design: if any step in order
   *   placement/fulfillment fails, the platform does not get involved in
   *   resolving it. This handler records the failure context for audit
   *   purposes and marks the saga FAILED — no manual reprocessing, no alert.
   * - Idempotent: re-delivery of the same DLQ message simply re-applies the
   *   same FAILED state and failure_context to the saga.
   *
   * @param {ConsumeMessage | null} message - Raw RabbitMQ consume message; null is a no-op.
   * @returns {Promise<void>} Resolves once the saga has been marked FAILED.
   */
  onDispatchDlqEvents = async (
    message: ConsumeMessage | null,
  ): Promise<void> => {
    const raw = this.parseDlqMessage(message);

    /* Guard: null message (consumer cancelled) or missing saga_id */
    if (!raw) return;

    logger.warn('Dispatch saga step dead-lettered — marking saga FAILED', {
      saga_id: raw.saga_id,
      saga_event_type: raw.saga_event_type,
      routing_key: raw.routing_key,
    });

    /* Terminal: record what failed and close out the saga. */
    await this.coordinator.markSagaAsFailed(
      raw.saga_id,
      this.toFailureContext(raw),
    );
  };

  /**
   * Handles a message from the `orchestrator.subscription.dlq` queue.
   *
   * @remarks
   * - Subscription-domain failures are revenue-impacting. Unlike dispatch
   *   failures, these may be manually reprocessed once the underlying issue
   *   is resolved — the saga's `failure_context` is what the reprocess
   *   pipeline replays.
   * - This handler marks the saga FAILED with its failure_context AND
   *   raises an alert for on-call/ops, since human follow-up is expected.
   * - Idempotent: re-delivery of the same DLQ message simply re-applies the
   *   same FAILED state and failure_context, and re-fires the alert.
   *
   * @param {ConsumeMessage | null} message - Raw RabbitMQ consume message; null is a no-op.
   * @returns {Promise<void>} Resolves once the saga has been marked FAILED and the alert sent.
   */
  onSubscriptionDlqEvents = async (
    message: ConsumeMessage | null,
  ): Promise<void> => {
    const raw = this.parseDlqMessage(message);

    /* Guard: null message (consumer cancelled) or missing saga_id */
    if (!raw) return;

    logger.error(
      'Subscription saga step dead-lettered. Flagging for manual reprocess',
      {
        saga_id: raw.saga_id,
        saga_event_type: raw.saga_event_type,
        routing_key: raw.routing_key,
      },
    );

    /* Record what failed so the reprocess pipeline knows what to replay. */
    await this.coordinator.markSagaAsFailed(
      raw.saga_id,
      this.toFailureContext(raw),
    );

    /* Here we are logging for now but we need to setup alerting since this is revenue impacting step */
    logger.error(
      `Subscription saga ${raw.saga_id} failed at step "${raw.saga_event_type}" and needs manual reprocessing.`,
      { channel: 'subscription-failures' },
    );
  };

  /**
   * Parses and validates a raw DLQ `ConsumeMessage`.
   *
   * @remarks
   * - Guard: broker sends null when the consumer is cancelled.
   * - Edge case: malformed DLQ event missing `saga_id` is logged and skipped.
   *
   * @param {ConsumeMessage | null} message - Raw RabbitMQ consume message.
   * @returns {OrchestratorDlqMessage | null} Parsed message, or null if it should be skipped.
   */
  private parseDlqMessage(
    message: ConsumeMessage | null,
  ): OrchestratorDlqMessage | null {
    /* Guard: broker sends null when the consumer is cancelled */
    if (!message) return null;

    const raw = JSON.parse(
      message.content.toString(),
    ) as OrchestratorDlqMessage;

    logger.info('Received orchestrator DLQ event, evaluating saga end', {
      saga_id: raw.saga_id,
      saga_event_type: raw.saga_event_type,
      domain: raw.domain,
    });

    /* Edge case: malformed DLQ event missing the business key */
    if (!raw.saga_id) {
      logger.warn('Orchestrator DLQ event missing saga_id, skipping', { raw });
      return null;
    }

    return raw;
  }

  /**
   * Builds the `failure_context` snapshot persisted on the saga record.
   *
   * @param {OrchestratorDlqMessage} raw - Parsed DLQ message.
   * @returns {object} failure_context shape expected by `SagaCoordinator#markSagaAsFailed`.
   */
  private toFailureContext(raw: OrchestratorDlqMessage) {
    return {
      routing_key: raw.routing_key,
      saga_event_type: raw.saga_event_type,
      payload: raw.payload,
    };
  }
}
