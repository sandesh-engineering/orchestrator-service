import { ConsumeMessage } from '@platform/queue-rabbitmq';
import { logger } from '@platform/logger';
import { SagaCoordinator } from '../saga-coordinator';

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

    const raw = JSON.parse(message.content.toString()) as PaymentOrderSucceededEvent;

    logger.info('Received payment.v1.order.succeeded, evaluating saga start', {
      order_id: raw.order_id,
      payment_reference: raw.payment_reference,
    });

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
  };
}
