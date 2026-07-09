import { RabbitMQEventBus } from '../events/event-bus';
import { OrchestratorOutbox } from '../entities/orchestrator-outbox.entity';
import { OrchestratorOutboxRepository } from '../repositories/orchestrator-outbox.repository';
import { DataSource } from 'typeorm';
import { logger } from '@platform/logger';
import pLimit from 'p-limit';
import { context, trace, withSpan } from '@platform/tracing';
import { OutboxTracingAttributes } from 'src/tracing/attributes/outbox.attributes';
import { ContextPropagation } from 'src/tracing/propagation/context';

export class OutboxService {
  private readonly MAX_RETRIES = 3;
  private readonly limit = pLimit(100);

  constructor(
    private readonly eventBus: RabbitMQEventBus,
    private readonly outboxRepository: OrchestratorOutboxRepository,
    private readonly datasource: DataSource,
  ) {}

  /**
   * Polls unprocessed orchestrator outbox events, publishes them to the message broker
   * using the shared event bus, and updates their status.
   *
   * @returns {Promise<void>} A promise resolving when the polling cycle completes.
   */
  async poll(): Promise<void> {
    await withSpan('Outbox Events In Batch', async () => {
    const span = trace.getSpan(context.active());

    /* Setting the initial batch attributes for trace */
    OutboxTracingAttributes.setInitialBatchAttributes(span);

    logger.debug('Polling orchestrator outbox for unprocessed events');

    /* Getting the unprocessed records */
    const records =
      await this.outboxRepository.fetchUnprocessedEventsInBatch(1000);

    /* Adding event trace for more info */
    OutboxTracingAttributes.setOutboxEventTrace(
      span,
      'Fetched unprocessed events',
    );

    if (records.length === 0) {
      return;
    }

    logger.info('Fetched unprocessed outbox events', {
      count: records.length,
    });

    OutboxTracingAttributes.setOutboxEventTrace(
      span,
      'Publishing outbox event',
    );

    /* Publishing the events */
    const results = await Promise.allSettled(
      records.map((record) => {
        const parentContext = ContextPropagation.extractContext(
          //@ts-ignore
          record.metadata?.trace ?? {},
        );

        logger.debug('Publishing outbox record to event bus', {
          recordId: record.id,
          routingKey: record.routing_key,
        });
        return this.limit(() => {
          return withSpan(
            'Publish Outbox Event',
            () => {
              const span = trace.getSpan(context.active());

              /* Setting the necessary trace attributes for publish child span */
              OutboxTracingAttributes.setPublishOutboxEventAttributes(
                span,
                record as unknown as Record<string, string | number>,
              );

              return this.eventBus.publishWithRoutingKey(
                record.routing_key,
                record.payload,
              );
            },
            parentContext,
          );
        });
      }),
    );

    OutboxTracingAttributes.setOutboxEventTrace(
      span,
      'Classifying obtained results',
    );
    /* Separating the event ids based on their success and failure rates such that we can reroute them later */
    const { dlqIds, failedIds, successIds } = this.setEventIdsInRelevantDomain({
      records,
      results,
    });

    /* Events that are failed such that they shall be sent to DLQ */
    const dlqEvents =
      dlqIds.length > 0
        ? await this.outboxRepository.findEventsByIds(dlqIds)
        : [];

    OutboxTracingAttributes.setOutboxEventTrace(span, 'Publishing dlq events');
    /* Publishing the events to DLQ */
    if (dlqEvents.length > 0) {
      logger.warn('Publishing failed events to DLQ exchange', {
        count: dlqEvents.length,
        dlqIds,
      });
      await Promise.all(
        dlqEvents.map((event) => {
          const parentContext = ContextPropagation.extractContext(
            //@ts-ignore
            event.metadata?.trace ?? {}
          );

          this.limit(() =>
            withSpan(
              'Publish DLQ Event',
              () => {
                const span = trace.getSpan(context.active());

                /* Trace attributes for DLQ events */
                OutboxTracingAttributes.setDlqOutboxEventAttributes(
                  span,
                  event as unknown as Record<string, string | number>,
                );

                return this.eventBus.publishToDLQ({
                  payload: event.payload,
                  saga_id: event.saga_id,
                  saga_event_type: event.saga_event_type,
                  routing_key: event.routing_key,
                  domain: event.domain,
                });
              },
              parentContext,
            ),
          );
        }),
      );
    }

    OutboxTracingAttributes.setOutboxEventTrace(
      span,
      'Updating statuses of processed and failed records',
    );

    /* Updating the statuses */
    await this.datasource.transaction(async (manager) => {
      logger.debug(
        'Updating statuses of processed and failed records in transaction',
        {
          successIdsCount: successIds.length,
          dlqIdsCount: dlqIds.length,
          failedIdsCount: failedIds.length,
        },
      );

      if (successIds.length > 0) {
        await this.outboxRepository.markProcessed(successIds, manager);
      }

      if (dlqIds.length > 0) {
        await this.outboxRepository.markDeadLettered(dlqIds, manager);
      }

      if (failedIds.length > 0) {
        await this.outboxRepository.incrementRetries(failedIds, manager);
      }
    });

    /* Additional info for parent trace */
    OutboxTracingAttributes.setFinalBatchAttributes(span, {
      successIdsLength: successIds.length,
      failedIdsLength: failedIds.length,
      dlqIdsLength: dlqEvents.length,
    });
    OutboxTracingAttributes.setOutboxEventTrace(
      span,
      'Finished batch processing',
    );

    logger.debug('Polling batch processing finished successfully');
    });
  }

  /**
   * Categorizes event IDs into success, failed, or DLQ (exceeded max retries) pools based on publication results.
   *
   * @param {object} params - The categorization parameters.
   * @param {PromiseSettledResult<void>[]} params.results - The settling results of the publish operations.
   * @param {OrchestratorOutbox[]} params.records - The original outbox records corresponding to the results.
   * @returns {object} An object containing successIds, failedIds, and dlqIds arrays.
   */
  private setEventIdsInRelevantDomain({
    records,
    results,
  }: {
    results: PromiseSettledResult<void>[];
    records: OrchestratorOutbox[];
  }): { successIds: string[]; failedIds: string[]; dlqIds: string[] } {
    const successIds: string[] = [];
    const failedIds: string[] = [];
    const dlqIds: string[] = [];

    results.forEach((result, index) => {
      const record = records[index];

      if (result.status === 'fulfilled') {
        successIds.push(record.id);
        logger.debug('Event published successfully', { recordId: record.id });
      } else {
        const errorMsg =
          result.reason instanceof Error
            ? result.reason.message
            : String(result.reason);
        logger.error('Failed to publish outbox event', {
          recordId: record.id,
          error: errorMsg,
        });

        if (record.retries + 1 >= this.MAX_RETRIES) {
          dlqIds.push(record.id);
        } else {
          failedIds.push(record.id);
        }
      }
    });

    return {
      successIds,
      failedIds,
      dlqIds,
    };
  }
}
