import { Span } from '@platform/tracing';

export class OutboxTracingAttributes {
  static setInitialBatchAttributes(span: Span | undefined) {
    span?.setAttributes({
      'batch.size': 1000,
      worker: 'workflow-orchestrator',
      'concurrency.limit': 100,
      'batch.max_retries': 3,
    });
  }

  static setPublishOutboxEventAttributes(
    span: Span | undefined,
    record: Record<string, string | number>,
  ) {
    span?.setAttributes({
      'outbox.id': record.id,
      'saga.id': record.saga_id,
      'outbox.domain': record.domain,
      'outbox.retries': record.retries,
      'messaging.routing_key': record.routing_key,
      'outbox.saga_event_type': record.saga_event_type,
    });
  }

  static setDlqOutboxEventAttributes(
    span: Span | undefined,
    record: Record<string, string | number>,
  ) {
    span?.setAttributes({
      'outbox.id': record.id,
      'saga.id': record.saga_id,
      'outbox.domain': record.domain,
      'outbox.retries': record.retries,
      'messaging.routing_key': record.routing_key,
    });
  }

  static setFinalBatchAttributes(
    span: Span | undefined,
    recordInfo: Record<string, number>,
  ) {
    span?.setAttributes({
      'batch.success': recordInfo.successIdsLength,
      'batch.failed': recordInfo.failedIdsLength,
      'batch.dlq': recordInfo.dlqIdsLength,
    });
  }

  static setOutboxEventTrace(span: Span | undefined, eventTitle: string) {
    span?.addEvent(eventTitle);
  }
}
