import { SagaEntity, SagaStatus } from 'src/entities/saga.entity';
import { SagaDomain } from 'src/enums/saga.domain.enum';
import { RabbitMQEventBus } from 'src/events/event-bus';

/**
 * Backs the "subscription failures awaiting reprocess" admin view,
 * and the action that actually replays a failed step.
 *
 * The saga table is the source of truth for "what needs attention" —
 * RabbitMQ's subscription DLQ is just transport; SubscriptionDLQConsumer
 * already drained it into saga.status/failure_context.
 */
export class SubscriptionReprocessService {
  constructor(
    private readonly sagaRepository: SagaRepository, // adjust to your actual repo type
    private readonly eventBus: RabbitMQEventBus, // adjust to your actual type/path
  ) {}

  /**
   * Powers the admin dashboard:
   *   SELECT * FROM saga
   *   WHERE domain = 'subscription' AND status = 'FAILED'
   *   ORDER BY failed_at ASC
   * (covered by idx_saga_domain_status)
   */
  async listPendingReprocess(): Promise<SagaEntity[]> {
    return this.sagaRepository.find({
      where: { domain: SagaDomain.SUBSCRIPTION, status: SagaStatus.FAILED },
      order: { failed_at: 'ASC' },
    });
  }

  /**
   * Republishes the failed step's original event back to the main
   * exchange so the saga continues from where it died.
   *
   * IMPORTANT: the downstream step handler MUST be idempotent. A
   * "failure" here may mean the upstream call (e.g. payment charge)
   * actually succeeded and only the saga's bookkeeping step failed —
   * replaying it blindly could double-charge a customer.
   */
  async reprocess(sagaId: string): Promise<void> {
    const saga = await this.sagaRepository.findOneOrFail({
      where: { id: sagaId },
    });

    if (saga.domain !== SagaDomain.SUBSCRIPTION) {
      throw new Error(
        `Saga ${sagaId} is domain="${saga.domain}", not "${SagaDomain.SUBSCRIPTION}" refusing to reprocess via this pipeline.`,
      );
    }

    if (saga.status !== SagaStatus.FAILED) {
      throw new Error(
        `Saga ${sagaId} is not in FAILED state (current: ${saga.status}).`,
      );
    }

    if (!saga.failure_context) {
      throw new Error(`Saga ${sagaId} has no failure_context to replay.`);
    }

    await this.eventBus.publishWithRoutingKey(
      saga.failure_context.routing_key,
      saga.failure_context.payload,
    );

    await this.sagaRepository.update(sagaId, {
      status: SagaStatus.RUNNING,
      reprocess_count: saga.reprocess_count + 1,
      last_reprocessed_at: new Date().toISOString(),
    });
  }
}
