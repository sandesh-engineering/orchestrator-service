import { SagaEntity, SagaStatus } from '../entities/saga.entity';
import { SagaDomain } from '../enums/saga.domain.enum';
import { RabbitMQEventBus } from '../events/event-bus';
import { SagaRepository } from '../repositories/saga.repository';

/**
 * Powers both the admin "subscription failures awaiting reprocess" view and
 * the action that replays a failed step back onto the exchange.
 *
 * @remarks
 * The saga table is the authoritative source for "what needs attention":
 * the subscription DLQ is only a transport mechanism — `SubscriptionDLQConsumer`
 * already drained it into `saga.status` and `saga.failure_context` before
 * this service is involved.
 */
export class SubscriptionReprocessService {
  constructor(
    private readonly sagaRepository: SagaRepository,
    private readonly eventBus: RabbitMQEventBus,
  ) {}

  /**
   * Returns all subscription-domain sagas that are currently in a FAILED state,
   * ordered oldest-failure-first for prioritised reprocessing.
   *
   * @remarks
   * - Backed by `idx_saga_domain_status` — avoids a sequential scan on large tables.
   * - Equivalent SQL: `SELECT * FROM saga WHERE domain = 'subscription' AND status = 'FAILED' ORDER BY failed_at ASC`
   *
   * @returns {Promise<SagaEntity[]>} Sagas awaiting manual reprocess, ordered by `failed_at ASC`.
   */
  async listPendingReprocess(): Promise<SagaEntity[]> {
    return this.sagaRepository.find({
      where: { domain: SagaDomain.SUBSCRIPTION, status: SagaStatus.FAILED },
      order: { failed_at: 'ASC' },
    });
  }

  /**
   * Replays the failed step's original event back onto the main orchestrator exchange
   * so the saga can continue from the exact point it died.
   *
   * @remarks
   * - **Idempotency requirement**: the downstream step handler MUST be idempotent before
   *   invoking this method. A step "failure" may mean the upstream call (e.g. a payment
   *   charge) actually succeeded but only the saga's bookkeeping step failed — replaying
   *   blindly without idempotency guards could double-charge a customer.
   * - After publishing, the saga is transitioned to `RUNNING` and its `reprocess_count`
   *   and `last_reprocessed_at` fields are updated for audit purposes.
   *
   * @param {string} sagaId - UUID of the saga to reprocess.
   * @returns {Promise<void>} Resolves once the event has been published and the saga row updated.
   * @throws {Error} Throws if the saga domain is not `subscription`, the status is not `FAILED`,
   *                 or the saga has no `failure_context` to replay.
   */
  async reprocess(sagaId: string): Promise<void> {
    const saga = await this.sagaRepository.findOneOrFail({
      where: { id: sagaId },
    });

    /* Guard: only subscription-domain sagas flow through this pipeline */
    if (saga.domain !== SagaDomain.SUBSCRIPTION) {
      throw new Error(
        `Saga ${sagaId} is domain="${saga.domain}", not "${SagaDomain.SUBSCRIPTION}" — refusing to reprocess via this pipeline.`,
      );
    }

    /* Guard: only FAILED sagas should be replayed */
    if (saga.status !== SagaStatus.FAILED) {
      throw new Error(
        `Saga ${sagaId} is not in FAILED state (current: ${saga.status}).`,
      );
    }

    /* Guard: without failure_context we have nothing to replay */
    if (!saga.failure_context) {
      throw new Error(`Saga ${sagaId} has no failure_context to replay.`);
    }

    await this.eventBus.publishWithRoutingKey(
      saga.failure_context.routing_key,
      saga.failure_context.payload,
    );

    await this.sagaRepository.updateById(sagaId, {
      status: SagaStatus.RUNNING,
      reprocess_count: saga.reprocess_count + 1,
      last_reprocessed_at: new Date().toISOString(),
    });
  }
}
