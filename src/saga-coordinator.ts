import { AppError, BAD_REQUEST, CONFLICT } from '@core/main';
import { logger } from '@platform/logger';
import type { ConsumeMessage } from '@platform/queue-rabbitmq';
import { randomUUID } from 'crypto';
import { DataSource, EntityManager, In, Not } from 'typeorm';
import { EVENTS } from './constants/saga.constants';
import { SagaEntity, SagaStatus } from './entities/saga.entity';
import { OrchestratorOutbox } from './entities/orchestrator-outbox.entity';
import { OrchestratorEventType } from './enums/orchestrator-event-type.enum';
import { RabbitMQEventBus } from './events/event-bus';
import { OrchestratorOutboxRepository } from './repositories/orchestrator-outbox.repository';
import { SagaRepository } from './repositories/saga.repository';
import { SagaStateDefinition } from './types/saga.types';

export interface StepSuccessPayload {
  type: 'STEP_SUCCESS';
  saga_id: string;
  event_id: string;
  payload: Record<string, unknown>;
}

export interface StepFailurePayload {
  type: 'STEP_FAILURE';
  saga_id: string;
  event_id: string;
  payload: Record<string, unknown>;
}

interface DomainStepReplyPayload {
  type?: string;
  saga_id?: string;
  event_id?: string;
  order_id?: string;
  payload?: {
    order_id?: string;
  };
  [key: string]: unknown;
}

export class SagaCoordinator {
  /**
   * Constructs the Saga Coordinator, subscribing to RabbitMQ event bus messages
   * and routing step success or failure events to their respective handlers.
   *
   * @param {string} name - The name of the Saga workflow.
   * @param {SagaStateDefinition[]} steps - The steps comprising the Saga workflow.
   * @param {SagaRepository} sagaRepository - Repository for persistence of Saga states.
   * @param {RabbitMQEventBus} eventBus - RabbitMQ event bus instance.
   * @param {DataSource} datasource - TypeORM DataSource for transaction management.
   */
  constructor(
    private readonly name: string,
    private readonly steps: SagaStateDefinition[],
    private readonly sagaRepository: SagaRepository,
    private readonly sagaOutboxRepository: OrchestratorOutboxRepository,
    private readonly eventBus: RabbitMQEventBus,
    private readonly datasource: DataSource,
  ) {
    logger.debug('SagaCoordinator constructed', { name: this.name });
  }

  /**
   * Subscribes to the orchestrator queue on the event bus, routing incoming messages
   * to `onStepSuccess` or `onStepFailure` depending on their type.
   *
   * @remarks
   * - Must be called after `RabbitMQEventBus.bootstrapRabbitMQ()` so the channel is live.
   * - Calling it before bootstrap results in an AppError (channel not initialized).
   *
   * @returns {Promise<void>} Resolves when the consumer is registered with the broker.
   */
  async start(): Promise<void> {
    logger.debug('SagaCoordinator subscribing to event bus', {
      name: this.name,
    });

    await this.eventBus.subscribe(async (message) => {
      if (!message) return;

      logger.debug('Received raw message on event bus', {
        length: message.content.length,
        routingKey: message.fields.routingKey,
      });

      const payload = await this.toSagaStepPayload(message);
      if (!payload) return;

      logger.debug('Parsed event bus message payload', {
        type: payload.type,
        sagaId: payload.saga_id,
        eventId: payload.event_id,
      });

      try {
        if (payload.type === 'STEP_SUCCESS') {
          await this.onStepSuccess(payload);
        } else if (payload.type === 'STEP_FAILURE') {
          await this.onStepFailure(payload);
        }
      } catch (err: unknown) {
        logger.error('Unhandled error in saga message handler', {
          type: payload.type,
          sagaId: payload.saga_id,
          error: err instanceof Error ? err.message : String(err),
        });
        throw err;
      }
    });
  }

  /**
   * Starts a new Saga workflow, creating a persistent pending record in the database.
   * If a saga already exists for this idempotency_key, returns the existing saga ID.
   *
   * @param idempotency_key  - Unique key per business event (e.g., "order-confirmation-orderId")
   * @param correlation_id   - Business entity ID for lookups (e.g., orderId)
   * @param {Record<string, unknown>} [payload] - Optional initial payload for the Saga.
   *
   * @returns {Promise<{ sagaId: string | undefined; isNew: boolean }>} A promise resolving to the created Saga ID.
   */
  async startSaga(
    idempotency_key: string,
    correlation_id: string,
    payload?: Record<string, unknown>,
  ): Promise<{ sagaId: string | undefined; isNew: boolean }> {
    logger.info('Starting new Saga workflow', {
      sagaName: this.name,
      idempotency_key,
      correlation_id,
    });

    const result = await this.datasource.transaction(async (manager) => {
      await manager
        .createQueryBuilder()
        .insert()
        .into(SagaEntity)
        .values({
          name: this.name,
          idempotency_key,
          correlation_id,
          // @ts-expect-error - TypeORM QueryBuilder has a known limitation with jsonb columns containing unknown types
          payload: payload ?? {},
          current_step_index: 0,
          status: SagaStatus.PENDING,
          started_at: new Date().toISOString(),
        })
        .orIgnore()
        .execute();

      return manager.findOne(SagaEntity, {
        where: { idempotency_key },
      });
    });

    const isNew =
      result?.status === SagaStatus.PENDING && result?.current_step_index === 0;

    logger.info('Saga start resolved', {
      sagaId: result?.id,
      isNew,
      status: result?.status,
    });

    return { sagaId: result?.id, isNew };
  }

  /**
   * Handles step success events, advancing the Saga state and inserting a command record into the outbox.
   *
   * @param {StepSuccessPayload} payload - The success event payload.
   * @returns {Promise<void>} A promise resolving when processing completes.
   * @throws {AppError} If the update fails (already processed).
   */
  private async onStepSuccess(payload: StepSuccessPayload): Promise<void> {
    logger.info('Handling step success event', {
      sagaId: payload.saga_id,
      eventId: payload.event_id,
    });

    const sagaRecord = await this.sagaRepository.findById(payload.saga_id);

    if (!sagaRecord) {
      logger.warn('Saga record not found for step success', {
        sagaId: payload.saga_id,
      });
      return;
    }

    if (
      sagaRecord.status !== SagaStatus.PENDING &&
      sagaRecord.status !== SagaStatus.RUNNING
    ) {
      logger.warn('Saga status is not PENDING or RUNNING, ignoring success event', {
        sagaId: payload.saga_id,
        status: sagaRecord.status,
      });
      return;
    }

    const finishedStep = this.steps[sagaRecord.current_step_index];
    const nextIndex = sagaRecord.current_step_index + 1;
    const isLastStep = nextIndex >= this.steps.length;

    await this.datasource.transaction(async (manager) => {
      logger.debug(
        'Executing update on SagaEntity and saving command outbox in transaction',
        { sagaId: payload.saga_id },
      );

      const updateMetadata = await manager.update(
        SagaEntity,
        {
          id: payload.saga_id,
          status: In([SagaStatus.PENDING, SagaStatus.RUNNING]),
        },
        {
          // @ts-expect-error - TypeORM QueryBuilder has a known limitation with jsonb columns containing unknown types
          payload: payload.payload,
          status: isLastStep ? SagaStatus.COMPLETED : SagaStatus.RUNNING,
          completed_steps: () => this.appendCompletedStepSql(finishedStep.name),
          current_step_index: nextIndex,
        },
      );

      if (updateMetadata.affected === 0) {
        logger.error(
          'Failed to update SagaEntity, transition conflict occurred',
          { sagaId: payload.saga_id },
        );
        throw new AppError(false, 'Already processed!', CONFLICT, true);
      }

      logger.debug('SagaEntity updated successfully', {
        sagaId: payload.saga_id,
        currentStep: finishedStep.name,
        isLastStep,
      });

      await this.dispatchNextStep({
        payload: payload.payload,
        sagaId: payload.saga_id,
        manager,
        stepIndex: nextIndex,
      });

      logger.debug(
        'OrchestratorOutbox command record created and saved in transaction',
        { sagaId: payload.saga_id, eventId: payload.event_id },
      );
    });

    logger.info('Step success processed successfully', {
      sagaId: payload.saga_id,
      nextIndex,
      isLastStep,
    });
  }

  /**
   * Handles step failure events, transitioning the Saga to compensating state,
   * inserting a compensation outbox event, and initiating compensation steps.
   *
   * @param {StepFailurePayload} payload - The failure event payload.
   * @returns {Promise<void>} A promise resolving when compensation processing completes.
   */
  private async onStepFailure(payload: StepFailurePayload): Promise<void> {
    logger.warn('Handling step failure event', {
      sagaId: payload.saga_id,
      eventId: payload.event_id,
    });

    const sagaRecord = await this.sagaRepository.findById(payload.saga_id);

    if (!sagaRecord) {
      logger.warn('Saga record not found for step failure', {
        sagaId: payload.saga_id,
      });
      return;
    }

    if (
      sagaRecord.status !== SagaStatus.PENDING &&
      sagaRecord.status !== SagaStatus.RUNNING
    ) {
      logger.warn(
        'Saga status is not PENDING or RUNNING, ignoring failure event',
        { sagaId: payload.saga_id, status: sagaRecord.status },
      );
      return;
    }

    const completedStates = sagaRecord.completed_steps
      .map((name) => this.steps.find((s) => s.name === name)!)
      .filter(Boolean)
      .reverse();

    await this.datasource.transaction(async (manager) => {
      logger.debug(
        'Updating SagaEntity status to COMPENSATING and saving outbox compensation record',
        { sagaId: payload.saga_id },
      );

      const updateMetadata = await manager.update(
        SagaEntity,
        {
          id: payload.saga_id,
          status: In([SagaStatus.PENDING, SagaStatus.RUNNING]),
        },
        {
          status: SagaStatus.COMPENSATING,
        },
      );

      if (updateMetadata.affected === 0) {
        logger.error(
          'Failed to update SagaEntity status on failure, transition conflict occurred',
          { sagaId: payload.saga_id },
        );
        throw new AppError(false, 'Already processed!', BAD_REQUEST, true);
      }

      logger.debug('SagaEntity status set to COMPENSATING', {
        sagaId: payload.saga_id,
      });

      for (const state of completedStates) {
        if (state.compensationRoutingKey) {
          const compensationInstance = manager.create(OrchestratorOutbox, {
            saga_id: payload.saga_id,
            event_id: payload.event_id,
            routing_key: state.compensationRoutingKey,
            payload: payload.payload,
            saga_event_type: OrchestratorEventType.COMPENSATION,
          });

          await manager.save(OrchestratorOutbox, compensationInstance);
        }
      }

      logger.debug(
        'OrchestratorOutbox compensation records created and saved in transaction',
        { sagaId: payload.saga_id, eventId: payload.event_id },
      );
    });

    logger.info('Step failure processed, initiating compensation sequence', {
      sagaId: payload.saga_id,
    });
  }

  /**
   * Dispatches the command for a given step index by writing to the outbox.
   *
   * @param {object} payload - Payload that is to be sent via the command
   * @param {string} sagaId - Saga id that determines to which saga id the command belongs to
   * @param {boolean} skipTransaction - Boolean flag that determines if the insert should be atomic or not
   * @param {number} stepIndex - Index that determines which step is to be inserted in outbox
   * @param {EntityManager} manager - Transaction manager that helps to perform atomic inserts
   *
   * @returns {void} A promise resolving when the insert completes
   */
  async dispatchNextStep({
    payload,
    sagaId,
    skipTransaction = false,
    stepIndex,
    manager,
  }: {
    sagaId: string;
    stepIndex: number;
    payload: Record<string, unknown>;
    manager?: EntityManager;
    skipTransaction?: boolean;
  }): Promise<void> {
    if (stepIndex >= this.steps.length) {
      logger.info('All steps completed, no further dispatch needed', {
        sagaId,
      });
      return;
    }

    const step = this.steps[stepIndex];

    logger.info('Dispatching step command via outbox', {
      sagaId,
      stepIndex,
      stepName: step.name,
    });

    const routingKey = step.commandRoutingKey;
    const eventId = this.createOutboxEventId();
    const commandPayload = { ...payload, event_id: eventId, saga_id: sagaId };

    if (!skipTransaction && manager) {
      const commandInstance = manager.create(OrchestratorOutbox, {
        saga_id: sagaId,
        event_id: eventId,
        routing_key: routingKey,
        payload: commandPayload,
        saga_event_type: OrchestratorEventType.COMMAND,
      });

      await manager.save(OrchestratorOutbox, commandInstance);
    } else {
      await this.sagaOutboxRepository.createAndSaveOutboxRecord({
        saga_id: sagaId,
        event_id: eventId,
        routing_key: routingKey,
        payload: commandPayload,
        saga_event_type: OrchestratorEventType.COMMAND,
      });
    }
  }

  /**
   * Creates the outbox event identifier for an orchestrator command.
   *
   * @remarks
   * - The orchestrator owns command event identity because each emitted command is a distinct event.
   * - Correlation and saga ids remain stable for saga lookup and idempotency decisions.
   *
   * @returns {string} Newly generated UUID for the outbox command event.
   */
  private createOutboxEventId(): string {
    return randomUUID();
  }

  private async toSagaStepPayload(
    message: ConsumeMessage,
  ): Promise<StepSuccessPayload | StepFailurePayload | null> {
    const raw = JSON.parse(message.content.toString()) as DomainStepReplyPayload;
    const routingKey = message.fields.routingKey;
    const explicitType = raw.type;

    const type =
      explicitType === 'STEP_SUCCESS' || explicitType === 'STEP_FAILURE'
        ? explicitType
        : this.resolveStepReplyType(routingKey);

    if (!type) {
      logger.warn('Ignoring unsupported orchestrator message', {
        routingKey,
        type: explicitType,
      });
      return null;
    }

    const sagaId = await this.resolveSagaId(raw);

    if (!sagaId) {
      logger.warn('Ignoring step reply without resolvable saga id', {
        routingKey,
        order_id: this.getOrderId(raw),
      });
      return null;
    }

    return {
      type,
      saga_id: sagaId,
      event_id: raw.event_id ?? this.createOutboxEventId(),
      payload: raw as Record<string, unknown>,
    };
  }

  private resolveStepReplyType(
    routingKey: string,
  ): StepSuccessPayload['type'] | StepFailurePayload['type'] | null {
    const successRoutingKeys = new Set<string>([
      EVENTS.ORDER_ACCEPTED_V1,
      EVENTS.DISPATCH_CREATED_V1,
      EVENTS.AGENT_NOTIFIED_V1,
      EVENTS.AGENT_ASSIGNED_V1,
    ]);

    const failureRoutingKeys = new Set<string>([
      EVENTS.ORDER_REJECTED_V1,
      EVENTS.DISPATCH_CREATION_REJECTED_V1,
    ]);

    if (successRoutingKeys.has(routingKey)) return 'STEP_SUCCESS';
    if (failureRoutingKeys.has(routingKey)) return 'STEP_FAILURE';
    return null;
  }

  private async resolveSagaId(
    raw: DomainStepReplyPayload,
  ): Promise<string | null> {
    if (raw.saga_id && raw.saga_id !== 'RESOLVED_AT_RUNTIME') {
      return raw.saga_id;
    }

    const orderId = this.getOrderId(raw);
    if (!orderId) return null;

    const saga = await this.sagaRepository.findByCorrelationId(orderId);
    return saga?.id ?? null;
  }

  private getOrderId(raw: DomainStepReplyPayload): string | undefined {
    return raw.order_id ?? raw.payload?.order_id;
  }

  private appendCompletedStepSql(stepName: string): string {
    return `completed_steps || '${JSON.stringify([stepName])}'::jsonb`;
  }

  async markSagaAsFailed(
    saga_id: string,
    failure_context: {
      routing_key: string;
      saga_event_type: string;
      payload: Record<string, unknown>;
    },
  ) {
    if (!saga_id) {
      logger.warn('Saga id missing. Skipping marking saga as failed!');
      return;
    }

    const updateMetadata = await this.sagaRepository.update({
      whereClause: { id: saga_id, status: Not(SagaStatus.FAILED) },
      payload: {
        status: SagaStatus.FAILED,
        failure_context,
        failed_at: () => 'NOW()',
      },
    });

    if (updateMetadata.affected === 0) {
      logger.error('Saga already marked as failed', { sagaId: saga_id });
      throw new AppError(
        false,
        'Saga already marked as failed!',
        BAD_REQUEST,
        true,
      );
    }

    logger.info('Saga marked as failed!', { sagaId: saga_id });
  }
}
