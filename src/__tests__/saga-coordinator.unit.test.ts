jest.mock('@platform/logger', () => ({
  logger: {
    debug: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
  },
}));

jest.mock('p-limit', () => ({}));

import { DataSource, EntityManager } from 'typeorm';
import { SagaCoordinator } from '../saga-coordinator';
import { OrchestratorEventType } from '../enums/orchestrator-event-type.enum';
import { OrchestratorOutbox } from '../entities/orchestrator-outbox.entity';
import { SagaStatus } from '../entities/saga.entity';
import { SagaRepository } from '../repositories/saga.repository';
import { OrchestratorOutboxRepository } from '../repositories/orchestrator-outbox.repository';
import { RabbitMQEventBus } from '../events/event-bus';

describe('SagaCoordinator', () => {
  const steps = [
    {
      name: 'AWAITING_RESTAURANT_CONFIRMATION',
      commandRoutingKey: 'restaurant.confirm',
      compensationRoutingKey: 'restaurant.cancel',
    },
  ];

  let sagaRepository: jest.Mocked<SagaRepository>;
  let outboxRepository: jest.Mocked<OrchestratorOutboxRepository>;
  let eventBus: jest.Mocked<RabbitMQEventBus>;
  let datasource: jest.Mocked<DataSource>;
  let coordinator: SagaCoordinator;

  beforeEach(() => {
    sagaRepository = {
      update: jest.fn(),
    } as unknown as jest.Mocked<SagaRepository>;

    outboxRepository = {
      createAndSaveOutboxRecord: jest.fn(),
    } as unknown as jest.Mocked<OrchestratorOutboxRepository>;

    eventBus = {
      subscribe: jest.fn(),
    } as unknown as jest.Mocked<RabbitMQEventBus>;

    datasource = {
      transaction: jest.fn(),
    } as unknown as jest.Mocked<DataSource>;

    coordinator = new SagaCoordinator(
      'dispatch-saga',
      steps,
      sagaRepository,
      outboxRepository,
      eventBus,
      datasource,
    );
  });

  describe('dispatchNextStep', () => {
    it('should generate event id when first saga command has no event id', async () => {
      /* Arrange */
      const payload = { order_id: 'order-1' };

      /* Act */
      await coordinator.dispatchNextStep({
        payload,
        sagaId: 'saga-1',
        skipTransaction: true,
        stepIndex: 0,
      });

      /* Assert */
      expect(outboxRepository.createAndSaveOutboxRecord).toHaveBeenCalledWith({
        saga_id: 'saga-1',
        event_id: 'generated-event-id',
        routing_key: 'restaurant.confirm',
        payload: {
          order_id: 'order-1',
          event_id: 'generated-event-id',
          saga_id: 'saga-1',
        },
        saga_event_type: OrchestratorEventType.COMMAND,
      });
    });

    it('should generate a new event id even when payload has an incoming event id', async () => {
      /* Arrange */
      const payload = { order_id: 'order-1', event_id: 'existing-event-id' };

      /* Act */
      await coordinator.dispatchNextStep({
        payload,
        sagaId: 'saga-1',
        skipTransaction: true,
        stepIndex: 0,
      });

      /* Assert */
      expect(outboxRepository.createAndSaveOutboxRecord).toHaveBeenCalledWith({
        saga_id: 'saga-1',
        event_id: 'generated-event-id',
        routing_key: 'restaurant.confirm',
        payload: {
          order_id: 'order-1',
          event_id: 'generated-event-id',
          saga_id: 'saga-1',
        },
        saga_event_type: OrchestratorEventType.COMMAND,
      });
    });

    it('should create transactional outbox record with resolved event id', async () => {
      /* Arrange */
      const create = jest.fn(
        (
          _entity: typeof OrchestratorOutbox,
          value: Partial<OrchestratorOutbox>,
        ) => value,
      );
      const save = jest.fn();
      const manager = {
        create,
        save,
      } as unknown as EntityManager;

      /* Act */
      await coordinator.dispatchNextStep({
        payload: { order_id: 'order-1' },
        sagaId: 'saga-1',
        manager,
        stepIndex: 0,
      });

      /* Assert */
      expect(create).toHaveBeenCalledWith(OrchestratorOutbox, {
        saga_id: 'saga-1',
        event_id: 'generated-event-id',
        routing_key: 'restaurant.confirm',
        payload: {
          order_id: 'order-1',
          event_id: 'generated-event-id',
          saga_id: 'saga-1',
        },
        saga_event_type: OrchestratorEventType.COMMAND,
      });
      expect(save).toHaveBeenCalledWith(OrchestratorOutbox, expect.any(Object));
    });

    it('should skip dispatch when there are no remaining steps', async () => {
      /* Arrange */
      const payload = { order_id: 'order-1' };

      /* Act */
      await coordinator.dispatchNextStep({
        payload,
        sagaId: 'saga-1',
        skipTransaction: true,
        stepIndex: 1,
      });

      /* Assert */
      expect(outboxRepository.createAndSaveOutboxRecord).not.toHaveBeenCalled();
    });
  });

  describe('markSagaAsFailed', () => {
    it('should skip failure update when saga id is missing', async () => {
      /* Arrange */
      const failureContext = {
        routing_key: 'dispatch.create',
        saga_event_type: OrchestratorEventType.COMMAND,
        payload: { order_id: 'order-1' },
      };

      /* Act */
      await coordinator.markSagaAsFailed('', failureContext);

      /* Assert */
      expect(sagaRepository.update).not.toHaveBeenCalled();
    });

    it('should mark saga as failed with failure context', async () => {
      /* Arrange */
      const failureContext = {
        routing_key: 'dispatch.create',
        saga_event_type: OrchestratorEventType.COMMAND,
        payload: { order_id: 'order-1' },
      };
      sagaRepository.update.mockResolvedValue({
        affected: 1,
        raw: [],
        generatedMaps: [],
      });

      /* Act */
      await coordinator.markSagaAsFailed('saga-1', failureContext);

      /* Assert */
      expect(sagaRepository.update).toHaveBeenCalledWith({
        whereClause: { id: 'saga-1', status: expect.any(Object) },
        payload: {
          status: SagaStatus.FAILED,
          failure_context: failureContext,
          failed_at: expect.any(Function),
        },
      });
    });
  });
});
