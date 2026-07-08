jest.mock('@platform/logger', () => ({
  logger: {
    debug: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
  },
}));

jest.mock('p-limit', () => ({}));

jest.mock('crypto', () => ({
  randomUUID: () => 'generated-event-id',
}));

import { DataSource, EntityManager, In } from 'typeorm';
import { SagaCoordinator } from '../saga-coordinator';
import { OrchestratorEventType } from '../enums/orchestrator-event-type.enum';
import { OrchestratorOutbox } from '../entities/orchestrator-outbox.entity';
import { SagaEntity, SagaStatus } from '../entities/saga.entity';
import { SagaDomain } from '../enums/saga.domain.enum';
import { SagaRepository } from '../repositories/saga.repository';
import { OrchestratorOutboxRepository } from '../repositories/orchestrator-outbox.repository';
import { RabbitMQEventBus } from '../events/event-bus';
import { AppError } from '@core/main';

type TransactionFn = <T>(
  runInTransaction: (entityManager: EntityManager) => Promise<T>,
) => Promise<T>;

describe('SagaCoordinator', () => {
  const steps = [
    {
      name: 'AWAITING_RESTAURANT_CONFIRMATION',
      commandRoutingKey: 'restaurant.confirm',
      compensationRoutingKey: 'restaurant.cancel',
    },
    {
      name: 'AWAITING_DELIVERY_ASSIGNMENT',
      commandRoutingKey: 'delivery.assign',
      compensationRoutingKey: 'delivery.cancel',
    },
  ];

  let sagaRepository: jest.Mocked<SagaRepository>;
  let outboxRepository: jest.Mocked<OrchestratorOutboxRepository>;
  let eventBus: jest.Mocked<RabbitMQEventBus>;
  let datasource: jest.Mocked<DataSource>;
  let coordinator: SagaCoordinator;

  beforeEach(() => {
    sagaRepository = {
      findById: jest.fn(),
      findByCorrelationId: jest.fn(),
      update: jest.fn(),
    } as unknown as jest.Mocked<SagaRepository>;

    outboxRepository = {
      createAndSaveOutboxRecord: jest.fn(),
    } as unknown as jest.Mocked<OrchestratorOutboxRepository>;

    eventBus = {
      subscribe: jest.fn(),
      publishWithRoutingKey: jest.fn(),
      publishToDLQ: jest.fn(),
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

  describe('startSaga', () => {
    it('should start a new Saga successfully and return true for isNew', async () => {
      /* Arrange */
      const mockQueryBuilder = {
        insert: jest.fn().mockReturnThis(),
        into: jest.fn().mockReturnThis(),
        values: jest.fn().mockReturnThis(),
        orIgnore: jest.fn().mockReturnThis(),
        execute: jest.fn().mockResolvedValue({}),
      };

      const mockSaga = createSagaEntity({
        id: 'new-saga-id',
        status: SagaStatus.PENDING,
        current_step_index: 0,
      });

      const manager = {
        createQueryBuilder: jest.fn().mockReturnValue(mockQueryBuilder),
        findOne: jest.fn().mockResolvedValue(mockSaga),
      } as unknown as EntityManager;

      const mockTransaction = datasource.transaction as unknown as jest.MockedFunction<TransactionFn>;
      mockTransaction.mockImplementation(async (callback) => {
        return callback(manager);
      });

      /* Act */
      const result = await coordinator.startSaga(
        'idempotence-key-1',
        'correlation-id-1',
        { order_id: 'order-1' },
      );

      /* Assert */
      expect(result).toEqual({ sagaId: 'new-saga-id', isNew: true });
      expect(manager.findOne).toHaveBeenCalledWith(SagaEntity, {
        where: { idempotency_key: 'idempotence-key-1' },
      });
    });

    it('should return isNew false if the Saga already exists', async () => {
      /* Arrange */
      const mockQueryBuilder = {
        insert: jest.fn().mockReturnThis(),
        into: jest.fn().mockReturnThis(),
        values: jest.fn().mockReturnThis(),
        orIgnore: jest.fn().mockReturnThis(),
        execute: jest.fn().mockResolvedValue({}),
      };

      const mockSaga = createSagaEntity({
        id: 'existing-saga-id',
        status: SagaStatus.RUNNING,
        current_step_index: 1,
      });

      const manager = {
        createQueryBuilder: jest.fn().mockReturnValue(mockQueryBuilder),
        findOne: jest.fn().mockResolvedValue(mockSaga),
      } as unknown as EntityManager;

      const mockTransaction = datasource.transaction as unknown as jest.MockedFunction<TransactionFn>;
      mockTransaction.mockImplementation(async (callback) => {
        return callback(manager);
      });

      /* Act */
      const result = await coordinator.startSaga(
        'idempotence-key-1',
        'correlation-id-1',
        { order_id: 'order-1' },
      );

      /* Assert */
      expect(result).toEqual({ sagaId: 'existing-saga-id', isNew: false });
    });
  });

  describe('start and event routing', () => {
    it('should subscribe to event bus and route STEP_SUCCESS messages to onStepSuccess', async () => {
      /* Arrange */
      let subscribeCallback: ((message: any) => Promise<void>) | undefined;
      eventBus.subscribe.mockImplementation(async (callback) => {
        subscribeCallback = callback;
      });

      const messagePayload = {
        type: 'STEP_SUCCESS',
        saga_id: 'saga-1',
        event_id: 'event-1',
        payload: { order_id: 'order-1' },
      };

      const amqpMessage = {
        content: Buffer.from(JSON.stringify(messagePayload)),
        fields: { routingKey: 'restaurant.confirm' },
        properties: { headers: {} },
      };

      const sagaRecord = createSagaEntity({
        id: 'saga-1',
        status: SagaStatus.PENDING,
        current_step_index: 0,
        completed_steps: [],
        payload: {},
      });

      sagaRepository.findById.mockResolvedValue(sagaRecord);

      const manager = {
        update: jest.fn().mockResolvedValue({ affected: 1 }),
        create: jest.fn((_entity, val) => val),
        save: jest.fn(),
      } as unknown as EntityManager;

      const mockTransaction = datasource.transaction as unknown as jest.MockedFunction<TransactionFn>;
      mockTransaction.mockImplementation(async (callback) => {
        return callback(manager);
      });

      /* Act */
      await coordinator.start();
      expect(subscribeCallback).toBeDefined();
      if (subscribeCallback) {
        await subscribeCallback(amqpMessage);
      }

      /* Assert */
      expect(sagaRepository.findById).toHaveBeenCalledWith('saga-1');
      expect(manager.update).toHaveBeenCalledWith(
        SagaEntity,
        { id: 'saga-1', status: In([SagaStatus.PENDING, SagaStatus.RUNNING]) },
        expect.objectContaining({
          status: SagaStatus.RUNNING,
          current_step_index: 1,
        }),
      );
      expect(manager.create).toHaveBeenCalledWith(OrchestratorOutbox, {
        saga_id: 'saga-1',
        event_id: 'generated-event-id',
        routing_key: 'delivery.assign',
        payload: {
          event_id: 'generated-event-id',
          saga_id: 'saga-1',
          type: 'STEP_SUCCESS',
          payload: { order_id: 'order-1' },
        },
        saga_event_type: OrchestratorEventType.COMMAND,
        metadata: { trace: {} },
      });
      expect(manager.save).toHaveBeenCalled();
    });

    it('should subscribe to event bus and route STEP_FAILURE messages to onStepFailure', async () => {
      /* Arrange */
      let subscribeCallback: ((message: any) => Promise<void>) | undefined;
      eventBus.subscribe.mockImplementation(async (callback) => {
        subscribeCallback = callback;
      });

      const messagePayload = {
        type: 'STEP_FAILURE',
        saga_id: 'saga-1',
        event_id: 'event-1',
        payload: { order_id: 'order-1' },
      };

      const amqpMessage = {
        content: Buffer.from(JSON.stringify(messagePayload)),
        fields: { routingKey: 'restaurant.confirm' },
        properties: { headers: {} },
      };

      const sagaRecord = createSagaEntity({
        id: 'saga-1',
        status: SagaStatus.RUNNING,
        current_step_index: 1,
        completed_steps: ['AWAITING_RESTAURANT_CONFIRMATION'],
        payload: {},
      });

      sagaRepository.findById.mockResolvedValue(sagaRecord);

      const manager = {
        update: jest.fn().mockResolvedValue({ affected: 1 }),
        create: jest.fn((_entity, val) => val),
        save: jest.fn(),
      } as unknown as EntityManager;

      const mockTransaction = datasource.transaction as unknown as jest.MockedFunction<TransactionFn>;
      mockTransaction.mockImplementation(async (callback) => {
        return callback(manager);
      });

      /* Act */
      await coordinator.start();
      expect(subscribeCallback).toBeDefined();
      if (subscribeCallback) {
        await subscribeCallback(amqpMessage);
      }

      /* Assert */
      expect(sagaRepository.findById).toHaveBeenCalledWith('saga-1');
      expect(manager.update).toHaveBeenCalledWith(
        SagaEntity,
        { id: 'saga-1', status: In([SagaStatus.PENDING, SagaStatus.RUNNING]) },
        { status: SagaStatus.COMPENSATING },
      );
      expect(manager.create).toHaveBeenCalledWith(OrchestratorOutbox, {
        saga_id: 'saga-1',
        event_id: 'event-1',
        routing_key: 'restaurant.cancel',
        payload: {
          type: 'STEP_FAILURE',
          saga_id: 'saga-1',
          event_id: 'event-1',
          payload: { order_id: 'order-1' },
        },
        saga_event_type: OrchestratorEventType.COMPENSATION,
      });
      expect(manager.save).toHaveBeenCalled();
    });

    it('should ignore event when saga record not found', async () => {
      /* Arrange */
      let subscribeCallback: ((message: any) => Promise<void>) | undefined;
      eventBus.subscribe.mockImplementation(async (callback) => {
        subscribeCallback = callback;
      });

      const messagePayload = {
        type: 'STEP_SUCCESS',
        saga_id: 'saga-not-found',
        event_id: 'event-1',
        payload: {},
      };

      const amqpMessage = {
        content: Buffer.from(JSON.stringify(messagePayload)),
        fields: { routingKey: 'restaurant.confirm' },
        properties: { headers: {} },
      };

      sagaRepository.findById.mockResolvedValue(null);

      /* Act */
      await coordinator.start();
      if (subscribeCallback) {
        await subscribeCallback(amqpMessage);
      }

      /* Assert */
      expect(sagaRepository.findById).toHaveBeenCalledWith('saga-not-found');
      expect(datasource.transaction).not.toHaveBeenCalled();
    });

    it('should ignore event when saga status is not PENDING or RUNNING', async () => {
      /* Arrange */
      let subscribeCallback: ((message: any) => Promise<void>) | undefined;
      eventBus.subscribe.mockImplementation(async (callback) => {
        subscribeCallback = callback;
      });

      const messagePayload = {
        type: 'STEP_SUCCESS',
        saga_id: 'saga-1',
        event_id: 'event-1',
        payload: {},
      };

      const amqpMessage = {
        content: Buffer.from(JSON.stringify(messagePayload)),
        fields: { routingKey: 'restaurant.confirm' },
        properties: { headers: {} },
      };

      const sagaRecord = createSagaEntity({
        id: 'saga-1',
        status: SagaStatus.COMPLETED,
        current_step_index: 2,
        completed_steps: [],
        payload: {},
      });

      sagaRepository.findById.mockResolvedValue(sagaRecord);

      /* Act */
      await coordinator.start();
      if (subscribeCallback) {
        await subscribeCallback(amqpMessage);
      }

      /* Assert */
      expect(sagaRepository.findById).toHaveBeenCalledWith('saga-1');
      expect(datasource.transaction).not.toHaveBeenCalled();
    });

    it('should throw AppError if updating saga fails due to conflict', async () => {
      /* Arrange */
      let subscribeCallback: ((message: any) => Promise<void>) | undefined;
      eventBus.subscribe.mockImplementation(async (callback) => {
        subscribeCallback = callback;
      });

      const messagePayload = {
        type: 'STEP_SUCCESS',
        saga_id: 'saga-1',
        event_id: 'event-1',
        payload: {},
      };

      const amqpMessage = {
        content: Buffer.from(JSON.stringify(messagePayload)),
        fields: { routingKey: 'restaurant.confirm' },
        properties: { headers: {} },
      };

      const sagaRecord = createSagaEntity({
        id: 'saga-1',
        status: SagaStatus.RUNNING,
        current_step_index: 0,
        completed_steps: [],
        payload: {},
      });

      sagaRepository.findById.mockResolvedValue(sagaRecord);

      const manager = {
        update: jest.fn().mockResolvedValue({ affected: 0 }),
      } as unknown as EntityManager;

      const mockTransaction = datasource.transaction as unknown as jest.MockedFunction<TransactionFn>;
      mockTransaction.mockImplementation(async (callback) => {
        return callback(manager);
      });

      /* Act & Assert */
      await coordinator.start();
      if (subscribeCallback) {
        await expect(subscribeCallback(amqpMessage)).rejects.toThrow(AppError);
      }
    });
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
        metadata: { trace: expect.any(Object) },
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
        metadata: { trace: expect.any(Object) },
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
        metadata: { trace: expect.any(Object) },
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
        stepIndex: 2,
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

/**
 * Builds a saga entity mock for tests.
 *
 * @param {Partial<SagaEntity>} overrides - Overrides for the default saga fields.
 * @returns {SagaEntity} A fully typed mock SagaEntity.
 */
function createSagaEntity(overrides: Partial<SagaEntity> = {}): SagaEntity {
  return {
    id: 'saga-1',
    idempotency_key: 'dispatch-saga:order:order-1',
    correlation_id: 'order-1',
    name: 'dispatch-saga',
    domain: SagaDomain.DISPATCH,
    payload: { order_id: 'order-1' },
    status: SagaStatus.PENDING,
    completed_steps: [],
    current_step_index: 0,
    reprocess_count: 0,
    last_reprocessed_at: null,
    started_at: '2026-06-29T00:00:00.000Z',
    failed_at: null,
    failure_context: null,
    created_at: new Date('2026-06-29T00:00:00.000Z'),
    updated_at: new Date('2026-06-29T00:00:00.000Z'),
    ...overrides,
  };
}
