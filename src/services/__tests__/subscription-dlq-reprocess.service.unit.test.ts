import { UpdateResult } from 'typeorm';
import { SagaEntity, SagaStatus } from '../../entities/saga.entity';
import { OrchestratorEventType } from '../../enums/orchestrator-event-type.enum';
import { SagaDomain } from '../../enums/saga.domain.enum';
import { RabbitMQEventBus } from '../../events/event-bus';
import { SagaRepository } from '../../repositories/saga.repository';
import { SubscriptionReprocessService } from '../subscription-dlq-reprocess.service';

describe('SubscriptionReprocessService', () => {
  let sagaRepository: jest.Mocked<SagaRepository>;
  let eventBus: jest.Mocked<RabbitMQEventBus>;
  let service: SubscriptionReprocessService;

  beforeEach(() => {
    sagaRepository = {
      find: jest.fn(),
      findOneOrFail: jest.fn(),
      updateById: jest.fn(),
    } as unknown as jest.Mocked<SagaRepository>;

    eventBus = {
      publishWithRoutingKey: jest.fn(),
    } as unknown as jest.Mocked<RabbitMQEventBus>;

    service = new SubscriptionReprocessService(sagaRepository, eventBus);
  });

  describe('listPendingReprocess', () => {
    it('should list failed subscription sagas ordered by failure time', async () => {
      /* Arrange */
      const sagas = [createSaga({ id: 'saga-1' })];
      sagaRepository.find.mockResolvedValue(sagas);

      /* Act */
      const result = await service.listPendingReprocess();

      /* Assert */
      expect(result).toBe(sagas);
      expect(sagaRepository.find).toHaveBeenCalledWith({
        where: { domain: SagaDomain.SUBSCRIPTION, status: SagaStatus.FAILED },
        order: { failed_at: 'ASC' },
      });
    });
  });

  describe('reprocess', () => {
    it('should replay failure context and move saga back to running', async () => {
      /* Arrange */
      const saga = createSaga({
        id: 'saga-1',
        reprocess_count: 2,
      });
      sagaRepository.findOneOrFail.mockResolvedValue(saga);
      sagaRepository.updateById.mockResolvedValue(createUpdateResult());
      eventBus.publishWithRoutingKey.mockResolvedValue();

      /* Act */
      await service.reprocess('saga-1');

      /* Assert */
      expect(eventBus.publishWithRoutingKey).toHaveBeenCalledWith(
        'subscription.charge',
        { subscription_id: 'subscription-1' },
      );
      expect(sagaRepository.updateById).toHaveBeenCalledWith('saga-1', {
        status: SagaStatus.RUNNING,
        reprocess_count: 3,
        last_reprocessed_at: expect.any(String),
      });
    });

    it('should reject non subscription sagas', async () => {
      /* Arrange */
      sagaRepository.findOneOrFail.mockResolvedValue(
        createSaga({ domain: SagaDomain.DISPATCH }),
      );

      /* Act */
      const result = service.reprocess('saga-1');

      /* Assert */
      await expect(result).rejects.toThrow('not "subscription"');
      expect(eventBus.publishWithRoutingKey).not.toHaveBeenCalled();
      expect(sagaRepository.updateById).not.toHaveBeenCalled();
    });

    it('should reject sagas that are not failed', async () => {
      /* Arrange */
      sagaRepository.findOneOrFail.mockResolvedValue(
        createSaga({ status: SagaStatus.RUNNING }),
      );

      /* Act */
      const result = service.reprocess('saga-1');

      /* Assert */
      await expect(result).rejects.toThrow('not in FAILED state');
      expect(eventBus.publishWithRoutingKey).not.toHaveBeenCalled();
      expect(sagaRepository.updateById).not.toHaveBeenCalled();
    });

    it('should reject sagas without failure context', async () => {
      /* Arrange */
      sagaRepository.findOneOrFail.mockResolvedValue(
        createSaga({ failure_context: null }),
      );

      /* Act */
      const result = service.reprocess('saga-1');

      /* Assert */
      await expect(result).rejects.toThrow('no failure_context');
      expect(eventBus.publishWithRoutingKey).not.toHaveBeenCalled();
      expect(sagaRepository.updateById).not.toHaveBeenCalled();
    });
  });
});

/**
 * Builds a saga entity for subscription reprocess unit tests.
 *
 * @param {Partial<SagaEntity>} overrides - Values that should replace the default saga fields.
 * @returns {SagaEntity} Saga record used by the reprocess service tests.
 */
function createSaga(overrides: Partial<SagaEntity> = {}): SagaEntity {
  return {
    id: 'saga-1',
    idempotency_key: 'subscription-saga:subscription-1',
    correlation_id: 'subscription-1',
    name: 'SubscriptionPaymentSaga',
    domain: SagaDomain.SUBSCRIPTION,
    payload: { subscription_id: 'subscription-1' },
    status: SagaStatus.FAILED,
    completed_steps: [],
    current_step_index: 0,
    started_at: '2026-06-29T00:00:00.000Z',
    failure_context: {
      routing_key: 'subscription.charge',
      saga_event_type: OrchestratorEventType.COMMAND,
      payload: { subscription_id: 'subscription-1' },
    },
    reprocess_count: 0,
    last_reprocessed_at: null,
    failed_at: '2026-06-29T00:00:00.000Z',
    created_at: new Date('2026-06-29T00:00:00.000Z'),
    updated_at: new Date('2026-06-29T00:00:00.000Z'),
    ...overrides,
  };
}

/**
 * Builds a minimal TypeORM update result for repository mock responses.
 *
 * @returns {UpdateResult} Successful update result.
 */
function createUpdateResult(): UpdateResult {
  return {
    affected: 1,
    raw: [],
    generatedMaps: [],
  };
}
