import { DataSource, EntityManager } from 'typeorm';
import { OutboxService } from '../outbox.service';
import { OrchestratorOutbox } from '../../entities/orchestrator-outbox.entity';
import { OrchestratorEventType } from '../../enums/orchestrator-event-type.enum';
import { SagaDomain } from '../../enums/saga.domain.enum';
import { RabbitMQEventBus } from '../../events/event-bus';
import { OrchestratorOutboxRepository } from '../../repositories/orchestrator-outbox.repository';

jest.mock('@platform/logger', () => ({
  logger: {
    debug: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
  },
}));

describe('OutboxService', () => {
  let eventBus: jest.Mocked<RabbitMQEventBus>;
  let outboxRepository: jest.Mocked<OrchestratorOutboxRepository>;
  let datasource: jest.Mocked<DataSource>;
  let manager: EntityManager;
  let service: OutboxService;

  beforeEach(() => {
    eventBus = {
      publishWithRoutingKey: jest.fn(),
      publishToDLQ: jest.fn(),
    } as unknown as jest.Mocked<RabbitMQEventBus>;

    outboxRepository = {
      fetchUnprocessedEventsInBatch: jest.fn(),
      findEventsByIds: jest.fn(),
      markProcessed: jest.fn(),
      markDeadLettered: jest.fn(),
      incrementRetries: jest.fn(),
    } as unknown as jest.Mocked<OrchestratorOutboxRepository>;

    manager = {} as EntityManager;
    datasource = {
      transaction: jest.fn(
        async (callback: (entityManager: EntityManager) => Promise<void>) => {
          await callback(manager);
        },
      ),
    } as unknown as jest.Mocked<DataSource>;

    service = new OutboxService(eventBus, outboxRepository, datasource);
  });

  it('should skip publishing when there are no unprocessed records', async () => {
    /* Arrange */
    outboxRepository.fetchUnprocessedEventsInBatch.mockResolvedValue([]);

    /* Act */
    await service.poll();

    /* Assert */
    expect(outboxRepository.fetchUnprocessedEventsInBatch).toHaveBeenCalledWith(
      1000,
    );
    expect(eventBus.publishWithRoutingKey).not.toHaveBeenCalled();
    expect(datasource.transaction).not.toHaveBeenCalled();
  });

  it('should mark successful records processed and retry failed records', async () => {
    /* Arrange */
    const successRecord = createOutboxRecord({
      id: 'outbox-success',
      retries: 0,
    });
    const failedRecord = createOutboxRecord({
      id: 'outbox-failed',
      retries: 1,
    });
    outboxRepository.fetchUnprocessedEventsInBatch.mockResolvedValue([
      successRecord,
      failedRecord,
    ]);
    eventBus.publishWithRoutingKey
      .mockResolvedValueOnce()
      .mockRejectedValueOnce(new Error('broker unavailable'));

    /* Act */
    await service.poll();

    /* Assert */
    expect(eventBus.publishWithRoutingKey).toHaveBeenNthCalledWith(
      1,
      successRecord.routing_key,
      successRecord.payload,
    );
    expect(eventBus.publishWithRoutingKey).toHaveBeenNthCalledWith(
      2,
      failedRecord.routing_key,
      failedRecord.payload,
    );
    expect(outboxRepository.markProcessed).toHaveBeenCalledWith(
      ['outbox-success'],
      manager,
    );
    expect(outboxRepository.incrementRetries).toHaveBeenCalledWith(
      ['outbox-failed'],
      manager,
    );
    expect(outboxRepository.markDeadLettered).not.toHaveBeenCalled();
    expect(eventBus.publishToDLQ).not.toHaveBeenCalled();
  });

  it('should publish exhausted retry records to dlq and mark them dead lettered', async () => {
    /* Arrange */
    const exhaustedRecord = createOutboxRecord({
      id: 'outbox-dlq',
      retries: 2,
    });
    outboxRepository.fetchUnprocessedEventsInBatch.mockResolvedValue([
      exhaustedRecord,
    ]);
    outboxRepository.findEventsByIds.mockResolvedValue([exhaustedRecord]);
    eventBus.publishWithRoutingKey.mockRejectedValue(
      new Error('broker unavailable'),
    );
    eventBus.publishToDLQ.mockResolvedValue();

    /* Act */
    await service.poll();

    /* Assert */
    expect(outboxRepository.findEventsByIds).toHaveBeenCalledWith([
      'outbox-dlq',
    ]);
    expect(eventBus.publishToDLQ).toHaveBeenCalledWith({
      payload: exhaustedRecord.payload,
      saga_id: exhaustedRecord.saga_id,
      saga_event_type: exhaustedRecord.saga_event_type,
      routing_key: exhaustedRecord.routing_key,
      domain: exhaustedRecord.domain,
    });
    expect(outboxRepository.markDeadLettered).toHaveBeenCalledWith(
      ['outbox-dlq'],
      manager,
    );
    expect(outboxRepository.incrementRetries).not.toHaveBeenCalled();
  });
});

/**
 * Builds an orchestrator outbox record for unit tests.
 *
 * @param {Partial<OrchestratorOutbox>} overrides - Values that should replace the default record fields.
 * @returns {OrchestratorOutbox} Outbox record used by the polling service tests.
 */
function createOutboxRecord(
  overrides: Partial<OrchestratorOutbox>,
): OrchestratorOutbox {
  return {
    id: 'outbox-1',
    event_id: 'event-1',
    routing_key: 'restaurant.confirm',
    version: 0,
    saga_id: 'saga-1',
    domain: SagaDomain.DISPATCH,
    saga_event_type: OrchestratorEventType.COMMAND,
    payload: { order_id: 'order-1' },
    retries: 0,
    processed: false,
    processed_at: null,
    failed_at: null,
    created_at: new Date('2026-06-29T00:00:00.000Z'),
    updated_at: new Date('2026-06-29T00:00:00.000Z'),
    ...overrides,
  };
}
