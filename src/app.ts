import { logger } from '@platform/logger';
import { createTracing } from '@platform/tracing';

import { RabbitMQEventBus } from './events/event-bus';
import { OrchestratorOutboxRepository } from './repositories/orchestrator-outbox.repository';
import { OutboxService } from './services/outbox.service';
import { datasource } from './database/data-source';
import dotenv from 'dotenv';
import { SagaCoordinator } from './saga-coordinator';
import { DispatchSaga } from './steps/dispatch.step';
import { SagaRepository } from './repositories/saga.repository';

dotenv.config();

const sdk = createTracing({
  serviceName: process.env.SERVICE_NAME ?? 'workflow-orchestrator',
  serviceVersion: '1.0.0',
  collectorUrl: 'http://localhost:4317',
  samplingRatio: 0.3,
});

sdk.start();

/* APPLICATION LEVEL CLASSES */
export const eventBus = new RabbitMQEventBus();
const outboxRepository = new OrchestratorOutboxRepository(datasource);
const sagaRepository = new SagaRepository(datasource);
const outboxService = new OutboxService(eventBus, outboxRepository, datasource);
const sagaCoordinator = new SagaCoordinator(
  DispatchSaga.name,
  [...DispatchSaga.steps],
  sagaRepository,
  outboxRepository,
  eventBus,
  datasource,
);

/**
 * Sleeps for a specified number of milliseconds.
 *
 * @param {number} ms - The number of milliseconds to sleep.
 * @returns {Promise<void>} A promise resolving when the sleep duration is reached.
 */
const sleep = (ms: number): Promise<void> => {
  return new Promise((resolve) => setTimeout(resolve, ms));
};

/**
 * Starts the outbox polling loop, calling `poll` repeatedly.
 *
 * @returns {Promise<void>} A promise resolving when the polling loop finishes (infinite loop).
 */
export async function startPolling(): Promise<void> {
  while (true) {
    try {
      await outboxService.poll();
    } catch (error) {
      if (error instanceof Error) {
        logger.error('Polling failed for orchestrator events!', {
          service_error: error.message,
        });
      }

      logger.error(error);
    }

    await sleep(3000);
  }
}
