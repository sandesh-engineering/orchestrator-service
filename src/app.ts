import { createTracing } from '@platform/tracing';

const sdk = createTracing({
  serviceName: process.env.SERVICE_NAME ?? 'workflow-orchestrator',
  serviceVersion: '1.0.0',
  collectorUrl:
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT ?? 'http://localhost:4317',
  samplingRatio: process.env.NODE_ENV === 'development' ? 1 : 0.3,
});

console.log(sdk, 'Tacer SDK');

sdk.start();

import express, { Application } from 'express';
import { logger } from '@platform/logger';

import { RabbitMQEventBus } from './events/event-bus';
import { OrchestratorOutboxRepository } from './repositories/orchestrator-outbox.repository';
import { OutboxService } from './services/outbox.service';
import { datasource } from './database/data-source';
import { SagaCoordinator } from './saga-coordinator';
import { DispatchSagaState } from './steps/dispatch.step';
import { SagaRepository } from './repositories/saga.repository';
import { OrchestratorListener } from './events/listeners';

export const app: Application = express();

/* APPLICATION LEVEL CLASSES */
export const eventBus = new RabbitMQEventBus();
const outboxRepository = new OrchestratorOutboxRepository(datasource);
const sagaRepository = new SagaRepository(datasource);
const outboxService = new OutboxService(eventBus, outboxRepository, datasource);
export const sagaCoordinator = new SagaCoordinator(
  'dispatch-saga',
  DispatchSagaState,
  sagaRepository,
  outboxRepository,
  eventBus,
  datasource,
);
export const orchestratorListener = new OrchestratorListener(sagaCoordinator);

/* HEALTH CHECK ROUTE */
app.get(['/health', '/ready'], (req, res) => {
  const isDbConnected = datasource.isInitialized;
  const isRabbitConnected = eventBus.getIsConnected();

  if (isDbConnected && isRabbitConnected) {
    res.status(200).json({ status: 'UP', database: 'UP', rabbitmq: 'UP' });
  } else {
    res.status(503).json({
      status: 'DOWN',
      database: isDbConnected ? 'UP' : 'DOWN',
      rabbitmq: isRabbitConnected ? 'UP' : 'DOWN',
    });
  }
});

/**
 * Sleeps for a specified number of milliseconds.
 *
 * @param {number} ms - The number of milliseconds to sleep.
 * @returns {Promise<void>} A promise resolving when the sleep duration is reached.
 */
const sleep = (ms: number): Promise<void> => {
  return new Promise((resolve) => setTimeout(resolve, ms));
};

let pollingActive = true;

/**
 * Stops the outbox polling loop gracefully.
 */
export function stopPolling(): void {
  pollingActive = false;
}

/**
 * Starts the outbox polling loop, calling `poll` repeatedly.
 *
 * @returns {Promise<void>} A promise resolving when the polling loop finishes.
 */
export async function startPolling(): Promise<void> {
  pollingActive = true;
  while (pollingActive) {
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

    if (pollingActive) {
      await sleep(3000);
    }
  }
}
