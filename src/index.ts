/* Load env vars FIRST — before any module that reads process.env at initialization time (logger, Redis) */
import 'dotenv/config';

import {
  eventBus,
  startPolling,
  orchestratorListener,
  sagaCoordinator,
} from './app';
import { logger } from '@platform/logger';
import { datasource } from './database/data-source';
import { DLQ_QUEUE_NAMES } from './constants/saga.constants';

/* Bootstrap the application database connection, event bus, and start outbox polling */
(async () => {
  try {
    logger.info('Initializing application database datasource...');
    await datasource.initialize();

    logger.info('Initializing RabbitMQ event bus...');
    await eventBus.bootstrapRabbitMQ();

    logger.info(
      'Starting saga coordinator subscription on orchestrator queue...',
    );
    await sagaCoordinator.start();

    logger.info('Subscribing to payment.v1.order.succeeded events...');
    await eventBus.subscribeToPaymentEvents(
      orchestratorListener.onPaymentOrderSucceeded,
    );

    logger.info('Subscribing to dispatch dlq events...');
    await eventBus.subscribe(
      orchestratorListener.onDispatchDlqEvents,
      DLQ_QUEUE_NAMES.dispatch,
    );

    logger.info('Subscribing to subscription dlq events...');
    await eventBus.subscribe(
      orchestratorListener.onSubscriptionDlqEvents,
      DLQ_QUEUE_NAMES.subscription,
    );

    logger.info('Starting outbox polling loop...');
    await startPolling();
  } catch (error) {
    logger.error('Failed to bootstrap the orchestrator service', {
      error:
        error instanceof Error
          ? { message: error.message, stack: error.stack, name: error.name }
          : String(error),
    });
    process.exit(1);
  }
})();
