import 'dotenv/config';
import { Server } from 'http';
import {
  app,
  eventBus,
  startPolling,
  stopPolling,
  orchestratorListener,
  sagaCoordinator,
} from './app';
import { logger } from '@platform/logger';
import { datasource } from './database/data-source';
import { DLQ_QUEUE_NAMES } from './constants/saga.constants';


let server: Server | null = null;
let isShuttingDown = false;

/* Bootstrap the application database connection, event bus, and start outbox polling */
(async () => {
  try {
    logger.info('Initializing application database datasource...');
    await datasource.initialize();

    logger.info('Initializing RabbitMQ event bus...');
    await eventBus.bootstrapRabbitMQ();

    logger.info('Starting HTTP health check server...');
    const port = process.env.PORT ?? 3000;
    server = app.listen(port, () => {
      logger.info(`Health check server listening on port ${port}`);
    });

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



/* Handle uncaught exceptions and unhandled rejections to prevent silent failures */
process.on('unhandledRejection', (reason) => {
  logger.error('Unhandled promise rejection', { reason: String(reason) });
  process.exit(1);
});

process.on('uncaughtException', (err) => {
  logger.error('Uncaught exception', {
    message: err.message,
    stack: err.stack,
  });
  process.exit(1);
});



const shutdown = async (signal: string) => {
  if (isShuttingDown) return;
  isShuttingDown = true;
  logger.info(`Received ${signal}, starting graceful shutdown...`);
  try {
    if (server) {
      logger.info('Closing health check server...');
      await new Promise<void>((resolve) => {
        server!.close(() => resolve());
      });
    }

    stopPolling();
    logger.info('Outbox polling stopped.');

    logger.info('Closing event bus...');
    await eventBus.close();

    logger.info('Closing database connection...');
    await datasource.destroy();

    logger.info('Graceful shutdown completed successfully.');
    process.exit(0);
  } catch (err) {
    logger.error('Error occurred during graceful shutdown', {
      error: err instanceof Error ? err.message : String(err),
    });
    process.exit(1);
  }
};

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));