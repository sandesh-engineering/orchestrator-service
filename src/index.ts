import { eventBus, startPolling } from './app';
import { logger } from '@platform/logger';
import { datasource } from './database/data-source';

/* Bootstrap the application database connection, event bus, and start outbox polling */
(async () => {
  try {
    logger.info('Initializing application database datasource...');
    await datasource.initialize();

    logger.info('Initializing RabbitMQ event bus...');
    await eventBus.bootstrapRabbitMQ();

    logger.info('Starting outbox polling loop...');
    await startPolling();
  } catch (error) {
    logger.error('Failed to bootstrap the orchestrator service', { error });
    process.exit(1);
  }
})();
