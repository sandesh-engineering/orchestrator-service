import {
  Channel,
  ConnectionManager,
  ConsumeMessage,
  RabbitMQService,
} from '@platform/queue-rabbitmq';

import { AppError, BAD_REQUEST } from '@core/main';

export class RabbitMQEventBus {
  private readonly ORCHESTRATOR_TYPE = 'direct';
  private readonly ORCHESTRATOR_ROUTING_KEY = 'orchestrator-routing-key';
  private readonly ORCHESTRATOR_EXCHANGE = 'orchestrator-exchange';
  private readonly ORCHESTRATOR_QUEUE = 'orchestrator-queue';
  private readonly ORCHESTRATOR_DLQ_EXCHANGE = 'orchestrator-dlq';

  private channel: Channel | null = null;

  /**
   * Bootstraps RabbitMQ connection, asserting exchanges, queues, and binds them.
   *
   * @returns {Promise<void>} A promise resolving when RabbitMQ is fully bootstrapped.
   */
  async bootstrapRabbitMQ(): Promise<void> {
    const manager = new ConnectionManager({
      url: String(process.env.RABBITMQ_1_CONNECTION_URL),
      heartbeat: 60,
      username: process.env.RABBITMQ_1_DEFAULT_USER,
      password: process.env.RABBITMQ_1_DEFAULT_PASS,
    });

    const rabbitmqService = new RabbitMQService(manager);

    /* Initialize the connection */
    await rabbitmqService.init();

    /* Creating a channel */
    await rabbitmqService.createChannel();

    /* We initialize the queue like asserting the queue */
    this.channel = rabbitmqService.getChannel();

    /* So this shall be dynamic since we actually don't know which exchange we are routing through (or we can just use a single exchange named orchestrator and the downstream services react to the messages sent through this exchange while on contrary there's chances of mixup in single one rather than different exchanges). Another reason for going with this one is asserting exchange on each publish or subscribe leads to delays as this is essentially a network call which eventually can be a bottleneck */
    await this.channel.assertExchange(
      this.ORCHESTRATOR_EXCHANGE,
      this.ORCHESTRATOR_TYPE,
      {
        durable: true,
      },
    );

    await this.channel.assertExchange(
      this.ORCHESTRATOR_DLQ_EXCHANGE,
      'direct',
      {
        durable: true,
      },
    );

    await this.channel.assertQueue(this.ORCHESTRATOR_QUEUE, { durable: true });

    await this.channel.bindQueue(
      this.ORCHESTRATOR_QUEUE,
      this.ORCHESTRATOR_EXCHANGE,
      this.ORCHESTRATOR_ROUTING_KEY,
    );
  }

  /**
   * Publishes an event payload to the main orchestrator exchange.
   *
   * @param {unknown} payload - The payload to publish.
   * @returns {Promise<void>} A promise resolving when publishing completes.
   */
  async publish(payload: unknown): Promise<void> {
    if (!this.channel)
      throw new AppError(
        false,
        'Rabbitmq channel not initialized!',
        BAD_REQUEST,
        true,
      );

    const ok = this.channel.publish(
      this.ORCHESTRATOR_EXCHANGE,
      this.ORCHESTRATOR_ROUTING_KEY,
      Buffer.from(JSON.stringify(payload)),
      { mandatory: true, persistent: true },
    );

    if (!ok) {
      await new Promise((resolve) => this.channel?.once('drain', resolve));
    }
  }

  /**
   * Publishes an event payload to the orchestrator Dead Letter Queue (DLQ) exchange.
   *
   * @param {unknown} payload - The payload to publish.
   * @returns {Promise<void>} A promise resolving when publishing completes.
   */
  async publishToDLQ(payload: unknown): Promise<void> {
    if (!this.channel)
      throw new AppError(
        false,
        'Rabbitmq channel not initialized!',
        BAD_REQUEST,
        true,
      );

    const ok = this.channel.publish(
      this.ORCHESTRATOR_DLQ_EXCHANGE,
      this.ORCHESTRATOR_ROUTING_KEY,
      Buffer.from(JSON.stringify(payload)),
      { mandatory: true, persistent: true },
    );

    if (!ok) {
      await new Promise((resolve) => this.channel?.once('drain', resolve));
    }
  }

  /**
   * Subscribes to the orchestrator queue, invoking the callback when a message is received.
   *
   * @param {function} callback - Callback invoked with the message.
   * @returns {Promise<void>} A promise resolving when the subscription is established.
   */
  async subscribe(
    callback: (message: ConsumeMessage | null) => void,
  ): Promise<void> {
    if (!this.channel)
      throw new AppError(
        false,
        'Rabbitmq channel not initialized!',
        BAD_REQUEST,
        true,
      );

    /* We can only distinguish the suitable number for this once we start testing so for now 30 */
    await this.channel.prefetch(30);

    await this.channel.consume(this.ORCHESTRATOR_QUEUE, callback);
  }
}
