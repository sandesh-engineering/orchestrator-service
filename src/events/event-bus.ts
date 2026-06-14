import {
  Channel,
  ConnectionManager,
  ConsumeMessage,
  RabbitMQService,
} from '@platform/queue-rabbitmq';

import { AppError, BAD_REQUEST } from '@core/main';
import { SagaDomain } from 'src/enums/saga.domain.enum';
import { OrchestratorEventType } from 'src/entities/orchestrator-outbox.entity';
import { DLQ_QUEUE_NAMES } from 'src/constants/saga.constants';

export class RabbitMQEventBus {
  private readonly ORCHESTRATOR_TYPE = 'direct';
  private readonly ORCHESTRATOR_ROUTING_KEY = 'orchestrator-routing-key';
  private readonly ORCHESTRATOR_EXCHANGE = 'orchestrator-exchange';
  private readonly ORCHESTRATOR_QUEUE = 'orchestrator-queue';

  private readonly ORCHESTRATOR_DLQ_TYPE = 'topic';
  private readonly ORCHESTRATOR_DLQ_EXCHANGE = 'orchestrator.dlq.exchange';

  private channel: Channel | null = null;

  private readonly;

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

    /* Setting up DLQs on bus initiation */
    await this.setupDLQTopology();
  }

  /* Setting up DLQs based on domain */
  async setupDLQTopology(): Promise<void> {
    if (!this.channel)
      throw new AppError(
        false,
        'Rabbitmq channel not initialized!',
        BAD_REQUEST,
        true,
      );

    /* Dispatch DLQ */
    await this.channel.assertExchange(
      this.ORCHESTRATOR_DLQ_EXCHANGE,
      this.ORCHESTRATOR_DLQ_TYPE,
      {
        durable: true,
      },
    );

    await this.channel.assertQueue(DLQ_QUEUE_NAMES[SagaDomain.DISPATCH], {
      durable: true,
      arguments: {
        'x-message-ttl':
          1000 *
          60 *
          60 *
          24 *
          7 /* Since we don't really manually reprocess dispatch DLQ events */,
      },
    });

    await this.channel.bindQueue(
      DLQ_QUEUE_NAMES[SagaDomain.DISPATCH],
      this.ORCHESTRATOR_DLQ_EXCHANGE,
      `dlq.${SagaDomain.DISPATCH}.#`,
    );

    /* Subscription DLQ */
    await this.channel.assertQueue(DLQ_QUEUE_NAMES[SagaDomain.SUBSCRIPTION], {
      durable: true,
    });

    await this.channel.bindQueue(
      DLQ_QUEUE_NAMES[SagaDomain.SUBSCRIPTION],
      this.ORCHESTRATOR_DLQ_EXCHANGE,
      `dlq.${SagaDomain.SUBSCRIPTION}.#`,
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
   * Publishes an event payload to the main orchestrator exchange with a specific routing key.
   *
   * @param {string} routingKey - The routing key to publish the event with.
   * @param {unknown} payload - The payload to publish.
   * @returns {Promise<void>} A promise resolving when publishing completes.
   */
  async publishWithRoutingKey(
    routingKey: string,
    payload: unknown,
  ): Promise<void> {
    if (!this.channel)
      throw new AppError(
        false,
        'Rabbitmq channel not initialized!',
        BAD_REQUEST,
        true,
      );

    const ok = this.channel.publish(
      this.ORCHESTRATOR_EXCHANGE,
      routingKey,
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
  async publishToDLQ(payload: {
    payload: unknown;
    saga_id: string;
    saga_event_type: OrchestratorEventType;
    domain: string;
    routing_key: string;
  }): Promise<void> {
    if (!this.channel)
      throw new AppError(
        false,
        'Rabbitmq channel not initialized!',
        BAD_REQUEST,
        true,
      );

    const dlqRoutingKey = `dlq.${payload.domain}.${payload.saga_event_type}`;

    const dlqMessage = {
      payload: payload.payload,
      saga_id: payload.saga_id,
      saga_event_type: payload.saga_event_type,
      routing_key: payload.routing_key,
      domain: payload.domain,
    };

    const ok = this.channel.publish(
      this.ORCHESTRATOR_DLQ_EXCHANGE,
      dlqRoutingKey,
      Buffer.from(JSON.stringify(dlqMessage)),
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
    queueName?: string,
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

    await this.channel.consume(queueName ?? this.ORCHESTRATOR_QUEUE, callback);
  }

  /**
   * Subscribes to the payment exchange on the `payment.v1.order.succeeded` routing key,
   * asserting a durable topic exchange and a dedicated orchestrator-bound queue before consuming.
   *
   * @remarks
   * - Asserts `payment.exchange` as a topic exchange (durable) — idempotent if already declared upstream.
   * - Asserts and binds `orchestrator.payment.succeeded.queue` so the orchestrator receives only
   *   successful payment events regardless of other consumers on the same exchange.
   * - Should be called once after `bootstrapRabbitMQ()` completes.
   *
   * @param {function} callback - Callback invoked with each consumed message.
   * @returns {Promise<void>} A promise resolving when the subscription is fully established.
   * @throws {AppError} Throws with HTTP 400 if the channel has not been initialized.
   */
  async subscribeToPaymentEvents(
    callback: (message: ConsumeMessage | null) => void,
  ): Promise<void> {
    if (!this.channel)
      throw new AppError(
        false,
        'Rabbitmq channel not initialized!',
        BAD_REQUEST,
        true,
      );

    const PAYMENT_EXCHANGE = 'payment.exchange';
    const PAYMENT_ROUTING_KEY = 'payment.v1.order.succeeded';
    const PAYMENT_CONSUMER_QUEUE = 'orchestrator.payment.succeeded.queue';

    /* Assert the upstream payment exchange as a topic — safe to re-assert with same args */
    await this.channel.assertExchange(PAYMENT_EXCHANGE, 'topic', {
      durable: true,
    });

    /* Assert and bind a dedicated queue so the orchestrator only receives order-succeeded events */
    await this.channel.assertQueue(PAYMENT_CONSUMER_QUEUE, { durable: true });

    await this.channel.bindQueue(
      PAYMENT_CONSUMER_QUEUE,
      PAYMENT_EXCHANGE,
      PAYMENT_ROUTING_KEY,
    );

    await this.channel.prefetch(30);

    await this.channel.consume(PAYMENT_CONSUMER_QUEUE, callback);
  }
}
