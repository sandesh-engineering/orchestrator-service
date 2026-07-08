import {
  Channel,
  ConnectionManager,
  ConsumeMessage,
  RabbitMQService,
} from '@platform/queue-rabbitmq';

import { AppError, BAD_REQUEST } from '@core/main';
import { logger } from '@platform/logger';
import { SagaDomain } from '../enums/saga.domain.enum';
import { OrchestratorEventType } from '../enums/orchestrator-event-type.enum';
import { DLQ_QUEUE_NAMES, EVENTS } from '../constants/saga.constants';
import { ContextPropagation } from 'src/tracing/propagation/context';

export class RabbitMQEventBus {
  private readonly ORCHESTRATOR_TYPE = 'direct';
  private readonly ORCHESTRATOR_ROUTING_KEY = 'orchestrator-routing-key';
  private readonly ORCHESTRATOR_EXCHANGE = 'orchestrator-exchange';
  private readonly ORCHESTRATOR_QUEUE = 'orchestrator-queue';

  private readonly ORCHESTRATOR_DLQ_TYPE = 'topic';
  private readonly ORCHESTRATOR_DLQ_EXCHANGE = 'orchestrator.dlq.exchange';

  private channel: Channel | null = null;
  private manager: ConnectionManager | null = null;

  /**
   * Bootstraps RabbitMQ connection, asserting exchanges, queues, and binds them.
   *
   * @returns {Promise<void>} A promise resolving when RabbitMQ is fully bootstrapped.
   */
  async bootstrapRabbitMQ(): Promise<void> {
    this.manager = new ConnectionManager({
      url: String(process.env.RABBITMQ_1_CONNECTION_URL),
      heartbeat: 60,
      username: process.env.RABBITMQ_1_DEFAULT_USER,
      password: process.env.RABBITMQ_1_DEFAULT_PASS,
    });

    const rabbitmqService = new RabbitMQService(this.manager);

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

    /* Note: DLQ exchange is asserted as 'topic' in setupDLQTopology() — no duplicate here */
    await this.channel.assertQueue(this.ORCHESTRATOR_QUEUE, {
      durable: true,
      arguments: {
        /* Nacked messages route automatically to the DLQ exchange */
        'x-dead-letter-exchange': this.ORCHESTRATOR_DLQ_EXCHANGE,
      },
    });

    await this.channel.bindQueue(
      this.ORCHESTRATOR_QUEUE,
      this.ORCHESTRATOR_EXCHANGE,
      this.ORCHESTRATOR_ROUTING_KEY,
    );

    const stepReplyRoutingKeys = [
      EVENTS.ORDER_ACCEPTED_V1,
      EVENTS.ORDER_REJECTED_V1,
      EVENTS.DISPATCH_CREATED_V1,
      EVENTS.DISPATCH_CREATION_REJECTED_V1,
      EVENTS.AGENT_NOTIFIED_V1,
      EVENTS.AGENT_ASSIGNED_V1,
    ];

    for (const routingKey of stepReplyRoutingKeys) {
      await this.channel.bindQueue(
        this.ORCHESTRATOR_QUEUE,
        this.ORCHESTRATOR_EXCHANGE,
        routingKey,
      );
    }

    /* Setting up DLQs on bus initiation */
    await this.setupDLQTopology();
  }

  /**
   * Checks if the RabbitMQ channel is connected.
   */
  getIsConnected(): boolean {
    return this.channel !== null;
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
        'x-max-length': 1000,
        'x-overflow': 'reject-publish',
      },
    });

    await this.channel.bindQueue(
      DLQ_QUEUE_NAMES[SagaDomain.DISPATCH],
      this.ORCHESTRATOR_DLQ_EXCHANGE,
      `dlq.${SagaDomain.DISPATCH}.#`,
    );

    /* Subscription DLQ — 30-day retention, capped at 10k messages */
    await this.channel.assertQueue(DLQ_QUEUE_NAMES[SagaDomain.SUBSCRIPTION], {
      durable: true,
      arguments: {
        'x-message-ttl': 1000 * 60 * 60 * 24 * 30 /* 30 days */,
        'x-max-length': 10_000,
        'x-overflow': 'reject-publish',
      },
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

    const carrier = ContextPropagation.createCarrier();

    const ok = this.channel.publish(
      this.ORCHESTRATOR_EXCHANGE,
      routingKey,
      Buffer.from(JSON.stringify(payload)),
      { mandatory: true, persistent: true, headers: { trace: carrier } },
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
   * Subscribes to a queue, invoking the async callback when a message is received.
   * Automatically acks on success and nacks (no requeue) on callback failure.
   *
   * @param {function} callback - Async callback invoked with the message.
   * @param {string} [queueName] - Optional override queue name; defaults to the orchestrator queue.
   * @returns {Promise<void>} A promise resolving when the subscription is established.
   */
  async subscribe(
    callback: (message: ConsumeMessage | null) => Promise<void>,
    queueName?: string,
  ): Promise<void> {
    if (!this.channel)
      throw new AppError(
        false,
        'Rabbitmq channel not initialized!',
        BAD_REQUEST,
        true,
      );

    const channel = this.channel;

    /* We can only distinguish the suitable number for this once we start testing so for now 30 */
    await channel.prefetch(30);

    await channel.consume(queueName ?? this.ORCHESTRATOR_QUEUE, async (msg) => {
      if (!msg) return;
      try {
        await callback(msg);
        channel.ack(msg);
      } catch (err) {
        logger.error(
          'Failed to process queued message — nacking without requeue',
          {
            queue: queueName ?? this.ORCHESTRATOR_QUEUE,
            error: err instanceof Error ? err.message : String(err),
          },
        );
        channel.nack(msg, false, false);
      }
    });
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
    callback: (message: ConsumeMessage | null) => Promise<void>,
  ): Promise<void> {
    if (!this.channel)
      throw new AppError(
        false,
        'Rabbitmq channel not initialized!',
        BAD_REQUEST,
        true,
      );

    const PAYMENT_EXCHANGE =
      process.env.PAYMENT_RABBITMQ_EXCHANGE ?? 'payment.exchange';
    const PAYMENT_ROUTING_KEY =
      process.env.PAYMENT_RABBITMQ_ROUTING_KEY ?? 'payment.v1.order.succeeded';
    const PAYMENT_CONSUMER_QUEUE = 'orchestrator.payment.succeeded.queue';

    const channel = this.channel;

    /* Assert the upstream payment exchange as a topic — safe to re-assert with same args */
    await channel.assertExchange(PAYMENT_EXCHANGE, 'topic', {
      durable: true,
    });

    /* Assert and bind a dedicated queue so the orchestrator only receives order-succeeded events */
    await channel.assertQueue(PAYMENT_CONSUMER_QUEUE, { durable: true });

    await channel.bindQueue(
      PAYMENT_CONSUMER_QUEUE,
      PAYMENT_EXCHANGE,
      PAYMENT_ROUTING_KEY,
    );

    await channel.prefetch(30);

    await channel.consume(PAYMENT_CONSUMER_QUEUE, async (msg) => {
      if (!msg) return;
      try {
        await callback(msg);
        channel.ack(msg);
      } catch (err) {
        logger.error(
          'Failed to process payment event — nacking without requeue',
          {
            error: err instanceof Error ? err.message : String(err),
          },
        );
        channel.nack(msg, false, false);
      }
    });
  }

  /**
   * Closes the RabbitMQ channel and connection manager cleanly.
   */
  async close(): Promise<void> {
    if (this.channel) {
      try {
        await this.channel.close();
      } catch (err) {
        logger.error('Error closing channel during shutdown', {
          error: err instanceof Error ? err.message : String(err),
        });
      }
      this.channel = null;
    }
    if (this.manager) {
      try {
        await this.manager.close();
      } catch (err) {
        logger.error('Error closing connection manager during shutdown', {
          error: err instanceof Error ? err.message : String(err),
        });
      }
      this.manager = null;
    }
  }
}
