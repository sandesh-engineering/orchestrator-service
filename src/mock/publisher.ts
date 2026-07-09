import 'dotenv/config';
import { ConnectionManager, RabbitMQService } from '@platform/queue-rabbitmq';
import { logger } from '@platform/logger';
import { EVENTS } from '../constants/saga.constants';
import { context, getTracer, trace, withSpan } from '@platform/tracing';
import { ContextPropagation } from 'src/tracing/propagation/context';

/* ─── CONFIG ──────────────────────────────────────────────────── */
const PAYMENT_EXCHANGE = 'payment.exchange';
const PAYMENT_ROUTING_KEY = 'payment.v1.order.succeeded';
const ORCHESTRATOR_EXCHANGE = 'orchestrator-exchange';

/* ─── SEED DATA ───────────────────────────────────────────────── */

/** Distinct order IDs so you can fire multiple independent sagas per run */
const MOCK_ORDERS = [
  {
    order_id: `mock-order-${Date.now()}-1`,
    user_id: 'mock-user-001',
    amount: 1250,
    currency: 'NPR',
    payment_reference: `pay_ref_${Date.now()}_1`,
    paid_at: new Date().toISOString(),
  },
  {
    order_id: `mock-order-${Date.now()}-2`,
    user_id: 'mock-user-002',
    amount: 850,
    currency: 'NPR',
    payment_reference: `pay_ref_${Date.now()}_2`,
    paid_at: new Date().toISOString(),
  },
  {
    order_id: `mock-order-${Date.now()}-3`,
    user_id: 'mock-user-003',
    amount: 2100,
    currency: 'NPR',
    payment_reference: `pay_ref_${Date.now()}_3`,
    paid_at: new Date().toISOString(),
  },
];

/** Step-reply events consumed from the orchestrator queue to advance saga steps */
const MOCK_STEP_REPLIES = [
  {
    routingKey: EVENTS.ORDER_ACCEPTED_V1,
    label: 'ORDER_ACCEPTED (step 0 → 1)',
    payload: (order_id: string) => ({
      saga_id: 'RESOLVED_AT_RUNTIME',
      order_id,
      restaurant_id: 'mock-restaurant-001',
      restaurant_coords: { latitude: '27.7172', longitude: '85.3240' },
      customer_coords: { latitude: '27.7000', longitude: '85.3350' },
      accepted_at: new Date().toISOString(),
    }),
  },
  {
    routingKey: EVENTS.DISPATCH_CREATED_V1,
    label: 'DISPATCH_CREATED (step 1 → 2)',
    payload: (order_id: string) => ({
      saga_id: 'RESOLVED_AT_RUNTIME',
      order_id,
      dispatch_id: `mock-dispatch-${Date.now()}`,
      created_at: new Date().toISOString(),
    }),
  },
  {
    routingKey: EVENTS.AGENT_NOTIFIED_V1,
    label: 'AGENT_NOTIFIED (step 2 → 3)',
    payload: (order_id: string) => ({
      saga_id: 'RESOLVED_AT_RUNTIME',
      order_id,
      agent_id: 'mock-agent-007',
      notified_at: new Date().toISOString(),
    }),
  },
  {
    routingKey: EVENTS.AGENT_ASSIGNED_V1,
    label: 'AGENT_ASSIGNED (step 3 → COMPLETED)',
    payload: (order_id: string) => ({
      saga_id: 'RESOLVED_AT_RUNTIME',
      order_id,
      agent_id: 'mock-agent-007',
      assigned_at: new Date().toISOString(),
    }),
  },
];

/* ─── HELPERS ─────────────────────────────────────────────────── */

const sleep = (ms: number) => new Promise((res) => setTimeout(res, ms));

/* ─── SCENARIOS ────────────────────────────────────────────────── */

/**
 * Scenario 1 — Happy Path.
 * Publishes a `payment.v1.order.succeeded` event for each mock order then
 * walks through each step-reply with a brief delay so the orchestrator has
 * time to write the outbox row before the next advancement.
 */
async function runHappyPath(svc: RabbitMQService): Promise<void> {
  logger.info('═══ SCENARIO: Happy Path — full dispatch saga ═══');

  await withSpan('Mock Order Placement', async () => {
    for (const order of MOCK_ORDERS) {
      logger.info('Publishing payment.v1.order.succeeded', {
        order_id: order.order_id,
        payment_reference: order.payment_reference,
      });

      await svc.publish(PAYMENT_EXCHANGE, PAYMENT_ROUTING_KEY, order, {
        persistent: true,
      });

      /* Wait briefly so the orchestrator can start the saga before we reply */
      await sleep(1_500);

      /* Walk through the 4-step dispatch saga */
      for (const reply of MOCK_STEP_REPLIES) {
        logger.info(`Publishing step reply: ${reply.label}`, {
          order_id: order.order_id,
          routingKey: reply.routingKey,
        });

        await svc.publish(
          ORCHESTRATOR_EXCHANGE,
          reply.routingKey,
          reply.payload(order.order_id),
          { persistent: true },
        );

        await sleep(1_000);
      }

      logger.info('Happy-path walk complete for order', {
        order_id: order.order_id,
      });
    }
  });
}

/**
 * Scenario 2 — Compensation Trigger.
 * Publishes a payment event then immediately sends an ORDER_REJECTED reply
 * to drive the orchestrator into compensation mode.
 */
async function runCompensationTrigger(svc: RabbitMQService): Promise<void> {
  logger.info('═══ SCENARIO: Compensation Trigger — restaurant rejects ═══');

  const order = {
    order_id: `mock-compensation-${Date.now()}`,
    user_id: 'mock-user-comp',
    amount: 500,
    currency: 'NPR',
    payment_reference: `pay_ref_comp_${Date.now()}`,
    paid_at: new Date().toISOString(),
  };

  logger.info('Publishing payment.v1.order.succeeded', {
    order_id: order.order_id,
  });
  await svc.publish(PAYMENT_EXCHANGE, PAYMENT_ROUTING_KEY, order, {
    persistent: true,
  });

  await sleep(1_500);

  logger.info('Publishing ORDER_REJECTED to trigger compensation', {
    order_id: order.order_id,
  });
  await svc.publish(
    ORCHESTRATOR_EXCHANGE,
    EVENTS.ORDER_REJECTED_V1,
    {
      saga_id: 'RESOLVED_AT_RUNTIME',
      order_id: order.order_id,
      rejected_at: new Date().toISOString(),
      reason: 'Mock rejection for testing compensation',
    },
    { persistent: true },
  );

  logger.info('Compensation trigger published.', { order_id: order.order_id });
}

/**
 * Scenario 3 — Duplicate / Idempotency Check.
 * Publishes the same `payment.v1.order.succeeded` event twice for one order.
 * The second publication should be silently dropped by the orchestrator
 * (saga already exists).
 */
async function runIdempotencyCheck(svc: RabbitMQService): Promise<void> {
  logger.info('═══ SCENARIO: Idempotency — duplicate payment event ═══');

  const order = {
    order_id: `mock-idempotent-${Date.now()}`,
    user_id: 'mock-user-idem',
    amount: 750,
    currency: 'NPR',
    payment_reference: `pay_ref_idem_${Date.now()}`,
    paid_at: new Date().toISOString(),
  };

  for (let i = 1; i <= 2; i++) {
    logger.info(`Publishing payment event (attempt ${i}/2)`, {
      order_id: order.order_id,
    });
    await svc.publish(PAYMENT_EXCHANGE, PAYMENT_ROUTING_KEY, order, {
      persistent: true,
    });
    await sleep(800);
  }

  logger.info('Idempotency check published. Second event should be a no-op.', {
    order_id: order.order_id,
  });
}

/* ─── ENTRYPOINT ──────────────────────────────────────────────── */

(async () => {
  const manager = new ConnectionManager({
    url: process.env.RABBITMQ_1_CONNECTION_URL ?? 'amqp://localhost:5672',
    heartbeat: 60,
    username: process.env.RABBITMQ_1_DEFAULT_USER,
    password: process.env.RABBITMQ_1_DEFAULT_PASS,
  });

  logger.info('Mock publisher connecting to RabbitMQ...');

  const svc = new RabbitMQService(manager);
  await svc.init();

  /* Ensure both exchanges exist before publishing */
  await svc.assertExchange(PAYMENT_EXCHANGE, 'topic', { durable: true });
  await svc.assertExchange(ORCHESTRATOR_EXCHANGE, 'direct', { durable: true });

  logger.info('Connected. Running mock scenarios...');

  await runHappyPath(svc);
  await sleep(2_000);

  await runCompensationTrigger(svc);
  await sleep(2_000);

  await runIdempotencyCheck(svc);

  logger.info('All scenarios published. Closing connection...');
  await manager.close();
  logger.info('Mock publisher done.');
  process.exit(0);
})();
