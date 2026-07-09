import { ConsumeMessage } from '@platform/queue-rabbitmq';
import { OrchestratorListener } from '../listeners';
import { OrchestratorEventType } from '../../enums/orchestrator-event-type.enum';
import { SagaDomain } from '../../enums/saga.domain.enum';
import { SagaCoordinator } from '../../saga-coordinator';

jest.mock('@platform/logger', () => ({
  logger: {
    debug: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
  },
}));

describe('OrchestratorListener', () => {
  let coordinator: jest.Mocked<SagaCoordinator>;
  let listener: OrchestratorListener;

  beforeEach(() => {
    coordinator = {
      startSaga: jest.fn(),
      dispatchNextStep: jest.fn(),
      markSagaAsFailed: jest.fn(),
    } as unknown as jest.Mocked<SagaCoordinator>;

    listener = new OrchestratorListener(coordinator);
  });

  describe('onPaymentOrderSucceeded', () => {
    it('should start saga and dispatch first step for new payment event', async () => {
      /* Arrange */
      const payload = {
        order_id: 'order-1',
        user_id: 'user-1',
        amount: 100,
        currency: 'USD',
        payment_reference: 'payment-1',
        paid_at: '2026-06-29T00:00:00.000Z',
      };
      coordinator.startSaga.mockResolvedValue({
        sagaId: 'saga-1',
        isNew: true,
      });

      /* Act */
      await listener.onPaymentOrderSucceeded(toMessage(payload));

      /* Assert */
      expect(coordinator.startSaga).toHaveBeenCalledWith(
        'dispatch-saga:order:order-1',
        'order-1',
        payload,
      );
      expect(coordinator.dispatchNextStep).toHaveBeenCalledWith({
        payload,
        sagaId: 'saga-1',
        skipTransaction: true,
        stepIndex: 0,
      });
    });

    it('should skip dispatch when saga is already known', async () => {
      /* Arrange */
      const payload = {
        order_id: 'order-1',
        user_id: 'user-1',
        amount: 100,
        currency: 'USD',
        payment_reference: 'payment-1',
        paid_at: '2026-06-29T00:00:00.000Z',
      };
      coordinator.startSaga.mockResolvedValue({
        sagaId: 'saga-1',
        isNew: false,
      });

      /* Act */
      await listener.onPaymentOrderSucceeded(toMessage(payload));

      /* Assert */
      expect(coordinator.dispatchNextStep).not.toHaveBeenCalled();
    });

    it('should ignore malformed payment event without order id', async () => {
      /* Arrange */
      const payload = {
        order_id: '',
        user_id: 'user-1',
        amount: 100,
        currency: 'USD',
        payment_reference: 'payment-1',
        paid_at: '2026-06-29T00:00:00.000Z',
      };

      /* Act */
      await listener.onPaymentOrderSucceeded(toMessage(payload));

      /* Assert */
      expect(coordinator.startSaga).not.toHaveBeenCalled();
      expect(coordinator.dispatchNextStep).not.toHaveBeenCalled();
    });
  });

  describe('onDispatchDlqEvents', () => {
    it('should mark dispatch saga as failed from dlq message', async () => {
      /* Arrange */
      const payload = {
        saga_id: 'saga-1',
        saga_event_type: OrchestratorEventType.COMMAND,
        routing_key: 'dispatch.create',
        payload: { order_id: 'order-1' },
        domain: SagaDomain.DISPATCH,
      };

      /* Act */
      await listener.onDispatchDlqEvents(toMessage(payload));

      /* Assert */
      expect(coordinator.markSagaAsFailed).toHaveBeenCalledWith('saga-1', {
        routing_key: 'dispatch.create',
        saga_event_type: OrchestratorEventType.COMMAND,
        payload: { order_id: 'order-1' },
      });
    });

    it('should skip dlq event when saga id is missing', async () => {
      /* Arrange */
      const payload = {
        saga_id: '',
        saga_event_type: OrchestratorEventType.COMMAND,
        routing_key: 'dispatch.create',
        payload: { order_id: 'order-1' },
        domain: SagaDomain.DISPATCH,
      };

      /* Act */
      await listener.onDispatchDlqEvents(toMessage(payload));

      /* Assert */
      expect(coordinator.markSagaAsFailed).not.toHaveBeenCalled();
    });
  });
});

/**
 * Builds a minimal RabbitMQ consume message for listener unit tests.
 *
 * @param {Record<string, unknown>} payload - JSON payload to expose through message content.
 * @returns {ConsumeMessage} Minimal consume message used by the orchestrator listener.
 */
function toMessage(payload: Record<string, unknown>): ConsumeMessage {
  return {
    content: Buffer.from(JSON.stringify(payload)),
  } as ConsumeMessage;
}
