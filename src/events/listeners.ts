import { logger } from '@platform/logger';
import { SagaCoordinator } from 'src/saga-coordinator';

export class OrchestratorListener {
  constructor(private readonly coordinator: SagaCoordinator) {}

  onOrderConfirmationRequest = async (payload: Record<string, unknown>) => {
    logger.info('Received OrderConfirmationRequested, initiating saga', {
      order_id: payload?.order_id,
    });

    const { order_id, ...rest } = payload;

    const idempotencyKey = `order-confirmation-${order_id}`;

    /* Initiate the saga */
    const { isNew, sagaId } = await this.coordinator.startSaga(
      idempotencyKey,
      order_id,
      rest,
    );

    if (!isNew) {
      logger.warn('Skipping saga run as it is already running', {
        sagaId,
        idempotencyKey,
        timestamp: new Date().toISOString(),
      });
      return;
    }

    /* Immediately dispatch the first step to avoid ambiguity */
    await this.coordinator.dispatchNextStep({
      payload: {
        order_id: payload.order_id,
        rest,
      },
      sagaId,
      skipTransaction: true,
      stepIndex: 0,
    });
  };
}
