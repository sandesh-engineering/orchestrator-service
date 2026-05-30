import { OrderPayload } from 'src/types/saga.types';

export class RestaurantService {
  static async acceptOrder(payload: OrderPayload) {
    /* Should change the order status */
    await Promise.resolve();

    return {
      id: payload.order_id,
      success: true,
      message: 'Order accepted!',
    };
  }

  static async rejectOrder<T extends OrderPayload>(payload: T) {
    /* Should coordinate the order status update as well as initiating refund process if the order is not marked as COD */

    await Promise.resolve();

    return {
      id: payload.order_id,
      success: true,
      message: 'Payment !',
    };
  }
}
