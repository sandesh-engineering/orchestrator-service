import { DispatchPayload } from 'src/types/saga.types';

export class DispatchService {
  static async findNearbyAgents(payload: DispatchPayload) {}

  static async releaseLockedDeliveryAgents(order_id: string) {}

  static async updateAcceptedDeliveryAgentStatus(agent_id: string) {}
}
