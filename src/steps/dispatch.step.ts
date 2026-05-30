import { DispatchService } from 'src/services/dispatch.service';
import { RestaurantService } from 'src/services/restaurant.service';
import {
  DispatchPayload,
  OrderPayload,
  SagaDefinition,
} from 'src/types/saga.types';

/* EXTRACT INTO SEPARATE FILES FOR BETTER VISIBILITY AND CONTROL */
export const DispatchSaga: SagaDefinition = {
  name: 'dispatch-saga',

  steps: [
    {
      name: 'request-restaurant-acceptance' /* THRESHOLD TIME ALLOCATED SUCH THAT BEFORE RUNNING THE NEXT COMMAND WE CHECK THE RULE SUCH THAT WE REJECT LATE ACCEPTANCE */,

      execute: async (payload: unknown): Promise<void> => {
        await RestaurantService.acceptOrder(payload as OrderPayload);
      },

      compensate: async (payload: unknown): Promise<void> => {
        await RestaurantService.rejectOrder(payload as OrderPayload);
      },
    },
    {
      name: 'create-dispatch',

      execute: async (payload: unknown): Promise<void> => {
        await DispatchService.findNearbyAgents(payload as DispatchPayload);
      },

      compensate: async (payload: unknown): Promise<void> => {
        await RestaurantService.rejectOrder(payload as OrderPayload);
      },
    },
    {
      name: 'notify-agents',

      execute: async (payload: unknown): Promise<void> => {},

      compensate: async (payload: unknown): Promise<void> => {},
    },
    {
      name: 'confirm-agent-assignment',

      execute: async (payload: unknown): Promise<void> => {
        const typedPayload = payload as { order_id: string; agent_id: string };
        await Promise.all([
          DispatchService.releaseLockedDeliveryAgents(typedPayload.order_id),
          DispatchService.updateAcceptedDeliveryAgentStatus(
            typedPayload.agent_id,
          ),
        ]);
      },

      compensate: async (payload: unknown): Promise<void> => {},
    },
  ],
};
