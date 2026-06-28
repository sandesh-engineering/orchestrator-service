import { SagaStateDefinition } from '../types/saga.types';

export const DispatchSagaState: SagaStateDefinition[] = [
  {
    name: 'AWAITING_RESTAURANT_CONFIRMATION',
    commandRoutingKey: 'restaurant.confirm',
    compensationRoutingKey: 'restaurant.cancel',
  },
  {
    name: 'CREATE_DISPATCH',
    commandRoutingKey: 'dispatch.create',
    compensationRoutingKey: 'restaurant.cancel',
  },
  {
    name: 'NOTIFY_AGENTS',
    commandRoutingKey: 'dispatch.notify-agents',
  },
  {
    name: 'CONFIRM_AGENT_ASSIGNMENT',
    commandRoutingKey: 'dispatch.confirm-agent-assignment',
  },
];
