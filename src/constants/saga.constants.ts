import { SagaDomain } from '../enums/saga.domain.enum';

export const COMMANDS = {
  REQUEST_ORDER_ACCEPTANCE_V1: 'order.v1.request-acceptance',
  REQUEST_DISPATCH_CREATION_V1: 'dispatch.v1.request-creation',
  REQUEST_AGENT_SELECTION_V1: 'dispatch.v1.request-agent-selection',
  CONFIRM_AGENT_ASSIGNMENT_V1: 'dispatch.v1.confirm-agent-assignment',
} as const;

export const EVENTS = {
  PAYMENT_SUCCEEDED_V1: 'payment.v1.succeeded',
  //   PAYMENT_FAILED_V1: 'payment.v1.failed',

  ORDER_ACCEPTED_V1: 'order.v1.accepted',
  ORDER_REJECTED_V1: 'order.v1.rejected',

  DISPATCH_CREATED_V1: 'dispatch.v1.created',
  DISPATCH_CREATION_REJECTED_V1: 'dispatch.v1.creation-rejected',

  AGENT_NOTIFIED_V1: 'agent.v1.notified',
  AGENT_ASSIGNED_V1: 'agent.v1.assigned',
} as const;

export const DLQ_QUEUE_NAMES: Record<SagaDomain, string> = {
  [SagaDomain.DISPATCH]: 'orchestrator.dispatch.dlq',
  [SagaDomain.SUBSCRIPTION]: 'orchestrator.subscription.dlq',
} as const;
