import { SagaDomain } from 'src/enums/saga.domain.enum';
import { SagaStatus } from '../entities/saga.entity';

export interface SagaInstance {
  id: string;
  type: string;
  status: SagaStatus;
  currentStep: number;
  payload: Record<string, unknown>;
}

export interface SagaStep {
  name: string;
  execute(payload: unknown): Promise<void>;
  compensate?(payload: unknown): Promise<void>;
}

export interface SagaDefinition {
  name: string;
  steps: SagaStep[];
}

/* PAYLOADS FOR THE STEPS */
export type OrderPayload = {
  order_id: string;
  restaurant_id: string;
  customer_id?: string;
};

type LocationCoords = {
  latitude: string;
  longitude: string;
};

export type DispatchPayload = {
  restaurant_coords: LocationCoords;
  customer_coords: LocationCoords;
} & OrderPayload;

export interface SagaStateDefinition {
  /* Human-readable name, stored in completed_steps */
  name: string;
  /* Routing key the outbox poller uses to publish the forward command */
  commandRoutingKey: string;
  /* Routing key for compensation — undefined means non-compensable */
  compensationRoutingKey?: string;
}

/**
 * Shape of messages published onto the orchestrator's domain-specific
 * DLQs by `EventBusService#publishToDLQ`.
 */
export interface OrchestratorDlqMessage {
  saga_id: string;
  saga_event_type: string;
  routing_key: string;
  payload: Record<string, unknown>;
  domain: SagaDomain;
}
