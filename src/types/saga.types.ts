export enum SagaStatus {
  RUNNING = 'running',
  COMPLETED = 'completed',
  FAILED = 'failed',
  COMPENSATING = 'compensating',
}

export interface SagaInstance {
  id: string;
  type: string;
  status: keyof typeof SagaStatus;
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
