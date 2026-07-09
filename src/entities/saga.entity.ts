import { SagaDomain } from '../enums/saga.domain.enum';
import {
  Column,
  CreateDateColumn,
  Entity,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
} from 'typeorm';
import { OrchestratorEventType } from '../enums/orchestrator-event-type.enum';

export enum SagaStatus {
  PENDING = 'PENDING',
  RUNNING = 'RUNNING',
  COMPLETED = 'COMPLETED',
  COMPENSATING = 'COMPENSATING',
  COMPENSATED = 'COMPENSATED',
  FAILED = 'FAILED',
}

@Entity()
export class SagaEntity {
  @PrimaryGeneratedColumn('uuid')
  id!: string;

  @Column({ type: 'varchar', unique: true })
  idempotency_key!: string;

  @Column({ type: 'varchar' })
  correlation_id!: string;

  @Column({ type: 'varchar' })
  name!: string;

  @Column({ type: 'enum', enum: SagaDomain, default: SagaDomain.DISPATCH })
  domain!: SagaDomain;

  @Column({ type: 'jsonb' })
  payload!: Record<string, unknown>;

  @Column({ type: 'enum', enum: SagaStatus, default: SagaStatus.RUNNING })
  status!: SagaStatus;

  @Column({ type: 'jsonb', default: [] })
  completed_steps!: string[];

  @Column({ type: 'int', default: 0 })
  current_step_index!: number;

  @Column({ type: 'timestamp' })
  started_at!: string;

  @Column({ type: 'jsonb', nullable: true })
  failure_context!: {
    routing_key: string;
    saga_event_type: OrchestratorEventType;
    payload: Record<string, unknown>;
    error?: string;
  } | null;

  @Column({ type: 'int', default: 0 })
  reprocess_count!: number;

  @Column({ type: 'timestamp', nullable: true })
  last_reprocessed_at!: string | null;

  @Column({ type: 'timestamp', nullable: true })
  failed_at!: string | null;

  @CreateDateColumn()
  created_at!: Date;

  @UpdateDateColumn()
  updated_at!: Date;
}
