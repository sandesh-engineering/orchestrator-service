import {
  Column,
  CreateDateColumn,
  Entity,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
} from 'typeorm';

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

  @Column({ type: 'jsonb' })
  payload!: Record<string, unknown>;

  @Column({ type: 'enum', enum: SagaStatus, default: SagaStatus.RUNNING })
  status!: SagaStatus;

  @Column({ type: 'jsonb', default: [] })
  completed_steps!: unknown[];

  @Column({ type: 'int', default: 0 })
  current_step_index!: number;

  @Column({ type: 'timestamp' })
  started_at!: string;

  @Column({ type: 'timestamp' })
  failed_at!: string;

  @CreateDateColumn()
  created_at!: Date;

  @UpdateDateColumn()
  updated_at!: Date;
}
