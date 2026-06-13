import {
  Column,
  CreateDateColumn,
  Entity,
  PrimaryColumn,
  UpdateDateColumn,
} from 'typeorm';

export enum OrchestratorEventType {
  COMMAND = 'COMMAND',
  COMPENSATION = 'COMPENSATION',
}

@Entity({ name: 'orchestrator_outbox' })
export class OrchestratorOutbox {
  @PrimaryColumn({ type: 'uuid' })
  id!: string;

  @Column({ type: 'varchar' })
  event_id!: string;

  @Column({ type: 'varchar' })
  routing_key!: string;

  @Column({ type: 'int', default: 0 })
  version!: number;

  @Column({ type: 'uuid' })
  saga_id!: string;

  @Column({
    type: 'enum',
    enum: OrchestratorEventType,
    default: OrchestratorEventType.COMMAND,
  })
  saga_event_type!: OrchestratorEventType;

  @Column({ type: 'jsonb' })
  payload!: Record<string, unknown>;

  @Column({ type: 'int', default: 0 })
  retries!: number;

  @Column({ type: 'boolean', default: false })
  processed!: boolean;

  @Column({ type: 'timestamp', nullable: true })
  processed_at!: Date | null;

  @Column({ type: 'timestamp', nullable: true })
  failed_at!: Date | null;

  @CreateDateColumn()
  created_at!: Date;

  @UpdateDateColumn()
  updated_at!: Date;
}
