import { MigrationInterface, QueryRunner } from "typeorm";

export class InitialMigration1779372605392 implements MigrationInterface {
    name = 'InitialMigration1779372605392'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`CREATE TYPE "public"."orchestrator_outbox_saga_event_type_enum" AS ENUM('COMMAND', 'COMPENSATION')`);
        await queryRunner.query(`CREATE TABLE "orchestrator_outbox" ("id" uuid NOT NULL, "event_id" character varying NOT NULL, "version" integer NOT NULL DEFAULT '0', "saga_id" uuid NOT NULL, "saga_event_type" "public"."orchestrator_outbox_saga_event_type_enum" NOT NULL DEFAULT 'COMMAND', "payload" jsonb NOT NULL, "retries" integer NOT NULL DEFAULT '0', "processed" boolean NOT NULL DEFAULT false, "processed_at" TIMESTAMP, "failed_at" TIMESTAMP, "created_at" TIMESTAMP NOT NULL DEFAULT now(), "updated_at" TIMESTAMP NOT NULL DEFAULT now(), CONSTRAINT "PK_ccd48d219a8c18dde4ec0d93950" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE TYPE "public"."saga_entity_status_enum" AS ENUM('PENDING', 'RUNNING', 'COMPLETED', 'COMPENSATING', 'COMPENSATED', 'FAILED')`);
        await queryRunner.query(`CREATE TABLE "saga_entity" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "name" character varying NOT NULL, "payload" jsonb NOT NULL, "status" "public"."saga_entity_status_enum" NOT NULL DEFAULT 'RUNNING', "completed_steps" jsonb NOT NULL DEFAULT '[]', "current_step_index" integer NOT NULL DEFAULT '0', "started_at" TIMESTAMP NOT NULL, "failed_at" TIMESTAMP NOT NULL, "created_at" TIMESTAMP NOT NULL DEFAULT now(), "updated_at" TIMESTAMP NOT NULL DEFAULT now(), CONSTRAINT "PK_77aebd7087d91b0488d7f7f3e71" PRIMARY KEY ("id"))`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`DROP TABLE "saga_entity"`);
        await queryRunner.query(`DROP TYPE "public"."saga_entity_status_enum"`);
        await queryRunner.query(`DROP TABLE "orchestrator_outbox"`);
        await queryRunner.query(`DROP TYPE "public"."orchestrator_outbox_saga_event_type_enum"`);
    }

}
