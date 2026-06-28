import { MigrationInterface, QueryRunner } from "typeorm";

export class NewColumns1781339310917 implements MigrationInterface {
    name = 'NewColumns1781339310917'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "saga_entity" ADD "idempotency_key" character varying NOT NULL`);
        await queryRunner.query(`ALTER TABLE "saga_entity" ADD CONSTRAINT "UQ_f4ad08e37aaf1fe3fb1f94c84a3" UNIQUE ("idempotency_key")`);
        await queryRunner.query(`ALTER TABLE "saga_entity" ADD "correlation_id" character varying NOT NULL`);
        await queryRunner.query(`ALTER TABLE "orchestrator_outbox" ADD "routing_key" character varying NOT NULL`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "orchestrator_outbox" DROP COLUMN "routing_key"`);
        await queryRunner.query(`ALTER TABLE "saga_entity" DROP COLUMN "correlation_id"`);
        await queryRunner.query(`ALTER TABLE "saga_entity" DROP CONSTRAINT "UQ_f4ad08e37aaf1fe3fb1f94c84a3"`);
        await queryRunner.query(`ALTER TABLE "saga_entity" DROP COLUMN "idempotency_key"`);
    }

}
