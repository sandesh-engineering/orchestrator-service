import { MigrationInterface, QueryRunner } from "typeorm";

export class MakeFailedAtNullable1782649637391 implements MigrationInterface {
    name = 'MakeFailedAtNullable1782649637391'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`CREATE TYPE "public"."saga_entity_domain_enum" AS ENUM('dispatch', 'subscription')`);
        await queryRunner.query(`ALTER TABLE "saga_entity" ADD "domain" "public"."saga_entity_domain_enum" NOT NULL DEFAULT 'dispatch'`);
        await queryRunner.query(`ALTER TABLE "saga_entity" ADD "failure_context" jsonb`);
        await queryRunner.query(`ALTER TABLE "saga_entity" ADD "reprocess_count" integer NOT NULL DEFAULT '0'`);
        await queryRunner.query(`ALTER TABLE "saga_entity" ADD "last_reprocessed_at" TIMESTAMP`);
        await queryRunner.query(`CREATE TYPE "public"."orchestrator_outbox_domain_enum" AS ENUM('dispatch', 'subscription')`);
        await queryRunner.query(`ALTER TABLE "orchestrator_outbox" ADD "domain" "public"."orchestrator_outbox_domain_enum" NOT NULL DEFAULT 'dispatch'`);
        await queryRunner.query(`ALTER TABLE "saga_entity" ALTER COLUMN "failed_at" DROP NOT NULL`);
        await queryRunner.query(`ALTER TABLE "orchestrator_outbox" ALTER COLUMN "id" SET DEFAULT uuid_generate_v4()`);
        await queryRunner.query(`ALTER TABLE "orchestrator_outbox" ADD CONSTRAINT "FK_87bbf6a16a003ea45253d163acc" FOREIGN KEY ("saga_id") REFERENCES "saga_entity"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "orchestrator_outbox" DROP CONSTRAINT "FK_87bbf6a16a003ea45253d163acc"`);
        await queryRunner.query(`ALTER TABLE "orchestrator_outbox" ALTER COLUMN "id" DROP DEFAULT`);
        await queryRunner.query(`ALTER TABLE "saga_entity" ALTER COLUMN "failed_at" SET NOT NULL`);
        await queryRunner.query(`ALTER TABLE "orchestrator_outbox" DROP COLUMN "domain"`);
        await queryRunner.query(`DROP TYPE "public"."orchestrator_outbox_domain_enum"`);
        await queryRunner.query(`ALTER TABLE "saga_entity" DROP COLUMN "last_reprocessed_at"`);
        await queryRunner.query(`ALTER TABLE "saga_entity" DROP COLUMN "reprocess_count"`);
        await queryRunner.query(`ALTER TABLE "saga_entity" DROP COLUMN "failure_context"`);
        await queryRunner.query(`ALTER TABLE "saga_entity" DROP COLUMN "domain"`);
        await queryRunner.query(`DROP TYPE "public"."saga_entity_domain_enum"`);
    }

}
