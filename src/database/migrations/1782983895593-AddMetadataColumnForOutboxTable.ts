import { MigrationInterface, QueryRunner } from "typeorm";

export class AddMetadataColumnForOutboxTable1782983895593 implements MigrationInterface {
    name = 'AddMetadataColumnForOutboxTable1782983895593'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "orchestrator_outbox" ADD "metadata" jsonb`);
        await queryRunner.query(`ALTER TABLE "orchestrator_outbox" ADD CONSTRAINT "FK_87bbf6a16a003ea45253d163acc" FOREIGN KEY ("saga_id") REFERENCES "saga_entity"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "orchestrator_outbox" DROP CONSTRAINT "FK_87bbf6a16a003ea45253d163acc"`);
        await queryRunner.query(`ALTER TABLE "orchestrator_outbox" DROP COLUMN "metadata"`);
    }

}
