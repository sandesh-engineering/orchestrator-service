import { DataSource, Repository } from 'typeorm';
import { SagaEntity } from '../entities/saga.entity';

export class SagaRepository {
  private readonly sagaRepo: Repository<SagaEntity>;

  constructor(private readonly datasource: DataSource) {
    this.sagaRepo = this.datasource.getRepository(SagaEntity);
  }

  /**
   * Creates and saves a new SagaEntity record.
   *
   * @param {Partial<SagaEntity>} payload - The values to initialize the saga record with.
   * @returns {Promise<SagaEntity>} A promise resolving to the saved SagaEntity.
   */
  async create(payload: Partial<SagaEntity>): Promise<SagaEntity> {
    const sagaInstance = this.sagaRepo.create(payload);
    return this.sagaRepo.save(sagaInstance);
  }

  /**
   * Saves an existing SagaEntity record with updated fields.
   *
   * @param {Partial<SagaEntity>} payload - The values to save.
   * @returns {Promise<SagaEntity>} A promise resolving to the saved SagaEntity.
   */
  async save(payload: Partial<SagaEntity>): Promise<SagaEntity> {
    return this.sagaRepo.save(payload);
  }

  /**
   * Updates SagaEntity records matching a specific where clause with target payload values.
   *
   * @param {object} params - Update parameters.
   * @param {Record<string, unknown>} params.whereClause - Criteria to match records for updating.
   * @param {Record<string, unknown>} params.payload - New field values to apply.
   * @returns {Promise<import('typeorm').UpdateResult>} A promise resolving to the TypeORM update result.
   */
  async update({
    whereClause,
    payload,
  }: {
    whereClause: Record<string, unknown>;
    payload: Record<string, unknown>;
  }): Promise<import('typeorm').UpdateResult> {
    return await this.sagaRepo.update({ ...whereClause }, { ...payload });
  }

  /**
   * Finds a SagaEntity by its ID, selecting specific fields relevant to workflow coordination.
   *
   * @param {string} id - The Saga ID to search for.
   * @returns {Promise<SagaEntity | null>} A promise resolving to the matching SagaEntity, or null if not found.
   */
  async findById(id: string): Promise<SagaEntity | null> {
    return this.sagaRepo.findOne({
      where: { id },
      select: {
        id: true,
        current_step_index: true,
        status: true,
        completed_steps: true,
        payload: true,
      },
    });
  }
}
