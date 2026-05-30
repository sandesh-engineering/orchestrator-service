import { DataSource, EntityManager, In, Repository } from 'typeorm';
import { OrchestratorOutbox } from '../entities/orchestrator-outbox.entity';

export class OrchestratorOutboxRepository {
  private readonly repo: Repository<OrchestratorOutbox>;

  constructor(private readonly datasource: DataSource) {
    this.repo = this.datasource.getRepository(OrchestratorOutbox);
  }

  /**
   * Fetches unprocessed outbox events in a batch using pessimistic locking to prevent concurrency issues.
   *
   * @param {number} batch_size - The maximum number of events to fetch.
   * @returns {Promise<OrchestratorOutbox[]>} A promise resolving to the list of unprocessed outbox events.
   */
  async fetchUnprocessedEventsInBatch(
    batch_size: number = 150,
  ): Promise<OrchestratorOutbox[]> {
    const result = await this.datasource.transaction(async (manager) => {
      const records = await manager
        .createQueryBuilder(OrchestratorOutbox, 'orchestrator_outbox')
        .setLock('pessimistic_write')
        .setOnLocked('skip_locked')
        .select([
          'orchestrator_outbox.id',
          'orchestrator_outbox.payload',
          'orchestrator_outbox.retries',
        ])
        .where('orchestrator_outbox.processed = :processed', {
          processed: false,
        })
        .orderBy('orchestrator_outbox.version', 'ASC')
        .addOrderBy('orchestrator_outbox.created_at', 'ASC')
        .limit(batch_size)
        .getMany();

      return records;
    });

    return result;
  }

  /**
   * Marks a list of outbox events as processed.
   *
   * @param {string[]} ids - The list of outbox event IDs to mark as processed.
   * @param {EntityManager} manager - The entity manager to run within a transaction.
   * @returns {Promise<void>} A promise resolving when the update completes.
   */
  async markProcessed(ids: string[], manager: EntityManager): Promise<void> {
    if (ids.length === 0) return;

    await manager
      .createQueryBuilder()
      .update(OrchestratorOutbox)
      .set({
        processed: true,
        processed_at: () => 'NOW()',
      })
      .whereInIds(ids)
      .execute();
  }

  /**
   * Increments the retry count and records failure timestamp for failed outbox events.
   *
   * @param {string[]} ids - The list of outbox event IDs to update.
   * @param {EntityManager} manager - The entity manager to run within a transaction.
   * @returns {Promise<void>} A promise resolving when the update completes.
   */
  async incrementRetries(ids: string[], manager: EntityManager): Promise<void> {
    if (ids.length === 0) return;

    await manager
      .createQueryBuilder()
      .update(OrchestratorOutbox)
      .set({
        retries: () => 'retries + 1',
        failed_at: () => 'NOW()',
      })
      .whereInIds(ids)
      .execute();
  }

  /**
   * Finds events by their IDs.
   *
   * @param {string[]} ids - The list of event IDs to find.
   * @returns {Promise<OrchestratorOutbox[]>} A promise resolving to the list of matching outbox events.
   */
  async findEventsByIds(ids: string[]): Promise<OrchestratorOutbox[]> {
    return await this.repo.find({
      where: { id: In(ids) },
      select: ['id', 'payload', 'retries'],
    });
  }

  async createAndSaveOutboxRecord(payload: Partial<OrchestratorOutbox>) {
    const recordInstance = this.repo.create(payload);

    return await this.repo.save(recordInstance);
  }
}
