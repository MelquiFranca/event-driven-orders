import { config } from '../config.js';
import { financialEventSchema } from '../domain/events.js';
import { PaymentService } from '../application/payment-service.js';
import { openDatabase } from '../infra/database.js';
import { createEventConsumer } from '../infra/kafka.js';
import { logger } from '../logger.js';

const db = openDatabase(config.databasePath);
const service = new PaymentService(db);
const consumer = await createEventConsumer(config.kafkaBrokers, config.kafkaGroupId, config.kafkaTopic);

await consumer.run({
  autoCommit: false,
  eachMessage: async ({ topic, partition, message }) => {
    let raw: unknown;
    try {
      raw = JSON.parse(message.value?.toString() ?? 'null');
      const parsed = financialEventSchema.safeParse(raw);
      if (!parsed.success) {
        if (raw && typeof raw === 'object' && 'eventId' in raw && typeof raw.eventId === 'string') {
          const eventType = 'eventType' in raw && typeof raw.eventType === 'string' ? raw.eventType : 'Unknown';
          db.prepare(
            `INSERT OR IGNORE INTO processed_events (event_id, event_type, outcome, reason)
             VALUES (?, ?, 'REJECTED', 'invalid event contract')`,
          ).run(raw.eventId, eventType);
        }
        logger.error({ validation: parsed.error.flatten(), raw }, 'invalid Kafka message rejected');
      } else {
        const result = service.process(parsed.data);
        logger.info({ result, partition, offset: message.offset }, 'financial event processed');
        if (config.failpoint === 'after-db-commit-before-offset-commit') {
          logger.fatal({ eventId: parsed.data.eventId }, 'failpoint triggered after SQLite commit');
          process.exit(70);
        }
      }
    } catch (error) {
      logger.error({ error, raw }, 'event processing failed; offset will not be committed');
      throw error;
    }
    await consumer.commitOffsets([{
      topic,
      partition,
      offset: (BigInt(message.offset) + 1n).toString(),
    }]);
  },
});

async function shutdown(): Promise<void> {
  await consumer.disconnect();
  db.close();
}
process.once('SIGINT', () => void shutdown());
process.once('SIGTERM', () => void shutdown());
