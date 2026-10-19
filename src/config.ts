import 'dotenv/config';
import { z } from 'zod';

const configSchema = z.object({
  PORT: z.coerce.number().int().positive().default(3000),
  DATABASE_PATH: z.string().default('./data/payments.sqlite'),
  KAFKA_BROKERS: z.string().default('localhost:9092'),
  KAFKA_TOPIC: z.string().default('financial-events'),
  KAFKA_GROUP_ID: z.string().default('payment-ledger'),
  RABBITMQ_URL: z.string().default('amqp://guest:guest@localhost:5672'),
  RABBIT_RETRY_DELAY_MS: z.coerce.number().int().positive().default(1000),
  RABBIT_MAX_ATTEMPTS: z.coerce.number().int().positive().default(3),
  OUTBOX_POLL_MS: z.coerce.number().int().positive().default(500),
  FAILPOINT: z.string().optional(),
});

const parsed = configSchema.parse(process.env);

export const config = {
  port: parsed.PORT,
  databasePath: parsed.DATABASE_PATH,
  kafkaBrokers: parsed.KAFKA_BROKERS.split(',').map((broker) => broker.trim()),
  kafkaTopic: parsed.KAFKA_TOPIC,
  kafkaGroupId: parsed.KAFKA_GROUP_ID,
  rabbitUrl: parsed.RABBITMQ_URL,
  rabbitRetryDelayMs: parsed.RABBIT_RETRY_DELAY_MS,
  rabbitMaxAttempts: parsed.RABBIT_MAX_ATTEMPTS,
  outboxPollMs: parsed.OUTBOX_POLL_MS,
  failpoint: parsed.FAILPOINT,
};
