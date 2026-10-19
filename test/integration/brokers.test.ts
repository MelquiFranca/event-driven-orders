import { randomUUID } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn, type ChildProcess } from 'node:child_process';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { PaymentService } from '../../src/application/payment-service.js';
import { openDatabase } from '../../src/infra/database.js';
import { createEventProducer, createKafka } from '../../src/infra/kafka.js';
import {
  configureRabbit,
  connectRabbit,
  publishConfirmed,
  rabbitNames,
  type NotificationTask,
} from '../../src/infra/rabbit.js';
import { confirmed } from '../helpers.js';

const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const rabbitUrl = process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672';
const tempDirectory = mkdtempSync(join(tmpdir(), 'duplicate-payment-integration-'));
const children = new Set<ChildProcess>();

async function waitFor(check: () => boolean | Promise<boolean>, timeoutMs = 15_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`condition not met within ${timeoutMs}ms`);
}

function startProcess(script: string, env: Record<string, string>): ChildProcess {
  const child = spawn(process.execPath, ['--import', 'tsx', script], {
    cwd: process.cwd(),
    env: { ...process.env, LOG_LEVEL: 'silent', ...env },
    stdio: 'pipe',
  });
  children.add(child);
  child.once('exit', () => children.delete(child));
  return child;
}

async function stop(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null) return;
  child.kill('SIGTERM');
  await new Promise<void>((resolve) => child.once('exit', () => resolve()));
}

async function createTopic(topic: string): Promise<void> {
  const admin = createKafka(brokers).admin();
  await admin.connect();
  await admin.createTopics({ topics: [{ topic, numPartitions: 1, replicationFactor: 1 }] });
  await admin.disconnect();
}

beforeAll(async () => {
  const admin = createKafka(brokers).admin();
  await admin.connect();
  await admin.listTopics();
  await admin.disconnect();
  const rabbit = await connectRabbit(rabbitUrl);
  await rabbit.close();
});

afterAll(async () => {
  await Promise.all([...children].map(stop));
  rmSync(tempDirectory, { recursive: true, force: true });
});

describe('real broker recovery', () => {
  it('records one payment after five Kafka deliveries', async () => {
    const suffix = randomUUID();
    const topic = `payments-duplicate-${suffix}`;
    const group = `group-${suffix}`;
    const databasePath = join(tempDirectory, `${suffix}.sqlite`);
    await createTopic(topic);
    const db = openDatabase(databasePath);
    new PaymentService(db).createInvoice({ invoiceId: 'inv-456', amount: 15_000, currency: 'BRL' });
    db.close();
    const consumer = startProcess('src/processes/payment-consumer.ts', {
      DATABASE_PATH: databasePath, KAFKA_TOPIC: topic, KAFKA_GROUP_ID: group,
    });
    await new Promise((resolve) => setTimeout(resolve, 1_500));
    const producer = await createEventProducer(brokers, topic);
    for (let index = 0; index < 5; index += 1) await producer.publish(confirmed());
    await producer.producer.disconnect();
    await waitFor(() => {
      const checkDb = openDatabase(databasePath);
      const count = (checkDb.prepare("SELECT COUNT(*) AS count FROM ledger_entries WHERE entry_type = 'PAYMENT'").get() as { count: number }).count;
      checkDb.close();
      return count === 1;
    });
    await stop(consumer);
    const resultDb = openDatabase(databasePath);
    expect(resultDb.prepare("SELECT COUNT(*) AS count FROM ledger_entries WHERE entry_type = 'PAYMENT'").get()).toEqual({ count: 1 });
    resultDb.close();
  });

  it('redelivers after a crash between SQLite commit and Kafka offset commit', async () => {
    const suffix = randomUUID();
    const topic = `payments-crash-${suffix}`;
    const group = `group-${suffix}`;
    const databasePath = join(tempDirectory, `${suffix}.sqlite`);
    await createTopic(topic);
    const db = openDatabase(databasePath);
    new PaymentService(db).createInvoice({ invoiceId: 'inv-456', amount: 15_000, currency: 'BRL' });
    db.close();

    const crashing = startProcess('src/processes/payment-consumer.ts', {
      DATABASE_PATH: databasePath, KAFKA_TOPIC: topic, KAFKA_GROUP_ID: group,
      FAILPOINT: 'after-db-commit-before-offset-commit',
    });
    await new Promise((resolve) => setTimeout(resolve, 1_500));
    const producer = await createEventProducer(brokers, topic);
    await producer.publish(confirmed());
    await producer.producer.disconnect();
    const exitCode = await new Promise<number | null>((resolve) => crashing.once('exit', resolve));
    expect(exitCode).toBe(70);

    const afterCrash = openDatabase(databasePath);
    expect(afterCrash.prepare("SELECT COUNT(*) AS count FROM ledger_entries WHERE entry_type = 'PAYMENT'").get()).toEqual({ count: 1 });
    afterCrash.close();

    const restarted = startProcess('src/processes/payment-consumer.ts', {
      DATABASE_PATH: databasePath, KAFKA_TOPIC: topic, KAFKA_GROUP_ID: group, FAILPOINT: '',
    });
    const admin = createKafka(brokers).admin();
    await admin.connect();
    await waitFor(async () => {
      const offsets = await admin.fetchOffsets({ groupId: group, topics: [topic] });
      return offsets[0]?.partitions[0]?.offset === '1';
    });
    await admin.disconnect();
    await stop(restarted);
    const resultDb = openDatabase(databasePath);
    expect(resultDb.prepare("SELECT COUNT(*) AS count FROM ledger_entries WHERE entry_type = 'PAYMENT'").get()).toEqual({ count: 1 });
    resultDb.close();
  });

  it('dispatches a pending outbox item after the dispatcher starts', async () => {
    const suffix = randomUUID();
    const databasePath = join(tempDirectory, `${suffix}.sqlite`);
    const rabbit = await connectRabbit(rabbitUrl);
    const channel = await rabbit.createConfirmChannel();
    await configureRabbit(channel, 200);
    await channel.purgeQueue(rabbitNames.queue);
    const db = openDatabase(databasePath);
    const service = new PaymentService(db);
    service.createInvoice({ invoiceId: 'inv-456', amount: 15_000, currency: 'BRL' });
    service.process(confirmed());
    db.close();
    const dispatcher = startProcess('src/processes/outbox-dispatcher.ts', {
      DATABASE_PATH: databasePath, RABBIT_RETRY_DELAY_MS: '200', OUTBOX_POLL_MS: '50',
    });
    await waitFor(() => {
      const checkDb = openDatabase(databasePath);
      const status = (checkDb.prepare('SELECT status FROM notification_outbox').get() as { status: string }).status;
      checkDb.close();
      return status === 'DISPATCHED';
    });
    expect(await channel.get(rabbitNames.queue, { noAck: true })).not.toBe(false);
    await stop(dispatcher);
    await channel.close();
    await rabbit.close();
  });

  it('retries temporary failures and dead-letters permanent or exhausted failures', async () => {
    const suffix = randomUUID();
    const databasePath = join(tempDirectory, `${suffix}.sqlite`);
    const rabbit = await connectRabbit(rabbitUrl);
    const channel = await rabbit.createConfirmChannel();
    await configureRabbit(channel, 200);
    await Promise.all([rabbitNames.queue, rabbitNames.retryQueue, rabbitNames.deadLetterQueue].map((queue) => channel.purgeQueue(queue)));
    const worker = startProcess('src/processes/notification-worker.ts', {
      DATABASE_PATH: databasePath, RABBIT_RETRY_DELAY_MS: '200', RABBIT_MAX_ATTEMPTS: '3',
    });
    await new Promise((resolve) => setTimeout(resolve, 500));
    const temporary: NotificationTask = {
      taskId: `temporary-${suffix}`, eventId: 'evt', invoiceId: 'inv', paymentId: 'pay',
      notificationType: 'TEST', simulateFailure: 'temporary-once',
    };
    await publishConfirmed(channel, rabbitNames.routing, temporary);
    await waitFor(() => {
      const checkDb = openDatabase(databasePath);
      const found = checkDb.prepare('SELECT 1 FROM completed_notifications WHERE task_id = ?').get(temporary.taskId) !== undefined;
      checkDb.close();
      return found;
    });

    const permanent = { ...temporary, taskId: `permanent-${suffix}`, simulateFailure: 'permanent' as const };
    const exhausted = { ...temporary, taskId: `exhausted-${suffix}`, simulateFailure: 'temporary-always' as const };
    await publishConfirmed(channel, rabbitNames.routing, permanent);
    await publishConfirmed(channel, rabbitNames.routing, exhausted);
    await waitFor(async () => (await channel.checkQueue(rabbitNames.deadLetterQueue)).messageCount === 2);
    expect((await channel.checkQueue(rabbitNames.deadLetterQueue)).messageCount).toBe(2);
    await stop(worker);
    await channel.close();
    await rabbit.close();
  });
});
