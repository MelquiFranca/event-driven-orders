import { randomUUID } from 'node:crypto';
import { spawn, type ChildProcess } from 'node:child_process';
import { config } from '../config.js';
import type { PaymentCancelled, PaymentConfirmed } from '../domain/events.js';
import { PaymentService } from '../application/payment-service.js';
import { openDatabase } from '../infra/database.js';
import { createEventProducer, createKafka } from '../infra/kafka.js';
import { configureRabbit, connectRabbit, publishConfirmed, rabbitNames, type NotificationTask } from '../infra/rabbit.js';

const scenario = process.argv[2];
const supported = ['duplicate-delivery', 'crash-before-offset', 'duplicate-payment', 'notification-retry', 'out-of-order'];
if (!scenario || !supported.includes(scenario)) {
  console.error(`Usage: npm run demo -- <${supported.join('|')}>`);
  process.exit(1);
}

const suffix = randomUUID().slice(0, 8);
const invoiceId = `inv-${suffix}`;
const paymentId = `pay-${suffix}`;
const topic = `${config.kafkaTopic}-${scenario}-${suffix}`;
const group = `${config.kafkaGroupId}-${scenario}-${suffix}`;

function confirmation(eventId = `evt-confirm-${suffix}`): PaymentConfirmed {
  return {
    schemaVersion: 1,
    eventId,
    eventType: 'PaymentConfirmed',
    invoiceId,
    paymentId,
    amount: 15_000,
    currency: 'BRL',
    occurredAt: new Date().toISOString(),
  };
}

function cancellation(): PaymentCancelled {
  return {
    schemaVersion: 1,
    eventId: `evt-cancel-${suffix}`,
    eventType: 'PaymentCancelled',
    invoiceId,
    paymentId,
    cancelsPaymentId: paymentId,
    amount: 15_000,
    currency: 'BRL',
    occurredAt: new Date(Date.now() - 60_000).toISOString(),
  };
}

async function waitFor(check: () => boolean, timeoutMs = 15_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`scenario timed out after ${timeoutMs}ms`);
}

function start(script: string, env: Record<string, string>): ChildProcess {
  return spawn(process.execPath, ['--import', 'tsx', script], {
    cwd: process.cwd(),
    env: { ...process.env, ...env },
    stdio: 'inherit',
  });
}

async function stop(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null) return;
  child.kill('SIGTERM');
  await new Promise<void>((resolve) => child.once('exit', () => resolve()));
}

async function prepareTopic(): Promise<void> {
  const admin = createKafka(config.kafkaBrokers).admin();
  await admin.connect();
  await admin.createTopics({ topics: [{ topic, numPartitions: 1, replicationFactor: 1 }] });
  await admin.disconnect();
}

function prepareInvoice(): PaymentService {
  const db = openDatabase(config.databasePath);
  const service = new PaymentService(db);
  service.createInvoice({ invoiceId, amount: 15_000, currency: 'BRL' });
  return service;
}

async function withConsumer(run: (publish: (event: PaymentConfirmed | PaymentCancelled) => Promise<void>, service: PaymentService) => Promise<void>): Promise<void> {
  await prepareTopic();
  const service = prepareInvoice();
  const consumer = start('src/processes/payment-consumer.ts', {
    KAFKA_TOPIC: topic,
    KAFKA_GROUP_ID: group,
  });
  await new Promise((resolve) => setTimeout(resolve, 1_500));
  const producer = await createEventProducer(config.kafkaBrokers, topic);
  try {
    await run(producer.publish, service);
  } finally {
    await producer.producer.disconnect();
    await stop(consumer);
    service['db'].close();
  }
}

async function duplicateDelivery(): Promise<void> {
  await withConsumer(async (publish, service) => {
    const event = confirmation();
    for (let index = 0; index < 5; index += 1) await publish(event);
    await waitFor(() => service.getInvoice(invoiceId)?.ledgerEntries.length === 1);
    console.log(JSON.stringify({ scenario, deliveries: 5, invoice: service.getInvoice(invoiceId) }, null, 2));
  });
}

async function duplicatePayment(): Promise<void> {
  await withConsumer(async (publish, service) => {
    await publish(confirmation());
    await publish(confirmation(`evt-confirm-again-${suffix}`));
    await waitFor(() => {
      const count = service.getInvoice(invoiceId)?.ledgerEntries.length;
      const events = service['db'].prepare('SELECT COUNT(*) AS count FROM processed_events WHERE event_id LIKE ?').get(`%${suffix}`) as { count: number };
      return count === 1 && events.count >= 2;
    });
    console.log(JSON.stringify({ scenario, invoice: service.getInvoice(invoiceId) }, null, 2));
  });
}

async function outOfOrder(): Promise<void> {
  await withConsumer(async (publish, service) => {
    await publish(cancellation());
    await publish(confirmation());
    await waitFor(() => service.getInvoice(invoiceId)?.status === 'CANCELLED');
    console.log(JSON.stringify({ scenario, note: 'occurredAt was not used for ordering', invoice: service.getInvoice(invoiceId) }, null, 2));
  });
}

async function crashBeforeOffset(): Promise<void> {
  await prepareTopic();
  const service = prepareInvoice();
  const env = { KAFKA_TOPIC: topic, KAFKA_GROUP_ID: group };
  const crashing = start('src/processes/payment-consumer.ts', { ...env, FAILPOINT: 'after-db-commit-before-offset-commit' });
  await new Promise((resolve) => setTimeout(resolve, 1_500));
  const producer = await createEventProducer(config.kafkaBrokers, topic);
  await producer.publish(confirmation());
  await producer.producer.disconnect();
  const exitCode = await new Promise<number | null>((resolve) => crashing.once('exit', resolve));
  if (exitCode !== 70) throw new Error(`expected failpoint exit 70, received ${exitCode}`);
  const afterCrash = service.getInvoice(invoiceId);
  const restarted = start('src/processes/payment-consumer.ts', { ...env, FAILPOINT: '' });
  const admin = createKafka(config.kafkaBrokers).admin();
  await admin.connect();
  const deadline = Date.now() + 15_000;
  let committed = false;
  while (Date.now() < deadline && !committed) {
    const offsets = await admin.fetchOffsets({ groupId: group, topics: [topic] });
    committed = offsets[0]?.partitions[0]?.offset === '1';
    if (!committed) await new Promise((resolve) => setTimeout(resolve, 100));
  }
  await admin.disconnect();
  await stop(restarted);
  if (!committed) throw new Error('restarted consumer did not commit the redelivered offset');
  console.log(JSON.stringify({ scenario, crashExitCode: exitCode, afterCrash, afterRestart: service.getInvoice(invoiceId) }, null, 2));
  service['db'].close();
}

async function notificationRetry(): Promise<void> {
  const service = prepareInvoice();
  const connection = await connectRabbit(config.rabbitUrl);
  const channel = await connection.createConfirmChannel();
  await configureRabbit(channel, config.rabbitRetryDelayMs);
  const worker = start('src/processes/notification-worker.ts', {});
  await new Promise((resolve) => setTimeout(resolve, 500));
  const task: NotificationTask = {
    taskId: `task-${suffix}`,
    eventId: `evt-${suffix}`,
    invoiceId,
    paymentId,
    notificationType: 'DEMO_RETRY',
    simulateFailure: 'temporary-once',
  };
  await publishConfirmed(channel, rabbitNames.routing, task);
  await waitFor(() => service['db'].prepare('SELECT 1 FROM completed_notifications WHERE task_id = ?').get(task.taskId) !== undefined);
  console.log(JSON.stringify({ scenario, taskId: task.taskId, result: 'completed after one retry' }, null, 2));
  await stop(worker);
  await channel.close();
  await connection.close();
  service['db'].close();
}

const actions: Record<string, () => Promise<void>> = {
  'duplicate-delivery': duplicateDelivery,
  'crash-before-offset': crashBeforeOffset,
  'duplicate-payment': duplicatePayment,
  'notification-retry': notificationRetry,
  'out-of-order': outOfOrder,
};

await actions[scenario]!();
