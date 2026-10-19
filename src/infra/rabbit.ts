import amqp, { type ChannelModel, type ConfirmChannel, type ConsumeMessage } from 'amqplib';
import type { SqliteDatabase } from './database.js';

export const rabbitNames = {
  exchange: 'notifications.exchange',
  queue: 'notifications.main',
  retryQueue: 'notifications.retry',
  deadLetterQueue: 'notifications.dlq',
  routing: 'notification',
  retryRouting: 'notification.retry',
  deadRouting: 'notification.dead',
} as const;

export interface NotificationTask {
  taskId: string;
  eventId: string;
  invoiceId: string;
  paymentId: string;
  notificationType: string;
  simulateFailure?: 'temporary-once' | 'temporary-always' | 'permanent';
}

export class TemporaryNotificationError extends Error {}
export class PermanentNotificationError extends Error {}

export async function connectRabbit(url: string): Promise<ChannelModel> {
  return amqp.connect(url);
}

export async function configureRabbit(channel: ConfirmChannel, retryDelayMs: number): Promise<void> {
  await channel.assertExchange(rabbitNames.exchange, 'direct', { durable: true });
  await channel.assertQueue(rabbitNames.queue, { durable: true });
  await channel.bindQueue(rabbitNames.queue, rabbitNames.exchange, rabbitNames.routing);

  await channel.assertQueue(rabbitNames.retryQueue, {
    durable: true,
    arguments: {
      'x-message-ttl': retryDelayMs,
      'x-dead-letter-exchange': rabbitNames.exchange,
      'x-dead-letter-routing-key': rabbitNames.routing,
    },
  });
  await channel.bindQueue(rabbitNames.retryQueue, rabbitNames.exchange, rabbitNames.retryRouting);

  await channel.assertQueue(rabbitNames.deadLetterQueue, { durable: true });
  await channel.bindQueue(rabbitNames.deadLetterQueue, rabbitNames.exchange, rabbitNames.deadRouting);
}

export async function publishConfirmed(
  channel: ConfirmChannel,
  routingKey: string,
  task: NotificationTask,
  headers: Record<string, unknown> = {},
): Promise<void> {
  channel.publish(rabbitNames.exchange, routingKey, Buffer.from(JSON.stringify(task)), {
    persistent: true,
    contentType: 'application/json',
    messageId: task.taskId,
    headers,
  });
  await channel.waitForConfirms();
}

export function parseNotification(message: ConsumeMessage): NotificationTask {
  return JSON.parse(message.content.toString()) as NotificationTask;
}

export function processNotification(db: SqliteDatabase, task: NotificationTask, attempt: number): 'COMPLETED' | 'DUPLICATE' {
  const completed = db.prepare('SELECT 1 FROM completed_notifications WHERE task_id = ?').get(task.taskId);
  if (completed) return 'DUPLICATE';
  if (task.simulateFailure === 'permanent') throw new PermanentNotificationError('simulated permanent failure');
  if (task.simulateFailure === 'temporary-once' && attempt === 0) {
    throw new TemporaryNotificationError('simulated temporary failure');
  }
  if (task.simulateFailure === 'temporary-always') {
    throw new TemporaryNotificationError('simulated persistent temporary failure');
  }
  db.prepare('INSERT INTO completed_notifications (task_id) VALUES (?)').run(task.taskId);
  return 'COMPLETED';
}

export function classifyNotificationFailure(error: unknown, attempt: number, maxAttempts: number): 'RETRY' | 'DEAD' {
  if (error instanceof PermanentNotificationError) return 'DEAD';
  return attempt + 1 < maxAttempts ? 'RETRY' : 'DEAD';
}
