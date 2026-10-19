import { config } from '../config.js';
import { openDatabase } from '../infra/database.js';
import {
  classifyNotificationFailure,
  configureRabbit,
  connectRabbit,
  parseNotification,
  processNotification,
  publishConfirmed,
  rabbitNames,
} from '../infra/rabbit.js';
import { logger } from '../logger.js';

const db = openDatabase(config.databasePath);
const connection = await connectRabbit(config.rabbitUrl);
const channel = await connection.createConfirmChannel();
await configureRabbit(channel, config.rabbitRetryDelayMs);
await channel.prefetch(1);

await channel.consume(rabbitNames.queue, async (message) => {
  if (!message) return;
  const attempt = Number(message.properties.headers?.attempt ?? 0);
  let task;
  try {
    task = parseNotification(message);
    const result = processNotification(db, task, attempt);
    channel.ack(message);
    logger.info({ taskId: task.taskId, attempt, result }, 'notification handled');
  } catch (error) {
    if (!task) {
      channel.nack(message, false, false);
      logger.error({ error }, 'invalid notification discarded');
      return;
    }
    const action = classifyNotificationFailure(error, attempt, config.rabbitMaxAttempts);
    const routing = action === 'RETRY' ? rabbitNames.retryRouting : rabbitNames.deadRouting;
    await publishConfirmed(channel, routing, task, { attempt: attempt + 1, error: String(error) });
    channel.ack(message);
    logger.warn({ taskId: task.taskId, attempt, action, error }, 'notification failed');
  }
}, { noAck: false });

async function shutdown(): Promise<void> {
  await channel.close();
  await connection.close();
  db.close();
}
process.once('SIGINT', () => void shutdown());
process.once('SIGTERM', () => void shutdown());
