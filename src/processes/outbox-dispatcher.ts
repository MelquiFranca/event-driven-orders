import { config } from '../config.js';
import { openDatabase } from '../infra/database.js';
import { configureRabbit, connectRabbit, publishConfirmed, rabbitNames, type NotificationTask } from '../infra/rabbit.js';
import { logger } from '../logger.js';

interface OutboxRow { task_id: string; payload_json: string }

const db = openDatabase(config.databasePath);
const connection = await connectRabbit(config.rabbitUrl);
const channel = await connection.createConfirmChannel();
await configureRabbit(channel, config.rabbitRetryDelayMs);
let timer: NodeJS.Timeout | undefined;

async function dispatchBatch(): Promise<void> {
  const rows = db.prepare(
    `SELECT task_id, payload_json FROM notification_outbox
     WHERE status = 'PENDING' ORDER BY created_at LIMIT 20`,
  ).all() as OutboxRow[];
  for (const row of rows) {
    const payload = JSON.parse(row.payload_json) as Omit<NotificationTask, 'taskId'>;
    await publishConfirmed(channel, rabbitNames.routing, { taskId: row.task_id, ...payload });
    db.prepare(
      `UPDATE notification_outbox SET status = 'DISPATCHED', dispatched_at = CURRENT_TIMESTAMP
       WHERE task_id = ? AND status = 'PENDING'`,
    ).run(row.task_id);
    logger.info({ taskId: row.task_id }, 'outbox task dispatched');
  }
}

async function tick(): Promise<void> {
  try {
    await dispatchBatch();
  } catch (error) {
    logger.error({ error }, 'outbox dispatch failed; task remains pending');
  }
  timer = setTimeout(() => void tick(), config.outboxPollMs);
}

void tick();

async function shutdown(): Promise<void> {
  if (timer) clearTimeout(timer);
  await channel.close();
  await connection.close();
  db.close();
}
process.once('SIGINT', () => void shutdown());
process.once('SIGTERM', () => void shutdown());
