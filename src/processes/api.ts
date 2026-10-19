import express from 'express';
import { z } from 'zod';
import { config } from '../config.js';
import { financialEventSchema, paymentCancelledSchema, paymentConfirmedSchema } from '../domain/events.js';
import { openDatabase } from '../infra/database.js';
import { createEventProducer } from '../infra/kafka.js';
import { connectRabbit } from '../infra/rabbit.js';
import { PaymentService } from '../application/payment-service.js';
import { logger } from '../logger.js';

const invoiceSchema = z.object({
  invoiceId: z.string().min(1),
  amount: z.number().int().positive(),
  currency: z.string().regex(/^[A-Z]{3}$/),
});

const db = openDatabase(config.databasePath);
const service = new PaymentService(db);
const kafka = await createEventProducer(config.kafkaBrokers, config.kafkaTopic);
let rabbitHealthy = false;
const rabbit = await connectRabbit(config.rabbitUrl).then((connection) => {
  rabbitHealthy = true;
  connection.on('close', () => { rabbitHealthy = false; });
  return connection;
}).catch((error: unknown) => {
  logger.warn({ error }, 'RabbitMQ unavailable during API startup');
  return null;
});

const app = express();
app.use(express.json());

app.post('/invoices', (request, response) => {
  const parsed = invoiceSchema.safeParse(request.body);
  if (!parsed.success) return response.status(400).json({ error: parsed.error.flatten() });
  try {
    return response.status(201).json(service.createInvoice(parsed.data));
  } catch (error) {
    if (error instanceof Error && error.message.includes('UNIQUE constraint failed')) {
      return response.status(409).json({ error: 'invoice already exists' });
    }
    throw error;
  }
});

app.get('/invoices/:invoiceId', (request, response) => {
  const invoice = service.getInvoice(request.params.invoiceId);
  return invoice ? response.json(invoice) : response.status(404).json({ error: 'invoice not found' });
});

async function publishWith(schema: typeof paymentConfirmedSchema | typeof paymentCancelledSchema, body: unknown) {
  const parsed = schema.parse(body);
  const event = financialEventSchema.parse(parsed);
  await kafka.publish(event);
  return event;
}

app.post('/events/payment-confirmed', async (request, response) => {
  try {
    return response.status(202).json(await publishWith(paymentConfirmedSchema, request.body));
  } catch (error) {
    if (error instanceof z.ZodError) return response.status(400).json({ error: error.flatten() });
    throw error;
  }
});

app.post('/events/payment-cancelled', async (request, response) => {
  try {
    return response.status(202).json(await publishWith(paymentCancelledSchema, request.body));
  } catch (error) {
    if (error instanceof z.ZodError) return response.status(400).json({ error: error.flatten() });
    throw error;
  }
});

app.get('/health', (_request, response) => {
  const sqliteHealthy = db.prepare('SELECT 1 AS ok').get() !== undefined;
  const status = sqliteHealthy && rabbitHealthy ? 200 : 503;
  response.status(status).json({
    status: status === 200 ? 'ok' : 'degraded',
    sqlite: sqliteHealthy,
    kafka: true,
    rabbitmq: rabbitHealthy,
  });
});

app.use((error: unknown, _request: express.Request, response: express.Response, _next: express.NextFunction) => {
  logger.error({ error }, 'unhandled API error');
  response.status(500).json({ error: 'internal server error' });
});

const server = app.listen(config.port, () => logger.info({ port: config.port }, 'API listening'));

async function shutdown(): Promise<void> {
  server.close();
  await kafka.producer.disconnect();
  if (rabbit) await rabbit.close();
  db.close();
}
process.once('SIGINT', () => void shutdown());
process.once('SIGTERM', () => void shutdown());
