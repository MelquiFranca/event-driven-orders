import { Kafka, logLevel, type Consumer, type Producer } from 'kafkajs';
import type { FinancialEvent } from '../domain/events.js';

export function createKafka(brokers: string[]) {
  return new Kafka({ clientId: 'duplicate-payment-poc', brokers, logLevel: logLevel.NOTHING });
}

export async function createEventProducer(brokers: string[], topic: string): Promise<{
  producer: Producer;
  publish: (event: FinancialEvent) => Promise<void>;
}> {
  const producer = createKafka(brokers).producer();
  await producer.connect();
  return {
    producer,
    publish: async (event) => {
      await producer.send({
        topic,
        messages: [{ key: event.invoiceId, value: JSON.stringify(event) }],
      });
    },
  };
}

export async function createEventConsumer(
  brokers: string[],
  groupId: string,
  topic: string,
): Promise<Consumer> {
  const consumer = createKafka(brokers).consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  return consumer;
}
