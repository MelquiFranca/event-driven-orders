import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { PaymentService } from '../src/application/payment-service.js';
import { openDatabase, type SqliteDatabase } from '../src/infra/database.js';
import { cancelled, confirmed } from './helpers.js';

describe('PaymentService with real SQLite transactions', () => {
  let db: SqliteDatabase;
  let service: PaymentService;

  beforeEach(() => {
    db = openDatabase(':memory:');
    service = new PaymentService(db);
    service.createInvoice({ invoiceId: 'inv-456', amount: 15_000, currency: 'BRL' });
  });

  afterEach(() => db.close());

  it('should_record_payment_once_even_when_message_is_delivered_multiple_times', () => {
    const event = confirmed();
    const outcomes = Array.from({ length: 5 }, () => service.process(event).outcome);
    expect(outcomes).toEqual(['APPLIED', 'DUPLICATE', 'DUPLICATE', 'DUPLICATE', 'DUPLICATE']);
    expect(service.getInvoice('inv-456')?.ledgerEntries).toHaveLength(1);
  });

  it('treats a new eventId for the same payment as a business duplicate', () => {
    service.process(confirmed());
    const result = service.process(confirmed({ eventId: 'evt-confirmed-again' }));
    expect(result.outcome).toBe('DUPLICATE');
    expect(service.getInvoice('inv-456')?.ledgerEntries).toHaveLength(1);
  });

  it('rejects a conflicting paymentId without changing either invoice', () => {
    service.createInvoice({ invoiceId: 'inv-other', amount: 15_000, currency: 'BRL' });
    service.process(confirmed());
    const result = service.process(confirmed({ eventId: 'evt-conflict', invoiceId: 'inv-other' }));
    expect(result.outcome).toBe('REJECTED');
    expect(service.getInvoice('inv-other')?.status).toBe('OPEN');
  });

  it('rolls back every write when processing fails before commit', () => {
    expect(() => service.process(confirmed(), { beforeCommit: () => { throw new Error('injected'); } })).toThrow('injected');
    expect(service.getInvoice('inv-456')?.status).toBe('OPEN');
    expect(service.getInvoice('inv-456')?.ledgerEntries).toHaveLength(0);
    expect(db.prepare('SELECT COUNT(*) AS count FROM processed_events').get()).toEqual({ count: 0 });
    expect(db.prepare('SELECT COUNT(*) AS count FROM notification_outbox').get()).toEqual({ count: 0 });
  });

  it('retains an early cancellation and atomically reapplies it after confirmation', () => {
    expect(service.process(cancelled()).outcome).toBe('DEFERRED');
    const result = service.process(confirmed());
    const invoice = service.getInvoice('inv-456');
    expect(result.reappliedPendingEventIds).toEqual(['evt-cancelled']);
    expect(invoice?.status).toBe('CANCELLED');
    expect(invoice?.ledgerEntries.map((entry) => entry.entryType)).toEqual(['PAYMENT', 'REVERSAL']);
    expect(invoice?.pendingEvents).toEqual([
      expect.objectContaining({ eventId: 'evt-cancelled', status: 'APPLIED' }),
    ]);
    expect(db.prepare('SELECT COUNT(*) AS count FROM notification_outbox').get()).toEqual({ count: 2 });
  });

  it('does not duplicate a reversal', () => {
    service.process(confirmed());
    service.process(cancelled());
    expect(service.process(cancelled({ eventId: 'evt-cancelled-again' })).outcome).toBe('DUPLICATE');
    expect(service.getInvoice('inv-456')?.ledgerEntries).toHaveLength(2);
  });

  it('drains multiple early cancellations without duplicating the reversal', () => {
    service.process(cancelled());
    service.process(cancelled({ eventId: 'evt-cancelled-again' }));
    service.process(confirmed());
    expect(service.getInvoice('inv-456')?.ledgerEntries).toHaveLength(2);
    expect(service.getInvoice('inv-456')?.pendingEvents.every((event) => event.status === 'APPLIED')).toBe(true);
    expect(db.prepare('SELECT outcome FROM processed_events WHERE event_id = ?').get('evt-cancelled-again'))
      .toEqual({ outcome: 'DUPLICATE' });
  });

  it('creates the outbox item in the same transaction as the payment', () => {
    service.process(confirmed());
    expect(db.prepare('SELECT event_id, status FROM notification_outbox').get()).toEqual({
      event_id: 'evt-confirmed',
      status: 'PENDING',
    });
  });
});
