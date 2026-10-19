import type { FinancialEvent, PaymentCancelled, PaymentConfirmed } from '../src/domain/events.js';

export function confirmed(overrides: Partial<PaymentConfirmed> = {}): PaymentConfirmed {
  return {
    schemaVersion: 1,
    eventId: 'evt-confirmed',
    eventType: 'PaymentConfirmed',
    invoiceId: 'inv-456',
    paymentId: 'pay-789',
    amount: 15_000,
    currency: 'BRL',
    occurredAt: '2026-10-19T12:00:00.000Z',
    ...overrides,
  };
}

export function cancelled(overrides: Partial<PaymentCancelled> = {}): PaymentCancelled {
  return {
    schemaVersion: 1,
    eventId: 'evt-cancelled',
    eventType: 'PaymentCancelled',
    invoiceId: 'inv-456',
    paymentId: 'pay-789',
    cancelsPaymentId: 'pay-789',
    amount: 15_000,
    currency: 'BRL',
    occurredAt: '2026-10-19T11:00:00.000Z',
    ...overrides,
  };
}

export function eventFor(type: 'confirmed' | 'cancelled'): FinancialEvent {
  return type === 'confirmed' ? confirmed() : cancelled();
}
