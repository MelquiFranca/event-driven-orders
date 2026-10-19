import { randomUUID } from 'node:crypto';
import type { SqliteDatabase } from '../infra/database.js';
import type { FinancialEvent, ProcessingOutcome } from '../domain/events.js';
import type { InvoiceStatus } from '../domain/state-machine.js';
import { decideTransition } from '../domain/state-machine.js';

interface InvoiceRow {
  invoice_id: string;
  amount: number;
  currency: string;
  status: InvoiceStatus;
}

interface LedgerRow {
  id: number;
  event_id: string;
  invoice_id: string;
  payment_id: string;
  entry_type: 'PAYMENT' | 'REVERSAL';
  amount: number;
  currency: string;
  created_at: string;
}

interface PendingRow {
  event_id: string;
  event_json: string;
}

export interface ProcessingResult {
  eventId: string;
  outcome: ProcessingOutcome;
  reason?: string;
  reappliedPendingEventIds?: string[];
}

export interface InvoiceView {
  invoiceId: string;
  amount: number;
  currency: string;
  status: InvoiceStatus;
  ledgerEntries: Array<{
    eventId: string;
    paymentId: string;
    entryType: 'PAYMENT' | 'REVERSAL';
    amount: number;
    currency: string;
    createdAt: string;
  }>;
  pendingEvents: Array<{ eventId: string; paymentId: string; status: string }>;
}

export class PaymentService {
  constructor(private readonly db: SqliteDatabase) {}

  createInvoice(input: { invoiceId: string; amount: number; currency: string }): InvoiceView {
    this.db.prepare(
      'INSERT INTO invoices (invoice_id, amount, currency) VALUES (?, ?, ?)',
    ).run(input.invoiceId, input.amount, input.currency);
    return this.getInvoice(input.invoiceId)!;
  }

  getInvoice(invoiceId: string): InvoiceView | null {
    const invoice = this.db.prepare('SELECT * FROM invoices WHERE invoice_id = ?').get(invoiceId) as InvoiceRow | undefined;
    if (!invoice) return null;
    const ledger = this.db.prepare(
      'SELECT * FROM ledger_entries WHERE invoice_id = ? ORDER BY id',
    ).all(invoiceId) as LedgerRow[];
    const pending = this.db.prepare(
      'SELECT event_id, payment_id, status FROM pending_events WHERE invoice_id = ? ORDER BY created_at',
    ).all(invoiceId) as Array<{ event_id: string; payment_id: string; status: string }>;
    return {
      invoiceId: invoice.invoice_id,
      amount: invoice.amount,
      currency: invoice.currency,
      status: invoice.status,
      ledgerEntries: ledger.map((entry) => ({
        eventId: entry.event_id,
        paymentId: entry.payment_id,
        entryType: entry.entry_type,
        amount: entry.amount,
        currency: entry.currency,
        createdAt: entry.created_at,
      })),
      pendingEvents: pending.map((event) => ({
        eventId: event.event_id,
        paymentId: event.payment_id,
        status: event.status,
      })),
    };
  }

  process(event: FinancialEvent, options: { beforeCommit?: () => void } = {}): ProcessingResult {
    return this.db.transaction(() => {
      const prior = this.db.prepare('SELECT outcome, reason FROM processed_events WHERE event_id = ?').get(event.eventId) as
        | { outcome: ProcessingOutcome; reason: string | null }
        | undefined;
      if (prior) return this.result(event.eventId, 'DUPLICATE', 'eventId already processed');

      this.db.prepare(
        `INSERT INTO processed_events (event_id, event_type, outcome, reason)
         VALUES (?, ?, 'REJECTED', 'processing started')`,
      ).run(event.eventId, event.eventType);

      const invoice = this.db.prepare('SELECT * FROM invoices WHERE invoice_id = ?').get(event.invoiceId) as InvoiceRow | undefined;
      if (!invoice) return this.finish(event.eventId, 'REJECTED', 'invoice not found', options);
      if (invoice.amount !== event.amount || invoice.currency !== event.currency) {
        return this.finish(event.eventId, 'REJECTED', 'event amount or currency differs from invoice', options);
      }

      const outcome = event.eventType === 'PaymentConfirmed'
        ? this.confirmPayment(invoice, event)
        : this.cancelPayment(invoice, event);
      options.beforeCommit?.();
      return outcome;
    })();
  }

  private confirmPayment(invoice: InvoiceRow, event: FinancialEvent): ProcessingResult {
    if (event.eventType !== 'PaymentConfirmed') throw new Error('unreachable');
    const existing = this.findLedger(event.paymentId, 'PAYMENT');
    if (existing) {
      const matching = existing.invoice_id === event.invoiceId
        && existing.amount === event.amount
        && existing.currency === event.currency;
      return this.finish(
        event.eventId,
        matching ? 'DUPLICATE' : 'REJECTED',
        matching ? 'paymentId already accounted with matching data' : 'paymentId conflicts with existing payment',
      );
    }

    const decision = decideTransition(invoice.status, event);
    if (decision !== 'APPLY') return this.finish(event.eventId, 'REJECTED', `cannot confirm invoice in ${invoice.status}`);

    this.insertLedger(event, 'PAYMENT');
    this.updateInvoice(event.invoiceId, 'PAID');
    this.addOutbox(event, 'PAYMENT_CONFIRMED');
    this.updateProcessed(event.eventId, 'APPLIED', null);

    const pending = this.db.prepare(
      `SELECT event_id, event_json FROM pending_events
       WHERE payment_id = ? AND status = 'PENDING' ORDER BY created_at, event_id`,
    ).all(event.paymentId) as PendingRow[];
    const reapplied: string[] = [];
    for (const [index, row] of pending.entries()) {
      const cancellation = JSON.parse(row.event_json) as FinancialEvent;
      if (cancellation.eventType !== 'PaymentCancelled') continue;
      if (index === 0) {
        this.insertLedger(cancellation, 'REVERSAL');
        this.updateInvoice(cancellation.invoiceId, 'CANCELLED');
        this.addOutbox(cancellation, 'PAYMENT_CANCELLED');
        this.updateProcessed(cancellation.eventId, 'APPLIED', 'reapplied after payment confirmation');
      } else {
        this.updateProcessed(cancellation.eventId, 'DUPLICATE', 'payment already reversed by an earlier cancellation');
      }
      this.db.prepare(
        `UPDATE pending_events SET status = 'APPLIED', applied_at = CURRENT_TIMESTAMP WHERE event_id = ?`,
      ).run(cancellation.eventId);
      reapplied.push(cancellation.eventId);
    }
    return { eventId: event.eventId, outcome: 'APPLIED', reappliedPendingEventIds: reapplied };
  }

  private cancelPayment(invoice: InvoiceRow, event: FinancialEvent): ProcessingResult {
    if (event.eventType !== 'PaymentCancelled') throw new Error('unreachable');
    const reversal = this.findLedger(event.paymentId, 'REVERSAL');
    if (reversal) return this.finish(event.eventId, 'DUPLICATE', 'payment already reversed');

    const payment = this.findLedger(event.paymentId, 'PAYMENT');
    if (!payment && decideTransition(invoice.status, event) === 'DEFER') {
      this.db.prepare(
        `INSERT INTO pending_events (event_id, invoice_id, payment_id, event_json)
         VALUES (?, ?, ?, ?)`,
      ).run(event.eventId, event.invoiceId, event.paymentId, JSON.stringify(event));
      return this.finish(event.eventId, 'DEFERRED', 'payment confirmation has not arrived');
    }
    if (!payment) return this.finish(event.eventId, 'REJECTED', 'payment does not exist');
    if (payment.invoice_id !== event.invoiceId || payment.amount !== event.amount || payment.currency !== event.currency) {
      return this.finish(event.eventId, 'REJECTED', 'cancellation conflicts with payment');
    }
    if (decideTransition(invoice.status, event) !== 'APPLY') {
      return this.finish(event.eventId, 'REJECTED', `cannot cancel invoice in ${invoice.status}`);
    }

    this.insertLedger(event, 'REVERSAL');
    this.updateInvoice(event.invoiceId, 'CANCELLED');
    this.addOutbox(event, 'PAYMENT_CANCELLED');
    return this.finish(event.eventId, 'APPLIED', null);
  }

  private findLedger(paymentId: string, type: 'PAYMENT' | 'REVERSAL'): LedgerRow | undefined {
    return this.db.prepare(
      'SELECT * FROM ledger_entries WHERE payment_id = ? AND entry_type = ?',
    ).get(paymentId, type) as LedgerRow | undefined;
  }

  private insertLedger(event: FinancialEvent, type: 'PAYMENT' | 'REVERSAL'): void {
    this.db.prepare(
      `INSERT INTO ledger_entries (event_id, invoice_id, payment_id, entry_type, amount, currency)
       VALUES (?, ?, ?, ?, ?, ?)`,
    ).run(event.eventId, event.invoiceId, event.paymentId, type, event.amount, event.currency);
  }

  private updateInvoice(invoiceId: string, status: InvoiceStatus): void {
    this.db.prepare(
      'UPDATE invoices SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE invoice_id = ?',
    ).run(status, invoiceId);
  }

  private addOutbox(event: FinancialEvent, notificationType: string): void {
    this.db.prepare(
      `INSERT INTO notification_outbox
       (task_id, event_id, invoice_id, notification_type, payload_json)
       VALUES (?, ?, ?, ?, ?)`,
    ).run(randomUUID(), event.eventId, event.invoiceId, notificationType, JSON.stringify({
      eventId: event.eventId,
      invoiceId: event.invoiceId,
      paymentId: event.paymentId,
      notificationType,
    }));
  }

  private updateProcessed(eventId: string, outcome: ProcessingOutcome, reason: string | null): void {
    this.db.prepare('UPDATE processed_events SET outcome = ?, reason = ? WHERE event_id = ?')
      .run(outcome, reason, eventId);
  }

  private finish(
    eventId: string,
    outcome: ProcessingOutcome,
    reason: string | null,
    options: { beforeCommit?: () => void } = {},
  ): ProcessingResult {
    this.updateProcessed(eventId, outcome, reason);
    options.beforeCommit?.();
    return this.result(eventId, outcome, reason ?? undefined);
  }

  private result(eventId: string, outcome: ProcessingOutcome, reason?: string): ProcessingResult {
    return reason ? { eventId, outcome, reason } : { eventId, outcome };
  }
}
