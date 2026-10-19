import type { FinancialEvent } from './events.js';

export type InvoiceStatus = 'OPEN' | 'PAID' | 'CANCELLED';
export type StateDecision = 'APPLY' | 'DEFER' | 'DUPLICATE' | 'REJECT';

export function decideTransition(status: InvoiceStatus, event: FinancialEvent): StateDecision {
  if (event.eventType === 'PaymentConfirmed') {
    if (status === 'OPEN') return 'APPLY';
    if (status === 'PAID') return 'DUPLICATE';
    return 'REJECT';
  }

  if (status === 'OPEN') return 'DEFER';
  if (status === 'PAID') return 'APPLY';
  return 'DUPLICATE';
}
