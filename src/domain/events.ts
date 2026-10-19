import { z } from 'zod';

const baseEvent = z.object({
  schemaVersion: z.literal(1).default(1),
  eventId: z.string().min(1),
  invoiceId: z.string().min(1),
  paymentId: z.string().min(1),
  amount: z.number().int().positive(),
  currency: z.string().regex(/^[A-Z]{3}$/),
  occurredAt: z.iso.datetime(),
});

export const paymentConfirmedSchema = baseEvent.extend({
  eventType: z.literal('PaymentConfirmed'),
});

export const paymentCancelledSchema = baseEvent.extend({
  eventType: z.literal('PaymentCancelled'),
  cancelsPaymentId: z.string().min(1),
}).refine((event) => event.paymentId === event.cancelsPaymentId, {
  message: 'paymentId must match cancelsPaymentId',
  path: ['cancelsPaymentId'],
});

export const financialEventSchema = z.discriminatedUnion('eventType', [
  paymentConfirmedSchema,
  paymentCancelledSchema,
]);

export type PaymentConfirmed = z.infer<typeof paymentConfirmedSchema>;
export type PaymentCancelled = z.infer<typeof paymentCancelledSchema>;
export type FinancialEvent = z.infer<typeof financialEventSchema>;
export type ProcessingOutcome = 'APPLIED' | 'DUPLICATE' | 'DEFERRED' | 'REJECTED';
