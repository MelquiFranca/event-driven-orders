import { describe, expect, it } from 'vitest';
import { financialEventSchema } from '../src/domain/events.js';
import { confirmed } from './helpers.js';

describe('financial event contract', () => {
  it('accepts integer cents', () => {
    expect(financialEventSchema.parse(confirmed()).amount).toBe(15_000);
  });

  it.each([150.5, 0, -1])('rejects invalid amount %s', (amount) => {
    expect(financialEventSchema.safeParse(confirmed({ amount })).success).toBe(false);
  });

  it('rejects invalid currency and timestamp', () => {
    expect(financialEventSchema.safeParse(confirmed({ currency: 'brl' })).success).toBe(false);
    expect(financialEventSchema.safeParse(confirmed({ occurredAt: 'yesterday' })).success).toBe(false);
  });
});
