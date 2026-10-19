import { describe, expect, it } from 'vitest';
import { decideTransition } from '../src/domain/state-machine.js';
import { cancelled, confirmed } from './helpers.js';

describe('invoice state machine', () => {
  it.each([
    ['OPEN', confirmed(), 'APPLY'],
    ['PAID', confirmed(), 'DUPLICATE'],
    ['CANCELLED', confirmed(), 'REJECT'],
    ['OPEN', cancelled(), 'DEFER'],
    ['PAID', cancelled(), 'APPLY'],
    ['CANCELLED', cancelled(), 'DUPLICATE'],
  ] as const)('%s + $eventType -> %s', (status, event, expected) => {
    expect(decideTransition(status, event)).toBe(expected);
  });
});
