import { describe, expect, it } from 'vitest';
import { classifyNotificationFailure, PermanentNotificationError, TemporaryNotificationError } from '../src/infra/rabbit.js';

describe('notification failure policy', () => {
  it('retries a temporary failure while attempts remain', () => {
    expect(classifyNotificationFailure(new TemporaryNotificationError(), 0, 3)).toBe('RETRY');
  });

  it('dead-letters exhausted and permanent failures', () => {
    expect(classifyNotificationFailure(new TemporaryNotificationError(), 2, 3)).toBe('DEAD');
    expect(classifyNotificationFailure(new PermanentNotificationError(), 0, 3)).toBe('DEAD');
  });
});
