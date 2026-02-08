/**
 * Tracks in-flight gateway startup and last failure for a single Worker isolate.
 * Used to: (1) deduplicate concurrent startup attempts, (2) surface failure to /api/status and loading page.
 */

export interface StartupFailure {
  message: string;
  hint?: string;
  at: number; // timestamp
}

let inFlightStartup: Promise<unknown> | null = null;
let lastFailure: StartupFailure | null = null;

export function getStartupFailure(): StartupFailure | null {
  return lastFailure;
}

export function clearStartupFailure(): void {
  lastFailure = null;
}

export function setStartupFailure(message: string, hint?: string): void {
  lastFailure = { message, hint, at: Date.now() };
}

export function isStartupInProgress(): boolean {
  return inFlightStartup !== null;
}

/**
 * Run a startup function (e.g. ensureMoltbotGateway) with a single-flight lock.
 * If the same startup is already in progress, returns the existing promise.
 * On rejection, stores the error for /api/status and rethrows.
 */
export async function withStartupLock<T>(fn: () => Promise<T>): Promise<T> {
  if (inFlightStartup) {
    return inFlightStartup as Promise<T>;
  }
  clearStartupFailure();
  const promise = fn().then(
    (result) => {
      inFlightStartup = null;
      return result;
    },
    (err: unknown) => {
      inFlightStartup = null;
      const message = err instanceof Error ? err.message : String(err);
      let hint = 'Check worker logs: npx wrangler tail';
      if (message.includes('ANTHROPIC_API_KEY') || message.includes('API key')) {
        hint = 'Set ANTHROPIC_API_KEY: npx wrangler secret put ANTHROPIC_API_KEY';
      } else if (message.includes('heap out of memory') || message.includes('OOM')) {
        hint = 'Gateway ran out of memory. Try again.';
      }
      setStartupFailure(message, hint);
      throw err;
    },
  );
  inFlightStartup = promise;
  return promise;
}

/** For tests: reset state between runs */
export function _resetStartupState(): void {
  inFlightStartup = null;
  lastFailure = null;
}
