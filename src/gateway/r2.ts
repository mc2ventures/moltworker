import type { Sandbox } from '@cloudflare/sandbox';
import type { MoltbotEnv } from '../types';
import { R2_MOUNT_PATH, getR2BucketName } from '../config';

/** Consistent log prefix for all R2 mount operations */
const LOG_PREFIX = '[R2 Mount]';

/**
 * In-flight mount promise used to deduplicate concurrent mount attempts.
 */
let inflightMount: Promise<boolean> | null = null;

/** Wait for a sandbox process to finish (up to ~2 s by default). */
async function waitForProcess(
  proc: { status: string },
  timeoutMs = 2000,
): Promise<void> {
  const interval = 200;
  const maxAttempts = Math.ceil(timeoutMs / interval);
  let attempts = 0;
  while (proc.status === 'running' && attempts < maxAttempts) {
    // eslint-disable-next-line no-await-in-loop -- intentional sequential polling
    await new Promise((r) => setTimeout(r, interval));
    attempts++;
  }
}

/**
 * Check if R2 is already mounted by looking at the mount table.
 */
async function isR2Mounted(sandbox: Sandbox, label?: string): Promise<boolean> {
  try {
    const proc = await sandbox.startProcess(`mount | grep "s3fs on ${R2_MOUNT_PATH}"`);
    await waitForProcess(proc);
    const logs = await proc.getLogs();
    const mounted = !!(logs.stdout && logs.stdout.includes('s3fs'));
    if (label) {
      console.log(`${LOG_PREFIX} Mount check (${label}): ${mounted ? 'MOUNTED' : 'not mounted'}`);
    }
    return mounted;
  } catch (err) {
    if (label) {
      console.log(`${LOG_PREFIX} Mount check (${label}): error -`, err instanceof Error ? err.message : err);
    }
    return false;
  }
}

/**
 * Mount R2 bucket for persistent storage.
 *
 * Follows the [official Sandbox persistent-storage tutorial](https://developers.cloudflare.com/sandbox/tutorials/persistent-storage/):
 * - Single mountBucket() call with endpoint; SDK auto-detects AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY from Worker secrets.
 * - If explicit credentials are set (R2_* or AWS_*), we pass them in options as a fallback.
 *
 * Requires production deployment (FUSE); does not work with wrangler dev.
 *
 * @param sandbox - The sandbox instance
 * @param env - Worker environment bindings
 * @returns true if mounted successfully, false otherwise
 */
export async function mountR2Storage(sandbox: Sandbox, env: MoltbotEnv): Promise<boolean> {
  if (!env.CF_ACCOUNT_ID) {
    console.log(`${LOG_PREFIX} Skipped — CF_ACCOUNT_ID not set`);
    return false;
  }

  if (inflightMount) {
    console.log(`${LOG_PREFIX} Waiting for in-flight mount attempt...`);
    return inflightMount;
  }

  inflightMount = doMount(sandbox, env);
  try {
    return await inflightMount;
  } finally {
    inflightMount = null;
  }
}

/**
 * Internal mount: single mountBucket() call to avoid duplicate passwd entries.
 *
 * The SDK/s3fs writes credentials to a passwd file. Calling mountBucket twice
 * (e.g. once without creds, once with) appends two entries for the same bucket
 * and causes "multiple entries for the same bucket(default) in the passwd file".
 * So we call mountBucket exactly once: with credentials if we have them,
 * otherwise endpoint only (SDK auto-detects AWS_* from Worker secrets).
 */
async function doMount(sandbox: Sandbox, env: MoltbotEnv): Promise<boolean> {
  const bucketName = getR2BucketName(env);
  const endpoint = `https://${env.CF_ACCOUNT_ID}.r2.cloudflarestorage.com`;
  const hasExplicitCreds =
    !!(env.AWS_ACCESS_KEY_ID && env.AWS_SECRET_ACCESS_KEY) ||
    !!(env.R2_ACCESS_KEY_ID && env.R2_SECRET_ACCESS_KEY);

  console.log(
    `${LOG_PREFIX} Starting — bucket=${bucketName}, path=${R2_MOUNT_PATH}, endpoint=${endpoint}, explicitCreds=${hasExplicitCreds}`,
  );
  const startTime = Date.now();

  if (await isR2Mounted(sandbox, 'fast-path')) {
    console.log(`${LOG_PREFIX} Already mounted — no action needed (${Date.now() - startTime}ms)`);
    return true;
  }

  // Single mountBucket call: with creds if set, else endpoint only (SDK auto-detects AWS_*)
  const mountOptions: { endpoint: string; credentials?: { accessKeyId: string; secretAccessKey: string } } = {
    endpoint,
  };
  if (hasExplicitCreds) {
    mountOptions.credentials = {
      accessKeyId: env.AWS_ACCESS_KEY_ID ?? env.R2_ACCESS_KEY_ID!,
      secretAccessKey: env.AWS_SECRET_ACCESS_KEY ?? env.R2_SECRET_ACCESS_KEY!,
    };
    console.log(`${LOG_PREFIX} Calling mountBucket once (with explicit credentials)`);
  } else {
    console.log(`${LOG_PREFIX} Calling mountBucket once (SDK will use AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY from secrets)`);
  }

  try {
    await sandbox.mountBucket(bucketName, R2_MOUNT_PATH, mountOptions);
    if (await isR2Mounted(sandbox, 'post-mount')) {
      console.log(`${LOG_PREFIX} SUCCESS (${Date.now() - startTime}ms)`);
      return true;
    }
    console.log(`${LOG_PREFIX} mountBucket returned OK but mount not detected in mount table`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.log(`${LOG_PREFIX} mountBucket threw: ${msg.slice(0, 250)} (${Date.now() - startTime}ms)`);
    if (msg.includes('fuse') || msg.includes('modprobe') || msg.includes('FUSE')) {
      console.error(
        `${LOG_PREFIX} R2 mount requires FUSE; only works in production (wrangler deploy). Not available in wrangler dev.`,
      );
    }
    if (!hasExplicitCreds && (msg.includes('Credentials') || msg.includes('credentials'))) {
      console.log(`${LOG_PREFIX} Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY via: npx wrangler secret put AWS_ACCESS_KEY_ID`);
    }
  }

  if (await isR2Mounted(sandbox, 'final-check')) {
    console.log(`${LOG_PREFIX} SUCCESS — mount detected on final check (${Date.now() - startTime}ms)`);
    return true;
  }

  const elapsed = Date.now() - startTime;
  console.error(
    `${LOG_PREFIX} FAILED (${elapsed}ms). Gateway will run without persistent storage. ` +
      'Binding backup (tar.gz + put) will be used for cron; set WORKER_URL + BACKUP_RESTORE_TOKEN for restore at startup.',
  );
  return false;
}

/** Exposed for testing only */
export function _resetMountLock(): void {
  inflightMount = null;
}
