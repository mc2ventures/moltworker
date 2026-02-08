import type { Sandbox } from '@cloudflare/sandbox';
import type { MoltbotEnv } from '../types';
import { R2_MOUNT_PATH, getR2BucketName } from '../config';

/** Consistent log prefix for all R2 mount operations */
const LOG_PREFIX = '[R2 Mount]';

/** Bucket mounting requires FUSE and does not work with wrangler dev — production only. */

/**
 * In-flight mount promise used to deduplicate concurrent mount attempts.
 *
 * Multiple concurrent requests (e.g. the loading-page waitUntil + the next
 * polling request) can both call mountR2Storage before the first one finishes.
 *
 * By caching the in-flight promise we ensure only one mount attempt runs at
 * a time within a Worker isolate.
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
 * Kept quiet by default; pass label for contextual debug logging.
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
 * Tries multiple strategies in order:
 *
 * 1. **SDK mountBucket() without credentials** — The Cloudflare Sandbox SDK
 *    may handle same-account R2 buckets automatically, or auto-detect
 *    AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY from Worker secrets.
 *
 * 2. **SDK mountBucket() with explicit credentials** — If R2_ACCESS_KEY_ID
 *    and R2_SECRET_ACCESS_KEY are configured, pass them to the SDK.
 *
 * 3. **Manual s3fs mount** — Direct s3fs inside the container with credential
 *    file management. This was the original approach and avoids the credential
 *    accumulation bug in older SDK versions where mountBucket() appended to
 *    the passwd file on every call. Kept as a last-resort fallback.
 *
 * Only CF_ACCOUNT_ID is required (for the R2 endpoint URL). Explicit R2
 * credentials (R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY) are optional — the
 * SDK may handle auth for same-account buckets without them.
 *
 * Concurrent calls are coalesced behind a single in-flight promise.
 *
 * @param sandbox - The sandbox instance
 * @param env - Worker environment bindings
 * @returns true if mounted successfully, false otherwise
 */
export async function mountR2Storage(sandbox: Sandbox, env: MoltbotEnv): Promise<boolean> {
  // CF_ACCOUNT_ID is the minimum requirement (needed for R2 endpoint URL)
  if (!env.CF_ACCOUNT_ID) {
    console.log(`${LOG_PREFIX} Skipped — CF_ACCOUNT_ID not set`);
    return false;
  }

  // If a mount is already in progress, wait for it instead of starting another
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
 * Internal mount implementation — always called at most once at a time.
 *
 * Tries SDK mountBucket() first (with and without credentials),
 * then falls back to manual s3fs if the SDK approach fails.
 */
async function doMount(sandbox: Sandbox, env: MoltbotEnv): Promise<boolean> {
  const bucketName = getR2BucketName(env);
  const hasExplicitCreds = !!(env.R2_ACCESS_KEY_ID && env.R2_SECRET_ACCESS_KEY);
  const totalStrategies = hasExplicitCreds ? 3 : 1;
  const endpoint = `https://${env.CF_ACCOUNT_ID}.r2.cloudflarestorage.com`;

  console.log(
    `${LOG_PREFIX} Starting — bucket=${bucketName}, path=${R2_MOUNT_PATH}, ` +
    `endpoint=${endpoint}, explicitCreds=${hasExplicitCreds}, strategies=${totalStrategies}`,
  );
  const startTime = Date.now();

  // Fast path: already mounted from a previous invocation
  if (await isR2Mounted(sandbox, 'fast-path')) {
    console.log(`${LOG_PREFIX} Already mounted — no action needed (${Date.now() - startTime}ms)`);
    return true;
  }

  // Strategy 1: SDK mountBucket() without explicit credentials.
  // The SDK auto-detects AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY from Worker
  // secrets, and same-account R2 buckets may work without any credentials.
  console.log(`${LOG_PREFIX} Strategy 1/${totalStrategies}: SDK mountBucket (no credentials)`);
  const sdkNoCredsResult = await tryMountBucketSDK(sandbox, bucketName, endpoint);
  if (sdkNoCredsResult) {
    console.log(`${LOG_PREFIX} SUCCESS via strategy 1 — SDK mountBucket without credentials (${Date.now() - startTime}ms)`);
    return true;
  }

  // Strategy 2: SDK mountBucket() with explicit credentials (if available).
  if (hasExplicitCreds) {
    console.log(`${LOG_PREFIX} Strategy 2/${totalStrategies}: SDK mountBucket (explicit credentials)`);
    const sdkCredsResult = await tryMountBucketSDK(sandbox, bucketName, endpoint, {
      accessKeyId: env.R2_ACCESS_KEY_ID!,
      secretAccessKey: env.R2_SECRET_ACCESS_KEY!,
    });
    if (sdkCredsResult) {
      console.log(`${LOG_PREFIX} SUCCESS via strategy 2 — SDK mountBucket with explicit credentials (${Date.now() - startTime}ms)`);
      return true;
    }

    // Strategy 3: Manual s3fs mount as last resort.
    // Writes credentials to /etc/passwd-s3fs (overwrite, not append) and calls
    // s3fs directly. This avoids the credential accumulation bug in older SDK
    // versions but requires explicit R2 API tokens.
    console.log(`${LOG_PREFIX} Strategy 3/${totalStrategies}: Manual s3fs mount`);
    const s3fsResult = await tryMountS3fs(sandbox, env, bucketName, endpoint);
    if (s3fsResult) {
      console.log(`${LOG_PREFIX} SUCCESS via strategy 3 — manual s3fs (${Date.now() - startTime}ms)`);
      return true;
    }
  }

  // Final check — the mount might have succeeded despite errors
  if (await isR2Mounted(sandbox, 'final-check')) {
    console.log(`${LOG_PREFIX} SUCCESS — mount detected on final check despite errors (${Date.now() - startTime}ms)`);
    return true;
  }

  // Don't fail the gateway — moltbot can still run without persistent storage
  const elapsed = Date.now() - startTime;
  console.error(
    `${LOG_PREFIX} FAILED — all ${totalStrategies} strategies exhausted (${elapsed}ms). ` +
    'Gateway will run without persistent storage.',
  );
  return false;
}

/**
 * Try mounting via the Sandbox SDK mountBucket() API.
 *
 * Handles the "already mounted" error gracefully by checking if
 * the mount is actually present.
 *
 * @param sandbox - The sandbox instance
 * @param bucketName - R2 bucket name
 * @param endpoint - R2 S3-compatible endpoint URL
 * @param credentials - Optional explicit credentials
 * @returns true if mount succeeded
 */
async function tryMountBucketSDK(
  sandbox: Sandbox,
  bucketName: string,
  endpoint: string,
  credentials?: { accessKeyId: string; secretAccessKey: string },
): Promise<boolean> {
  const label = credentials ? 'explicit-creds' : 'no-creds';
  const t0 = Date.now();
  try {
    const options: { endpoint: string; credentials?: { accessKeyId: string; secretAccessKey: string } } = { endpoint };
    if (credentials) {
      options.credentials = credentials;
    }

    await sandbox.mountBucket(bucketName, R2_MOUNT_PATH, options);
    console.log(`${LOG_PREFIX}   mountBucket(${label}) returned OK (${Date.now() - t0}ms), verifying...`);

    if (await isR2Mounted(sandbox, `post-sdk-${label}`)) {
      return true;
    }
    console.log(`${LOG_PREFIX}   mountBucket(${label}) returned OK but mount not detected in mount table`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    const errType = msg.includes('Credentials') ? 'credentials' :
                    msg.includes('already in use') || msg.includes('already mounted') ? 'already-mounted' :
                    'other';
    console.log(`${LOG_PREFIX}   mountBucket(${label}) threw [${errType}]: ${msg.slice(0, 200)} (${Date.now() - t0}ms)`);
    if (msg.includes('fuse') || msg.includes('modprobe')) {
      console.error(
        `${LOG_PREFIX} R2 mount needs FUSE; only works in production (wrangler deploy). Not available in wrangler dev.`,
      );
    }

    // If "already mounted" error, check if it's actually mounted
    if (errType === 'already-mounted') {
      if (await isR2Mounted(sandbox, `sdk-already-mounted-${label}`)) {
        console.log(`${LOG_PREFIX}   Confirmed: mount is present despite "already mounted" error`);
        return true;
      }
    }
  }
  return false;
}

/**
 * Fall back to mounting via s3fs directly inside the container.
 *
 * Writes credentials to /etc/passwd-s3fs (overwrite, not append) and runs
 * s3fs. This avoids the credential accumulation bug where mountBucket()
 * appends a new entry on every call.
 *
 * @param sandbox - The sandbox instance
 * @param env - Worker environment bindings (needs R2_ACCESS_KEY_ID + R2_SECRET_ACCESS_KEY)
 * @param bucketName - R2 bucket name
 * @param endpoint - R2 S3-compatible endpoint URL
 * @returns true if mount succeeded
 */
async function tryMountS3fs(
  sandbox: Sandbox,
  env: MoltbotEnv,
  bucketName: string,
  endpoint: string,
): Promise<boolean> {
  const t0 = Date.now();
  try {
    // Write credentials to the s3fs passwd file inside the container.
    // Using '>' (overwrite) instead of '>>' ensures exactly one entry
    // regardless of how many times this runs.
    //
    // Credentials are base64-encoded to avoid shell escaping issues and
    // to keep raw secrets out of the command string / process logs.
    console.log(`${LOG_PREFIX}   Writing s3fs credentials to /etc/passwd-s3fs...`);
    const credLine = `${env.R2_ACCESS_KEY_ID}:${env.R2_SECRET_ACCESS_KEY}`;
    const credB64 = btoa(credLine);
    const setupProc = await sandbox.startProcess(
      `echo '${credB64}' | base64 -d > /etc/passwd-s3fs && chmod 600 /etc/passwd-s3fs`,
    );
    await waitForProcess(setupProc);

    const setupLogs = await setupProc.getLogs();
    if (setupLogs.stderr) {
      console.log(`${LOG_PREFIX}   passwd-s3fs stderr: ${setupLogs.stderr.slice(0, 200)}`);
    }
    console.log(`${LOG_PREFIX}   Credentials written (${Date.now() - t0}ms). Running s3fs mount...`);

    // Mount with s3fs directly inside the container
    const mountProc = await sandbox.startProcess(
      `mkdir -p ${R2_MOUNT_PATH} && ` +
        `s3fs ${bucketName} ${R2_MOUNT_PATH}` +
        ` -o passwd_file=/etc/passwd-s3fs` +
        ` -o url=${endpoint}` +
        ` -o use_path_request_style`,
    );
    // s3fs mount can take a few seconds
    await waitForProcess(mountProc, 10000);

    const mountLogs = await mountProc.getLogs();
    const mountElapsed = Date.now() - t0;
    if (mountLogs.stderr) {
      console.log(`${LOG_PREFIX}   s3fs stderr: ${mountLogs.stderr.slice(0, 300)}`);
      if (mountLogs.stderr.includes('fuse') || mountLogs.stderr.includes('modprobe')) {
        console.error(
          `${LOG_PREFIX} R2 mount needs FUSE; only works in production (wrangler deploy). Not available in wrangler dev.`,
        );
      }
    }
    console.log(`${LOG_PREFIX}   s3fs process exited (exit=${mountProc.exitCode}, ${mountElapsed}ms), verifying...`);

    // Verify the mount succeeded
    if (await isR2Mounted(sandbox, 'post-s3fs')) {
      return true;
    }

    console.log(`${LOG_PREFIX}   s3fs exited but mount not detected in mount table`);
  } catch (err) {
    const errorMessage = err instanceof Error ? err.message : String(err);
    console.log(`${LOG_PREFIX}   s3fs error: ${errorMessage} (${Date.now() - t0}ms)`);
  }
  return false;
}

/** Exposed for testing only — reset the in-flight lock between tests */
export function _resetMountLock(): void {
  inflightMount = null;
}
