import type { Sandbox } from '@cloudflare/sandbox';
import type { MoltbotEnv } from '../types';
import { waitForProcess } from './utils';

const BACKUP_KEY = 'openclaw/backup.tar.gz';
const LAST_SYNC_KEY = 'openclaw/.last-sync';

export interface SyncResult {
  success: boolean;
  lastSync?: string;
  error?: string;
  details?: string;
}

/**
 * Decode base64 string to Uint8Array (binary-safe for gzip).
 */
function base64ToBytes(base64: string): Uint8Array {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

/**
 * Sync OpenClaw config and workspace to R2 via Worker binding (no FUSE mount).
 * Runs tar in the container, captures base64 stdout, decodes and puts to R2.
 * More efficient than mount + rsync when FUSE is unavailable or for simpler ops.
 *
 * @param sandbox - The sandbox instance
 * @param env - Worker environment bindings (must have MOLTBOT_BUCKET)
 * @returns SyncResult
 */
export async function syncToR2Binding(
  sandbox: Sandbox,
  env: MoltbotEnv,
): Promise<SyncResult> {
  if (!env.MOLTBOT_BUCKET) {
    return { success: false, error: 'R2 bucket binding (MOLTBOT_BUCKET) not available' };
  }

  // Determine which config directory exists
  let configDir = '.openclaw';
  try {
    const checkNew = await sandbox.startProcess('test -f /root/.openclaw/openclaw.json');
    await waitForProcess(checkNew, 5000);
    if (checkNew.exitCode !== 0) {
      const checkLegacy = await sandbox.startProcess('test -f /root/.clawdbot/clawdbot.json');
      await waitForProcess(checkLegacy, 5000);
      if (checkLegacy.exitCode === 0) {
        configDir = '.clawdbot';
      } else {
        return {
          success: false,
          error: 'Sync aborted: no config file found',
          details: 'Neither openclaw.json nor clawdbot.json found.',
        };
      }
    }
  } catch (err) {
    return {
      success: false,
      error: 'Failed to verify source files',
      details: err instanceof Error ? err.message : 'Unknown error',
    };
  }

  // tar cz from /root: config dir + clawd (exclude temp files), then base64 for safe stdout
  const tarCmd = `tar cz --exclude='*.lock' --exclude='*.log' --exclude='*.tmp' -C /root ${configDir} clawd 2>/dev/null | base64 -w0`;
  console.log('[SyncBinding] Running tar in container, then uploading to R2...');

  try {
    const proc = await sandbox.startProcess(tarCmd);
    await waitForProcess(proc, 60000); // 60s for large dirs

    const logs = await proc.getLogs();
    const base64 = logs.stdout?.trim();
    if (!base64) {
      const stderr = logs.stderr?.trim() || '';
      return {
        success: false,
        error: 'Backup produced no output',
        details: stderr || 'tar may have failed (check container logs)',
      };
    }

    const body = base64ToBytes(base64);
    const timestamp = new Date().toISOString();

    await env.MOLTBOT_BUCKET.put(BACKUP_KEY, body, {
      httpMetadata: { contentType: 'application/gzip' },
      customMetadata: { 'last-sync': timestamp },
    });
    await env.MOLTBOT_BUCKET.put(LAST_SYNC_KEY, timestamp, {
      httpMetadata: { contentType: 'text/plain' },
    });

    console.log('[SyncBinding] Backup uploaded to R2:', BACKUP_KEY, 'size:', body.length, 'bytes');
    return { success: true, lastSync: timestamp };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[SyncBinding] Upload failed:', message);
    return {
      success: false,
      error: 'Binding backup failed',
      details: message,
    };
  }
}
