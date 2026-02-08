import type { Sandbox } from '@cloudflare/sandbox';
import type { MoltbotEnv } from '../types';
import { R2_MOUNT_PATH } from '../config';
import { mountR2Storage } from './r2';
import { syncToR2Binding } from './sync-binding';
import { waitForProcess } from './utils';

export interface SyncResult {
  success: boolean;
  lastSync?: string;
  error?: string;
  details?: string;
}

/**
 * Sync OpenClaw config and workspace from container to R2 for persistence.
 *
 * Tries mount-based sync first (rsync to mounted R2). If mount is unavailable,
 * falls back to binding-based sync: tar in container, Worker puts to R2 (no FUSE).
 *
 * Mount path syncs: config, workspace, skills to R2 prefixes.
 * Binding path: single openclaw/backup.tar.gz + openclaw/.last-sync.
 *
 * @param sandbox - The sandbox instance
 * @param env - Worker environment bindings
 * @returns SyncResult with success status and optional error details
 */
export async function syncToR2(sandbox: Sandbox, env: MoltbotEnv): Promise<SyncResult> {
  // Try mount first when configured (requires CF_ACCOUNT_ID and FUSE)
  if (env.CF_ACCOUNT_ID) {
    const mounted = await mountR2Storage(sandbox, env);
    if (mounted) {
      return syncToR2ViaMount(sandbox, env);
    }
  }

  // Fallback: tar.gz + Worker put (no FUSE, works with just MOLTBOT_BUCKET binding)
  console.log('[Sync] Using binding backup (tar.gz + put)');
  return syncToR2Binding(sandbox, env);
}

/**
 * Mount-based sync: rsync config, workspace, skills to mounted R2 path.
 * Called when mountR2Storage succeeded.
 */
async function syncToR2ViaMount(sandbox: Sandbox, env: MoltbotEnv): Promise<SyncResult> {

  // Determine which config directory exists
  let configDir = '/root/.openclaw';
  try {
    const checkNew = await sandbox.startProcess('test -f /root/.openclaw/openclaw.json');
    await waitForProcess(checkNew, 5000);
    if (checkNew.exitCode !== 0) {
      const checkLegacy = await sandbox.startProcess('test -f /root/.clawdbot/clawdbot.json');
      await waitForProcess(checkLegacy, 5000);
      if (checkLegacy.exitCode === 0) {
        configDir = '/root/.clawdbot';
      } else {
        return {
          success: false,
          error: 'Sync aborted: no config file found',
          details: 'Neither openclaw.json nor clawdbot.json found in config directory.',
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

  const syncCmd = `rsync -r --no-times --delete --exclude='*.lock' --exclude='*.log' --exclude='*.tmp' ${configDir}/ ${R2_MOUNT_PATH}/openclaw/ && rsync -r --no-times --delete --exclude='skills' /root/clawd/ ${R2_MOUNT_PATH}/workspace/ && rsync -r --no-times --delete /root/clawd/skills/ ${R2_MOUNT_PATH}/skills/ && date -Iseconds > ${R2_MOUNT_PATH}/.last-sync`;

  try {
    const proc = await sandbox.startProcess(syncCmd);
    await waitForProcess(proc, 30000);

    const timestampProc = await sandbox.startProcess(`cat ${R2_MOUNT_PATH}/.last-sync`);
    await waitForProcess(timestampProc, 5000);
    const timestampLogs = await timestampProc.getLogs();
    const lastSync = timestampLogs.stdout?.trim();

    if (lastSync && lastSync.match(/^\d{4}-\d{2}-\d{2}/)) {
      return { success: true, lastSync };
    }
    const logs = await proc.getLogs();
    return {
      success: false,
      error: 'Sync failed',
      details: logs.stderr || logs.stdout || 'No timestamp file created',
    };
  } catch (err) {
    return {
      success: false,
      error: 'Sync error',
      details: err instanceof Error ? err.message : 'Unknown error',
    };
  }
}
