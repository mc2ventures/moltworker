import { describe, it, expect, beforeEach, vi } from 'vitest';
import { syncToR2 } from './sync';
import {
  createMockEnv,
  createMockEnvWithR2,
  createMockProcess,
  createMockSandbox,
  suppressConsole,
} from '../test-utils';

describe('syncToR2', () => {
  beforeEach(() => {
    suppressConsole();
  });

  describe('configuration checks', () => {
    it('falls back to binding backup when CF_ACCOUNT_ID is missing', async () => {
      const { sandbox, startProcessMock } = createMockSandbox();
      const putMock = vi.fn().mockResolvedValue(undefined);
      const env = createMockEnv({
        MOLTBOT_BUCKET: { put: putMock } as any,
      });
      // syncToR2Binding: test openclaw.json (exists), then tar (base64 output)
      startProcessMock
        .mockResolvedValueOnce(createMockProcess('', { exitCode: 0 }))   // test -f openclaw
        .mockResolvedValueOnce(createMockProcess('H4sIAAAAAAAAA+3BMQ0AAADCIPuntsYYwQAAAAAAAAAAAAAAAAAAAPBvBAgBAAAA')); // minimal gzip base64

      const result = await syncToR2(sandbox, env);

      expect(result.success).toBe(true);
      expect(putMock).toHaveBeenCalled();
    });

    it('proceeds with only CF_ACCOUNT_ID (no explicit R2 credentials)', async () => {
      const { sandbox, startProcessMock } = createMockSandbox();
      // mountR2Storage: isR2Mounted → mountBucket → isR2Mounted (mounted)
      // sync: check openclaw.json → rsync → cat timestamp
      startProcessMock
        .mockResolvedValueOnce(createMockProcess('s3fs on /data/moltbot type fuse.s3fs\n')) // already mounted
        .mockResolvedValueOnce(createMockProcess('ok'))  // check openclaw.json
        .mockResolvedValueOnce(createMockProcess(''))    // rsync
        .mockResolvedValueOnce(createMockProcess('2026-01-27T12:00:00+00:00'));  // timestamp

      const env = createMockEnv({ CF_ACCOUNT_ID: 'account123' });

      const result = await syncToR2(sandbox, env);

      expect(result.success).toBe(true);
    });

    it('falls back to binding when mount fails', async () => {
      const { sandbox, startProcessMock, mountBucketMock } = createMockSandbox();
      mountBucketMock.mockRejectedValue(new Error('Mount failed'));
      const putMock = vi.fn().mockResolvedValue(undefined);
      const env = createMockEnvWithR2({
        MOLTBOT_BUCKET: { put: putMock } as any,
      });
      // Mount path: isR2Mounted, tryMountS3fs (setupProc, mountProc, isR2Mounted post-s3fs), isR2Mounted final-check.
      // Binding path: test openclaw (exit 1), test clawdbot (exit 1) -> no config.
      startProcessMock
        .mockResolvedValueOnce(createMockProcess(''))  // isR2Mounted fast-path
        .mockResolvedValueOnce(createMockProcess(''))  // s3fs write creds
        .mockResolvedValueOnce(createMockProcess('', { exitCode: 1 }))  // s3fs mount (e.g. no FUSE)
        .mockResolvedValueOnce(createMockProcess(''))  // isR2Mounted post-s3fs
        .mockResolvedValueOnce(createMockProcess(''))  // isR2Mounted final-check
        .mockResolvedValueOnce(createMockProcess('', { exitCode: 1 }))  // test -f openclaw
        .mockResolvedValueOnce(createMockProcess('', { exitCode: 1 })); // test -f clawdbot

      const result = await syncToR2(sandbox, env);

      expect(result.success).toBe(false);
      expect(result.error).toBe('Sync aborted: no config file found');
      expect(putMock).not.toHaveBeenCalled();
    });
  });

  describe('sanity checks', () => {
    it('returns error when source has no config file', async () => {
      const { sandbox, startProcessMock } = createMockSandbox();
      startProcessMock
        .mockResolvedValueOnce(createMockProcess('s3fs on /data/moltbot type fuse.s3fs\n'))
        .mockResolvedValueOnce(createMockProcess('', { exitCode: 1 })) // No openclaw.json
        .mockResolvedValueOnce(createMockProcess('', { exitCode: 1 })); // No clawdbot.json either

      const env = createMockEnvWithR2();

      const result = await syncToR2(sandbox, env);

      expect(result.success).toBe(false);
      expect(result.error).toBe('Sync aborted: no config file found');
    });
  });

  describe('sync execution', () => {
    it('returns success when sync completes', async () => {
      const { sandbox, startProcessMock } = createMockSandbox();
      const timestamp = '2026-01-27T12:00:00+00:00';

      // Calls: mount check, check openclaw.json, rsync, cat timestamp
      startProcessMock
        .mockResolvedValueOnce(createMockProcess('s3fs on /data/moltbot type fuse.s3fs\n'))
        .mockResolvedValueOnce(createMockProcess('ok'))
        .mockResolvedValueOnce(createMockProcess(''))
        .mockResolvedValueOnce(createMockProcess(timestamp));

      const env = createMockEnvWithR2();

      const result = await syncToR2(sandbox, env);

      expect(result.success).toBe(true);
      expect(result.lastSync).toBe(timestamp);
    });

    it('returns error when rsync fails (no timestamp created)', async () => {
      const { sandbox, startProcessMock } = createMockSandbox();

      // Calls: mount check, check openclaw.json, rsync (fails), cat timestamp (empty)
      startProcessMock
        .mockResolvedValueOnce(createMockProcess('s3fs on /data/moltbot type fuse.s3fs\n'))
        .mockResolvedValueOnce(createMockProcess('ok'))
        .mockResolvedValueOnce(createMockProcess('', { exitCode: 1 }))
        .mockResolvedValueOnce(createMockProcess(''));

      const env = createMockEnvWithR2();

      const result = await syncToR2(sandbox, env);

      expect(result.success).toBe(false);
      expect(result.error).toBe('Sync failed');
    });

    it('verifies rsync command is called with correct flags', async () => {
      const { sandbox, startProcessMock } = createMockSandbox();
      const timestamp = '2026-01-27T12:00:00+00:00';

      startProcessMock
        .mockResolvedValueOnce(createMockProcess('s3fs on /data/moltbot type fuse.s3fs\n'))
        .mockResolvedValueOnce(createMockProcess('ok'))
        .mockResolvedValueOnce(createMockProcess(''))
        .mockResolvedValueOnce(createMockProcess(timestamp));

      const env = createMockEnvWithR2();

      await syncToR2(sandbox, env);

      // Third call should be rsync to openclaw/ R2 prefix
      const rsyncCall = startProcessMock.mock.calls[2][0];
      expect(rsyncCall).toContain('rsync');
      expect(rsyncCall).toContain('--no-times');
      expect(rsyncCall).toContain('--delete');
      expect(rsyncCall).toContain('/root/.openclaw/');
      expect(rsyncCall).toContain('/data/moltbot/openclaw/');
    });
  });
});
