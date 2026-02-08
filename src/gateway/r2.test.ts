import { describe, it, expect, beforeEach } from 'vitest';
import { mountR2Storage, _resetMountLock } from './r2';
import {
  createMockEnv,
  createMockEnvWithR2,
  createMockProcess,
  createMockSandbox,
  suppressConsole,
} from '../test-utils';

describe('mountR2Storage', () => {
  beforeEach(() => {
    suppressConsole();
    _resetMountLock();
  });

  describe('configuration validation', () => {
    it('returns false when CF_ACCOUNT_ID is missing', async () => {
      const { sandbox } = createMockSandbox();
      const env = createMockEnv();

      const result = await mountR2Storage(sandbox, env);

      expect(result).toBe(false);
      expect(console.log).toHaveBeenCalledWith(
        expect.stringContaining('CF_ACCOUNT_ID not set'),
      );
    });

    it('returns false when CF_ACCOUNT_ID is missing even with R2 credentials', async () => {
      const { sandbox } = createMockSandbox();
      const env = createMockEnv({
        R2_ACCESS_KEY_ID: 'key123',
        R2_SECRET_ACCESS_KEY: 'secret',
      });

      const result = await mountR2Storage(sandbox, env);

      expect(result).toBe(false);
    });

    it('proceeds with only CF_ACCOUNT_ID (no explicit R2 credentials)', async () => {
      const { sandbox, mountBucketMock, startProcessMock } = createMockSandbox({ mounted: false });
      // isR2Mounted (not mounted) → mountBucket succeeds → isR2Mounted (mounted)
      startProcessMock
        .mockResolvedValueOnce(createMockProcess(''))   // isR2Mounted check
        .mockResolvedValueOnce(createMockProcess('s3fs on /data/moltbot type fuse.s3fs\n'));  // verify after SDK mount

      const env = createMockEnv({ CF_ACCOUNT_ID: 'account123' });

      const result = await mountR2Storage(sandbox, env);

      expect(result).toBe(true);
      expect(mountBucketMock).toHaveBeenCalledTimes(1);
      // Should be called without explicit credentials
      expect(mountBucketMock).toHaveBeenCalledWith(
        'moltbot-data',
        '/data/moltbot',
        { endpoint: 'https://account123.r2.cloudflarestorage.com' },
      );
    });
  });

  describe('SDK mountBucket strategy', () => {
    it('tries mountBucket without credentials first', async () => {
      const { sandbox, mountBucketMock, startProcessMock } = createMockSandbox({ mounted: false });
      startProcessMock
        .mockResolvedValueOnce(createMockProcess(''))   // isR2Mounted check
        .mockResolvedValueOnce(createMockProcess('s3fs on /data/moltbot type fuse.s3fs\n'));  // verify

      const env = createMockEnvWithR2();

      await mountR2Storage(sandbox, env);

      // First mountBucket call should be without credentials
      expect(mountBucketMock).toHaveBeenCalledWith(
        'moltbot-data',
        '/data/moltbot',
        { endpoint: 'https://test-account-id.r2.cloudflarestorage.com' },
      );
    });

    it('tries mountBucket with explicit credentials when no-creds fails', async () => {
      const { sandbox, mountBucketMock, startProcessMock } = createMockSandbox({ mounted: false });
      // First mountBucket (no creds) fails, second (with creds) succeeds
      mountBucketMock
        .mockRejectedValueOnce(new Error('MissingCredentialsError: No credentials found'))
        .mockResolvedValueOnce(undefined);

      startProcessMock
        .mockResolvedValueOnce(createMockProcess(''))   // isR2Mounted (initial check)
        .mockResolvedValueOnce(createMockProcess('s3fs on /data/moltbot type fuse.s3fs\n'));  // verify after creds mount

      const env = createMockEnvWithR2();

      const result = await mountR2Storage(sandbox, env);

      expect(result).toBe(true);
      expect(mountBucketMock).toHaveBeenCalledTimes(2);
      // Second call should include credentials
      expect(mountBucketMock).toHaveBeenLastCalledWith(
        'moltbot-data',
        '/data/moltbot',
        {
          endpoint: 'https://test-account-id.r2.cloudflarestorage.com',
          credentials: {
            accessKeyId: 'test-key-id',
            secretAccessKey: 'test-secret-key',
          },
        },
      );
    });

    it('handles "already in use" error by checking if actually mounted', async () => {
      const { sandbox, mountBucketMock, startProcessMock } = createMockSandbox({ mounted: false });
      mountBucketMock.mockRejectedValue(new Error('InvalidMountConfigError: Mount path already in use'));

      startProcessMock
        .mockResolvedValueOnce(createMockProcess(''))   // initial isR2Mounted check (not mounted)
        .mockResolvedValueOnce(createMockProcess('s3fs on /data/moltbot type fuse.s3fs\n'));  // check after error

      const env = createMockEnv({ CF_ACCOUNT_ID: 'account123' });

      const result = await mountR2Storage(sandbox, env);

      expect(result).toBe(true);
      expect(console.log).toHaveBeenCalledWith(
        expect.stringContaining('mount is present despite "already mounted" error'),
      );
    });

    it('uses custom bucket name from R2_BUCKET_NAME env var', async () => {
      const { sandbox, mountBucketMock, startProcessMock } = createMockSandbox({ mounted: false });
      startProcessMock
        .mockResolvedValueOnce(createMockProcess(''))
        .mockResolvedValueOnce(createMockProcess('s3fs on /data/moltbot type fuse.s3fs\n'));

      const env = createMockEnv({ CF_ACCOUNT_ID: 'account123', R2_BUCKET_NAME: 'custom-bucket' });

      await mountR2Storage(sandbox, env);

      expect(mountBucketMock).toHaveBeenCalledWith(
        'custom-bucket',
        '/data/moltbot',
        expect.objectContaining({ endpoint: expect.any(String) }),
      );
    });
  });

  describe('s3fs fallback strategy', () => {
    it('falls back to s3fs when all SDK attempts fail', async () => {
      const { sandbox, mountBucketMock, startProcessMock } = createMockSandbox({ mounted: false });
      // Both mountBucket calls fail
      mountBucketMock.mockRejectedValue(new Error('SDK mount failed'));

      startProcessMock
        .mockResolvedValueOnce(createMockProcess(''))   // isR2Mounted check
        // After SDK no-creds fail: (no "already in use" so no extra isR2Mounted check)
        // After SDK with-creds fail: (no "already in use" so no extra isR2Mounted check)
        .mockResolvedValueOnce(createMockProcess(''))   // passwd file write
        .mockResolvedValueOnce(createMockProcess(''))   // s3fs mount
        .mockResolvedValueOnce(createMockProcess('s3fs on /data/moltbot type fuse.s3fs\n'));  // verify

      const env = createMockEnvWithR2();

      const result = await mountR2Storage(sandbox, env);

      expect(result).toBe(true);
      expect(mountBucketMock).toHaveBeenCalledTimes(2); // no-creds + with-creds
      // Verify s3fs fallback was used
      expect(startProcessMock).toHaveBeenCalledWith(
        expect.stringContaining('base64 -d > /etc/passwd-s3fs'),
      );
      expect(startProcessMock).toHaveBeenCalledWith(
        expect.stringContaining('s3fs moltbot-data /data/moltbot'),
      );
    });

    it('does not try s3fs fallback without explicit R2 credentials', async () => {
      const { sandbox, mountBucketMock, startProcessMock } = createMockSandbox({ mounted: false });
      mountBucketMock.mockRejectedValue(new Error('SDK mount failed'));

      startProcessMock
        .mockResolvedValueOnce(createMockProcess(''))   // isR2Mounted check
        .mockResolvedValueOnce(createMockProcess(''));   // final isR2Mounted check

      // Only CF_ACCOUNT_ID, no R2 credentials
      const env = createMockEnv({ CF_ACCOUNT_ID: 'account123' });

      const result = await mountR2Storage(sandbox, env);

      expect(result).toBe(false);
      // Should NOT have attempted s3fs (no base64 credential write)
      const s3fsCalls = startProcessMock.mock.calls.filter((call: unknown[]) =>
        (call[0] as string).includes('base64'),
      );
      expect(s3fsCalls).toHaveLength(0);
    });
  });

  describe('fast path', () => {
    it('returns true immediately when bucket is already mounted', async () => {
      const { sandbox, mountBucketMock, startProcessMock } = createMockSandbox({ mounted: true });
      const env = createMockEnvWithR2();

      const result = await mountR2Storage(sandbox, env);

      expect(result).toBe(true);
      // Only one startProcess call (the isR2Mounted check) — no mount attempted
      expect(startProcessMock).toHaveBeenCalledTimes(1);
      expect(mountBucketMock).not.toHaveBeenCalled();
      expect(console.log).toHaveBeenCalledWith(
        expect.stringContaining('Already mounted'),
      );
    });
  });

  describe('error handling', () => {
    it('returns false when all strategies fail', async () => {
      const { sandbox, mountBucketMock, startProcessMock } = createMockSandbox({ mounted: false });
      mountBucketMock.mockRejectedValue(new Error('SDK mount failed'));

      startProcessMock
        .mockResolvedValueOnce(createMockProcess(''))   // isR2Mounted check
        .mockResolvedValueOnce(createMockProcess(''))   // passwd file write
        .mockResolvedValueOnce(createMockProcess('', { exitCode: 1, stderr: 'mount error' }))  // s3fs fails
        .mockResolvedValueOnce(createMockProcess(''))   // verify (not mounted)
        .mockResolvedValueOnce(createMockProcess(''));   // final check (not mounted)

      const env = createMockEnvWithR2();

      const result = await mountR2Storage(sandbox, env);

      expect(result).toBe(false);
      expect(console.error).toHaveBeenCalledWith(
        expect.stringContaining('FAILED'),
      );
    });

    it('returns true if final mount check passes despite all errors', async () => {
      const { sandbox, mountBucketMock, startProcessMock } = createMockSandbox({ mounted: false });
      mountBucketMock.mockRejectedValue(new Error('SDK mount failed'));

      startProcessMock
        .mockResolvedValueOnce(createMockProcess(''))   // isR2Mounted check
        .mockRejectedValueOnce(new Error('passwd write failed'))    // s3fs passwd throws
        .mockResolvedValueOnce(createMockProcess('s3fs on /data/moltbot type fuse.s3fs\n'));  // final check

      const env = createMockEnvWithR2();

      const result = await mountR2Storage(sandbox, env);

      expect(result).toBe(true);
      expect(console.log).toHaveBeenCalledWith(
        expect.stringContaining('mount detected on final check despite errors'),
      );
    });
  });

  describe('concurrent mount protection', () => {
    it('only runs mount once when invoked concurrently', async () => {
      const { sandbox, mountBucketMock, startProcessMock } = createMockSandbox({ mounted: false });
      startProcessMock
        .mockResolvedValueOnce(createMockProcess(''))   // isR2Mounted
        .mockResolvedValueOnce(createMockProcess('s3fs on /data/moltbot type fuse.s3fs\n'));  // verify

      const env = createMockEnvWithR2();

      const [result1, result2] = await Promise.all([
        mountR2Storage(sandbox, env),
        mountR2Storage(sandbox, env),
      ]);

      expect(result1).toBe(true);
      expect(result2).toBe(true);
      // mountBucket should only be called once (from the single mount attempt)
      expect(mountBucketMock).toHaveBeenCalledTimes(1);
    });

    it('resets lock after failure so next attempt can retry', async () => {
      const { sandbox, mountBucketMock, startProcessMock } = createMockSandbox({ mounted: false });
      // First attempt: all strategies fail
      mountBucketMock
        .mockRejectedValueOnce(new Error('fail'))  // no-creds attempt 1
        .mockRejectedValueOnce(new Error('fail'))  // with-creds attempt 1
        .mockResolvedValueOnce(undefined);          // no-creds attempt 2 succeeds

      startProcessMock
        // First attempt: isR2Mounted, s3fs passwd, s3fs mount, verify, final check
        .mockResolvedValueOnce(createMockProcess(''))
        .mockResolvedValueOnce(createMockProcess(''))
        .mockResolvedValueOnce(createMockProcess('', { exitCode: 1 }))
        .mockResolvedValueOnce(createMockProcess(''))
        .mockResolvedValueOnce(createMockProcess(''))
        // Second attempt: isR2Mounted, verify after SDK mount
        .mockResolvedValueOnce(createMockProcess(''))
        .mockResolvedValueOnce(createMockProcess('s3fs on /data/moltbot type fuse.s3fs\n'));

      const env = createMockEnvWithR2();

      const result1 = await mountR2Storage(sandbox, env);
      expect(result1).toBe(false);

      const result2 = await mountR2Storage(sandbox, env);
      expect(result2).toBe(true);
    });
  });
});
