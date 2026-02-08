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

    it('proceeds with only CF_ACCOUNT_ID (no explicit credentials)', async () => {
      const { sandbox, mountBucketMock, startProcessMock } = createMockSandbox({ mounted: false });
      startProcessMock
        .mockResolvedValueOnce(createMockProcess(''))   // isR2Mounted check
        .mockResolvedValueOnce(createMockProcess('s3fs on /data/moltbot type fuse.s3fs\n'));  // verify after SDK mount

      const env = createMockEnv({ CF_ACCOUNT_ID: 'account123' });

      const result = await mountR2Storage(sandbox, env);

      expect(result).toBe(true);
      expect(mountBucketMock).toHaveBeenCalledTimes(1);
      expect(mountBucketMock).toHaveBeenCalledWith(
        'moltdata',
        '/data/moltbot',
        { endpoint: 'https://account123.r2.cloudflarestorage.com' },
      );
    });
  });

  describe('SDK mountBucket (official pattern)', () => {
    it('tries mountBucket with endpoint only first (SDK auto-detects AWS_*)', async () => {
      const { sandbox, mountBucketMock, startProcessMock } = createMockSandbox({ mounted: false });
      startProcessMock
        .mockResolvedValueOnce(createMockProcess(''))   // isR2Mounted check
        .mockResolvedValueOnce(createMockProcess('s3fs on /data/moltbot type fuse.s3fs\n'));  // verify

      const env = createMockEnvWithR2();

      await mountR2Storage(sandbox, env);

      expect(mountBucketMock).toHaveBeenCalledWith(
        'moltdata',
        '/data/moltbot',
        { endpoint: 'https://test-account-id.r2.cloudflarestorage.com' },
      );
    });

    it('tries mountBucket with explicit credentials when no-creds fails', async () => {
      const { sandbox, mountBucketMock, startProcessMock } = createMockSandbox({ mounted: false });
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
      expect(mountBucketMock).toHaveBeenLastCalledWith(
        'moltdata',
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
    it('returns false when both mountBucket attempts fail', async () => {
      const { sandbox, mountBucketMock, startProcessMock } = createMockSandbox({ mounted: false });
      mountBucketMock.mockRejectedValue(new Error('SDK mount failed'));

      startProcessMock
        .mockResolvedValueOnce(createMockProcess(''))   // isR2Mounted check
        .mockResolvedValueOnce(createMockProcess('')); // final check (not mounted)

      const env = createMockEnvWithR2();

      const result = await mountR2Storage(sandbox, env);

      expect(result).toBe(false);
      expect(console.error).toHaveBeenCalledWith(
        expect.stringContaining('FAILED'),
      );
    });

    it('returns true if final mount check passes', async () => {
      const { sandbox, mountBucketMock, startProcessMock } = createMockSandbox({ mounted: false });
      mountBucketMock.mockRejectedValue(new Error('SDK mount failed'));

      startProcessMock
        .mockResolvedValueOnce(createMockProcess(''))   // isR2Mounted check
        .mockResolvedValueOnce(createMockProcess('s3fs on /data/moltbot type fuse.s3fs\n'));  // final check

      const env = createMockEnvWithR2();

      const result = await mountR2Storage(sandbox, env);

      expect(result).toBe(true);
      expect(console.log).toHaveBeenCalledWith(
        expect.stringContaining('mount detected on final check'),
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
      expect(mountBucketMock).toHaveBeenCalledTimes(1);
    });

    it('resets lock after failure so next attempt can retry', async () => {
      const { sandbox, mountBucketMock, startProcessMock } = createMockSandbox({ mounted: false });
      mountBucketMock
        .mockRejectedValueOnce(new Error('fail'))  // no-creds
        .mockRejectedValueOnce(new Error('fail'))  // with-creds
        .mockResolvedValueOnce(undefined);        // no-creds on retry succeeds

      startProcessMock
        .mockResolvedValueOnce(createMockProcess(''))   // first attempt: isR2Mounted
        .mockResolvedValueOnce(createMockProcess(''))   // first attempt: final check
        .mockResolvedValueOnce(createMockProcess(''))   // second attempt: isR2Mounted
        .mockResolvedValueOnce(createMockProcess('s3fs on /data/moltbot type fuse.s3fs\n'));  // second: verify

      const env = createMockEnvWithR2();

      const result1 = await mountR2Storage(sandbox, env);
      expect(result1).toBe(false);

      const result2 = await mountR2Storage(sandbox, env);
      expect(result2).toBe(true);
    });
  });
});
