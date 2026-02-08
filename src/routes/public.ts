import { Hono } from 'hono';
import type { AppEnv } from '../types';
import { MOLTBOT_PORT } from '../config';
import { ensureMoltbotGateway, findExistingMoltbotProcess } from '../gateway';

const BACKUP_KEY = 'openclaw/backup.tar.gz';

/**
 * Public routes - NO Cloudflare Access authentication required
 *
 * These routes are mounted BEFORE the auth middleware is applied.
 * Includes: health checks, static assets, and public API endpoints.
 */
const publicRoutes = new Hono<AppEnv>();

// GET /sandbox-health - Health check endpoint
publicRoutes.get('/sandbox-health', (c) => {
  return c.json({
    status: 'ok',
    service: 'moltbot-sandbox',
    gateway_port: MOLTBOT_PORT,
  });
});

// GET /logo.png - Serve logo from ASSETS binding
publicRoutes.get('/logo.png', (c) => {
  return c.env.ASSETS.fetch(c.req.raw);
});

// GET /logo-small.png - Serve small logo from ASSETS binding
publicRoutes.get('/logo-small.png', (c) => {
  return c.env.ASSETS.fetch(c.req.raw);
});

// GET /api/status - Public health check for gateway status (no auth required)
publicRoutes.get('/api/status', async (c) => {
  const sandbox = c.get('sandbox');

  try {
    const process = await findExistingMoltbotProcess(sandbox);
    if (!process) {
      console.log('[Status] No gateway process yet — triggering startup if not already in progress');
      // Ensure startup is triggered (e.g. if first request served loading page but waitUntil didn't run)
      c.executionCtx.waitUntil(
        ensureMoltbotGateway(sandbox, c.env).catch((err: Error) => {
          console.error('[Status] Background startup attempt failed:', err?.message ?? err);
        }),
      );
      return c.json({
        ok: false,
        status: 'no_process',
        message:
          'No gateway process yet. Startup in progress (cold start can take 1–2 minutes).',
        hint: 'If this persists, check worker logs: npx wrangler tail',
      });
    }

    // Process exists, check if it's actually responding
    console.log('[Status] Process found:', process.id, 'status:', process.status, '- checking port');
    try {
      await process.waitForPort(18789, { mode: 'tcp', timeout: 5000 });
      console.log('[Status] Gateway is running and responding on port 18789');
      return c.json({
        ok: true,
        status: 'running',
        processId: process.id,
        message: 'Gateway is ready.',
      });
    } catch {
      console.log('[Status] Process exists but port 18789 not responding yet');
      return c.json({
        ok: false,
        status: 'starting',
        processId: process.id,
        processStatus: process.status,
        message: 'Gateway process is starting, waiting for it to listen on port 18789...',
        hint: 'If this persists >3 min, check worker logs: npx wrangler tail',
      });
    }
  } catch (err) {
    const errorMessage = err instanceof Error ? err.message : 'Unknown error';
    console.error('[Status] Error checking gateway:', errorMessage);
    return c.json({
      ok: false,
      status: 'error',
      error: errorMessage,
      message: 'Failed to check gateway status.',
      hint: 'Check worker logs: npx wrangler tail',
    });
  }
});

// GET /internal/backup - Stream R2 backup tarball for container restore (token auth, no CF Access)
publicRoutes.get('/internal/backup', async (c) => {
  const token =
    c.req.query('token') ?? c.req.header('X-Backup-Token') ?? c.req.header('Authorization')?.replace(/^Bearer\s+/i, '');
  if (!c.env.BACKUP_RESTORE_TOKEN || token !== c.env.BACKUP_RESTORE_TOKEN) {
    return c.json({ error: 'Unauthorized' }, 401);
  }
  const obj = await c.env.MOLTBOT_BUCKET.get(BACKUP_KEY);
  if (!obj || !obj.body) {
    return c.json({ error: 'No backup found' }, 404);
  }
  return new Response(obj.body, {
    headers: {
      'Content-Type': 'application/gzip',
      'Content-Disposition': 'attachment; filename="backup.tar.gz"',
    },
  });
});

// GET /_admin/assets/* - Admin UI static assets (CSS, JS need to load for login redirect)
// Assets are built to dist/client with base "/_admin/"
publicRoutes.get('/_admin/assets/*', async (c) => {
  const url = new URL(c.req.url);
  // Rewrite /_admin/assets/* to /assets/* for the ASSETS binding
  const assetPath = url.pathname.replace('/_admin/assets/', '/assets/');
  const assetUrl = new URL(assetPath, url.origin);
  return c.env.ASSETS.fetch(new Request(assetUrl.toString(), c.req.raw));
});

export { publicRoutes };
