import type { Sandbox, Process } from '@cloudflare/sandbox';
import type { MoltbotEnv } from '../types';
import { MOLTBOT_PORT, STARTUP_TIMEOUT_MS } from '../config';
import { buildEnvVars } from './env';
import { mountR2Storage } from './r2';
import { withStartupLock } from './startup-state';

/**
 * Pre-flight: ensure at least one AI provider is configured so we fail fast with a clear message.
 */
function assertAiProviderConfigured(env: MoltbotEnv): void {
  const hasCloudflare =
    !!(env.CLOUDFLARE_AI_GATEWAY_API_KEY && env.CF_AI_GATEWAY_ACCOUNT_ID && env.CF_AI_GATEWAY_GATEWAY_ID);
  const hasLegacy = !!(env.AI_GATEWAY_API_KEY && env.AI_GATEWAY_BASE_URL);
  const hasAnthropic = !!env.ANTHROPIC_API_KEY;
  const hasOpenAI = !!env.OPENAI_API_KEY;
  if (!hasCloudflare && !hasLegacy && !hasAnthropic && !hasOpenAI) {
    throw new Error(
      'No AI provider configured. Set ANTHROPIC_API_KEY, OPENAI_API_KEY, or Cloudflare AI Gateway secrets.',
    );
  }
}

/**
 * Find an existing OpenClaw gateway process
 *
 * @param sandbox - The sandbox instance
 * @returns The process if found and running/starting, null otherwise
 */
const LOG_PREFIX = '[Gateway]';

export async function findExistingMoltbotProcess(sandbox: Sandbox): Promise<Process | null> {
  try {
    const processes = await sandbox.listProcesses();
    for (const proc of processes) {
      // Match gateway process (openclaw gateway or legacy clawdbot gateway)
      // Don't match CLI commands like "openclaw devices list"
      const isGatewayProcess =
        proc.command.includes('start-openclaw.sh') ||
        proc.command.includes('openclaw gateway') ||
        // Legacy: match old startup script during transition
        proc.command.includes('start-moltbot.sh') ||
        proc.command.includes('clawdbot gateway');
      const isCliCommand =
        proc.command.includes('openclaw devices') ||
        proc.command.includes('openclaw --version') ||
        proc.command.includes('openclaw onboard') ||
        proc.command.includes('clawdbot devices') ||
        proc.command.includes('clawdbot --version');

      if (isGatewayProcess && !isCliCommand) {
        if (proc.status === 'starting' || proc.status === 'running') {
          return proc;
        }
      }
    }
  } catch (e) {
    console.error(LOG_PREFIX, 'Could not list processes:', e);
  }
  return null;
}

/**
 * Ensure the OpenClaw gateway is running (internal: runs inside withStartupLock).
 * 1. Pre-flight AI provider check
 * 2. Mount R2 if configured
 * 3. Reuse existing process or start new one and wait for port
 */
async function doEnsureMoltbotGateway(sandbox: Sandbox, env: MoltbotEnv): Promise<Process> {
  const t0 = Date.now();
  const elapsed = () => `${Date.now() - t0}ms`;

  console.log(LOG_PREFIX, 'ensureMoltbotGateway: starting');
  assertAiProviderConfigured(env);

  // Mount R2 storage (non-blocking)
  console.log(LOG_PREFIX, 'Step 1/3: Mounting R2 storage (if configured)...');
  const r2Mounted = await mountR2Storage(sandbox, env);
  if (!r2Mounted) {
    console.log(LOG_PREFIX, 'Step 1/3: R2 not mounted — gateway will still start (' + elapsed() + ')');
  } else {
    console.log(LOG_PREFIX, 'Step 1/3: R2 mount done (' + elapsed() + ')');
  }

  console.log(LOG_PREFIX, 'Step 2/3: Checking for existing gateway process...');
  const existingProcess = await findExistingMoltbotProcess(sandbox);
  if (existingProcess) {
    console.log(
      LOG_PREFIX,
      'Found existing gateway process:',
      existingProcess.id,
      'status:',
      existingProcess.status,
      '(' + elapsed() + ')',
    );
    try {
      await existingProcess.waitForPort(MOLTBOT_PORT, {
        mode: 'tcp',
        timeout: STARTUP_TIMEOUT_MS,
      });
      console.log(LOG_PREFIX, 'Gateway is reachable on port', MOLTBOT_PORT, '(' + elapsed() + ')');
      return existingProcess;
    } catch (_e) {
      console.error(LOG_PREFIX, 'Existing process not reachable after timeout — killing and will restart (' + elapsed() + ')');
      try {
        await existingProcess.kill();
      } catch (killError) {
        console.error(LOG_PREFIX, 'Failed to kill process:', killError);
      }
    }
  } else {
    console.log(LOG_PREFIX, 'No existing gateway process (' + elapsed() + ')');
  }

  console.log(LOG_PREFIX, 'Step 3/3: Starting new OpenClaw gateway...');
  const envVars = buildEnvVars(env);
  const command = '/usr/local/bin/start-openclaw.sh';
  console.log(LOG_PREFIX, 'Command:', command, '| Env keys:', Object.keys(envVars).length);

  let process: Process;
  try {
    process = await sandbox.startProcess(command, {
      env: Object.keys(envVars).length > 0 ? envVars : undefined,
    });
    console.log(LOG_PREFIX, 'Process started — id:', process.id, 'status:', process.status, '(' + elapsed() + ')');
  } catch (startErr) {
    console.error(LOG_PREFIX, 'Failed to start process:', startErr, '(' + elapsed() + ')');
    throw startErr;
  }

  try {
    await process.waitForPort(MOLTBOT_PORT, { mode: 'tcp', timeout: STARTUP_TIMEOUT_MS });
    console.log(LOG_PREFIX, 'OpenClaw gateway is ready on port', MOLTBOT_PORT, '(' + elapsed() + ')');
    const logs = await process.getLogs();
    if (logs.stdout) console.log(LOG_PREFIX, 'stdout (recent):', logs.stdout.slice(-500));
    if (logs.stderr) console.log(LOG_PREFIX, 'stderr (recent):', logs.stderr?.slice(-500));
  } catch (e) {
    console.error(LOG_PREFIX, 'waitForPort failed:', e, '(' + elapsed() + ')');
    try {
      const logs = await process.getLogs();
      console.error(LOG_PREFIX, 'Startup failed. Stderr:', logs.stderr || '(empty)');
      console.error(LOG_PREFIX, 'Startup failed. Stdout:', logs.stdout || '(empty)');
      throw new Error(`OpenClaw gateway failed to start. Stderr: ${(logs.stderr || '(empty)').slice(0, 500)}`, {
        cause: e,
      });
    } catch (logErr) {
      console.error(LOG_PREFIX, 'Failed to get logs:', logErr);
      throw e;
    }
  }

  return process;
}

/**
 * Ensure the OpenClaw gateway is running. Deduplicates concurrent calls and records failure for /api/status.
 */
export async function ensureMoltbotGateway(sandbox: Sandbox, env: MoltbotEnv): Promise<Process> {
  return withStartupLock(() => doEnsureMoltbotGateway(sandbox, env));
}
