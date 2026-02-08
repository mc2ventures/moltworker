import type { Sandbox, Process } from '@cloudflare/sandbox';
import type { MoltbotEnv } from '../types';
import { MOLTBOT_PORT, STARTUP_TIMEOUT_MS } from '../config';
import { buildEnvVars } from './env';
import { mountR2Storage } from './r2';

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
 * Ensure the OpenClaw gateway is running
 *
 * This will:
 * 1. Mount R2 storage if configured
 * 2. Check for an existing gateway process
 * 3. Wait for it to be ready, or start a new one
 *
 * @param sandbox - The sandbox instance
 * @param env - Worker environment bindings
 * @returns The running gateway process
 */
export async function ensureMoltbotGateway(sandbox: Sandbox, env: MoltbotEnv): Promise<Process> {
  console.log(LOG_PREFIX, 'ensureMoltbotGateway: starting');

  // Mount R2 storage for persistent data. Non-blocking: if mount fails (e.g. FUSE unavailable),
  // we continue; gateway starts and only backup/restore is skipped.
  console.log(LOG_PREFIX, 'Step 1/3: Mounting R2 storage (if configured)...');
  const r2Mounted = await mountR2Storage(sandbox, env);
  if (!r2Mounted) {
    console.log(LOG_PREFIX, 'Step 1/3: R2 not mounted — gateway will still start; backup/restore skipped.');
  } else {
    console.log(LOG_PREFIX, 'Step 1/3: R2 mount done');
  }

  // Check if gateway is already running or starting
  console.log(LOG_PREFIX, 'Step 2/3: Checking for existing gateway process...');
  const existingProcess = await findExistingMoltbotProcess(sandbox);
  if (existingProcess) {
    console.log(
      LOG_PREFIX,
      'Found existing gateway process:',
      existingProcess.id,
      'status:',
      existingProcess.status,
    );

    // Always use full startup timeout - a process can be "running" but not ready yet
    try {
      console.log(
        LOG_PREFIX,
        'Waiting for port',
        MOLTBOT_PORT,
        '(timeout',
        STARTUP_TIMEOUT_MS,
        'ms)...',
      );
      await existingProcess.waitForPort(MOLTBOT_PORT, {
        mode: 'tcp',
        timeout: STARTUP_TIMEOUT_MS,
      });
      console.log(LOG_PREFIX, 'Gateway is reachable on port', MOLTBOT_PORT);
      return existingProcess;
      // eslint-disable-next-line no-unused-vars
    } catch (_e) {
      console.error(
        LOG_PREFIX,
        'Existing process not reachable after timeout — killing and will restart',
      );
      try {
        await existingProcess.kill();
      } catch (killError) {
        console.error(LOG_PREFIX, 'Failed to kill process:', killError);
      }
    }
  } else {
    console.log(LOG_PREFIX, 'No existing gateway process');
  }

  // Start a new OpenClaw gateway
  console.log(LOG_PREFIX, 'Step 3/3: Starting new OpenClaw gateway...');
  const envVars = buildEnvVars(env);
  const command = '/usr/local/bin/start-openclaw.sh';
  console.log(LOG_PREFIX, 'Command:', command, '| Env keys:', Object.keys(envVars).length);

  let process: Process;
  try {
    process = await sandbox.startProcess(command, {
      env: Object.keys(envVars).length > 0 ? envVars : undefined,
    });
    console.log(LOG_PREFIX, 'Process started — id:', process.id, 'status:', process.status);
  } catch (startErr) {
    console.error(LOG_PREFIX, 'Failed to start process:', startErr);
    throw startErr;
  }

  // Wait for the gateway to be ready
  try {
    console.log(
      LOG_PREFIX,
      'Waiting for OpenClaw to listen on port',
      MOLTBOT_PORT,
      '(timeout',
      STARTUP_TIMEOUT_MS,
      'ms)...',
    );
    await process.waitForPort(MOLTBOT_PORT, { mode: 'tcp', timeout: STARTUP_TIMEOUT_MS });
    console.log(LOG_PREFIX, 'OpenClaw gateway is ready on port', MOLTBOT_PORT);

    const logs = await process.getLogs();
    if (logs.stdout) console.log(LOG_PREFIX, 'stdout (recent):', logs.stdout.slice(-500));
    if (logs.stderr) console.log(LOG_PREFIX, 'stderr (recent):', logs.stderr?.slice(-500));
  } catch (e) {
    console.error(LOG_PREFIX, 'waitForPort failed:', e);
    try {
      const logs = await process.getLogs();
      console.error(LOG_PREFIX, 'Startup failed. Stderr:', logs.stderr || '(empty)');
      console.error(LOG_PREFIX, 'Startup failed. Stdout:', logs.stdout || '(empty)');
      throw new Error(`OpenClaw gateway failed to start. Stderr: ${logs.stderr || '(empty)'}`, {
        cause: e,
      });
    } catch (logErr) {
      console.error(LOG_PREFIX, 'Failed to get logs:', logErr);
      throw e;
    }
  }

  return process;
}
