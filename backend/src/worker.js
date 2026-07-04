/**
 * Standalone Worker Service.
 *
 * Run as its own process: `node src/worker.js`
 * Multiple instances can run in parallel (different terminals/machines) --
 * they all talk to the API over HTTP, and the server performs the atomic
 * claim, so two workers can never grab the same job.
 */
const API = process.env.API_URL || 'http://localhost:4000/api';
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS || 1000);
const HEARTBEAT_INTERVAL_MS = Number(process.env.HEARTBEAT_INTERVAL_MS || 5000);
const CONCURRENCY = Number(process.env.WORKER_CONCURRENCY || 3);

// Which queues this worker instance services (comma-separated queue ids), or "all discoverable" if left blank.
const QUEUE_IDS = (process.env.QUEUE_IDS || '').split(',').filter(Boolean);

let workerId = null;
let shuttingDown = false;
let activeJobs = 0;

async function api(path, opts = {}) {
  const res = await fetch(`${API}${path}`, {
    method: opts.method || 'GET',
    headers: { 'Content-Type': 'application/json' },
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  if (res.status === 204) return null;
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
  return data;
}

/** Simulated job execution. Replace with real job-handler dispatch by job payload/type. */
async function executeJob(job) {
  const durationMs = 300 + Math.random() * 1200;
  await new Promise((resolve) => setTimeout(resolve, durationMs));
  // Simulate an ~15% failure rate so retry/backoff/DLQ logic is exercised.
  if (Math.random() < 0.15) {
    throw new Error('Simulated processing error');
  }
  return { processed: true, payload_echo: job.payload, took_ms: Math.round(durationMs) };
}

async function pollLoop() {
  while (!shuttingDown) {
    if (activeJobs >= CONCURRENCY) {
      await sleep(POLL_INTERVAL_MS);
      continue;
    }
    try {
      const queueIds = QUEUE_IDS.length ? QUEUE_IDS : await discoverAllQueueIds();
      if (queueIds.length === 0) {
        await sleep(POLL_INTERVAL_MS);
        continue;
      }
      const claimed = await api(`/workers/${workerId}/claim`, { method: 'POST', body: { queueIds } });
      if (!claimed) {
        await sleep(POLL_INTERVAL_MS);
        continue;
      }
      activeJobs++;
      runJob(claimed).finally(() => activeJobs--);
    } catch (err) {
      console.error('[worker] poll error:', err.message);
      await sleep(POLL_INTERVAL_MS);
    }
  }
}

async function runJob({ job, execution }) {
  console.log(`[worker ${workerId}] running job ${job.id} (attempt ${execution.attempt_number})`);
  try {
    const result = await executeJob(job);
    await api(`/jobs/${job.id}/complete`, { method: 'POST', body: { executionId: execution.id, result } });
    console.log(`[worker ${workerId}] job ${job.id} completed`);
  } catch (err) {
    await api(`/jobs/${job.id}/fail`, { method: 'POST', body: { executionId: execution.id, error: err.message } });
    console.log(`[worker ${workerId}] job ${job.id} failed: ${err.message}`);
  }
}

// NOTE: requires a way to list queues without a user JWT. For the demo we allow
// passing QUEUE_IDS explicitly (recommended) -- see README "Running the demo".
async function discoverAllQueueIds() {
  return QUEUE_IDS;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function heartbeatLoop() {
  while (!shuttingDown) {
    try {
      await api(`/workers/${workerId}/heartbeat`, {
        method: 'POST',
        body: { active_jobs: activeJobs, cpu_load: Math.random().toFixed(2), memory_mb: 120 + Math.round(Math.random() * 40) },
      });
    } catch (err) {
      console.error('[worker] heartbeat error:', err.message);
    }
    await sleep(HEARTBEAT_INTERVAL_MS);
  }
}

async function main() {
  const hostname = process.env.WORKER_NAME || `worker-${process.pid}`;
  const worker = await api('/workers/register', { method: 'POST', body: { hostname, concurrency: CONCURRENCY } });
  workerId = worker.id;
  console.log(`[worker] registered as ${hostname} (id=${workerId}), concurrency=${CONCURRENCY}`);
  if (QUEUE_IDS.length === 0) {
    console.log('[worker] WARNING: no QUEUE_IDS set -- worker will idle. Set QUEUE_IDS=<id1>,<id2> env var.');
  }

  pollLoop();
  heartbeatLoop();
}

// ---- Graceful shutdown: stop claiming new work, let in-flight jobs finish ----
async function shutdown() {
  console.log('[worker] shutdown signal received, draining...');
  shuttingDown = true;
  try {
    if (workerId) await api(`/workers/${workerId}/drain`, { method: 'POST' });
  } catch {}
  const waitStart = Date.now();
  while (activeJobs > 0 && Date.now() - waitStart < 30000) {
    await sleep(300);
  }
  console.log('[worker] drained, exiting.');
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

main().catch((err) => {
  console.error('[worker] fatal error on startup:', err.message);
  process.exit(1);
});
