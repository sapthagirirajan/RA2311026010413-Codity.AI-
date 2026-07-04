const { insert, findById, findMany, update, db } = require('../db/store');
const { now, newId, computeRetryDelayMs } = require('../utils/helpers');
const { broadcast } = require('./ws');

const VALID_TYPES = ['immediate', 'delayed', 'scheduled', 'recurring', 'batch'];

/** In-memory per-queue rate-limit tracker (bonus feature: rate limiting). */
const rateWindows = new Map(); // queueId -> [timestamps]

function checkQueueRateLimit(queue) {
  if (!queue.rate_limit_per_min) return true;
  const windowStart = Date.now() - 60_000;
  const hits = (rateWindows.get(queue.id) || []).filter((t) => t > windowStart);
  if (hits.length >= queue.rate_limit_per_min) {
    rateWindows.set(queue.id, hits);
    return false;
  }
  hits.push(Date.now());
  rateWindows.set(queue.id, hits);
  return true;
}

function log(jobId, message, level = 'info', executionId = null) {
  insert('job_logs', {
    id: newId(),
    job_id: jobId,
    execution_id: executionId,
    level,
    message,
    created_at: now(),
  });
}

/** Create a job of any of the 5 required types. */
function createJob(queue, { type, payload, run_at, cron_expression, priority, max_retries, idempotency_key, batch_id }) {
  if (!VALID_TYPES.includes(type)) {
    throw Object.assign(new Error(`Invalid job type. Must be one of ${VALID_TYPES.join(', ')}`), { status: 400 });
  }
  if (idempotency_key) {
    const existing = findMany('jobs', (j) => j.queue_id === queue.id && j.idempotency_key === idempotency_key)[0];
    if (existing) return existing; // idempotent create
  }
  if (!checkQueueRateLimit(queue)) {
    throw Object.assign(new Error('Queue rate limit exceeded, try again shortly'), { status: 429 });
  }

  let status = 'queued';
  if (type === 'delayed' || type === 'scheduled') {
    if (!run_at) throw Object.assign(new Error(`'run_at' is required for ${type} jobs`), { status: 400 });
    status = 'scheduled';
  }
  if (type === 'recurring' && !cron_expression) {
    throw Object.assign(new Error(`'cron_expression' is required for recurring jobs`), { status: 400 });
  }

  const job = insert('jobs', {
    id: newId(),
    queue_id: queue.id,
    type,
    payload: payload || {},
    status,
    priority: priority ?? queue.priority ?? 0,
    run_at: run_at || null,
    cron_expression: cron_expression || null,
    batch_id: batch_id || null,
    attempt_count: 0,
    max_retries: max_retries ?? 3,
    idempotency_key: idempotency_key || null,
    claimed_by: null,
    claimed_at: null,
    started_at: null,
    completed_at: null,
    created_at: now(),
    updated_at: now(),
  });
  log(job.id, `Job created (type=${type}, status=${status})`);
  broadcast({ event: 'job:created', job });
  return job;
}

/** Create N jobs at once (batch job type). */
function createBatch(queue, jobsSpec, batchOpts = {}) {
  const batchId = newId();
  return jobsSpec.map((spec) =>
    createJob(queue, { ...spec, ...batchOpts, type: spec.type || 'immediate', batch_id: batchId })
  );
}

/**
 * Atomically claim the next eligible job for a worker.
 * "Atomic" here means: the read-eligibility-check + status-flip happens
 * inside one synchronous function call, so no other request can interleave
 * (see store.js comment). In Postgres this maps to:
 *   UPDATE jobs SET status='claimed', claimed_by=$1
 *   WHERE id = (SELECT id FROM jobs WHERE status='queued' AND queue_id = ANY($2)
 *               ORDER BY priority DESC, created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED)
 *   RETURNING *;
 */
function claimNextJob(workerId, queueIds) {
  const eligible = findMany(
    'jobs',
    (j) => queueIds.includes(j.queue_id) && j.status === 'queued'
  ).sort((a, b) => b.priority - a.priority || new Date(a.created_at) - new Date(b.created_at));

  for (const job of eligible) {
    const queue = findById('queues', job.queue_id);
    if (queue.is_paused) continue;
    const runningInQueue = findMany('jobs', (j) => j.queue_id === queue.id && j.status === 'running').length;
    if (runningInQueue >= queue.concurrency_limit) continue;

    // Flip status immediately -- this is the atomic claim.
    update('jobs', job.id, { status: 'claimed', claimed_by: workerId, claimed_at: now() });
    const execution = insert('job_executions', {
      id: newId(),
      job_id: job.id,
      worker_id: workerId,
      attempt_number: job.attempt_count + 1,
      status: 'running',
      started_at: now(),
      finished_at: null,
      duration_ms: null,
      error_message: null,
      result: null,
    });
    update('jobs', job.id, { status: 'running', started_at: now() });
    log(job.id, `Claimed by worker ${workerId}, attempt ${execution.attempt_number}`, 'info', execution.id);
    broadcast({ event: 'job:claimed', job: findById('jobs', job.id) });
    return { job: findById('jobs', job.id), execution };
  }
  return null;
}

function completeJob(jobId, executionId, result) {
  const finishedAt = now();
  const execution = findById('job_executions', executionId);
  update('job_executions', executionId, {
    status: 'completed',
    finished_at: finishedAt,
    duration_ms: new Date(finishedAt) - new Date(execution.started_at),
    result: result || null,
  });
  const job = update('jobs', jobId, {
    status: 'completed',
    completed_at: finishedAt,
    attempt_count: (findById('jobs', jobId).attempt_count || 0) + 1,
  });
  log(jobId, 'Job completed successfully', 'info', executionId);
  broadcast({ event: 'job:completed', job });
  return job;
}

/** Handle a failed attempt: retry with backoff, or move to Dead Letter Queue. */
function failJob(jobId, executionId, errorMessage) {
  const finishedAt = now();
  const execution = findById('job_executions', executionId);
  update('job_executions', executionId, {
    status: 'failed',
    finished_at: finishedAt,
    duration_ms: new Date(finishedAt) - new Date(execution.started_at),
    error_message: errorMessage,
  });

  const job = findById('jobs', jobId);
  const newAttemptCount = job.attempt_count + 1;
  const queue = findById('queues', job.queue_id);
  const policy = findMany('retry_policies', (p) => p.queue_id === queue.id)[0] || {
    strategy: 'exponential', base_delay_ms: 1000, max_retries: job.max_retries, max_delay_ms: 60000,
  };

  log(jobId, `Attempt ${newAttemptCount} failed: ${errorMessage}`, 'error', executionId);

  if (newAttemptCount >= (job.max_retries ?? policy.max_retries)) {
    // Permanent failure -> Dead Letter Queue
    update('jobs', jobId, { status: 'dead_letter', attempt_count: newAttemptCount });
    insert('dead_letter_queue', {
      id: newId(),
      job_id: jobId,
      queue_id: queue.id,
      final_error: errorMessage,
      attempt_count: newAttemptCount,
      moved_at: now(),
      payload_snapshot: job.payload,
    });
    log(jobId, `Moved to Dead Letter Queue after ${newAttemptCount} attempts`, 'error', executionId);
    broadcast({ event: 'job:dead_letter', job: findById('jobs', jobId) });
    return findById('jobs', jobId);
  }

  const delayMs = computeRetryDelayMs(policy, newAttemptCount);
  const nextRunAt = new Date(Date.now() + delayMs).toISOString();
  const updated = update('jobs', jobId, {
    status: 'scheduled',
    attempt_count: newAttemptCount,
    run_at: nextRunAt,
    claimed_by: null,
  });
  log(jobId, `Scheduled retry #${newAttemptCount + 1} in ${delayMs}ms (${policy.strategy} backoff)`, 'warn', executionId);
  broadcast({ event: 'job:retry_scheduled', job: updated });
  return updated;
}

/** Promote scheduled/delayed jobs whose run_at has passed -> queued. Called by the scheduler tick. */
function promoteDueJobs() {
  const due = findMany('jobs', (j) => j.status === 'scheduled' && j.run_at && new Date(j.run_at) <= new Date());
  for (const job of due) {
    update('jobs', job.id, { status: 'queued' });
    broadcast({ event: 'job:queued', job: findById('jobs', job.id) });
  }
  return due.length;
}

/** Find silent workers, mark them offline, and reschedule their running jobs. */
function sweepOfflineWorkers() {
  const staleTime = 15000; // 15 seconds
  const activeWorkers = findMany('workers', (w) => w.status !== 'offline');
  let sweptCount = 0;

  for (const worker of activeWorkers) {
    const elapsed = Date.now() - new Date(worker.last_seen_at || worker.registered_at).getTime();
    if (elapsed > staleTime) {
      update('workers', worker.id, { status: 'offline' });
      const runningJobs = findMany('jobs', (j) => j.claimed_by === worker.id && (j.status === 'running' || j.status === 'claimed'));
      
      for (const job of runningJobs) {
        const activeExec = findMany('job_executions', (e) => e.job_id === job.id && e.worker_id === worker.id && e.status === 'running')[0];
        if (activeExec) {
          failJob(job.id, activeExec.id, 'Worker heartbeat timeout: worker went offline during execution');
        } else {
          // Fallback if no execution is found: just reschedule directly
          const newAttemptCount = job.attempt_count + 1;
          if (newAttemptCount >= job.max_retries) {
            update('jobs', job.id, { status: 'dead_letter', attempt_count: newAttemptCount });
            insert('dead_letter_queue', {
              id: newId(),
              job_id: job.id,
              queue_id: job.queue_id,
              final_error: 'Worker went offline (no execution trace)',
              attempt_count: newAttemptCount,
              moved_at: now(),
              payload_snapshot: job.payload,
            });
            broadcast({ event: 'job:dead_letter', job: findById('jobs', job.id) });
          } else {
            update('jobs', job.id, {
              status: 'queued',
              attempt_count: newAttemptCount,
              claimed_by: null,
              started_at: null,
            });
            broadcast({ event: 'job:queued', job: findById('jobs', job.id) });
          }
        }
      }
      sweptCount++;
    }
  }
  return sweptCount;
}

function retryDlqJob(dlqEntryId) {
  const entry = findById('dead_letter_queue', dlqEntryId);
  if (!entry) return null;
  const job = update('jobs', entry.job_id, { status: 'queued', attempt_count: 0, claimed_by: null });
  log(job.id, 'Manually re-queued from Dead Letter Queue');
  broadcast({ event: 'job:requeued', job });
  return job;
}

module.exports = {
  createJob, createBatch, claimNextJob, completeJob, failJob,
  promoteDueJobs, sweepOfflineWorkers, retryDlqJob, log,
};
