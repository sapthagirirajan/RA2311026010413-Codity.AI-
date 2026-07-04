const test = require('node:test');
const assert = require('node:assert');

const { computeRetryDelayMs } = require('./utils/helpers');
const { insert, db } = require('./db/store');
const jobService = require('./services/jobService');

test('computeRetryDelayMs - fixed strategy', () => {
  const policy = { strategy: 'fixed', base_delay_ms: 1000, max_delay_ms: 5000 };
  const delay = computeRetryDelayMs(policy, 2);
  assert.ok(delay >= 1000);
  assert.ok(delay <= 1100);
});

test('computeRetryDelayMs - linear strategy', () => {
  const policy = { strategy: 'linear', base_delay_ms: 1000, max_delay_ms: 5000 };
  const delay = computeRetryDelayMs(policy, 3);
  assert.ok(delay >= 3000);
  assert.ok(delay <= 3300);
});

test('computeRetryDelayMs - exponential strategy', () => {
  const policy = { strategy: 'exponential', base_delay_ms: 1000, max_delay_ms: 5000 };
  const delay = computeRetryDelayMs(policy, 3);
  assert.ok(delay >= 4000);
  assert.ok(delay <= 4400);
});

test('computeRetryDelayMs - max delay capped', () => {
  const policy = { strategy: 'exponential', base_delay_ms: 1000, max_delay_ms: 2000 };
  const delay = computeRetryDelayMs(policy, 5);
  assert.ok(delay <= 2200);
});

test('jobService - createJob and claimNextJob', () => {
  // Clear for isolation
  db.jobs = [];
  db.queues = [];
  db.retry_policies = [];

  const queue = insert('queues', {
    id: 'q-test-1',
    project_id: 'proj-test',
    name: 'test-queue',
    priority: 5,
    concurrency_limit: 2,
    is_paused: false,
    rate_limit_per_min: null,
    created_at: new Date().toISOString()
  });

  insert('retry_policies', {
    id: 'p-test-1',
    queue_id: queue.id,
    strategy: 'fixed',
    base_delay_ms: 1000,
    max_retries: 3,
    max_delay_ms: 10000
  });

  const job1 = jobService.createJob(queue, {
    type: 'immediate',
    payload: { task: 'test1' },
    priority: 10
  });

  const job2 = jobService.createJob(queue, {
    type: 'immediate',
    payload: { task: 'test2' },
    priority: 5
  });

  assert.strictEqual(db.jobs.length, 2);
  assert.strictEqual(job1.status, 'queued');

  // Claim next job: job1 has priority 10, job2 has 5
  const claimed1 = jobService.claimNextJob('worker-1', [queue.id]);
  assert.ok(claimed1);
  assert.strictEqual(claimed1.job.id, job1.id);
  assert.strictEqual(claimed1.job.status, 'running');

  // Claim next job: should be job2
  const claimed2 = jobService.claimNextJob('worker-1', [queue.id]);
  assert.ok(claimed2);
  assert.strictEqual(claimed2.job.id, job2.id);

  // Concurrency limit reached, should block third claim
  jobService.createJob(queue, { type: 'immediate', payload: { task: 'test3' } });
  const claimed3 = jobService.claimNextJob('worker-1', [queue.id]);
  assert.strictEqual(claimed3, null);
});

test('jobService - paused queue ignores jobs', () => {
  db.jobs = [];
  db.queues = [];
  
  const queue = insert('queues', {
    id: 'q-test-paused',
    name: 'paused-queue',
    is_paused: true,
    concurrency_limit: 5,
    priority: 1
  });

  jobService.createJob(queue, { type: 'immediate', payload: { data: 1 } });
  const claimed = jobService.claimNextJob('worker-1', [queue.id]);
  assert.strictEqual(claimed, null);
});

test('jobService - sweepOfflineWorkers reschedules job when worker goes silent', () => {
  db.jobs = [];
  db.queues = [];
  db.workers = [];
  db.job_executions = [];
  db.retry_policies = [];

  const queue = insert('queues', {
    id: 'q-test-sweep',
    name: 'sweep-queue',
    is_paused: false,
    concurrency_limit: 5,
    priority: 1
  });

  insert('retry_policies', {
    id: 'p-test-sweep',
    queue_id: queue.id,
    strategy: 'fixed',
    base_delay_ms: 1000,
    max_retries: 3,
    max_delay_ms: 10000
  });

  const worker = insert('workers', {
    id: 'worker-stale',
    hostname: 'worker-stale',
    status: 'online',
    concurrency: 5,
    registered_at: new Date(Date.now() - 20000).toISOString(),
    last_seen_at: new Date(Date.now() - 20000).toISOString(),
  });

  const job = jobService.createJob(queue, { type: 'immediate', payload: { test: 1 } });
  
  // Claim the job so it starts running
  const claimed = jobService.claimNextJob(worker.id, [queue.id]);
  assert.ok(claimed);
  assert.strictEqual(claimed.job.status, 'running');

  // Run sweep -- should mark worker offline and reschedule the job
  const sweptCount = jobService.sweepOfflineWorkers();
  assert.strictEqual(sweptCount, 1);

  // Check worker status
  const updatedWorker = db.workers.find(w => w.id === worker.id);
  assert.strictEqual(updatedWorker.status, 'offline');

  // Check job status (since retry is scheduled, it should be 'scheduled' with updated run_at and claim cleared)
  const updatedJob = db.jobs.find(j => j.id === job.id);
  assert.strictEqual(updatedJob.status, 'scheduled');
  assert.strictEqual(updatedJob.claimed_by, null);
  assert.strictEqual(updatedJob.attempt_count, 1);
});
