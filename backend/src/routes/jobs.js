const express = require('express');
const { findById, findMany } = require('../db/store');
const jobService = require('../services/jobService');
const { loadQueueOr404 } = require('./queues');

const router = express.Router();

// Create a job (immediate | delayed | scheduled | recurring | batch)
router.post('/queues/:id/jobs', (req, res) => {
  const queue = loadQueueOr404(req, res);
  if (!queue) return;
  try {
    if (req.body.type === 'batch') {
      const jobs = jobService.createBatch(queue, req.body.jobs || [], {
        max_retries: req.body.max_retries,
        priority: req.body.priority,
      });
      return res.status(201).json({ data: jobs, count: jobs.length });
    }
    const job = jobService.createJob(queue, req.body);
    res.status(201).json(job);
  } catch (err) {
    res.status(err.status || 500).json({ error: err.message });
  }
});

// List jobs with pagination + filtering (by status, type, queue)
router.get('/queues/:id/jobs', (req, res) => {
  const queue = loadQueueOr404(req, res);
  if (!queue) return;
  const { status, type, page = 1, limit = 20 } = req.query;

  let jobs = findMany('jobs', (j) => j.queue_id === queue.id);
  if (status) jobs = jobs.filter((j) => j.status === status);
  if (type) jobs = jobs.filter((j) => j.type === type);
  jobs.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));

  const p = Math.max(1, parseInt(page));
  const l = Math.min(100, Math.max(1, parseInt(limit)));
  const start = (p - 1) * l;
  const paged = jobs.slice(start, start + l);

  res.json({ data: paged, page: p, limit: l, total: jobs.length, total_pages: Math.ceil(jobs.length / l) });
});

function generateAiFailureSummary(error, type) {
  if (!error) return null;
  const msg = error.toLowerCase();
  let cause = '';
  let remediation = '';
  
  if (msg.includes('simulated') && msg.includes('error')) {
    cause = 'This failure was intentionally triggered by the simulated worker execution failure rate (~15% chance per attempt).';
    remediation = 'To eliminate simulated errors in development, locate `executeJob()` in [worker.js](file:///d:/Project/job-scheduler/backend/src/worker.js) and remove or adjust the random error generator block (lines 37-40).';
  } else if (msg.includes('timeout') || msg.includes('heartbeat') || msg.includes('deadlock')) {
    cause = 'The worker failed to respond in time or experienced a resource deadlock. No heartbeat was received during the execution threshold.';
    remediation = 'Verify worker system resources (CPU/Memory). If executing heavy computations, consider increasing `HEARTBEAT_INTERVAL_MS` or running more worker instances to scale execution horizontally.';
  } else if (msg.includes('fetch') || msg.includes('network') || msg.includes('conn')) {
    cause = 'A network exception occurred when the worker process attempted to report results to the API server.';
    remediation = 'Ensure the API server at `http://localhost:4000` is fully online and responsive. Check firewall rules or virtual network setups if running workers on a different machine.';
  } else if (msg.includes('json') || msg.includes('syntax')) {
    cause = 'A parsing exception occurred, likely due to invalid payload formatting or malformed response data in the execution handler.';
    remediation = 'Inspect the job payload JSON structure. Ensure the input parameters align with the expectation of the job handler.';
  } else {
    cause = `An unhandled runtime error was thrown: "${error}".`;
    remediation = 'Inspect worker standard error logs for a full stack trace. Add comprehensive try/catch boundaries within the worker task executor to capture more granular exception context.';
  }

  return `### 🧠 Pulse AI™ Diagnostic Report

**Detected Exception**: \`${error}\`  
**Root Cause Analysis**: ${cause}

#### 📋 Recommended Remediation Steps:
1. ${remediation}
2. Validate that the queue's retry policy is configured with backoff limits suitable for transient network anomalies.
3. Review job execution history details in the logs below to correlate worker status.`;
}

// Job detail: includes executions + logs (execution history requirement)
router.get('/jobs/:jobId', (req, res) => {
  const job = findById('jobs', req.params.jobId);
  if (!job) return res.status(404).json({ error: 'Job not found' });
  const executions = findMany('job_executions', (e) => e.job_id === job.id);
  const logs = findMany('job_logs', (l) => l.job_id === job.id).sort(
    (a, b) => new Date(a.created_at) - new Date(b.created_at)
  );

  const lastFailedExecution = executions
    .filter(e => e.status === 'failed')
    .sort((a, b) => new Date(b.finished_at) - new Date(a.finished_at))[0];
  const lastError = lastFailedExecution ? lastFailedExecution.error_message : null;
  const ai_failure_summary = lastError ? generateAiFailureSummary(lastError, job.type) : null;

  res.json({ ...job, executions, logs, ai_failure_summary });
});

// Dead Letter Queue: list + manual retry
router.get('/queues/:id/dlq', (req, res) => {
  const queue = loadQueueOr404(req, res);
  if (!queue) return;
  const entries = findMany('dead_letter_queue', (e) => e.queue_id === queue.id);
  res.json({ data: entries, count: entries.length });
});

router.post('/dlq/:dlqEntryId/retry', (req, res) => {
  const job = jobService.retryDlqJob(req.params.dlqEntryId);
  if (!job) return res.status(404).json({ error: 'Dead Letter Queue entry not found' });
  res.json(job);
});

module.exports = router;
