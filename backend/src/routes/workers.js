const express = require('express');
const { insert, findById, findMany, update } = require('../db/store');
const { newId, now } = require('../utils/helpers');
const jobService = require('../services/jobService');
const { broadcast } = require('../services/ws');

const router = express.Router();

router.post('/workers/register', (req, res) => {
  const { hostname, concurrency = 5 } = req.body;
  const worker = insert('workers', {
    id: newId(),
    hostname: hostname || `worker-${Math.random().toString(36).slice(2, 8)}`,
    status: 'online',
    concurrency,
    registered_at: now(),
    last_seen_at: now(),
  });
  res.status(201).json(worker);
});

router.get('/workers', (req, res) => {
  const workers = findMany('workers', () => true).map((w) => {
    const lastHb = findMany('worker_heartbeats', (h) => h.worker_id === w.id).slice(-1)[0];
    const activeJobs = findMany('jobs', (j) => j.claimed_by === w.id && j.status === 'running').length;
    const isStale = Date.now() - new Date(w.last_seen_at).getTime() > 15000;
    return { ...w, status: isStale ? 'offline' : w.status, active_jobs: activeJobs, last_heartbeat: lastHb || null };
  });
  res.json({ data: workers, count: workers.length });
});

// Worker polls this to atomically claim its next job across a set of queues.
router.post('/workers/:id/claim', (req, res) => {
  const worker = findById('workers', req.params.id);
  if (!worker) return res.status(404).json({ error: 'Worker not registered' });
  const { queueIds } = req.body;
  if (!Array.isArray(queueIds) || queueIds.length === 0) {
    return res.status(400).json({ error: 'queueIds (array) is required' });
  }
  const claimed = jobService.claimNextJob(worker.id, queueIds);
  update('workers', worker.id, { last_seen_at: now() });
  if (!claimed) return res.status(204).end();
  res.json(claimed);
});

router.post('/workers/:id/heartbeat', (req, res) => {
  const worker = findById('workers', req.params.id);
  if (!worker) return res.status(404).json({ error: 'Worker not registered' });
  const { active_jobs = 0, cpu_load = null, memory_mb = null } = req.body;
  const hb = insert('worker_heartbeats', {
    id: newId(),
    worker_id: worker.id,
    active_jobs,
    cpu_load,
    memory_mb,
    created_at: now(),
  });
  update('workers', worker.id, { last_seen_at: now(), status: 'online' });
  broadcast({ event: 'worker:heartbeat', worker_id: worker.id, heartbeat: hb });
  res.status(201).json(hb);
});

// Graceful shutdown signal from a worker
router.post('/workers/:id/drain', (req, res) => {
  const worker = update('workers', req.params.id, { status: 'draining' });
  if (!worker) return res.status(404).json({ error: 'Worker not registered' });
  res.json(worker);
});

router.post('/jobs/:jobId/complete', (req, res) => {
  const { executionId, result } = req.body;
  try {
    const job = jobService.completeJob(req.params.jobId, executionId, result);
    res.json(job);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

router.post('/jobs/:jobId/fail', (req, res) => {
  const { executionId, error } = req.body;
  try {
    const job = jobService.failJob(req.params.jobId, executionId, error || 'Unknown error');
    res.json(job);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

module.exports = router;
