const express = require('express');
const { insert, findById, findMany, update } = require('../db/store');
const { newId, now } = require('../utils/helpers');

const router = express.Router();

function loadProjectOr404(req, res) {
  const project = findById('projects', req.params.projectId);
  if (!project || project.organization_id !== req.user.organization_id) {
    res.status(404).json({ error: 'Project not found' });
    return null;
  }
  return project;
}

function loadQueueOr404(req, res) {
  const queue = findById('queues', req.params.queueId || req.params.id);
  if (!queue) {
    res.status(404).json({ error: 'Queue not found' });
    return null;
  }
  const project = findById('projects', queue.project_id);
  if (project.organization_id !== req.user.organization_id) {
    res.status(404).json({ error: 'Queue not found' });
    return null;
  }
  return queue;
}

// Create queue under a project
router.post('/projects/:projectId/queues', (req, res) => {
  const project = loadProjectOr404(req, res);
  if (!project) return;
  const { name, priority = 0, concurrency_limit = 5, rate_limit_per_min, retry_policy } = req.body;
  if (!name) return res.status(400).json({ error: 'name is required' });

  const queue = insert('queues', {
    id: newId(),
    project_id: project.id,
    name,
    priority,
    concurrency_limit,
    is_paused: false,
    rate_limit_per_min: rate_limit_per_min || null,
    created_at: now(),
  });

  insert('retry_policies', {
    id: newId(),
    queue_id: queue.id,
    strategy: retry_policy?.strategy || 'exponential',
    base_delay_ms: retry_policy?.base_delay_ms ?? 1000,
    max_retries: retry_policy?.max_retries ?? 3,
    max_delay_ms: retry_policy?.max_delay_ms ?? 60000,
  });

  res.status(201).json(queue);
});

router.get('/projects/:projectId/queues', (req, res) => {
  const project = loadProjectOr404(req, res);
  if (!project) return;
  const queues = findMany('queues', (q) => q.project_id === project.id).map(withStats);
  res.json({ data: queues, count: queues.length });
});

function withStats(queue) {
  const jobs = findMany('jobs', (j) => j.queue_id === queue.id);
  const byStatus = jobs.reduce((acc, j) => ((acc[j.status] = (acc[j.status] || 0) + 1), acc), {});
  const policy = findMany('retry_policies', (p) => p.queue_id === queue.id)[0];
  return { ...queue, retry_policy: policy, stats: { total_jobs: jobs.length, ...byStatus } };
}

router.get('/queues/:id', (req, res) => {
  const queue = loadQueueOr404(req, res);
  if (!queue) return;
  res.json(withStats(queue));
});

router.patch('/queues/:id', (req, res) => {
  const queue = loadQueueOr404(req, res);
  if (!queue) return;
  const { name, priority, concurrency_limit, rate_limit_per_min, retry_policy } = req.body;
  const patch = {};
  if (name !== undefined) patch.name = name;
  if (priority !== undefined) patch.priority = priority;
  if (concurrency_limit !== undefined) patch.concurrency_limit = concurrency_limit;
  if (rate_limit_per_min !== undefined) patch.rate_limit_per_min = rate_limit_per_min;
  const updated = update('queues', queue.id, patch);

  if (retry_policy) {
    const policy = findMany('retry_policies', (p) => p.queue_id === queue.id)[0];
    if (policy) update('retry_policies', policy.id, retry_policy);
  }
  res.json(withStats(updated));
});

router.post('/queues/:id/pause', (req, res) => {
  const queue = loadQueueOr404(req, res);
  if (!queue) return;
  res.json(update('queues', queue.id, { is_paused: true }));
});

router.post('/queues/:id/resume', (req, res) => {
  const queue = loadQueueOr404(req, res);
  if (!queue) return;
  res.json(update('queues', queue.id, { is_paused: false }));
});

module.exports = { router, loadQueueOr404 };
