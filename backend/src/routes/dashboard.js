const express = require('express');
const { findMany } = require('../db/store');

const router = express.Router();

router.get('/metrics/overview', (req, res) => {
  const orgProjectIds = findMany('projects', (p) => p.organization_id === req.user.organization_id).map((p) => p.id);
  const queues = findMany('queues', (q) => orgProjectIds.includes(q.project_id));
  const queueIds = queues.map((q) => q.id);
  const jobs = findMany('jobs', (j) => queueIds.includes(j.queue_id));
  const workers = findMany('workers', () => true);

  const byStatus = jobs.reduce((acc, j) => ((acc[j.status] = (acc[j.status] || 0) + 1), acc), {});
  const completed = jobs.filter((j) => j.status === 'completed' && j.completed_at && j.started_at);
  const avgDurationMs = completed.length
    ? completed.reduce((sum, j) => sum + (new Date(j.completed_at) - new Date(j.started_at)), 0) / completed.length
    : 0;

  const last24h = jobs.filter((j) => Date.now() - new Date(j.created_at).getTime() < 24 * 3600 * 1000);
  const throughputByHour = {};
  for (const j of last24h.filter((j) => j.status === 'completed')) {
    const hour = new Date(j.completed_at).toISOString().slice(0, 13);
    throughputByHour[hour] = (throughputByHour[hour] || 0) + 1;
  }

  res.json({
    queues: queues.length,
    workers: workers.length,
    jobs_total: jobs.length,
    jobs_by_status: byStatus,
    avg_execution_ms: Math.round(avgDurationMs),
    throughput_last_24h_by_hour: throughputByHour,
    dead_letter_count: byStatus.dead_letter || 0,
  });
});

module.exports = router;
