const cron = require('node-cron');
const { findMany, update } = require('../db/store');
const jobService = require('./jobService');

/** Tracks last-fired time per recurring job id to avoid double-firing within the same tick. */
const lastFired = new Map();

function start() {
  // Every second: promote delayed/scheduled jobs whose run_at has passed.
  setInterval(() => {
    jobService.promoteDueJobs();
  }, 1000);

  // Every 5 seconds: sweep stale workers and reschedule their running jobs.
  setInterval(() => {
    jobService.sweepOfflineWorkers();
  }, 5000);

  // Every minute: check recurring (cron) job definitions and spawn due instances.
  cron.schedule('* * * * * *', () => {
    const recurringDefs = findMany('jobs', (j) => j.type === 'recurring' && j.cron_expression);
    for (const def of recurringDefs) {
      if (!cron.validate(def.cron_expression)) continue;
      const key = def.id;
      const shouldFire = isDueNow(def.cron_expression);
      const last = lastFired.get(key);
      if (shouldFire && last !== currentMinuteBucket()) {
        lastFired.set(key, currentMinuteBucket());
        const { insert, findById } = require('../db/store');
        const { newId, now } = require('../utils/helpers');
        insert('jobs', {
          id: newId(),
          queue_id: def.queue_id,
          type: 'immediate',
          payload: def.payload,
          status: 'queued',
          priority: def.priority,
          run_at: null,
          cron_expression: null,
          batch_id: null,
          attempt_count: 0,
          max_retries: def.max_retries,
          idempotency_key: null,
          claimed_by: null,
          claimed_at: null,
          started_at: null,
          completed_at: null,
          created_at: now(),
          updated_at: now(),
        });
        jobService.log(def.id, `Recurring definition fired -> spawned new job instance`);
      }
    }
  });

  console.log('Scheduler started (promotes due jobs every 1s, checks cron defs every minute)');
}

function currentMinuteBucket() {
  return new Date().toISOString().slice(0, 16);
}

// Minimal cron "is due this minute" check using node-cron's parser via a temp task tick simulation.
function isDueNow(expr) {
  try {
    const parts = expr.trim().split(/\s+/);
    const [min, hour, dom, month, dow] = parts.length === 5 ? parts : parts.slice(1);
    const d = new Date();
    const match = (field, value) => field === '*' || field.split(',').includes(String(value));
    return (
      match(min, d.getMinutes()) &&
      match(hour, d.getHours()) &&
      match(dom, d.getDate()) &&
      match(month, d.getMonth() + 1) &&
      match(dow, d.getDay())
    );
  } catch {
    return false;
  }
}

module.exports = { start };
