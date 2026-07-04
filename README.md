# Distributed Job Scheduler — Codity.AI Intern Assignment

A production-inspired distributed job scheduling platform: REST API, worker
service, retry/backoff + Dead Letter Queue handling, and a live dashboard.

## Architecture

```
                       ┌────────────────────┐
   Dashboard (HTML) ───┤   Express API       │───┐
   REST (poll) + WS    │  /api/auth          │   │
                       │  /api/projects      │   │   in-memory store
                       │  /api/queues        │───┤   (mirrors schema.sql)
                       │  /api/jobs          │   │   + JSON snapshot on disk
                       │  /api/workers       │   │
                       │  /api/dashboard     │   │
                       └─────────┬───────────┘   │
                                 │ HTTP           │
                    ┌────────────┴────────────┐  │
                    │  Worker process(es)     │──┘
                    │  poll → claim → run →   │
                    │  complete/fail          │
                    └─────────────────────────┘
```

- **API server** (`backend/src/server.js`) owns the single source of truth
  and performs the *atomic* job claim, so multiple worker processes can run
  in parallel without double-processing a job.
- **Worker service** (`backend/src/worker.js`) is a separate Node process.
  Run as many as you like — each registers itself, polls for work, executes
  it, sends heartbeats, and drains gracefully on `SIGTERM`/`SIGINT`.
- **Dashboard** (`frontend/index.html`) is a static single-page app: REST
  polling for state + a WebSocket for live event updates. No build step.

### Why an in-memory store instead of Postgres/SQLite?
This sandbox can't compile native modules (`better-sqlite3`) or reach a real
Postgres instance. `backend/schema.sql` is the **actual relational schema**
this project is designed against (tables, keys, indexes, cascade rules,
trade-off notes). `backend/src/db/store.js` implements the identical table
shapes and relationships in pure JS, persisted to `backend/data/db.json`, so
the app is fully runnable here. Swapping `store.js` for a real Postgres
client (`pg`/`knex`/`prisma`) is a drop-in change — no other module talks to
the database directly or writes raw SQL outside of `schema.sql`.

## Setup

```bash
cd backend
npm install

# terminal 1 — API server
npm start
# → http://localhost:4000  (WebSocket at ws://localhost:4000/ws)

# terminal 2 — one or more workers
QUEUE_IDS=<queue-id-1>,<queue-id-2> npm run worker
```

Then open `frontend/index.html` directly in a browser (or serve it with any
static server) — it talks to `http://localhost:4000` by default.

`QUEUE_IDS` tells a worker which queues to service; get queue ids from the
dashboard or `GET /api/projects/:id/queues` after creating one. You can run
several `npm run worker` processes (different terminals) pointed at the same
or different queues to see distributed concurrency and atomic claiming in
action — no two workers will ever pick up the same job.

## Core requirements implemented

- **Auth + projects**: JWT auth, register/login, each project owns queues.
- **Queue config**: priority, concurrency limit, retry policy, pause/resume,
  live stats (`PATCH /api/queues/:id`, `/pause`, `/resume`).
- **All 5 job types**: immediate, delayed, scheduled, recurring (cron),
  batch — `POST /api/queues/:id/jobs`.
- **Worker service**: polls, atomically claims (`POST /workers/:id/claim`),
  executes concurrently (configurable via `WORKER_CONCURRENCY`), sends
  heartbeats, and drains in-flight jobs on shutdown signals.
- **Full job lifecycle**: `queued → scheduled → claimed → running →
  completed`, with `dead_letter` for permanent failures.
- **Retry strategies**: fixed delay, linear backoff, exponential backoff
  (`backend/src/utils/helpers.js:computeRetryDelayMs`).
- **Observability**: every job has execution history, retry history, worker
  assignment, timestamps, and per-attempt logs (`GET /api/jobs/:id`).
- **Dashboard**: queue health, job explorer with filters, worker status,
  DLQ view + manual retry, throughput/health metrics, live event feed.

## Bonus features implemented (2 of 8 listed)

The assignment lists 8 optional bonus features and says to add some without
sacrificing core quality. I implemented:

1. **Rate limiting** — two layers:
   - Global API rate limiting (`express-rate-limit`) protecting all
     `/api/*` routes (300 req/min/IP) with a stricter limit on `/api/auth`.
   - Per-queue throughput rate limiting (`rate_limit_per_min` on a queue) —
     job creation on that queue is rejected with `429` once the limit is hit
     in a rolling 60s window (`jobService.js:checkQueueRateLimit`).
2. **WebSocket live updates** — the dashboard's "Live activity" feed and
   auto-refreshing metrics are driven by a WebSocket (`services/ws.js`,
   `ws://localhost:4000/ws`), not polling, for job/worker state changes
   (`job:created`, `job:claimed`, `job:completed`, `job:retry_scheduled`,
   `job:dead_letter`, `worker:heartbeat`).

I also implemented lightweight **Role-Based Access Control** (`admin` /
`member` roles, `requireRole()` middleware) as groundwork, but didn't wire
it into every route yet — see "What I'd do with more time" below.

## Design decisions & trade-offs

- **Atomicity without a real DB transaction**: `claimNextJob` performs the
  eligibility check and the status flip inside one synchronous function call.
  Node is single-threaded, so nothing else can interleave mid-function — the
  same guarantee a SQL `UPDATE ... WHERE status='queued' RETURNING *` gives
  you inside a transaction. This only holds because there's one server
  process; a real deployment would use `SELECT ... FOR UPDATE SKIP LOCKED`
  or an equivalent conditional update in Postgres, exactly as commented in
  `jobService.js`.
- **Workers talk over HTTP, not directly to the store**: this keeps the
  claim atomic (single source of truth) and mirrors how a real distributed
  deployment works — workers can live on different machines.
- **Idempotency**: job creation accepts an `idempotency_key`; a duplicate
  create call with the same key on the same queue returns the existing job
  instead of creating a second one.
- **DLQ keeps a payload snapshot** independent of the parent job so audit
  data survives even if `jobs` rows are later purged.

## What I'd do with more time

- Wire RBAC (`requireRole('admin')`) onto destructive endpoints (queue
  pause/resume, DLQ retry, project creation).
- Workflow dependencies (job B waits on job A) — natural next bonus feature
  given the `batch_id` grouping already in place.
- Swap `store.js` for real Postgres using `schema.sql` as-is, and add
  `SELECT ... FOR UPDATE SKIP LOCKED` for true multi-instance atomicity.
- Automated tests (Jest) for `claimNextJob`, `failJob` retry math, and the
  API's auth/validation paths — currently only manually verified (see
  `backend/TESTING.md`... not yet written).

## Project structure

```
backend/
  schema.sql                  # canonical relational schema + design notes
  src/
    db/store.js                # pure-JS embedded store (schema.sql mirror)
    middleware/auth.js          # JWT auth + RBAC
    services/
      jobService.js             # create/claim/complete/fail/DLQ/retry math
      scheduler.js               # promotes due jobs, expands cron jobs
      ws.js                       # WebSocket broadcaster (bonus)
    routes/
      auth.js  projects.js  queues.js  jobs.js  workers.js  dashboard.js
    server.js                   # Express app wiring
    worker.js                   # standalone worker process
frontend/
  index.html                  # dashboard (vanilla JS, WS + polling)
```
