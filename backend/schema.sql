-- ============================================================================
-- Distributed Job Scheduler - Relational Schema (PostgreSQL dialect)
-- This is the canonical schema design. The demo app in src/db/store.js
-- implements the same structure using a pure-JS embedded store (no native
-- build tools available in this sandbox), but every table, key, and index
-- below is what would be used in a real Postgres/MySQL deployment.
-- ============================================================================

CREATE TABLE organizations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(255) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE users (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,
    email           VARCHAR(255) NOT NULL UNIQUE,
    password_hash   VARCHAR(255) NOT NULL,
    role            VARCHAR(20)  NOT NULL DEFAULT 'member', -- admin | member (RBAC bonus)
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_users_org ON users(organization_id);

CREATE TABLE projects (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    owner_id        UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    name            VARCHAR(255) NOT NULL,
    api_key         VARCHAR(64) NOT NULL UNIQUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_projects_org ON projects(organization_id);

CREATE TABLE queues (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id          UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name                VARCHAR(255) NOT NULL,
    priority            INTEGER NOT NULL DEFAULT 0,          -- higher = served first
    concurrency_limit   INTEGER NOT NULL DEFAULT 5,
    is_paused           BOOLEAN NOT NULL DEFAULT false,
    rate_limit_per_min  INTEGER,                              -- bonus: rate limiting
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(project_id, name)
);
CREATE INDEX idx_queues_project ON queues(project_id);

CREATE TABLE retry_policies (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_id        UUID NOT NULL REFERENCES queues(id) ON DELETE CASCADE,
    strategy        VARCHAR(20) NOT NULL DEFAULT 'exponential', -- fixed | linear | exponential
    base_delay_ms   INTEGER NOT NULL DEFAULT 1000,
    max_retries     INTEGER NOT NULL DEFAULT 3,
    max_delay_ms    INTEGER NOT NULL DEFAULT 60000
);
CREATE INDEX idx_retry_policies_queue ON retry_policies(queue_id);

CREATE TABLE jobs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_id            UUID NOT NULL REFERENCES queues(id) ON DELETE CASCADE,
    type                VARCHAR(20) NOT NULL,   -- immediate|delayed|scheduled|recurring|batch
    payload             JSONB NOT NULL,
    status              VARCHAR(20) NOT NULL DEFAULT 'queued',
                        -- queued|scheduled|claimed|running|completed|failed|dead_letter|cancelled
    priority            INTEGER NOT NULL DEFAULT 0,
    run_at              TIMESTAMPTZ,             -- for delayed/scheduled jobs
    cron_expression     VARCHAR(100),            -- for recurring jobs
    batch_id            UUID,                    -- groups batch-created jobs
    attempt_count       INTEGER NOT NULL DEFAULT 0,
    max_retries         INTEGER NOT NULL DEFAULT 3,
    idempotency_key     VARCHAR(255),
    claimed_by          UUID,                    -- worker id
    claimed_at          TIMESTAMPTZ,
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_idempotency UNIQUE (queue_id, idempotency_key)
);
CREATE INDEX idx_jobs_queue_status ON jobs(queue_id, status);
CREATE INDEX idx_jobs_run_at ON jobs(run_at) WHERE status IN ('queued','scheduled');
CREATE INDEX idx_jobs_claim ON jobs(status, priority DESC, created_at ASC);
CREATE INDEX idx_jobs_batch ON jobs(batch_id);

CREATE TABLE job_executions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    worker_id       UUID NOT NULL,
    attempt_number  INTEGER NOT NULL,
    status          VARCHAR(20) NOT NULL, -- running|completed|failed
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    duration_ms     INTEGER,
    error_message   TEXT,
    result          JSONB
);
CREATE INDEX idx_executions_job ON job_executions(job_id);

CREATE TABLE job_logs (
    id              BIGSERIAL PRIMARY KEY,
    job_id          UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    execution_id    UUID REFERENCES job_executions(id) ON DELETE CASCADE,
    level           VARCHAR(10) NOT NULL DEFAULT 'info', -- info|warn|error
    message         TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_logs_job ON job_logs(job_id);

CREATE TABLE dead_letter_queue (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    queue_id        UUID NOT NULL REFERENCES queues(id) ON DELETE CASCADE,
    final_error     TEXT,
    attempt_count   INTEGER NOT NULL,
    moved_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload_snapshot JSONB NOT NULL
);
CREATE INDEX idx_dlq_queue ON dead_letter_queue(queue_id);

CREATE TABLE workers (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hostname        VARCHAR(255) NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'online', -- online|offline|draining
    concurrency     INTEGER NOT NULL DEFAULT 5,
    registered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE worker_heartbeats (
    id              BIGSERIAL PRIMARY KEY,
    worker_id       UUID NOT NULL REFERENCES workers(id) ON DELETE CASCADE,
    active_jobs     INTEGER NOT NULL DEFAULT 0,
    cpu_load        REAL,
    memory_mb       INTEGER,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_heartbeats_worker ON worker_heartbeats(worker_id, created_at DESC);

-- ============================================================================
-- Design notes (trade-offs):
-- 1. UUIDs as PKs: enables safe distributed/offline ID generation across
--    multiple worker processes/services without a central sequence.
-- 2. jobs.status + priority + created_at composite index: this is the exact
--    access pattern the worker's claim query uses (SELECT ... ORDER BY
--    priority DESC, created_at ASC LIMIT n FOR UPDATE SKIP LOCKED).
-- 3. ON DELETE CASCADE from jobs -> job_executions/job_logs: keeps history
--    scoped to a job's lifetime; DLQ keeps a payload_snapshot independently
--    so audit data survives even if the parent job is later purged.
-- 4. idempotency_key + UNIQUE(queue_id, idempotency_key): lets producers
--    safely retry job-creation calls without creating duplicate jobs.
-- 5. Partial index on jobs(run_at) WHERE status IN ('queued','scheduled'):
--    keeps the scheduler's "what's due" scan cheap as the jobs table grows,
--    since completed/failed jobs (the majority over time) are excluded.
-- ============================================================================
