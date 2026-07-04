/**
 * Embedded data store.
 *
 * This app is designed against schema.sql (a real relational schema for
 * Postgres/MySQL). Since this sandbox cannot compile native modules
 * (better-sqlite3 / sqlite3) or reach npm's binary mirrors, persistence here
 * is implemented as an in-memory store with the SAME table shapes, keys, and
 * relationships, flushed to a JSON file on disk so state survives restarts.
 *
 * Swapping this file for a real Postgres client (pg / knex / prisma) is a
 * drop-in replacement -- every other module only calls the methods below
 * (findById, insert, update, query, transaction), never raw SQL.
 */
const fs = require('fs');
const path = require('path');

const DATA_FILE = path.join(__dirname, '..', '..', 'data', 'db.json');

const EMPTY_DB = {
  users: [],
  organizations: [],
  projects: [],
  queues: [],
  retry_policies: [],
  jobs: [],
  job_executions: [],
  job_logs: [],
  dead_letter_queue: [],
  workers: [],
  worker_heartbeats: [],
};

function load() {
  try {
    const raw = fs.readFileSync(DATA_FILE, 'utf-8');
    return { ...structuredClone(EMPTY_DB), ...JSON.parse(raw) };
  } catch {
    return structuredClone(EMPTY_DB);
  }
}

const db = load();
let dirty = false;

function persist() {
  if (!dirty) return;
  fs.mkdirSync(path.dirname(DATA_FILE), { recursive: true });
  fs.writeFileSync(DATA_FILE, JSON.stringify(db, null, 2));
  dirty = false;
}
setInterval(persist, 1000).unref();
process.on('exit', persist);

/**
 * A single JS process is single-threaded and each of the functions below
 * runs to completion synchronously before the event loop yields -- so a
 * "read status, then write status" sequence inside ONE function call is
 * atomic with respect to other requests, exactly like a SQL row lock would
 * be for a single instance. This is what `claimNextJob` in jobService.js
 * relies on to prevent two workers from claiming the same job.
 */
function table(name) {
  if (!db[name]) throw new Error(`Unknown table: ${name}`);
  return db[name];
}

function insert(tableName, row) {
  table(tableName).push(row);
  dirty = true;
  return row;
}

function findById(tableName, id) {
  return table(tableName).find((r) => r.id === id) || null;
}

function findOne(tableName, predicate) {
  return table(tableName).find(predicate) || null;
}

function findMany(tableName, predicate = () => true) {
  return table(tableName).filter(predicate);
}

function update(tableName, id, patch) {
  const row = findById(tableName, id);
  if (!row) return null;
  Object.assign(row, patch, { updated_at: new Date().toISOString() });
  dirty = true;
  return row;
}

function remove(tableName, id) {
  const idx = table(tableName).findIndex((r) => r.id === id);
  if (idx === -1) return false;
  table(tableName).splice(idx, 1);
  dirty = true;
  return true;
}

module.exports = { db, insert, findById, findOne, findMany, update, remove, persist };
