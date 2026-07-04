const express = require('express');
const cors = require('cors');
const http = require('http');
const rateLimit = require('express-rate-limit');

const { authenticate } = require('./middleware/auth');
const authRoutes = require('./routes/auth');
const projectRoutes = require('./routes/projects');
const { router: queueRoutes } = require('./routes/queues');
const jobRoutes = require('./routes/jobs');
const workerRoutes = require('./routes/workers');
const dashboardRoutes = require('./routes/dashboard');
const ws = require('./services/ws');
const scheduler = require('./services/scheduler');

const app = express();
app.use(cors());
app.use(express.json({ limit: '2mb' }));

// ---- Bonus feature: API-wide rate limiting ----
const apiLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 300, // requests per minute per IP
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many requests, please slow down.' },
});
app.use('/api/', apiLimiter);

const authLimiter = rateLimit({ windowMs: 60 * 1000, max: 20 });
app.use('/api/auth', authLimiter);

// ---- Request logging (structured) ----
app.use((req, res, next) => {
  const start = Date.now();
  res.on('finish', () => {
    console.log(JSON.stringify({
      ts: new Date().toISOString(), method: req.method, path: req.path,
      status: res.statusCode, duration_ms: Date.now() - start,
    }));
  });
  next();
});

app.get('/health', (req, res) => res.json({ status: 'ok', time: new Date().toISOString() }));

// ---- Public routes ----
app.use('/api/auth', authRoutes);

// Worker endpoints are identified by worker id (not a user JWT) since they're
// called by backend worker processes. Mounted BEFORE the user-authenticated
// routes so those requests never reach the `authenticate` middleware below
// (Express falls through to later middleware only if the router calls next()).
app.use('/api', workerRoutes);

// ---- Authenticated (user JWT) routes ----
app.use('/api/projects', authenticate, projectRoutes);
app.use('/api', authenticate, queueRoutes);   // /api/projects/:projectId/queues, /api/queues/:id
app.use('/api', authenticate, jobRoutes);      // /api/queues/:id/jobs, /api/jobs/:jobId, /api/dlq/*
app.use('/api/dashboard', authenticate, dashboardRoutes);

// ---- Structured error handling ----
app.use((req, res) => res.status(404).json({ error: 'Not found' }));
app.use((err, req, res, next) => {
  console.error(err);
  res.status(err.status || 500).json({ error: err.message || 'Internal server error' });
});

const PORT = process.env.PORT || 4000;
const server = http.createServer(app);
ws.init(server);
scheduler.start();

server.listen(PORT, () => {
  console.log(`Distributed Job Scheduler API listening on http://localhost:${PORT}`);
  console.log(`WebSocket live updates on ws://localhost:${PORT}/ws`);
});

module.exports = { app, server };
