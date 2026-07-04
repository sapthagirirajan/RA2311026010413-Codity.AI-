const express = require('express');
const crypto = require('crypto');
const { insert, findMany, findById } = require('../db/store');
const { newId, now } = require('../utils/helpers');

const router = express.Router();

router.post('/', (req, res) => {
  const { name } = req.body;
  if (!name) return res.status(400).json({ error: 'name is required' });
  const project = insert('projects', {
    id: newId(),
    organization_id: req.user.organization_id,
    owner_id: req.user.id,
    name,
    api_key: crypto.randomBytes(16).toString('hex'),
    created_at: now(),
  });
  res.status(201).json(project);
});

router.get('/', (req, res) => {
  const projects = findMany('projects', (p) => p.organization_id === req.user.organization_id);
  res.json({ data: projects, count: projects.length });
});

router.get('/:id', (req, res) => {
  const project = findById('projects', req.params.id);
  if (!project || project.organization_id !== req.user.organization_id) {
    return res.status(404).json({ error: 'Project not found' });
  }
  res.json(project);
});

module.exports = router;
