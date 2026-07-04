const express = require('express');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const { insert, findOne, findById, findMany } = require('../db/store');
const { newId, now } = require('../utils/helpers');
const { JWT_SECRET } = require('../middleware/auth');

const router = express.Router();

router.post('/register', (req, res) => {
  const { email, password, organization_name, role } = req.body;
  if (!email || !password) return res.status(400).json({ error: 'email and password are required' });
  if (findOne('users', (u) => u.email === email)) {
    return res.status(409).json({ error: 'A user with this email already exists' });
  }

  const org = insert('organizations', {
    id: newId(),
    name: organization_name || `${email}'s Org`,
    created_at: now(),
  });

  const isFirstUser = findMany('users', () => true).length === 0;
  const user = insert('users', {
    id: newId(),
    organization_id: org.id,
    email,
    password_hash: bcrypt.hashSync(password, 10),
    role: role === 'admin' || isFirstUser ? 'admin' : 'member', // bonus: RBAC
    created_at: now(),
  });

  const token = jwt.sign({ sub: user.id }, JWT_SECRET, { expiresIn: '7d' });
  res.status(201).json({
    token,
    user: { id: user.id, email: user.email, role: user.role, organization_id: org.id },
  });
});

router.post('/login', (req, res) => {
  const { email, password } = req.body;
  const user = findOne('users', (u) => u.email === email);
  if (!user || !bcrypt.compareSync(password || '', user.password_hash)) {
    return res.status(401).json({ error: 'Invalid email or password' });
  }
  const token = jwt.sign({ sub: user.id }, JWT_SECRET, { expiresIn: '7d' });
  res.json({ token, user: { id: user.id, email: user.email, role: user.role, organization_id: user.organization_id } });
});

module.exports = router;
