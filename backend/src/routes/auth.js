const express = require('express');
const router = express.Router();
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');
const nodemailer = require('nodemailer');
const { body, validationResult } = require('express-validator');
const { pool, logAudit } = require('../database/db');
const { auth } = require('../middleware/auth');

const emailConfigKeys = ['SMTP_HOST', 'SMTP_PORT', 'SMTP_USER', 'SMTP_PASS', 'EMAIL_FROM'];
const missingEmailConfig = emailConfigKeys.filter((key) => !process.env[key]?.trim());
const frontendUrl = process.env.FRONTEND_URL?.trim()?.replace(/\/$/, '');

let transporter = null;

if (missingEmailConfig.length === 0) {
  const smtpPort = Number(process.env.SMTP_PORT) || 587;
  transporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: smtpPort,
    secure: smtpPort === 465,
    auth: {
      user: process.env.SMTP_USER,
      pass: process.env.SMTP_PASS,
    },
  });
} else if (missingEmailConfig.length !== emailConfigKeys.length) {
  console.warn(`[EMAIL WARN] Email features disabled. Missing env vars: ${missingEmailConfig.join(', ')}`);
}

if (transporter && !frontendUrl) {
  console.warn('[EMAIL WARN] Password reset emails need FRONTEND_URL to build reset links.');
}

// Register
router.post('/register', [
  body('email').isEmail().withMessage('Valid email is required'),
  body('username').trim().notEmpty().withMessage('Username is required'),
  body('password').isLength({ min: 6 }).withMessage('Password must be at least 6 characters'),
  body('confirmPassword').custom((value, { req }) => value === req.body.password).withMessage('Passwords do not match'),
], async (req, res) => {
  try {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        errors: errors.array()
      });
    }

    const { email, username, password } = req.body;

    const userExists = await pool.query(
      'SELECT id FROM users WHERE username = $1 OR email = $2',
      [username, email]
    );

    if (userExists.rows.length > 0) {
      return res.status(400).json({
        success: false,
        message: 'Username or email already exists'
      });
    }

    const passwordHash = await bcrypt.hash(password, 10);

    const insertResult = await pool.query(
      'INSERT INTO users (username, email, password_hash, name, mobile, is_active, can_delete) VALUES ($1, $2, $3, $4, $5, true, false) RETURNING id, username, email, name, mobile, can_delete',
      [username, email, passwordHash, username, '']
    );

    const newUser = insertResult.rows[0];
    const token = jwt.sign(
      { userId: newUser.id, username: newUser.username },
      process.env.JWT_SECRET,
      { expiresIn: process.env.JWT_EXPIRES_IN || '24h' }
    );

    await logAudit('users', newUser.id, 'REGISTER', null, { username, email }, newUser.id);

    res.json({
      success: true,
      message: 'Registration successful',
      data: {
        token,
        user: newUser
      }
    });
  } catch (error) {
    console.error('Registration error:', error);
    res.status(500).json({
      success: false,
      message: 'Registration failed'
    });
  }
});

// Login
router.post('/login', async (req, res) => {
  try {
    const username = typeof req.body?.username === 'string' ? req.body.username.trim() : '';
    const password = typeof req.body?.password === 'string' ? req.body.password : '';

    if (!username || !password) {
      return res.status(400).json({
        success: false,
        errors: [
          !username ? { msg: 'Username is required', path: 'username' } : null,
          !password ? { msg: 'Password is required', path: 'password' } : null,
        ].filter(Boolean)
      });
    }

    // Find user by username or email
    const result = await pool.query(
      `SELECT id, username, email, name, mobile, can_delete, is_active, password_hash
       FROM users
       WHERE LOWER(username) = LOWER($1) OR LOWER(email) = LOWER($1)
       LIMIT 1`,
      [username]
    );

    if (result.rows.length === 0 || result.rows[0].is_active !== true) {
      return res.status(401).json({
        success: false,
        message: 'Invalid username or password'
      });
    }

    const user = result.rows[0];

    // Verify password
    if (typeof user.password_hash !== 'string' || !user.password_hash) {
      console.error(`Login error: missing password hash for user ${user.id}`);
      return res.status(401).json({
        success: false,
        message: 'Invalid username or password'
      });
    }

    const isValidPassword = await bcrypt.compare(password, user.password_hash);
    if (!isValidPassword) {
      return res.status(401).json({
        success: false,
        message: 'Invalid username or password'
      });
    }

    // Generate JWT token
    const token = jwt.sign(
      { userId: user.id, username: user.username },
      process.env.JWT_SECRET,
      { expiresIn: process.env.JWT_EXPIRES_IN || '24h' }
    );

    // Log the login
    await logAudit('users', user.id, 'LOGIN', null, { timestamp: new Date() }, user.id);

    res.json({
      success: true,
      message: 'Login successful',
      data: {
        token,
        user: {
          id: user.id,
          username: user.username,
          email: user.email,
          name: user.name,
          mobile: user.mobile,
          can_delete: user.can_delete || false
        }
      }
    });
  } catch (error) {
    console.error('Login error:', error);
    res.status(500).json({
      success: false,
      message: 'Login failed'
    });
  }
});

// Forgot Password
router.post('/forgot-password', [
  body('email').isEmail().withMessage('Valid email is required'),
], async (req, res) => {
  try {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        errors: errors.array()
      });
    }

    const { email } = req.body;

    const result = await pool.query(
      'SELECT id, email, is_active FROM users WHERE email = $1 AND is_active = true',
      [email]
    );

    if (result.rows.length === 0) {
      // Don't reveal if email exists
      return res.json({
        success: true,
        message: 'If the email exists, a reset link has been sent'
      });
    }

    const user = result.rows[0];

    // Generate reset token
    const resetToken = crypto.randomBytes(32).toString('hex');
    const resetTokenExpires = new Date(Date.now() + 3600000); // 1 hour

    // Save token to database
    await pool.query(
      'UPDATE users SET reset_token = $1, reset_token_expires = $2 WHERE id = $3',
      [resetToken, resetTokenExpires, user.id]
    );

    const resetUrl = frontendUrl ? `${frontendUrl}/reset-password/${resetToken}` : null;

    if (transporter && resetUrl) {
      try {
        await transporter.sendMail({
          from: process.env.EMAIL_FROM,
          to: email,
          subject: 'Password Reset - JCHPL MIS',
          html: `
            <h2>Password Reset Request</h2>
            <p>You requested a password reset for your JCHPL MIS account.</p>
            <p>Click the link below to reset your password:</p>
            <a href="${resetUrl}">${resetUrl}</a>
            <p>This link expires in 1 hour.</p>
            <p>If you didn't request this, please ignore this email.</p>
          `
        });
      } catch (emailError) {
        console.error('Email sending failed:', emailError);
      }
    } else {
      console.warn('[EMAIL WARN] Skipping password reset email because SMTP or FRONTEND_URL is not fully configured.');
    }

    res.json({
      success: true,
      message: 'If the email exists, a reset link has been sent'
    });
  } catch (error) {
    console.error('Forgot password error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to process request'
    });
  }
});

// Reset Password
router.post('/reset-password', [
  body('token').notEmpty().withMessage('Reset token is required'),
  body('password').isLength({ min: 6 }).withMessage('Password must be at least 6 characters'),
], async (req, res) => {
  try {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        errors: errors.array()
      });
    }

    const { token, password } = req.body;

    const result = await pool.query(
      `SELECT id, is_active
       FROM users
       WHERE reset_token = $1 AND reset_token_expires > NOW() AND is_active = true`,
      [token]
    );

    if (result.rows.length === 0) {
      return res.status(400).json({
        success: false,
        message: 'Invalid or expired reset token'
      });
    }

    const user = result.rows[0];

    // Hash new password
    const hashedPassword = await bcrypt.hash(password, 10);

    // Update password and clear reset token
    await pool.query(
      'UPDATE users SET password_hash = $1, reset_token = NULL, reset_token_expires = NULL WHERE id = $2',
      [hashedPassword, user.id]
    );

    await logAudit('users', user.id, 'PASSWORD_RESET', null, { timestamp: new Date() }, user.id);

    res.json({
      success: true,
      message: 'Password reset successful'
    });
  } catch (error) {
    console.error('Reset password error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to reset password'
    });
  }
});

// Get current user
router.get('/me', auth, async (req, res) => {
  res.json({
    success: true,
    data: req.user
  });
});

// Logout (client-side token removal, server logs the action)
router.post('/logout', auth, async (req, res) => {
  try {
    await logAudit('users', req.user.id, 'LOGOUT', null, { timestamp: new Date() }, req.user.id);
    res.json({
      success: true,
      message: 'Logged out successfully'
    });
  } catch (error) {
    console.error('Logout error:', error);
    res.status(500).json({
      success: false,
      message: 'Logout failed'
    });
  }
});

module.exports = router;
