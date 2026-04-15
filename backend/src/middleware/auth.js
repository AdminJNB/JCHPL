const jwt = require('jsonwebtoken');
const { pool } = require('../database/db');

const auth = async (req, res, next) => {
  try {
    const authHeader = req.headers.authorization;
    
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({
        success: false,
        message: 'Access denied. No token provided.'
      });
    }

    const token = authHeader.split(' ')[1];
    
    try {
      console.log('[AUTH DEBUG] JWT_SECRET defined:', !!process.env.JWT_SECRET, 'length:', (process.env.JWT_SECRET || '').length);
      console.log('[AUTH DEBUG] Token (first 20):', token.substring(0, 20));
      const decoded = jwt.verify(token, process.env.JWT_SECRET);
      console.log('[AUTH DEBUG] Decoded OK:', decoded.userId);
      
      // Verify user still exists and is active
      const result = await pool.query(
        'SELECT id, username, email, name, mobile, is_active, can_delete FROM users WHERE id = $1',
        [decoded.userId]
      );
      
      if (result.rows.length === 0 || !result.rows[0].is_active) {
        return res.status(401).json({
          success: false,
          message: 'User not found or inactive'
        });
      }
      
      req.user = result.rows[0];
      next();
    } catch (jwtError) {
      console.log('[AUTH DEBUG] JWT verify failed:', jwtError.message, jwtError.name);
      return res.status(401).json({
        success: false,
        message: 'Invalid or expired token'
      });
    }
  } catch (error) {
    console.error('Auth middleware error:', error);
    res.status(500).json({
      success: false,
      message: 'Authentication error'
    });
  }
};

const requireDelete = (req, res, next) => {
  if (!req.user || !req.user.can_delete) {
    return res.status(403).json({
      success: false,
      message: 'You do not have permission to delete records.'
    });
  }
  next();
};

module.exports = { auth, requireDelete };
