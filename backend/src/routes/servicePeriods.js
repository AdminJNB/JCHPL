const express = require('express');
const router = express.Router();
const { pool } = require('../database/db');
const { auth } = require('../middleware/auth');

// Get all service periods
router.get('/', auth, async (req, res) => {
  try {
    const { includeInactive, financialYear, all } = req.query;
    
    let query = 'SELECT * FROM service_periods WHERE 1=1';
    const params = [];
    
    if (all !== 'true') {
      query += " AND start_date >= (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month')::date";
    }
    
    if (!includeInactive) {
      query += ' AND is_active = true';
    }
    
    if (financialYear) {
      params.push(financialYear);
      query += ` AND financial_year = $${params.length}`;
    }
    
    query += ' ORDER BY start_date';
    
    const result = await pool.query(query, params);
    
    res.json({
      success: true,
      data: result.rows
    });
  } catch (error) {
    console.error('Get service periods error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch service periods'
    });
  }
});

// Get financial years list
router.get('/financial-years', auth, async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT DISTINCT financial_year 
      FROM service_periods 
      WHERE is_active = true
        AND start_date >= (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month')::date
      ORDER BY financial_year
    `);
    
    res.json({
      success: true,
      data: result.rows.map(r => r.financial_year)
    });
  } catch (error) {
    console.error('Get financial years error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch financial years'
    });
  }
});

// Get current period
router.get('/current', auth, async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT * FROM service_periods 
      WHERE is_active = true 
        AND start_date <= CURRENT_DATE 
        AND end_date >= CURRENT_DATE
      LIMIT 1
    `);
    
    if (result.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'No current period found'
      });
    }
    
    res.json({
      success: true,
      data: result.rows[0]
    });
  } catch (error) {
    console.error('Get current period error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch current period'
    });
  }
});

module.exports = router;
