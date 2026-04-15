const express = require('express');
const router = express.Router();
const { body, validationResult } = require('express-validator');
const { pool, logAudit } = require('../database/db');
const { auth, requireDelete } = require('../middleware/auth');

let feeMasterSchemaPromise = null;

async function ensureFeeMasterSchema() {
  if (!feeMasterSchemaPromise) {
    feeMasterSchemaPromise = (async () => {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS fee_masters (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          unique_key VARCHAR(500) NOT NULL UNIQUE,
          billing_name_id UUID REFERENCES billing_names(id),
          client_id UUID REFERENCES clients(id) NOT NULL,
          service_type_id UUID REFERENCES service_types(id) NOT NULL,
          start_period VARCHAR(10) NOT NULL,
          end_period VARCHAR(10),
          fee_amount DECIMAL(15, 2) NOT NULL,
          currency VARCHAR(3) DEFAULT 'INR',
          bill_from VARCHAR(255),
          is_active BOOLEAN DEFAULT true,
          is_ended BOOLEAN DEFAULT false,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          created_by UUID REFERENCES users(id),
          updated_by UUID REFERENCES users(id)
        );
      `);

      await pool.query(`
        CREATE TABLE IF NOT EXISTS fee_reviewer_allocations (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          fee_master_id UUID REFERENCES fee_masters(id) ON DELETE CASCADE,
          reviewer_id UUID REFERENCES teams(id),
          allocation_percentage DECIMAL(5, 2),
          allocation_amount DECIMAL(15, 2),
          allocation_method VARCHAR(20) DEFAULT 'percentage',
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);

      await pool.query('CREATE INDEX IF NOT EXISTS idx_fee_masters_client ON fee_masters(client_id)');
      await pool.query('CREATE INDEX IF NOT EXISTS idx_fee_masters_service_type ON fee_masters(service_type_id)');
      await pool.query('CREATE INDEX IF NOT EXISTS idx_fee_masters_unique_key ON fee_masters(unique_key)');
      await pool.query('CREATE INDEX IF NOT EXISTS idx_fee_masters_periods ON fee_masters(start_period, end_period)');
    })().catch((error) => {
      feeMasterSchemaPromise = null;
      throw error;
    });
  }

  return feeMasterSchemaPromise;
}

// Popular currency list
const CURRENCIES = [
  { code: 'INR', name: 'Indian Rupee', symbol: '₹' },
  { code: 'USD', name: 'US Dollar', symbol: '$' },
  { code: 'EUR', name: 'Euro', symbol: '€' },
  { code: 'GBP', name: 'British Pound', symbol: '£' },
  { code: 'AED', name: 'UAE Dirham', symbol: 'د.إ' },
  { code: 'SGD', name: 'Singapore Dollar', symbol: 'S$' },
  { code: 'AUD', name: 'Australian Dollar', symbol: 'A$' },
  { code: 'CAD', name: 'Canadian Dollar', symbol: 'C$' },
  { code: 'JPY', name: 'Japanese Yen', symbol: '¥' },
  { code: 'CHF', name: 'Swiss Franc', symbol: 'CHF' },
];

// Get currencies list
router.get('/currencies', auth, (req, res) => {
  res.json({
    success: true,
    data: CURRENCIES
  });
});

// Generate unique key from client and service type
const generateUniqueKey = (clientId, serviceTypeId) => {
  return `${clientId}_${serviceTypeId}`;
};

// Parse period from MMM-YY format to date (first day of month)
const parsePeriodToDate = (period) => {
  if (!period) return null;
  const months = {
    'Jan': 0, 'Feb': 1, 'Mar': 2, 'Apr': 3, 'May': 4, 'Jun': 5,
    'Jul': 6, 'Aug': 7, 'Sep': 8, 'Oct': 9, 'Nov': 10, 'Dec': 11
  };
  const [mon, yr] = period.split('-');
  const year = parseInt('20' + yr);
  const month = months[mon];
  return new Date(year, month, 1);
};

// Get all fee masters (active only by default, for data entry)
router.get('/', auth, async (req, res) => {
  try {
    await ensureFeeMasterSchema();
    const { includeInactive, includeEnded, clientId, serviceTypeId } = req.query;
    
    let query = `
      SELECT fm.*, 
             c.name as client_name,
             st.name as service_type_name,
             st.hsn_code,
             st.gst_rate,
             bn.name as billing_name,
             COALESCE(json_agg(
               json_build_object(
                 'id', fra.id,
                 'reviewer_id', fra.reviewer_id,
                 'reviewer_name', t.name,
                 'allocation_percentage', fra.allocation_percentage,
                 'allocation_amount', fra.allocation_amount,
                 'allocation_method', fra.allocation_method
               ) ORDER BY t.name
             ) FILTER (WHERE fra.id IS NOT NULL), '[]') as reviewer_allocations
      FROM fee_masters fm
      LEFT JOIN clients c ON fm.client_id = c.id
      LEFT JOIN service_types st ON fm.service_type_id = st.id
      LEFT JOIN billing_names bn ON fm.billing_name_id = bn.id
      LEFT JOIN fee_reviewer_allocations fra ON fm.id = fra.fee_master_id
      LEFT JOIN teams t ON fra.reviewer_id = t.id
      WHERE 1=1
    `;
    const params = [];
    
    if (!includeInactive) {
      query += ' AND fm.is_active = true';
    }
    
    // For data entry, exclude ended fees (unless explicitly requested)
    if (!includeEnded) {
      query += ' AND fm.is_ended = false';
    }
    
    if (clientId) {
      params.push(clientId);
      query += ` AND fm.client_id = $${params.length}`;
    }
    
    if (serviceTypeId) {
      params.push(serviceTypeId);
      query += ` AND fm.service_type_id = $${params.length}`;
    }
    
    query += ' GROUP BY fm.id, c.name, st.name, st.hsn_code, st.gst_rate, bn.name ORDER BY c.name, st.name';
    
    const result = await pool.query(query, params);
    
    res.json({
      success: true,
      data: result.rows
    });
  } catch (error) {
    console.error('Get fee masters error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch fee masters'
    });
  }
});

// Get fee masters for data entry (filters out ended ones)
router.get('/for-data-entry', auth, async (req, res) => {
  try {
    await ensureFeeMasterSchema();
    const { clientId } = req.query;
    
    // Get current date for period check
    const now = new Date();
    const currentPeriod = now.toLocaleString('en-US', { month: 'short' }) + '-' + 
                         now.getFullYear().toString().slice(-2);
    
    let query = `
      SELECT fm.*, 
             c.name as client_name,
             st.name as service_type_name,
             st.hsn_code,
             st.gst_rate,
             bn.name as billing_name
      FROM fee_masters fm
      LEFT JOIN clients c ON fm.client_id = c.id
      LEFT JOIN service_types st ON fm.service_type_id = st.id
      LEFT JOIN billing_names bn ON fm.billing_name_id = bn.id
      WHERE fm.is_active = true AND fm.is_ended = false
    `;
    const params = [];
    
    if (clientId) {
      params.push(clientId);
      query += ` AND fm.client_id = $${params.length}`;
    }
    
    query += ' ORDER BY c.name, st.name';
    
    const result = await pool.query(query, params);
    
    res.json({
      success: true,
      data: result.rows
    });
  } catch (error) {
    console.error('Get fee masters for data entry error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch fee masters'
    });
  }
});

// Get single fee master
router.get('/:id', auth, async (req, res) => {
  try {
    await ensureFeeMasterSchema();
    const result = await pool.query(`
      SELECT fm.*, 
             c.name as client_name,
             st.name as service_type_name,
             st.hsn_code,
             st.gst_rate,
             bn.name as billing_name
      FROM fee_masters fm
      LEFT JOIN clients c ON fm.client_id = c.id
      LEFT JOIN service_types st ON fm.service_type_id = st.id
      LEFT JOIN billing_names bn ON fm.billing_name_id = bn.id
      WHERE fm.id = $1
    `, [req.params.id]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Fee master not found'
      });
    }
    
    // Get reviewer allocations
    const allocations = await pool.query(`
      SELECT fra.*, t.name as reviewer_name
      FROM fee_reviewer_allocations fra
      LEFT JOIN teams t ON fra.reviewer_id = t.id
      WHERE fra.fee_master_id = $1
      ORDER BY t.name
    `, [req.params.id]);
    
    res.json({
      success: true,
      data: {
        ...result.rows[0],
        reviewer_allocations: allocations.rows
      }
    });
  } catch (error) {
    console.error('Get fee master error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch fee master'
    });
  }
});

// Create fee master
router.post('/', auth, [
  body('client_id').notEmpty().withMessage('Client is required').bail().isUUID().withMessage('Invalid client ID'),
  body('service_type_id').notEmpty().withMessage('Service type is required').bail().isUUID().withMessage('Invalid service type ID'),
  body('billing_name_id').optional({ values: 'falsy' }).isUUID().withMessage('Invalid billing name ID'),
  body('start_period').notEmpty().withMessage('Start period is required')
    .matches(/^[A-Z][a-z]{2}-\d{2}$/).withMessage('Start period must be in MMM-YY format (e.g., Apr-24)'),
  body('end_period').optional({ values: 'falsy' })
    .matches(/^[A-Z][a-z]{2}-\d{2}$/).withMessage('End period must be in MMM-YY format (e.g., Mar-25)'),
  body('fee_amount').notEmpty().withMessage('Fee amount is required')
    .isFloat({ min: 0 }).withMessage('Fee amount must be a positive number'),
  body('currency').optional().isIn(CURRENCIES.map(c => c.code)).withMessage('Invalid currency'),
  body('bill_from').optional().trim(),
  body('reviewer_allocations').optional().isArray(),
], async (req, res) => {
  const client = await pool.connect();
  
  try {
    await ensureFeeMasterSchema();
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        errors: errors.array()
      });
    }

    const { 
      client_id, service_type_id, billing_name_id, start_period, end_period,
      fee_amount, currency, bill_from, reviewer_allocations 
    } = req.body;
    
    const uniqueKey = generateUniqueKey(client_id, service_type_id);
    
    // Check if a non-ended fee master already exists for this combination
    const existing = await client.query(
      'SELECT id FROM fee_masters WHERE unique_key = $1 AND is_ended = false',
      [uniqueKey]
    );
    
    if (existing.rows.length > 0) {
      return res.status(400).json({
        success: false,
        message: 'An active fee master already exists for this client and service type combination. End the existing one first.'
      });
    }
    
    // Validate reviewer allocations if provided
    if (reviewer_allocations && reviewer_allocations.length > 0) {
      const totalAllocation = reviewer_allocations.reduce((sum, a) => {
        if (a.allocation_method === 'percentage') {
          return sum + (parseFloat(a.allocation_percentage) || 0);
        } else {
          return sum + (parseFloat(a.allocation_amount) || 0);
        }
      }, 0);
      
      const isPercentageMethod = reviewer_allocations[0]?.allocation_method === 'percentage';
      
      if (isPercentageMethod && Math.abs(totalAllocation - 100) > 0.01) {
        return res.status(400).json({
          success: false,
          message: `Reviewer allocations must total 100% (current: ${totalAllocation.toFixed(2)}%)`
        });
      }
      
      if (!isPercentageMethod && Math.abs(totalAllocation - parseFloat(fee_amount)) > 0.01) {
        return res.status(400).json({
          success: false,
          message: `Reviewer allocations must total the fee amount (${fee_amount}). Current: ${totalAllocation.toFixed(2)}`
        });
      }
    }
    
    await client.query('BEGIN');
    
    // Check if end_period is provided and set is_ended
    const isEnded = !!end_period;
    
    const result = await client.query(`
      INSERT INTO fee_masters (
        unique_key, client_id, service_type_id, billing_name_id, 
        start_period, end_period, fee_amount, currency, bill_from,
        is_ended, created_by, updated_by
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11)
      RETURNING *
    `, [
      uniqueKey, client_id, service_type_id, billing_name_id || null,
      start_period, end_period || null, fee_amount, currency || 'INR', bill_from || null,
      isEnded, req.user.id
    ]);
    
    const feeMasterId = result.rows[0].id;
    
    // Insert reviewer allocations
    if (reviewer_allocations && reviewer_allocations.length > 0) {
      for (const allocation of reviewer_allocations) {
        await client.query(`
          INSERT INTO fee_reviewer_allocations (
            fee_master_id, reviewer_id, allocation_percentage, 
            allocation_amount, allocation_method
          ) VALUES ($1, $2, $3, $4, $5)
        `, [
          feeMasterId,
          allocation.reviewer_id,
          allocation.allocation_percentage || null,
          allocation.allocation_amount || null,
          allocation.allocation_method || 'percentage'
        ]);
      }
    }
    
    await client.query('COMMIT');
    
    await logAudit('fee_masters', feeMasterId, 'CREATE', null, result.rows[0], req.user.id);
    
    res.status(201).json({
      success: true,
      message: 'Fee master created successfully',
      data: result.rows[0]
    });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Create fee master error:', error);
    
    if (error.code === '23505') { // Unique violation
      return res.status(400).json({
        success: false,
        message: 'A fee master with this client and service type combination already exists'
      });
    }
    
    res.status(500).json({
      success: false,
      message: 'Failed to create fee master'
    });
  } finally {
    client.release();
  }
});

// Update fee master
router.put('/:id', auth, [
  body('billing_name_id').optional({ values: 'falsy' }).isUUID().withMessage('Invalid billing name ID'),
  body('start_period').optional({ values: 'falsy' })
    .matches(/^[A-Z][a-z]{2}-\d{2}$/).withMessage('Start period must be in MMM-YY format'),
  body('end_period').optional({ values: 'falsy' })
    .matches(/^[A-Z][a-z]{2}-\d{2}$/).withMessage('End period must be in MMM-YY format'),
  body('fee_amount').optional()
    .isFloat({ min: 0 }).withMessage('Fee amount must be a positive number'),
  body('currency').optional().isIn(CURRENCIES.map(c => c.code)).withMessage('Invalid currency'),
  body('bill_from').optional().trim(),
  body('is_active').optional().isBoolean(),
  body('reviewer_allocations').optional().isArray(),
], async (req, res) => {
  const client = await pool.connect();
  
  try {
    await ensureFeeMasterSchema();
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        errors: errors.array()
      });
    }

    const { id } = req.params;
    const { 
      billing_name_id, start_period, end_period, fee_amount, 
      currency, bill_from, is_active, reviewer_allocations 
    } = req.body;
    
    // Get current values for audit
    const current = await client.query('SELECT * FROM fee_masters WHERE id = $1', [id]);
    if (current.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Fee master not found'
      });
    }
    
    const currentFee = current.rows[0];
    const updatedFeeAmount = fee_amount !== undefined ? fee_amount : currentFee.fee_amount;
    
    // Validate reviewer allocations if provided
    if (reviewer_allocations && reviewer_allocations.length > 0) {
      const totalAllocation = reviewer_allocations.reduce((sum, a) => {
        if (a.allocation_method === 'percentage') {
          return sum + (parseFloat(a.allocation_percentage) || 0);
        } else {
          return sum + (parseFloat(a.allocation_amount) || 0);
        }
      }, 0);
      
      const isPercentageMethod = reviewer_allocations[0]?.allocation_method === 'percentage';
      
      if (isPercentageMethod && Math.abs(totalAllocation - 100) > 0.01) {
        return res.status(400).json({
          success: false,
          message: `Reviewer allocations must total 100% (current: ${totalAllocation.toFixed(2)}%)`
        });
      }
      
      if (!isPercentageMethod && Math.abs(totalAllocation - parseFloat(updatedFeeAmount)) > 0.01) {
        return res.status(400).json({
          success: false,
          message: `Reviewer allocations must total the fee amount (${updatedFeeAmount}). Current: ${totalAllocation.toFixed(2)}`
        });
      }
    }
    
    await client.query('BEGIN');
    
    const updates = [];
    const values = [];
    let paramIndex = 1;
    
    if (billing_name_id !== undefined) {
      updates.push(`billing_name_id = $${paramIndex++}`);
      values.push(billing_name_id || null);
    }
    if (start_period !== undefined) {
      updates.push(`start_period = $${paramIndex++}`);
      values.push(start_period);
    }
    if (end_period !== undefined) {
      updates.push(`end_period = $${paramIndex++}`);
      values.push(end_period || null);
      // If end_period is set, mark as ended
      if (end_period) {
        updates.push(`is_ended = $${paramIndex++}`);
        values.push(true);
      }
    }
    if (fee_amount !== undefined) {
      updates.push(`fee_amount = $${paramIndex++}`);
      values.push(fee_amount);
    }
    if (currency !== undefined) {
      updates.push(`currency = $${paramIndex++}`);
      values.push(currency);
    }
    if (bill_from !== undefined) {
      updates.push(`bill_from = $${paramIndex++}`);
      values.push(bill_from || null);
    }
    if (is_active !== undefined) {
      updates.push(`is_active = $${paramIndex++}`);
      values.push(is_active);
    }
    
    updates.push(`updated_by = $${paramIndex++}`);
    values.push(req.user.id);
    
    values.push(id);
    const query = `UPDATE fee_masters SET ${updates.join(', ')} WHERE id = $${paramIndex} RETURNING *`;
    const result = await client.query(query, values);
    
    // Update reviewer allocations if provided
    if (reviewer_allocations !== undefined) {
      // Delete existing allocations
      await client.query('DELETE FROM fee_reviewer_allocations WHERE fee_master_id = $1', [id]);
      
      // Insert new allocations
      if (reviewer_allocations && reviewer_allocations.length > 0) {
        for (const allocation of reviewer_allocations) {
          await client.query(`
            INSERT INTO fee_reviewer_allocations (
              fee_master_id, reviewer_id, allocation_percentage, 
              allocation_amount, allocation_method
            ) VALUES ($1, $2, $3, $4, $5)
          `, [
            id,
            allocation.reviewer_id,
            allocation.allocation_percentage || null,
            allocation.allocation_amount || null,
            allocation.allocation_method || 'percentage'
          ]);
        }
      }
    }
    
    await client.query('COMMIT');
    
    await logAudit('fee_masters', id, 'UPDATE', currentFee, result.rows[0], req.user.id);
    
    res.json({
      success: true,
      message: 'Fee master updated successfully',
      data: result.rows[0]
    });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Update fee master error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to update fee master'
    });
  } finally {
    client.release();
  }
});

// End a fee master (set end_period and mark as ended)
router.put('/:id/end', auth, [
  body('end_period').notEmpty().withMessage('End period is required')
    .matches(/^[A-Z][a-z]{2}-\d{2}$/).withMessage('End period must be in MMM-YY format'),
], async (req, res) => {
  try {
    await ensureFeeMasterSchema();
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        errors: errors.array()
      });
    }

    const { id } = req.params;
    const { end_period } = req.body;
    
    const current = await pool.query('SELECT * FROM fee_masters WHERE id = $1', [id]);
    if (current.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Fee master not found'
      });
    }
    
    if (current.rows[0].is_ended) {
      return res.status(400).json({
        success: false,
        message: 'This fee master is already ended'
      });
    }
    
    const result = await pool.query(`
      UPDATE fee_masters 
      SET end_period = $1, is_ended = true, updated_by = $2
      WHERE id = $3
      RETURNING *
    `, [end_period, req.user.id, id]);
    
    await logAudit('fee_masters', id, 'END', current.rows[0], result.rows[0], req.user.id);
    
    res.json({
      success: true,
      message: 'Fee master ended successfully',
      data: result.rows[0]
    });
  } catch (error) {
    console.error('End fee master error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to end fee master'
    });
  }
});

// Reactivate/Renew a fee master (create new with same client+service type)
router.post('/:id/renew', auth, [
  body('start_period').notEmpty().withMessage('Start period is required')
    .matches(/^[A-Z][a-z]{2}-\d{2}$/).withMessage('Start period must be in MMM-YY format'),
  body('fee_amount').optional().isFloat({ min: 0 }).withMessage('Fee amount must be a positive number'),
], async (req, res) => {
  const client = await pool.connect();
  
  try {
    await ensureFeeMasterSchema();
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        errors: errors.array()
      });
    }

    const { id } = req.params;
    const { start_period, fee_amount, reviewer_allocations } = req.body;
    
    // Get the original fee master
    const original = await client.query(`
      SELECT fm.*, 
             COALESCE(json_agg(
               json_build_object(
                 'reviewer_id', fra.reviewer_id,
                 'allocation_percentage', fra.allocation_percentage,
                 'allocation_amount', fra.allocation_amount,
                 'allocation_method', fra.allocation_method
               )
             ) FILTER (WHERE fra.id IS NOT NULL), '[]') as reviewer_allocations
      FROM fee_masters fm
      LEFT JOIN fee_reviewer_allocations fra ON fm.id = fra.fee_master_id
      WHERE fm.id = $1
      GROUP BY fm.id
    `, [id]);
    
    if (original.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Fee master not found'
      });
    }
    
    const orig = original.rows[0];
    
    // Check if there's already an active fee master for this combination
    const existing = await client.query(
      'SELECT id FROM fee_masters WHERE unique_key = $1 AND is_ended = false AND id != $2',
      [orig.unique_key, id]
    );
    
    if (existing.rows.length > 0) {
      return res.status(400).json({
        success: false,
        message: 'An active fee master already exists for this client and service type'
      });
    }
    
    const finalFeeAmount = fee_amount !== undefined ? fee_amount : orig.fee_amount;
    const finalAllocations = reviewer_allocations || orig.reviewer_allocations;
    
    // Validate allocations
    if (finalAllocations && finalAllocations.length > 0 && finalAllocations[0]) {
      const totalAllocation = finalAllocations.reduce((sum, a) => {
        if (a.allocation_method === 'percentage') {
          return sum + (parseFloat(a.allocation_percentage) || 0);
        } else {
          return sum + (parseFloat(a.allocation_amount) || 0);
        }
      }, 0);
      
      const isPercentageMethod = finalAllocations[0]?.allocation_method === 'percentage';
      
      if (isPercentageMethod && Math.abs(totalAllocation - 100) > 0.01) {
        return res.status(400).json({
          success: false,
          message: `Reviewer allocations must total 100% (current: ${totalAllocation.toFixed(2)}%)`
        });
      }
      
      if (!isPercentageMethod && Math.abs(totalAllocation - finalFeeAmount) > 0.01) {
        return res.status(400).json({
          success: false,
          message: `Reviewer allocations must total the fee amount. Current: ${totalAllocation.toFixed(2)}`
        });
      }
    }
    
    await client.query('BEGIN');
    
    // Create new fee master
    const result = await client.query(`
      INSERT INTO fee_masters (
        unique_key, client_id, service_type_id, billing_name_id, 
        start_period, fee_amount, currency, bill_from,
        is_ended, created_by, updated_by
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, false, $9, $9)
      RETURNING *
    `, [
      orig.unique_key, orig.client_id, orig.service_type_id, orig.billing_name_id,
      start_period, finalFeeAmount, orig.currency, orig.bill_from, req.user.id
    ]);
    
    const newFeeMasterId = result.rows[0].id;
    
    // Copy reviewer allocations
    if (finalAllocations && finalAllocations.length > 0 && finalAllocations[0]) {
      for (const allocation of finalAllocations) {
        if (allocation.reviewer_id) {
          // Recalculate amount if fee changed and method is percentage
          let allocAmount = allocation.allocation_amount;
          if (allocation.allocation_method === 'percentage' && fee_amount !== undefined) {
            allocAmount = (allocation.allocation_percentage / 100) * finalFeeAmount;
          }
          
          await client.query(`
            INSERT INTO fee_reviewer_allocations (
              fee_master_id, reviewer_id, allocation_percentage, 
              allocation_amount, allocation_method
            ) VALUES ($1, $2, $3, $4, $5)
          `, [
            newFeeMasterId,
            allocation.reviewer_id,
            allocation.allocation_percentage || null,
            allocAmount || null,
            allocation.allocation_method || 'percentage'
          ]);
        }
      }
    }
    
    await client.query('COMMIT');
    
    await logAudit('fee_masters', newFeeMasterId, 'RENEW', { original_id: id }, result.rows[0], req.user.id);
    
    res.status(201).json({
      success: true,
      message: 'Fee master renewed successfully',
      data: result.rows[0]
    });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Renew fee master error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to renew fee master'
    });
  } finally {
    client.release();
  }
});

// Delete fee master (soft delete)
router.delete('/:id', auth, requireDelete, async (req, res) => {
  try {
    await ensureFeeMasterSchema();
    const { id } = req.params;
    
    const current = await pool.query('SELECT * FROM fee_masters WHERE id = $1', [id]);
    if (current.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Fee master not found'
      });
    }
    
    await pool.query(
      'UPDATE fee_masters SET is_active = false, updated_by = $1 WHERE id = $2',
      [req.user.id, id]
    );
    
    await logAudit('fee_masters', id, 'DELETE', current.rows[0], { is_active: false }, req.user.id);
    
    res.json({
      success: true,
      message: 'Fee master deleted successfully'
    });
  } catch (error) {
    console.error('Delete fee master error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to delete fee master'
    });
  }
});

module.exports = router;
