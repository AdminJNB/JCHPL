const express = require('express');
const router = express.Router();
const { body, validationResult } = require('express-validator');
const { pool, logAudit } = require('../database/db');
const { auth, requireDelete } = require('../middleware/auth');
const {
  buildDeleteBlockedMessage,
  buildDependencyEntry,
  buildLinkedItem,
  formatPeriodRange,
  queryLinkedRows,
  safeText,
} = require('../utils/deleteDependencyHelpers');

// HSN to GST Rate mapping (common rates)
const HSN_GST_MAP = {
  '9971': 18, // Financial and related services
  '9972': 18, // Real estate services
  '9973': 18, // Leasing services
  '9983': 18, // Other professional services
  '9984': 18, // Telecommunications
  '9985': 18, // Support services
  '9986': 18, // Support services
  '9987': 18, // Maintenance and repair
  '9988': 18, // Manufacturing services
  '9989': 18, // Other manufacturing services
  '9991': 18, // Public administration
  '9992': 18, // Education
  '9993': 18, // Human health
  '9994': 18, // Sewage and waste
  '9995': 18, // Membership organizations
  '9996': 18, // Recreational services
  '9997': 18, // Other services
  '9998': 18, // Domestic services
  '9999': 18, // Extra-territorial services
  '998311': 18, // Management consulting
  '998312': 18, // Business consulting
  '998313': 18, // IT consulting
  '998314': 18, // IT design and development
  '998315': 18, // Hosting and IT infrastructure
};

// Get GST rate suggestion based on HSN code
router.get('/hsn-gst/:hsn', auth, async (req, res) => {
  const { hsn } = req.params;
  
  // Try exact match first, then prefix matches
  let gstRate = HSN_GST_MAP[hsn];
  
  if (!gstRate) {
    // Try progressively shorter prefixes
    for (let i = hsn.length - 1; i >= 4; i--) {
      const prefix = hsn.substring(0, i);
      if (HSN_GST_MAP[prefix]) {
        gstRate = HSN_GST_MAP[prefix];
        break;
      }
    }
  }
  
  res.json({
    success: true,
    data: {
      hsn_code: hsn,
      suggested_gst_rate: gstRate || 18 // Default to 18%
    }
  });
});

// Get all service types
router.get('/', auth, async (req, res) => {
  try {
    const { includeInactive } = req.query;
    
    let query = 'SELECT * FROM service_types';
    if (!includeInactive) {
      query += ' WHERE is_active = true';
    }
    query += ' ORDER BY name';
    
    const result = await pool.query(query);
    
    res.json({
      success: true,
      data: result.rows
    });
  } catch (error) {
    console.error('Get service types error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch service types'
    });
  }
});

// Get single service type
router.get('/:id', auth, async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT * FROM service_types WHERE id = $1',
      [req.params.id]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Service type not found'
      });
    }
    
    res.json({
      success: true,
      data: result.rows[0]
    });
  } catch (error) {
    console.error('Get service type error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch service type'
    });
  }
});

// Create service type
router.post('/', auth, [
  body('name').trim().notEmpty().withMessage('Name is required'),
  body('hsn_code').optional().trim(),
  body('gst_rate').optional().isFloat({ min: 0, max: 28 }).withMessage('GST rate must be between 0 and 28'),
], async (req, res) => {
  try {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        errors: errors.array()
      });
    }

    const { name, hsn_code, gst_rate } = req.body;

    // Auto-suggest GST rate if not provided but HSN code is
    let finalGstRate = gst_rate;
    if (hsn_code && !gst_rate) {
      finalGstRate = HSN_GST_MAP[hsn_code] || 18;
    }

    const result = await pool.query(
      'INSERT INTO service_types (name, hsn_code, gst_rate) VALUES ($1, $2, $3) RETURNING *',
      [name, hsn_code || null, finalGstRate || 18]
    );

    await logAudit('service_types', result.rows[0].id, 'CREATE', null, result.rows[0], req.user.id);

    res.status(201).json({
      success: true,
      message: 'Service type created successfully',
      data: result.rows[0]
    });
  } catch (error) {
    console.error('Create service type error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to create service type'
    });
  }
});

// Update service type
router.put('/:id', auth, [
  body('name').optional().trim().notEmpty().withMessage('Name cannot be empty'),
  body('hsn_code').optional().trim(),
  body('gst_rate').optional().isFloat({ min: 0, max: 28 }).withMessage('GST rate must be between 0 and 28'),
  body('is_active').optional().isBoolean(),
], async (req, res) => {
  try {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        errors: errors.array()
      });
    }

    const { name, hsn_code, gst_rate, is_active } = req.body;
    const { id } = req.params;

    const current = await pool.query('SELECT * FROM service_types WHERE id = $1', [id]);
    if (current.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Service type not found'
      });
    }

    const updates = [];
    const values = [];
    let paramIndex = 1;

    if (name !== undefined) {
      updates.push(`name = $${paramIndex++}`);
      values.push(name);
    }
    if (hsn_code !== undefined) {
      updates.push(`hsn_code = $${paramIndex++}`);
      values.push(hsn_code || null);
    }
    if (gst_rate !== undefined) {
      updates.push(`gst_rate = $${paramIndex++}`);
      values.push(gst_rate);
    }
    if (is_active !== undefined) {
      updates.push(`is_active = $${paramIndex++}`);
      values.push(is_active);
    }

    if (updates.length === 0) {
      return res.status(400).json({
        success: false,
        message: 'No fields to update'
      });
    }

    values.push(id);
    const query = `UPDATE service_types SET ${updates.join(', ')} WHERE id = $${paramIndex} RETURNING *`;
    const result = await pool.query(query, values);

    await logAudit('service_types', id, 'UPDATE', current.rows[0], result.rows[0], req.user.id);

    res.json({
      success: true,
      message: 'Service type updated successfully',
      data: result.rows[0]
    });
  } catch (error) {
    console.error('Update service type error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to update service type'
    });
  }
});

// Check dependencies for a service type
const getServiceTypeDependencies = async (dbClient, id) => {
  const revenueResult = await queryLinkedRows(dbClient, `
    SELECT
      r.id,
      COUNT(*) OVER() AS total_count,
      COALESCE(sp.display_name, TO_CHAR(r.date, 'Mon-YY')) AS period_name,
      c.name AS group_name,
      bn.name AS client_name
    FROM revenues r
    LEFT JOIN service_periods sp ON sp.id = r.service_period_id
    LEFT JOIN clients c ON c.id = r.client_id
    LEFT JOIN billing_names bn ON bn.id = r.billing_name_id
    WHERE r.service_type_id = $1
    ORDER BY sp.start_date NULLS LAST, r.date NULLS LAST, c.name NULLS LAST
    LIMIT $2
  `, [id]);

  const expenseResult = await queryLinkedRows(dbClient, `
    SELECT
      e.id,
      COUNT(*) OVER() AS total_count,
      COALESCE(sp.display_name, TO_CHAR(e.date, 'Mon-YY')) AS period_name,
      c.name AS group_name,
      t.name AS team_name
    FROM expenses e
    LEFT JOIN service_periods sp ON sp.id = e.service_period_id
    LEFT JOIN clients c ON c.id = e.client_id
    LEFT JOIN teams t ON t.id = e.team_id
    WHERE e.service_type_id = $1
    ORDER BY sp.start_date NULLS LAST, e.date NULLS LAST, t.name NULLS LAST
    LIMIT $2
  `, [id]);

  const billingRowResult = await queryLinkedRows(dbClient, `
    SELECT
      cbr.id,
      COUNT(*) OVER() AS total_count,
      c.name AS group_name,
      cbr.billing_name,
      bf.name AS bill_from_name,
      t.name AS reviewer_name,
      cbr.start_period,
      cbr.end_period
    FROM client_billing_rows cbr
    JOIN clients c ON c.id = cbr.client_id
    LEFT JOIN bill_from_masters bf ON bf.id = cbr.bill_from_id
    LEFT JOIN teams t ON t.id = cbr.reviewer_id
    WHERE cbr.service_type_id = $1
    ORDER BY c.name NULLS LAST, cbr.billing_name NULLS LAST, cbr.start_period NULLS LAST
    LIMIT $2
  `, [id]);

  const feeMasterResult = await queryLinkedRows(dbClient, `
    SELECT
      fm.id,
      COUNT(*) OVER() AS total_count,
      c.name AS group_name,
      fm.start_period,
      fm.end_period
    FROM fee_masters fm
    LEFT JOIN clients c ON c.id = fm.client_id
    WHERE fm.service_type_id = $1
      AND fm.is_active = true
    ORDER BY fm.start_period NULLS LAST, c.name NULLS LAST
    LIMIT $2
  `, [id]);

  const dependencies = [
    buildDependencyEntry({
      type: 'Revenue',
      module: 'revenue',
      count: revenueResult.count,
      rows: revenueResult.rows,
      mapRow: (row) => buildLinkedItem({
        id: row.id,
        label: `${safeText(row.group_name, 'Unassigned group')} | ${safeText(row.period_name, 'No period')}`,
        line: `Client: ${safeText(row.client_name, 'Not assigned')}`,
        module: 'revenue',
        type: 'Revenue',
      }),
    }),
    buildDependencyEntry({
      type: 'Expense',
      module: 'expenses',
      count: expenseResult.count,
      rows: expenseResult.rows,
      mapRow: (row) => buildLinkedItem({
        id: row.id,
        label: `${safeText(row.team_name, 'Unassigned team')} | ${safeText(row.period_name, 'No period')}`,
        line: `Group: ${safeText(row.group_name, 'Not assigned')}`,
        module: 'expenses',
        type: 'Expense',
      }),
    }),
    buildDependencyEntry({
      type: 'Group Billing Line',
      module: 'groups',
      count: billingRowResult.count,
      rows: billingRowResult.rows,
      mapRow: (row) => buildLinkedItem({
        id: row.id,
        label: `${safeText(row.group_name)} | ${safeText(row.billing_name, 'No client label')}`,
        line: `${formatPeriodRange(row.start_period, row.end_period)} | Bill from: ${safeText(row.bill_from_name, 'Not assigned')} | Reviewer: ${safeText(row.reviewer_name, 'Not assigned')}`,
        module: 'groups',
        type: 'Group Billing Line',
      }),
    }),
    buildDependencyEntry({
      type: 'Fee Master',
      module: 'revenue',
      count: feeMasterResult.count,
      rows: feeMasterResult.rows,
      mapRow: (row) => buildLinkedItem({
        id: row.id,
        label: safeText(row.group_name, 'Unassigned group'),
        line: `Fee period: ${formatPeriodRange(row.start_period, row.end_period)}`,
        module: 'revenue',
        type: 'Fee Master',
      }),
    }),
  ].filter(Boolean);

  return {
    dependencies,
    total: dependencies.reduce((sum, dependency) => sum + dependency.count, 0),
    message: buildDeleteBlockedMessage('service type master', dependencies),
  };
};

router.get('/:id/dependencies', auth, async (req, res) => {
  try {
    const { id } = req.params;
    const data = await getServiceTypeDependencies(pool, id);
    res.json({ success: true, data });
  } catch (error) {
    console.error('Check service type dependencies error:', error);
    res.status(500).json({ success: false, message: 'Failed to check dependencies' });
  }
});

// Delete service type
// Check all linked records (active and inactive) for hard delete blocker
const getServiceTypeHardDeleteBlockers = async (dbClient, id) => {
  const allRevenues = await queryLinkedRows(dbClient, `
    SELECT COUNT(*) as count FROM revenues WHERE service_type_id = $1
  `, [id]);

  const allExpenses = await queryLinkedRows(dbClient, `
    SELECT COUNT(*) as count FROM expenses WHERE service_type_id = $1
  `, [id]);

  const allBillingRows = await queryLinkedRows(dbClient, `
    SELECT COUNT(*) as count FROM client_billing_rows WHERE service_type_id = $1
  `, [id]);

  const allFeeMasters = await queryLinkedRows(dbClient, `
    SELECT COUNT(*) as count FROM fee_masters WHERE service_type_id = $1
  `, [id]);

  return (
    parseInt(allRevenues.rows[0]?.count || 0, 10) +
    parseInt(allExpenses.rows[0]?.count || 0, 10) +
    parseInt(allBillingRows.rows[0]?.count || 0, 10) +
    parseInt(allFeeMasters.rows[0]?.count || 0, 10)
  );
};

router.delete('/:id', auth, requireDelete, async (req, res) => {
  try {
    const { id } = req.params;

    const current = await pool.query('SELECT * FROM service_types WHERE id = $1', [id]);
    if (current.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Service type not found'
      });
    }

    const wasActive = current.rows[0].is_active;

    // For active soft-delete, check only active/recent dependencies
    if (wasActive) {
      const dependencyData = await getServiceTypeDependencies(pool, id);
      if (dependencyData.total > 0) {
        return res.status(409).json({
          success: false,
          message: dependencyData.message,
          data: dependencyData,
        });
      }
      await pool.query('UPDATE service_types SET is_active = false WHERE id = $1', [id]);
    } else {
      // For hard delete, check ALL linked records (active and inactive)
      const hardDeleteBlockers = await getServiceTypeHardDeleteBlockers(pool, id);
      if (hardDeleteBlockers > 0) {
        return res.status(409).json({
          success: false,
          message: `Delete blocked. This service type master still has ${hardDeleteBlockers} linked record(s) that prevent permanent deletion. This includes inactive records. Verify all linked items are truly unnecessary before retry.`,
        });
      }
      
      // Perform hard delete
      try {
        const deleteResult = await pool.query('DELETE FROM service_types WHERE id = $1', [id]);
        console.log('Service type delete result:', deleteResult.rowCount);
      } catch (dbError) {
        console.error('Database delete error:', dbError.code, dbError.message, dbError.detail);
        if (dbError.code === '23503') {
          return res.status(409).json({
            success: false,
            message: 'Delete blocked: This service type is still referenced by other records in the database. Please ensure all linked records are removed before attempting to delete.',
          });
        }
        throw dbError;
      }
    }

    try {
      await logAudit('service_types', id, 'DELETE', current.rows[0], null, req.user.id);
    } catch (auditError) {
      console.error('Audit log error (non-blocking):', auditError);
    }

    res.json({
      success: true,
      message: wasActive ? 'Service type deleted successfully' : 'Service type permanently deleted successfully'
    });
  } catch (error) {
    console.error('Delete service type error:', error.message, error.code, error.detail);
    res.status(500).json({
      success: false,
      message: 'Failed to delete service type',
      error: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

module.exports = router;
