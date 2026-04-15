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

// PAN validation regex
const PAN_REGEX = /^[A-Z]{5}[0-9]{4}[A-Z]{1}$/;
// GSTIN validation regex
const GSTIN_REGEX = /^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}Z[0-9A-Z]{1}$/;
const derivePanFromGstin = (gstin) => (gstin ? gstin.toUpperCase().slice(2, 12) : null);

// Get all billing names
router.get('/', auth, async (req, res) => {
  try {
    const { includeInactive, clientId } = req.query;
    
    let query = `
      SELECT bn.*, c.name as client_name
      FROM billing_names bn
      LEFT JOIN clients c ON bn.client_id = c.id
      WHERE 1=1
    `;
    const params = [];
    
    if (!includeInactive) {
      query += ' AND bn.is_active = true';
    }
    
    if (clientId) {
      params.push(clientId);
      query += ` AND bn.client_id = $${params.length}`;
    }
    
    query += ' ORDER BY bn.name';
    
    const result = await pool.query(query, params);
    
    res.json({
      success: true,
      data: result.rows
    });
  } catch (error) {
    console.error('Get billing names error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch billing names'
    });
  }
});

// Get single billing name
router.get('/:id', auth, async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT bn.*, c.name as client_name 
       FROM billing_names bn 
       LEFT JOIN clients c ON bn.client_id = c.id 
       WHERE bn.id = $1`,
      [req.params.id]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Billing name not found'
      });
    }
    
    res.json({
      success: true,
      data: result.rows[0]
    });
  } catch (error) {
    console.error('Get billing name error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch billing name'
    });
  }
});

// Create billing name (client_id is optional — billing names can be standalone Client masters)
router.post('/', auth, [
  body('name').trim().notEmpty().withMessage('Name is required'),
  body('pan').optional({ values: 'falsy' }).matches(PAN_REGEX).withMessage('Invalid PAN format (e.g., ABCDE1234F)'),
  body('client_id').optional({ values: 'falsy' }).isUUID().withMessage('Invalid client ID'),
  body('gstin').optional({ values: 'falsy' }).matches(GSTIN_REGEX).withMessage('Invalid GSTIN format'),
], async (req, res) => {
  try {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        errors: errors.array()
      });
    }

    const { name, pan, client_id, gstin } = req.body;
    const normalizedGstin = gstin ? gstin.toUpperCase() : null;
    const normalizedPan = normalizedGstin ? derivePanFromGstin(normalizedGstin) : (pan ? pan.toUpperCase() : null);

    // Rule: GSTIN must be unique when provided
    if (normalizedGstin) {
      const gstinConflict = await pool.query(
        `SELECT id, name FROM billing_names WHERE UPPER(TRIM(gstin)) = $1 AND is_active = true LIMIT 1`,
        [normalizedGstin]
      );
      if (gstinConflict.rows.length > 0) {
        return res.status(400).json({ success: false, message: `GSTIN already in use by client "${gstinConflict.rows[0].name}"` });
      }
    }

    // Rule: when no GSTIN, PAN must be unique across ALL entries
    if (!normalizedGstin && normalizedPan) {
      const panConflict = await pool.query(
        `SELECT id, name FROM billing_names WHERE UPPER(TRIM(pan)) = $1 AND is_active = true LIMIT 1`,
        [normalizedPan]
      );
      if (panConflict.rows.length > 0) {
        return res.status(400).json({ success: false, message: `PAN already in use by client "${panConflict.rows[0].name}" (add GSTIN to allow multiple entities with the same PAN)` });
      }
    }

    const result = await pool.query(
      'INSERT INTO billing_names (name, pan, client_id, gstin) VALUES ($1, $2, $3, $4) RETURNING *',
      [name, normalizedPan, client_id || null, normalizedGstin]
    );

    await logAudit('billing_names', result.rows[0].id, 'CREATE', null, result.rows[0], req.user.id);

    res.status(201).json({
      success: true,
      message: 'Billing name created successfully',
      data: result.rows[0]
    });
  } catch (error) {
    console.error('Create billing name error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to create billing name'
    });
  }
});

// Update billing name (client_id is optional)
router.put('/:id', auth, [
  body('name').optional().trim().notEmpty().withMessage('Name cannot be empty'),
  body('pan').optional({ values: 'falsy' }).matches(PAN_REGEX).withMessage('Invalid PAN format'),
  body('client_id').optional({ values: 'falsy' }).isUUID().withMessage('Invalid client ID'),
  body('gstin').optional({ values: 'falsy' }).matches(GSTIN_REGEX).withMessage('Invalid GSTIN format'),
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

    const { name, pan, client_id, gstin, is_active } = req.body;
    const { id } = req.params;

    const current = await pool.query('SELECT * FROM billing_names WHERE id = $1', [id]);
    if (current.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Billing name not found'
      });
    }

    const updates = [];
    const values = [];
    let paramIndex = 1;

    if (name !== undefined) {
      updates.push(`name = $${paramIndex++}`);
      values.push(name);
    }
    const resolvedGstin = gstin !== undefined ? (gstin ? gstin.toUpperCase() : null) : current.rows[0].gstin;
    const resolvedPan = gstin !== undefined
      ? (resolvedGstin ? derivePanFromGstin(resolvedGstin) : (pan !== undefined ? (pan ? pan.toUpperCase() : null) : current.rows[0].pan))
      : (pan !== undefined ? (pan ? pan.toUpperCase() : null) : current.rows[0].pan);

    // Rule: GSTIN must be unique when provided (excluding the current record)
    if (resolvedGstin) {
      const gstinConflict = await pool.query(
        `SELECT id, name FROM billing_names WHERE UPPER(TRIM(gstin)) = $1 AND id <> $2 AND is_active = true LIMIT 1`,
        [resolvedGstin, id]
      );
      if (gstinConflict.rows.length > 0) {
        return res.status(400).json({ success: false, message: `GSTIN already in use by client "${gstinConflict.rows[0].name}"` });
      }
    }

    // Rule: when no GSTIN, PAN must be unique across ALL entries (excluding current record)
    if (!resolvedGstin && resolvedPan) {
      const panConflict = await pool.query(
        `SELECT id, name FROM billing_names WHERE UPPER(TRIM(pan)) = $1 AND id <> $2 AND is_active = true LIMIT 1`,
        [resolvedPan, id]
      );
      if (panConflict.rows.length > 0) {
        return res.status(400).json({ success: false, message: `PAN already in use by client "${panConflict.rows[0].name}" (add GSTIN to allow multiple entities with the same PAN)` });
      }
    }

    if (pan !== undefined || gstin !== undefined) {
      updates.push(`pan = $${paramIndex++}`);
      values.push(resolvedPan);
    }
    if (client_id !== undefined) {
      updates.push(`client_id = $${paramIndex++}`);
      values.push(client_id || null);
    }
    if (gstin !== undefined) {
      updates.push(`gstin = $${paramIndex++}`);
      values.push(resolvedGstin);
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
    const query = `UPDATE billing_names SET ${updates.join(', ')} WHERE id = $${paramIndex} RETURNING *`;
    const result = await pool.query(query, values);

    await logAudit('billing_names', id, 'UPDATE', current.rows[0], result.rows[0], req.user.id);

    res.json({
      success: true,
      message: 'Billing name updated successfully',
      data: result.rows[0]
    });
  } catch (error) {
    console.error('Update billing name error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to update billing name'
    });
  }
});

// Delete billing name
// Check dependencies for a billing name (client)
const getBillingNameDependencies = async (dbClient, id) => {
  const revenueResult = await queryLinkedRows(dbClient, `
    SELECT
      r.id,
      COUNT(*) OVER() AS total_count,
      COALESCE(sp.display_name, TO_CHAR(r.date, 'Mon-YY')) AS period_name,
      c.name AS group_name,
      r.invoice_no,
      r.is_unbilled
    FROM revenues r
    LEFT JOIN service_periods sp ON sp.id = r.service_period_id
    LEFT JOIN clients c ON c.id = r.client_id
    WHERE r.billing_name_id = $1
    ORDER BY sp.start_date NULLS LAST, r.date NULLS LAST, c.name NULLS LAST, r.invoice_no NULLS LAST
    LIMIT $2
  `, [id]);

  const feeMasterResult = await queryLinkedRows(dbClient, `
    SELECT
      fm.id,
      COUNT(*) OVER() AS total_count,
      c.name AS group_name,
      st.name AS service_type_name,
      fm.start_period,
      fm.end_period
    FROM fee_masters fm
    LEFT JOIN clients c ON c.id = fm.client_id
    LEFT JOIN service_types st ON st.id = fm.service_type_id
    WHERE fm.billing_name_id = $1
    ORDER BY fm.start_period NULLS LAST, c.name NULLS LAST, st.name NULLS LAST
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
        line: row.invoice_no
          ? `Invoice: ${row.invoice_no}`
          : (row.is_unbilled ? 'Unbilled revenue line' : 'Revenue line'),
        module: 'revenue',
        type: 'Revenue',
      }),
    }),
    buildDependencyEntry({
      type: 'Fee Master',
      module: 'revenue',
      count: feeMasterResult.count,
      rows: feeMasterResult.rows,
      mapRow: (row) => buildLinkedItem({
        id: row.id,
        label: `${safeText(row.group_name, 'Unassigned group')} | ${safeText(row.service_type_name, 'No service type')}`,
        line: `Fee period: ${formatPeriodRange(row.start_period, row.end_period)}`,
        module: 'revenue',
        type: 'Fee Master',
      }),
    }),
  ].filter(Boolean);

  return {
    dependencies,
    total: dependencies.reduce((sum, dependency) => sum + dependency.count, 0),
    message: buildDeleteBlockedMessage('client master', dependencies),
  };
};

router.get('/:id/dependencies', auth, async (req, res) => {
  try {
    const { id } = req.params;
    const data = await getBillingNameDependencies(pool, id);
    res.json({ success: true, data });
  } catch (error) {
    console.error('Check billing name dependencies error:', error);
    res.status(500).json({ success: false, message: 'Failed to check dependencies' });
  }
});

router.delete('/:id', auth, requireDelete, async (req, res) => {
  const dbClient = await pool.connect();
  try {
    const { id } = req.params;

    const current = await dbClient.query('SELECT * FROM billing_names WHERE id = $1', [id]);
    if (current.rows.length === 0) {
      return res.status(404).json({ success: false, message: 'Client not found' });
    }

    const dependencyData = await getBillingNameDependencies(dbClient, id);
    if (dependencyData.total > 0) {
      return res.status(409).json({
        success: false,
        message: dependencyData.message,
        data: dependencyData,
      });
    }

    await dbClient.query('BEGIN');
    await dbClient.query('DELETE FROM billing_names WHERE id = $1', [id]);

    await dbClient.query('COMMIT');

    try {
      await logAudit('billing_names', id, 'DELETE', current.rows[0], null, req.user.id);
    } catch (auditError) {
      console.error('Billing name delete audit log error:', auditError);
    }

    res.json({
      success: true,
      message: 'Client deleted successfully',
      data: {}
    });
  } catch (error) {
    await dbClient.query('ROLLBACK').catch(() => {});
    console.error('Delete billing name error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to delete client'
    });
  } finally {
    dbClient.release();
  }
});

module.exports = router;
