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

let vendorSchemaPromise = null;

async function ensureVendorSchema() {
  if (!vendorSchemaPromise) {
    vendorSchemaPromise = (async () => {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS vendors (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          name VARCHAR(255) NOT NULL,
          gstin VARCHAR(15),
          pan VARCHAR(10),
          contact_person VARCHAR(255),
          email VARCHAR(255),
          mobile VARCHAR(20),
          is_active BOOLEAN DEFAULT true,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);
      await pool.query('CREATE INDEX IF NOT EXISTS idx_vendors_name ON vendors(name)');
    })().catch((error) => {
      vendorSchemaPromise = null;
      throw error;
    });
  }
  return vendorSchemaPromise;
}

// Get all vendors
router.get('/', auth, async (req, res) => {
  try {
    await ensureVendorSchema();
    const { includeInactive } = req.query;

    let query = 'SELECT * FROM vendors WHERE 1=1';
    if (!includeInactive && includeInactive !== 'true') {
      query += ' AND is_active = true';
    }
    query += ' ORDER BY name';

    const result = await pool.query(query);
    res.json({ success: true, data: result.rows });
  } catch (error) {
    console.error('Get vendors error:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch vendors' });
  }
});

// Get single vendor
router.get('/:id', auth, async (req, res) => {
  try {
    await ensureVendorSchema();
    const result = await pool.query('SELECT * FROM vendors WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, message: 'Vendor not found' });
    }
    res.json({ success: true, data: result.rows[0] });
  } catch (error) {
    console.error('Get vendor error:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch vendor' });
  }
});

// Create vendor
router.post('/', auth, [
  body('name').trim().notEmpty().withMessage('Name is required'),
  body('gstin').optional({ values: 'falsy' }).trim(),
  body('pan').optional({ values: 'falsy' }).trim(),
  body('contact_person').optional({ values: 'falsy' }).trim(),
  body('email').optional({ values: 'falsy' }).isEmail().withMessage('Invalid email format'),
  body('mobile').optional({ values: 'falsy' }).trim(),
], async (req, res) => {
  try {
    await ensureVendorSchema();
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ success: false, errors: errors.array() });
    }

    const { name, contact_person, email, mobile } = req.body;
    const gstin = req.body.gstin ? req.body.gstin.trim().toUpperCase() : null;
    // Auto-extract PAN from GSTIN if not explicitly provided
    const gstinPan = gstin && gstin.length === 15 ? gstin.substring(2, 12) : null;
    const pan = (req.body.pan ? req.body.pan.trim().toUpperCase() : null) || gstinPan;

    // Check duplicate name
    const nameCheck = await pool.query(
      'SELECT id FROM vendors WHERE LOWER(name) = LOWER($1) AND is_active = true',
      [name.trim()]
    );
    if (nameCheck.rows.length > 0) {
      return res.status(400).json({ success: false, message: 'A vendor with this name already exists' });
    }

    // Check GSTIN uniqueness
    if (gstin) {
      const gstinCheck = await pool.query(
        'SELECT id, name FROM vendors WHERE UPPER(gstin) = $1 AND is_active = true',
        [gstin]
      );
      if (gstinCheck.rows.length > 0) {
        return res.status(400).json({
          success: false,
          message: `GSTIN already registered for vendor: ${gstinCheck.rows[0].name}`
        });
      }
    }

    // PAN uniqueness: only enforce when no GSTIN is provided.
    // If a GSTIN is present it is already unique, so the same PAN under a different
    // GSTIN is a valid multi-state registration and must be allowed.
    if (pan && !gstin) {
      const panCheck = await pool.query(
        `SELECT id, name FROM vendors
         WHERE (
           UPPER(pan) = $1
           OR UPPER(SUBSTRING(COALESCE(gstin, ''), 3, 10)) = $1
         ) AND is_active = true`,
        [pan]
      );
      if (panCheck.rows.length > 0) {
        return res.status(400).json({
          success: false,
          message: `PAN ${pan} already exists for vendor: ${panCheck.rows[0].name}. Duplicate entry not allowed.`
        });
      }
    }

    const result = await pool.query(
      `INSERT INTO vendors (name, gstin, pan, contact_person, email, mobile)
       VALUES ($1, $2, $3, $4, $5, $6) RETURNING *`,
      [name.trim(), gstin || null, pan || null, contact_person || null, email || null, mobile || null]
    );

    await logAudit('vendors', result.rows[0].id, 'CREATE', null, result.rows[0], req.user.id);

    res.status(201).json({
      success: true,
      message: 'Vendor created successfully',
      data: result.rows[0]
    });
  } catch (error) {
    console.error('Create vendor error:', error);
    res.status(500).json({ success: false, message: 'Failed to create vendor' });
  }
});

// Update vendor
router.put('/:id', auth, [
  body('name').optional().trim().notEmpty().withMessage('Name cannot be empty'),
  body('gstin').optional({ values: 'falsy' }).trim(),
  body('pan').optional({ values: 'falsy' }).trim(),
  body('contact_person').optional({ values: 'falsy' }).trim(),
  body('email').optional({ values: 'falsy' }).isEmail().withMessage('Invalid email format'),
  body('mobile').optional({ values: 'falsy' }).trim(),
  body('is_active').optional().isBoolean(),
], async (req, res) => {
  try {
    await ensureVendorSchema();
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ success: false, errors: errors.array() });
    }

    const { id } = req.params;
    const current = await pool.query('SELECT * FROM vendors WHERE id = $1', [id]);
    if (current.rows.length === 0) {
      return res.status(404).json({ success: false, message: 'Vendor not found' });
    }

    const { name, contact_person, email, mobile, is_active } = req.body;
    const gstin = req.body.gstin !== undefined
      ? (req.body.gstin ? req.body.gstin.trim().toUpperCase() : null)
      : undefined;
    // Auto-extract PAN from new GSTIN if PAN not explicitly provided
    const gstinPan = gstin && gstin.length === 15 ? gstin.substring(2, 12) : null;
    const pan = req.body.pan !== undefined
      ? (req.body.pan ? req.body.pan.trim().toUpperCase() : null) || gstinPan
      : undefined;

    // Check duplicate name if name is being changed
    if (name !== undefined && name.trim().toLowerCase() !== current.rows[0].name.toLowerCase()) {
      const nameCheck = await pool.query(
        'SELECT id FROM vendors WHERE LOWER(name) = LOWER($1) AND is_active = true AND id != $2',
        [name.trim(), id]
      );
      if (nameCheck.rows.length > 0) {
        return res.status(400).json({ success: false, message: 'A vendor with this name already exists' });
      }
    }

    // Check GSTIN uniqueness if GSTIN is being changed
    const newGstin = gstin !== undefined ? gstin : current.rows[0].gstin;
    if (gstin !== undefined && gstin && gstin !== (current.rows[0].gstin || '').toUpperCase()) {
      const gstinCheck = await pool.query(
        'SELECT id, name FROM vendors WHERE UPPER(gstin) = $1 AND is_active = true AND id != $2',
        [gstin, id]
      );
      if (gstinCheck.rows.length > 0) {
        return res.status(400).json({
          success: false,
          message: `GSTIN already registered for vendor: ${gstinCheck.rows[0].name}`
        });
      }
    }

    // PAN uniqueness: only enforce when the vendor being saved has no GSTIN.
    // A vendor with a GSTIN already has a unique identifier; the same PAN under a
    // different GSTIN is a valid multi-state registration and must be allowed.
    const effectiveGstin = gstin !== undefined ? gstin : (current.rows[0].gstin || null);
    if (pan !== undefined && pan && !effectiveGstin) {
      const existingPan = current.rows[0].pan ? current.rows[0].pan.toUpperCase() : null;
      if (pan !== existingPan) {
        const panCheck = await pool.query(
          `SELECT id, name FROM vendors
           WHERE (
             UPPER(pan) = $1
             OR UPPER(SUBSTRING(COALESCE(gstin, ''), 3, 10)) = $1
           ) AND is_active = true AND id != $2`,
          [pan, id]
        );
        if (panCheck.rows.length > 0) {
          return res.status(400).json({
            success: false,
            message: `PAN ${pan} already exists for vendor: ${panCheck.rows[0].name}. Duplicate entry not allowed.`
          });
        }
      }
    }

    const updates = [];
    const values = [];
    let paramIndex = 1;

    if (name !== undefined) { updates.push(`name = $${paramIndex++}`); values.push(name.trim()); }
    if (gstin !== undefined) { updates.push(`gstin = $${paramIndex++}`); values.push(gstin || null); }
    if (pan !== undefined) { updates.push(`pan = $${paramIndex++}`); values.push(pan || null); }
    if (contact_person !== undefined) { updates.push(`contact_person = $${paramIndex++}`); values.push(contact_person || null); }
    if (email !== undefined) { updates.push(`email = $${paramIndex++}`); values.push(email || null); }
    if (mobile !== undefined) { updates.push(`mobile = $${paramIndex++}`); values.push(mobile || null); }
    if (is_active !== undefined) { updates.push(`is_active = $${paramIndex++}`); values.push(is_active); }
    if (updates.length === 0) {
      return res.status(400).json({ success: false, message: 'No fields to update' });
    }

    values.push(id);
    const query = `UPDATE vendors SET ${updates.join(', ')} WHERE id = $${paramIndex} RETURNING *`;
    const result = await pool.query(query, values);

    await logAudit('vendors', id, 'UPDATE', current.rows[0], result.rows[0], req.user.id);

    res.json({ success: true, message: 'Vendor updated successfully', data: result.rows[0] });
  } catch (error) {
    console.error('Update vendor error:', error);
    res.status(500).json({ success: false, message: 'Failed to update vendor' });
  }
});

// Check dependencies for a vendor
const getVendorDependencies = async (dbClient, id) => {
  const recurringResult = await queryLinkedRows(dbClient, `
    SELECT
      re.id,
      COUNT(*) OVER() AS total_count,
      eh.name AS expense_head_name,
      re.start_period,
      re.end_period
    FROM recurring_expenses re
    LEFT JOIN expense_heads eh ON eh.id = re.expense_head_id
    WHERE re.vendor_id = $1
    ORDER BY re.start_period NULLS LAST, eh.name NULLS LAST
    LIMIT $2
  `, [id]);

  const dependencies = [
    buildDependencyEntry({
      type: 'Recurring Expense',
      module: 'recurring',
      count: recurringResult.count,
      rows: recurringResult.rows,
      mapRow: (row) => buildLinkedItem({
        id: row.id,
        label: safeText(row.expense_head_name, 'No expense head'),
        line: `Recurring period: ${formatPeriodRange(row.start_period, row.end_period)}`,
        module: 'recurring',
        type: 'Recurring Expense',
      }),
    }),
  ].filter(Boolean);

  return {
    dependencies,
    total: dependencies.reduce((sum, dependency) => sum + dependency.count, 0),
    message: buildDeleteBlockedMessage('vendor master', dependencies),
  };
};

router.get('/:id/dependencies', auth, async (req, res) => {
  try {
    await ensureVendorSchema();
    const { id } = req.params;
    const data = await getVendorDependencies(pool, id);
    res.json({ success: true, data });
  } catch (error) {
    console.error('Check vendor dependencies error:', error);
    res.status(500).json({ success: false, message: 'Failed to check dependencies' });
  }
});

// Check all linked records (active and inactive) for hard delete blocker
const getVendorHardDeleteBlockers = async (dbClient, id) => {
  const allRecurring = await queryLinkedRows(dbClient, `
    SELECT COUNT(*) as count FROM recurring_expenses WHERE vendor_id = $1
  `, [id]);

  return parseInt(allRecurring.rows[0]?.count || 0, 10);
};

// Delete vendor (soft delete / hard delete if already inactive)
router.delete('/:id', auth, requireDelete, async (req, res) => {
  try {
    await ensureVendorSchema();
    const { id } = req.params;

    const current = await pool.query('SELECT * FROM vendors WHERE id = $1', [id]);
    if (current.rows.length === 0) {
      return res.status(404).json({ success: false, message: 'Vendor not found' });
    }

    const wasActive = current.rows[0].is_active;

    // For active soft-delete, check only active/recent dependencies
    if (wasActive) {
      const dependencyData = await getVendorDependencies(pool, id);
      if (dependencyData.total > 0) {
        return res.status(409).json({
          success: false,
          message: dependencyData.message,
          data: dependencyData,
        });
      }
      await pool.query('UPDATE vendors SET is_active = false WHERE id = $1', [id]);
    } else {
      // For hard delete, check ALL linked records (active and inactive)
      const hardDeleteBlockers = await getVendorHardDeleteBlockers(pool, id);
      if (hardDeleteBlockers > 0) {
        return res.status(409).json({
          success: false,
          message: `Delete blocked. This vendor master still has ${hardDeleteBlockers} linked record(s) that prevent permanent deletion. This includes inactive records. Verify all linked items are truly unnecessary before retry.`,
        });
      }
      
      // Perform hard delete
      try {
        const deleteResult = await pool.query('DELETE FROM vendors WHERE id = $1', [id]);
        console.log('Vendor delete result:', deleteResult.rowCount);
      } catch (dbError) {
        console.error('Database delete error:', dbError.code, dbError.message, dbError.detail);
        if (dbError.code === '23503') {
          return res.status(409).json({
            success: false,
            message: 'Delete blocked: This vendor is still referenced by other records in the database. Please ensure all linked records are removed before attempting to delete.',
          });
        }
        throw dbError;
      }
    }

    try {
      await logAudit('vendors', id, 'DELETE', current.rows[0], null, req.user.id);
    } catch (auditError) {
      console.error('Audit log error (non-blocking):', auditError);
    }

    res.json({ success: true, message: wasActive ? 'Vendor deleted successfully' : 'Vendor permanently deleted successfully' });
  } catch (error) {
    console.error('Delete vendor error:', error.message, error.code, error.detail);
    res.status(500).json({ success: false, message: 'Failed to delete vendor', error: process.env.NODE_ENV === 'development' ? error.message : undefined });
  }
});

module.exports = router;
