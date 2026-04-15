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

let billFromSchemaPromise = null;

async function ensureBillFromSchema() {
  if (!billFromSchemaPromise) {
    billFromSchemaPromise = (async () => {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS bill_from_masters (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          name VARCHAR(255) NOT NULL UNIQUE,
          is_active BOOLEAN DEFAULT true,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);

      await pool.query('CREATE INDEX IF NOT EXISTS idx_bill_from_masters_name ON bill_from_masters(name)');
    })().catch((error) => {
      billFromSchemaPromise = null;
      throw error;
    });
  }

  return billFromSchemaPromise;
}

router.get('/', auth, async (req, res) => {
  try {
    await ensureBillFromSchema();
    const { includeInactive } = req.query;

    let query = 'SELECT * FROM bill_from_masters WHERE 1=1';
    if (!includeInactive) {
      query += ' AND is_active = true';
    }
    query += ' ORDER BY name';

    const result = await pool.query(query);

    res.json({
      success: true,
      data: result.rows
    });
  } catch (error) {
    console.error('Get bill from masters error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch bill from masters'
    });
  }
});

router.get('/:id', auth, async (req, res) => {
  try {
    await ensureBillFromSchema();
    const result = await pool.query(
      'SELECT * FROM bill_from_masters WHERE id = $1',
      [req.params.id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Bill from master not found'
      });
    }

    res.json({
      success: true,
      data: result.rows[0]
    });
  } catch (error) {
    console.error('Get bill from master error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch bill from master'
    });
  }
});

router.post('/', auth, [
  body('name').trim().notEmpty().withMessage('Name is required'),
], async (req, res) => {
  try {
    await ensureBillFromSchema();
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        errors: errors.array()
      });
    }

    const { name } = req.body;

    const result = await pool.query(
      'INSERT INTO bill_from_masters (name) VALUES ($1) RETURNING *',
      [name]
    );

    await logAudit('bill_from_masters', result.rows[0].id, 'CREATE', null, result.rows[0], req.user.id);

    res.status(201).json({
      success: true,
      message: 'Bill from master created successfully',
      data: result.rows[0]
    });
  } catch (error) {
    console.error('Create bill from master error:', error);

    if (error.code === '23505') {
      return res.status(400).json({
        success: false,
        message: 'This bill from value already exists'
      });
    }

    res.status(500).json({
      success: false,
      message: 'Failed to create bill from master'
    });
  }
});

router.put('/:id', auth, [
  body('name').optional().trim().notEmpty().withMessage('Name cannot be empty'),
  body('is_active').optional().isBoolean(),
], async (req, res) => {
  try {
    await ensureBillFromSchema();
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        errors: errors.array()
      });
    }

    const { id } = req.params;
    const { name, is_active } = req.body;

    const current = await pool.query('SELECT * FROM bill_from_masters WHERE id = $1', [id]);
    if (current.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Bill from master not found'
      });
    }

    const updates = [];
    const values = [];
    let paramIndex = 1;

    if (name !== undefined) {
      updates.push(`name = $${paramIndex++}`);
      values.push(name);
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
    const result = await pool.query(
      `UPDATE bill_from_masters SET ${updates.join(', ')} WHERE id = $${paramIndex} RETURNING *`,
      values
    );

    await logAudit('bill_from_masters', id, 'UPDATE', current.rows[0], result.rows[0], req.user.id);

    res.json({
      success: true,
      message: 'Bill from master updated successfully',
      data: result.rows[0]
    });
  } catch (error) {
    console.error('Update bill from master error:', error);

    if (error.code === '23505') {
      return res.status(400).json({
        success: false,
        message: 'This bill from value already exists'
      });
    }

    res.status(500).json({
      success: false,
      message: 'Failed to update bill from master'
    });
  }
});

// Check dependencies for a bill from master
const getBillFromDependencies = async (dbClient, id) => {
  const billingRowResult = await queryLinkedRows(dbClient, `
    SELECT
      cbr.id,
      COUNT(*) OVER() AS total_count,
      c.name AS group_name,
      cbr.billing_name,
      st.name AS service_type_name,
      t.name AS reviewer_name,
      cbr.start_period,
      cbr.end_period
    FROM client_billing_rows cbr
    JOIN clients c ON c.id = cbr.client_id
    LEFT JOIN service_types st ON st.id = cbr.service_type_id
    LEFT JOIN teams t ON t.id = cbr.reviewer_id
    WHERE cbr.bill_from_id = $1
    ORDER BY c.name NULLS LAST, cbr.billing_name NULLS LAST, cbr.start_period NULLS LAST
    LIMIT $2
  `, [id]);

  const dependencies = [
    buildDependencyEntry({
      type: 'Group Billing Line',
      module: 'groups',
      count: billingRowResult.count,
      rows: billingRowResult.rows,
      mapRow: (row) => buildLinkedItem({
        id: row.id,
        label: `${safeText(row.group_name)} | ${safeText(row.billing_name, 'No client label')}`,
        line: `${safeText(row.service_type_name, 'No service type')} | Reviewer: ${safeText(row.reviewer_name, 'Not assigned')} | ${formatPeriodRange(row.start_period, row.end_period)}`,
        module: 'groups',
        type: 'Group Billing Line',
      }),
    }),
  ].filter(Boolean);

  return {
    dependencies,
    total: dependencies.reduce((sum, dependency) => sum + dependency.count, 0),
    message: buildDeleteBlockedMessage('bill from master', dependencies),
  };
};

router.get('/:id/dependencies', auth, async (req, res) => {
  try {
    await ensureBillFromSchema();
    const { id } = req.params;
    const data = await getBillFromDependencies(pool, id);
    res.json({ success: true, data });
  } catch (error) {
    console.error('Check bill from dependencies error:', error);
    res.status(500).json({ success: false, message: 'Failed to check dependencies' });
  }
});

// Check all linked records (active and inactive) for hard delete blocker
const getBillFromHardDeleteBlockers = async (dbClient, id) => {
  const allBillingRows = await queryLinkedRows(dbClient, `
    SELECT COUNT(*) as count FROM client_billing_rows WHERE bill_from_id = $1
  `, [id]);

  return parseInt(allBillingRows.rows[0]?.count || 0, 10);
};

router.delete('/:id', auth, requireDelete, async (req, res) => {
  try {
    await ensureBillFromSchema();
    const { id } = req.params;

    const current = await pool.query('SELECT * FROM bill_from_masters WHERE id = $1', [id]);
    if (current.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Bill from master not found'
      });
    }

    const wasActive = current.rows[0].is_active;

    // For active soft-delete, check only active/recent dependencies
    if (wasActive) {
      const dependencyData = await getBillFromDependencies(pool, id);
      if (dependencyData.total > 0) {
        return res.status(409).json({
          success: false,
          message: dependencyData.message,
          data: dependencyData,
        });
      }
      await pool.query('UPDATE bill_from_masters SET is_active = false WHERE id = $1', [id]);
    } else {
      // For hard delete, check ALL linked records (active and inactive)
      const hardDeleteBlockers = await getBillFromHardDeleteBlockers(pool, id);
      if (hardDeleteBlockers > 0) {
        return res.status(409).json({
          success: false,
          message: `Delete blocked. This bill from master still has ${hardDeleteBlockers} linked record(s) that prevent permanent deletion. This includes inactive records. Verify all linked items are truly unnecessary before retry.`,
        });
      }
      
      // Perform hard delete
      try {
        const deleteResult = await pool.query('DELETE FROM bill_from_masters WHERE id = $1', [id]);
        console.log('Bill from delete result:', deleteResult.rowCount);
      } catch (dbError) {
        console.error('Database delete error:', dbError.code, dbError.message, dbError.detail);
        if (dbError.code === '23503') {
          return res.status(409).json({
            success: false,
            message: 'Delete blocked: This bill from record is still referenced by other records in the database. Please ensure all linked records are removed before attempting to delete.',
          });
        }
        throw dbError;
      }
    }

    try {
      await logAudit('bill_from_masters', id, 'DELETE', current.rows[0], null, req.user.id);
    } catch (auditError) {
      console.error('Audit log error (non-blocking):', auditError);
    }

    res.json({
      success: true,
      message: wasActive ? 'Bill from master deleted successfully' : 'Bill from master permanently deleted successfully'
    });
  } catch (error) {
    console.error('Delete bill from master error:', error.message, error.code, error.detail);
    res.status(500).json({
      success: false,
      message: 'Failed to delete bill from master',
      error: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

module.exports = router;
