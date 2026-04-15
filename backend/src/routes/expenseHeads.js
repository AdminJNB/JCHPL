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

let expenseHeadSchemaPromise = null;

async function ensureExpenseHeadSchema() {
  if (!expenseHeadSchemaPromise) {
    expenseHeadSchemaPromise = (async () => {
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

      await pool.query(`
        CREATE TABLE IF NOT EXISTS recurring_expenses (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          expense_head_id UUID REFERENCES expense_heads(id) NOT NULL,
          amount DECIMAL(15, 2) NOT NULL DEFAULT 0,
          start_period VARCHAR(10) NOT NULL,
          end_period VARCHAR(10),
          is_active BOOLEAN DEFAULT true,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          created_by UUID REFERENCES users(id),
          updated_by UUID REFERENCES users(id)
        );
      `);

      await pool.query(`
        ALTER TABLE recurring_expenses
          ADD COLUMN IF NOT EXISTS vendor_id UUID REFERENCES vendors(id);
      `).catch(() => {});

      await pool.query(`
        ALTER TABLE teams
          ADD COLUMN IF NOT EXISTS expense_head_id UUID REFERENCES expense_heads(id),
          ADD COLUMN IF NOT EXISTS start_period VARCHAR(10),
          ADD COLUMN IF NOT EXISTS end_period VARCHAR(10);
      `);

      await pool.query(`
        CREATE TABLE IF NOT EXISTS team_client_allocations (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          team_id UUID REFERENCES teams(id) ON DELETE CASCADE,
          client_id UUID REFERENCES clients(id) ON DELETE CASCADE,
          allocation_percentage DECIMAL(8,4) NOT NULL DEFAULT 0,
          allocation_amount DECIMAL(15,2),
          allocation_method VARCHAR(20) DEFAULT 'percentage',
          expense_head_id UUID REFERENCES expense_heads(id),
          start_period VARCHAR(10),
          end_period VARCHAR(10),
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);

      await pool.query(`
        ALTER TABLE team_client_allocations
          ADD COLUMN IF NOT EXISTS expense_head_id UUID REFERENCES expense_heads(id),
          ADD COLUMN IF NOT EXISTS start_period VARCHAR(10),
          ADD COLUMN IF NOT EXISTS end_period VARCHAR(10);
      `).catch(() => {});
    })().catch((error) => {
      expenseHeadSchemaPromise = null;
      throw error;
    });
  }

  return expenseHeadSchemaPromise;
}

// Get all expense heads
router.get('/', auth, async (req, res) => {
  try {
    const { includeInactive } = req.query;
    
    let query = 'SELECT * FROM expense_heads WHERE 1=1';
    
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
    console.error('Get expense heads error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch expense heads'
    });
  }
});

// Get single expense head
router.get('/:id', auth, async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT * FROM expense_heads WHERE id = $1',
      [req.params.id]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Expense head not found'
      });
    }
    
    res.json({
      success: true,
      data: result.rows[0]
    });
  } catch (error) {
    console.error('Get expense head error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch expense head'
    });
  }
});

// Create expense head
router.post('/', auth, [
  body('name').trim().notEmpty().withMessage('Name is required'),
], async (req, res) => {
  try {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        errors: errors.array()
      });
    }

    const { name } = req.body;

    const result = await pool.query(
      'INSERT INTO expense_heads (name, is_recurring) VALUES ($1, $2) RETURNING *',
      [name, false]
    );

    await logAudit('expense_heads', result.rows[0].id, 'CREATE', null, result.rows[0], req.user.id);

    res.status(201).json({
      success: true,
      message: 'Expense head created successfully',
      data: result.rows[0]
    });
  } catch (error) {
    console.error('Create expense head error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to create expense head'
    });
  }
});

// Update expense head
router.put('/:id', auth, [
  body('name').optional().trim().notEmpty().withMessage('Name cannot be empty'),
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

    const { name, is_active } = req.body;
    const { id } = req.params;

    const current = await pool.query('SELECT * FROM expense_heads WHERE id = $1', [id]);
    if (current.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Expense head not found'
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
    const query = `UPDATE expense_heads SET ${updates.join(', ')} WHERE id = $${paramIndex} RETURNING *`;
    const result = await pool.query(query, values);

    await logAudit('expense_heads', id, 'UPDATE', current.rows[0], result.rows[0], req.user.id);

    res.json({
      success: true,
      message: 'Expense head updated successfully',
      data: result.rows[0]
    });
  } catch (error) {
    console.error('Update expense head error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to update expense head'
    });
  }
});

// Check dependencies for an expense head
const getExpenseHeadDependencies = async (dbClient, id, options = {}) => {
  const { mode = 'soft' } = options;
  const isHardDeleteMode = mode === 'hard';
  const expenseWhereClause = isHardDeleteMode
    ? `
      WHERE e.expense_head_id = $1
        AND e.is_active = true
    `
    : `
      WHERE e.expense_head_id = $1
        AND e.is_active = true
        AND e.updated_at > e.created_at + interval '2 seconds'
    `;
  const teamWhereClause = isHardDeleteMode
    ? `
      WHERE t.expense_head_id = $1
        AND t.is_active = true
    `
    : 'WHERE t.expense_head_id = $1';

  const expenseResult = await queryLinkedRows(dbClient, `
    SELECT
      e.id,
      COUNT(*) OVER() AS total_count,
      e.is_active,
      COALESCE(sp.display_name, TO_CHAR(e.date, 'Mon-YY')) AS period_name,
      t.name AS team_name,
      c.name AS group_name
    FROM expenses e
    LEFT JOIN service_periods sp ON sp.id = e.service_period_id
    LEFT JOIN teams t ON t.id = e.team_id
    LEFT JOIN clients c ON c.id = e.client_id
    ${expenseWhereClause}
    ORDER BY sp.start_date NULLS LAST, e.date NULLS LAST, t.name NULLS LAST
    LIMIT $2
  `, [id]);

  const recurringResult = await queryLinkedRows(dbClient, `
    SELECT
      re.id,
      COUNT(*) OVER() AS total_count,
      v.name AS vendor_name,
      re.start_period,
      re.end_period
    FROM recurring_expenses re
    LEFT JOIN vendors v ON v.id = re.vendor_id
    WHERE re.expense_head_id = $1
    ORDER BY re.start_period NULLS LAST, v.name NULLS LAST
    LIMIT $2
  `, [id]);

  const teamAllocationResult = await queryLinkedRows(dbClient, `
    SELECT
      tca.id,
      COUNT(*) OVER() AS total_count,
      t.name AS team_name,
      c.name AS group_name,
      tca.start_period,
      tca.end_period
    FROM team_client_allocations tca
    LEFT JOIN teams t ON t.id = tca.team_id
    LEFT JOIN clients c ON c.id = tca.client_id
    WHERE tca.expense_head_id = $1
    ORDER BY t.name NULLS LAST, c.name NULLS LAST, tca.start_period NULLS LAST
    LIMIT $2
  `, [id]);

  const teamDefaultResult = await queryLinkedRows(dbClient, `
    SELECT
      t.id,
      COUNT(*) OVER() AS total_count,
      t.name,
      t.is_active,
      t.start_period,
      t.end_period
    FROM teams t
    ${teamWhereClause}
    ORDER BY t.name NULLS LAST
    LIMIT $2
  `, [id]);

  const dependencies = [
    buildDependencyEntry({
      type: 'Expense',
      module: 'expenses',
      count: expenseResult.count,
      rows: expenseResult.rows,
      mapRow: (row) => buildLinkedItem({
        id: row.id,
        label: `${safeText(row.team_name, 'Unassigned team')} | ${safeText(row.period_name, 'No period')}`,
        line: `Status: ${row.is_active ? 'Active' : 'Inactive'} | Group: ${safeText(row.group_name, 'Not assigned')}`,
        module: 'expenses',
        type: 'Expense',
      }),
    }),
    buildDependencyEntry({
      type: 'Recurring Expense',
      module: 'recurring',
      count: recurringResult.count,
      rows: recurringResult.rows,
      mapRow: (row) => buildLinkedItem({
        id: row.id,
        label: safeText(row.vendor_name, 'No vendor'),
        line: `Recurring period: ${formatPeriodRange(row.start_period, row.end_period)}`,
        module: 'recurring',
        type: 'Recurring Expense',
      }),
    }),
    buildDependencyEntry({
      type: 'Team Allocation',
      module: 'teams',
      count: teamAllocationResult.count,
      rows: teamAllocationResult.rows,
      mapRow: (row) => buildLinkedItem({
        id: row.id,
        label: `${safeText(row.team_name, 'Unassigned team')} | ${safeText(row.group_name, 'Unassigned group')}`,
        line: `Allocation period: ${formatPeriodRange(row.start_period, row.end_period)}`,
        module: 'teams',
        type: 'Team Allocation',
      }),
    }),
    buildDependencyEntry({
      type: 'Team Master',
      module: 'teams',
      count: teamDefaultResult.count,
      rows: teamDefaultResult.rows,
      mapRow: (row) => buildLinkedItem({
        id: row.id,
        label: safeText(row.name, 'Unnamed team'),
        line: `Status: ${row.is_active ? 'Active' : 'Inactive'} | Compensation period: ${formatPeriodRange(row.start_period, row.end_period)}`,
        module: 'teams',
        type: 'Team Master',
      }),
    }),
  ].filter(Boolean);

  return {
    dependencies,
    total: dependencies.reduce((sum, dependency) => sum + dependency.count, 0),
    message: buildDeleteBlockedMessage('expense head master', dependencies),
  };
};

router.get('/:id/dependencies', auth, async (req, res) => {
  try {
    const { id } = req.params;
    await ensureExpenseHeadSchema();

    const current = await pool.query('SELECT id, is_active FROM expense_heads WHERE id = $1', [id]);
    if (current.rows.length === 0) {
      return res.status(404).json({ success: false, message: 'Expense head not found' });
    }

    const data = await getExpenseHeadDependencies(pool, id, {
      mode: current.rows[0].is_active === false ? 'hard' : 'soft',
    });
    res.json({ success: true, data });
  } catch (error) {
    console.error('Check expense head dependencies error:', error);
    res.status(500).json({ success: false, message: 'Failed to check dependencies' });
  }
});

const clearAutoDetachableExpenseHeadReferences = async (dbClient, id) => {
  await dbClient.query(
    'UPDATE expenses SET expense_head_id = NULL WHERE expense_head_id = $1 AND is_active = false',
    [id]
  );

  await dbClient.query(
    'UPDATE teams SET expense_head_id = NULL WHERE expense_head_id = $1 AND is_active = false',
    [id]
  );
};

const buildHardDeleteMessage = (dependencyData) => {
  const recurringCount = dependencyData.dependencies.find((entry) => entry.type === 'Recurring Expense')?.count || 0;
  const clearableCount = dependencyData.total - recurringCount;

  if (dependencyData.total === 0) {
    return '';
  }

  if (recurringCount > 0) {
    const recurringPart = `${recurringCount} recurring expense record(s) must be deleted or reassigned first because recurring expenses cannot exist without an expense head.`;
    const clearablePart = clearableCount > 0
      ? ` After that, force delete can clear the remaining ${clearableCount} expense/team reference(s).`
      : '';
    return `Delete blocked. This expense head master still has ${dependencyData.total} linked record(s). ${recurringPart}${clearablePart}`;
  }

  return `Delete blocked. This expense head master still has ${dependencyData.total} linked record(s) that prevent permanent deletion. Use force=true to clear those expense/team references before retrying.`;
};

// Delete expense head
router.delete('/:id', auth, requireDelete, async (req, res) => {
  try {
    await ensureExpenseHeadSchema();
    const { id } = req.params;
    const { force } = req.query;
    const forceDelete = force === 'true';

    const current = await pool.query('SELECT * FROM expense_heads WHERE id = $1', [id]);
    if (current.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Expense head not found'
      });
    }

    const wasActive = current.rows[0].is_active;

    // For active soft-delete, check only active/recent dependencies
    if (wasActive) {
      const dependencyData = await getExpenseHeadDependencies(pool, id);
      if (dependencyData.total > 0) {
        return res.status(409).json({
          success: false,
          message: dependencyData.message,
          data: dependencyData,
        });
      }
      await pool.query('UPDATE expense_heads SET is_active = false WHERE id = $1', [id]);
    } else {
      // For hard delete of inactive records
      const hardDependencyData = await getExpenseHeadDependencies(pool, id, {
        mode: 'hard',
      });
      const recurringCount = hardDependencyData.dependencies.find((entry) => entry.type === 'Recurring Expense')?.count || 0;

      if (forceDelete) {
        if (recurringCount > 0) {
          return res.status(409).json({
            success: false,
            message: buildHardDeleteMessage(hardDependencyData),
            data: hardDependencyData,
          });
        }

        const dbClient = await pool.connect();
        try {
          await dbClient.query('BEGIN');

          await dbClient.query('UPDATE expenses SET expense_head_id = NULL WHERE expense_head_id = $1', [id]);
          await dbClient.query('UPDATE team_client_allocations SET expense_head_id = NULL WHERE expense_head_id = $1', [id]);
          await dbClient.query('UPDATE teams SET expense_head_id = NULL WHERE expense_head_id = $1', [id]);

          const deleteResult = await dbClient.query('DELETE FROM expense_heads WHERE id = $1', [id]);
          console.log('Expense head force-deleted result:', deleteResult.rowCount);

          await dbClient.query('COMMIT');
        } catch (dbError) {
          await dbClient.query('ROLLBACK');
          console.error('Database delete error:', dbError.code, dbError.message, dbError.detail);
          throw dbError;
        } finally {
          dbClient.release();
        }
      } else {
        // Standard hard delete: check ALL linked records (active and inactive)
        if (hardDependencyData.total > 0) {
          return res.status(409).json({
            success: false,
            message: buildHardDeleteMessage(hardDependencyData),
            data: hardDependencyData,
          });
        }
        
        // Perform hard delete
        const dbClient = await pool.connect();
        try {
          await dbClient.query('BEGIN');
          await clearAutoDetachableExpenseHeadReferences(dbClient, id);
          const deleteResult = await dbClient.query('DELETE FROM expense_heads WHERE id = $1', [id]);
          console.log('Expense head delete result:', deleteResult.rowCount);
          await dbClient.query('COMMIT');
        } catch (dbError) {
          await dbClient.query('ROLLBACK');
          console.error('Database delete error:', dbError.code, dbError.message, dbError.detail);
          // Check if it's a FK constraint violation
          if (dbError.code === '23503') {
            return res.status(409).json({
              success: false,
              message: 'Delete blocked: This expense head is still referenced by other records in the database. Please ensure all linked records are removed before attempting to delete.',
            });
          }
          throw dbError;
        } finally {
          dbClient.release();
        }
      }
    }

    try {
      await logAudit('expense_heads', id, 'DELETE', current.rows[0], null, req.user.id);
    } catch (auditError) {
      console.error('Audit log error (non-blocking):', auditError);
      // Don't fail the delete if audit logging fails
    }

    res.json({
      success: true,
      message: wasActive ? 'Expense head deleted successfully' : (forceDelete ? 'Expense head permanently deleted with references cleared' : 'Expense head permanently deleted successfully')
    });
  } catch (error) {
    console.error('Delete expense head error:', error.message, error.code, error.detail);
    res.status(500).json({
      success: false,
      message: 'Failed to delete expense head',
      error: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

module.exports = router;
