const express = require('express');
const router = express.Router();
const { body, validationResult } = require('express-validator');
const { pool, logAudit } = require('../database/db');
const { auth, requireDelete } = require('../middleware/auth');
const { cleanupStaleRecurringSourceRecords } = require('../utils/recurringSourceCleanup');
const {
  buildDeleteBlockedMessage,
  buildDependencyEntry,
  buildLinkedItem,
  queryLinkedRows,
  safeText,
} = require('../utils/deleteDependencyHelpers');

let recurringSchemaPromise = null;
const PERIOD_REGEX = /^[A-Z][a-z]{2}-\d{2}$/;

async function ensureRecurringExpenseSchema() {
  if (!recurringSchemaPromise) {
    recurringSchemaPromise = (async () => {
      // Main recurring expense table
      await pool.query(`
        CREATE TABLE IF NOT EXISTS recurring_expenses (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          expense_head_id UUID REFERENCES expense_heads(id) NOT NULL,
          amount DECIMAL(15, 2) NOT NULL,
          start_period VARCHAR(10) NOT NULL,
          end_period VARCHAR(10),
          is_active BOOLEAN DEFAULT true,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          created_by UUID REFERENCES users(id),
          updated_by UUID REFERENCES users(id)
        );
      `);

      // Ensure vendors table exists
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

      // Add vendor_id column if not exists
      await pool.query(`
        ALTER TABLE recurring_expenses
          ADD COLUMN IF NOT EXISTS vendor_id UUID REFERENCES vendors(id);
      `).catch(() => {});

      // Team allocations for recurring expense (kept for backward compat + expense JOIN)
      await pool.query(`
        CREATE TABLE IF NOT EXISTS recurring_expense_teams (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          recurring_expense_id UUID REFERENCES recurring_expenses(id) ON DELETE CASCADE,
          team_id UUID REFERENCES teams(id) NOT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);

      // Client allocations — now also carry team_id and recurring_expense_id directly (flat structure)
      await pool.query(`
        CREATE TABLE IF NOT EXISTS recurring_expense_clients (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          recurring_expense_team_id UUID REFERENCES recurring_expense_teams(id) ON DELETE CASCADE,
          client_id UUID REFERENCES clients(id) NOT NULL,
          reviewer_id UUID REFERENCES teams(id),
          start_period VARCHAR(10),
          end_period VARCHAR(10),
          amount DECIMAL(15, 2) NOT NULL DEFAULT 0,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);

      // Allow nullable start_period
      await pool.query(`
        ALTER TABLE recurring_expense_clients ALTER COLUMN start_period DROP NOT NULL;
      `).catch(() => {});

      // Add reviewer_id column if not exists
      await pool.query(`
        ALTER TABLE recurring_expense_clients
          ADD COLUMN IF NOT EXISTS reviewer_id UUID REFERENCES teams(id);
      `).catch(() => {});

      // Add team_id directly to recurring_expense_clients (denormalised, for flat API)
      await pool.query(`
        ALTER TABLE recurring_expense_clients
          ADD COLUMN IF NOT EXISTS team_id UUID REFERENCES teams(id);
      `).catch(() => {});

      // Add recurring_expense_id directly to recurring_expense_clients (denormalised)
      await pool.query(`
        ALTER TABLE recurring_expense_clients
          ADD COLUMN IF NOT EXISTS recurring_expense_id UUID REFERENCES recurring_expenses(id) ON DELETE CASCADE;
      `).catch(() => {});

      // Fix existing FK if it lacks ON DELETE CASCADE
      await pool.query(`
        DO $$ BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.referential_constraints
            WHERE constraint_name = 'recurring_expense_clients_recurring_expense_id_fkey'
            AND delete_rule != 'CASCADE'
          ) THEN
            ALTER TABLE recurring_expense_clients
              DROP CONSTRAINT recurring_expense_clients_recurring_expense_id_fkey,
              ADD CONSTRAINT recurring_expense_clients_recurring_expense_id_fkey
                FOREIGN KEY (recurring_expense_id) REFERENCES recurring_expenses(id) ON DELETE CASCADE;
          END IF;
        END $$;
      `).catch(() => {});

      // Add is_admin flag to recurring_expenses
      await pool.query(`
        ALTER TABLE recurring_expenses
          ADD COLUMN IF NOT EXISTS is_admin BOOLEAN DEFAULT false;
      `).catch(() => {});

      // Backfill team_id and recurring_expense_id for existing rows
      await pool.query(`
        UPDATE recurring_expense_clients rec
        SET team_id = ret.team_id,
            recurring_expense_id = ret.recurring_expense_id
        FROM recurring_expense_teams ret
        WHERE ret.id = rec.recurring_expense_team_id
          AND (rec.team_id IS NULL OR rec.recurring_expense_id IS NULL);
      `).catch(() => {});

      await pool.query('CREATE INDEX IF NOT EXISTS idx_recurring_expenses_head ON recurring_expenses(expense_head_id)');
      await pool.query('CREATE INDEX IF NOT EXISTS idx_recurring_expense_teams_expense ON recurring_expense_teams(recurring_expense_id)');
      await pool.query('CREATE INDEX IF NOT EXISTS idx_recurring_expense_clients_team ON recurring_expense_clients(recurring_expense_team_id)');
      await pool.query('CREATE INDEX IF NOT EXISTS idx_recurring_expense_clients_re ON recurring_expense_clients(recurring_expense_id)');
    })().catch((error) => {
      recurringSchemaPromise = null;
      throw error;
    });
  }

  return recurringSchemaPromise;
}

const parsePeriod = (period) => {
  if (!period || !PERIOD_REGEX.test(period)) return null;
  const monthMap = {
    Jan: 0, Feb: 1, Mar: 2, Apr: 3, May: 4, Jun: 5,
    Jul: 6, Aug: 7, Sep: 8, Oct: 9, Nov: 10, Dec: 11
  };
  const [mon, yy] = period.split('-');
  return new Date(2000 + parseInt(yy, 10), monthMap[mon], 1);
};

const comparePeriods = (a, b) => {
  const dateA = parsePeriod(a);
  const dateB = parsePeriod(b);
  if (!dateA && !dateB) return 0;
  if (!dateA) return -1;
  if (!dateB) return 1;
  return dateA - dateB;
};

const nextPeriod = (period) => {
  const date = parsePeriod(period);
  if (!date) return null;
  date.setMonth(date.getMonth() + 1);
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  return `${months[date.getMonth()]}-${String(date.getFullYear()).slice(-2)}`;
};

// Flat allocation query (live + historical) for one recurring expense
const getRecurringExpenseAllocations = async (recurringExpenseId) => {
  const result = await pool.query(`
    SELECT
      rec.id,
      COALESCE(rec.team_id, ret.team_id) AS team_id,
      t.name AS team_name,
      rec.client_id,
      c.name AS client_name,
      rec.reviewer_id,
      rv.name AS reviewer_name,
      rec.amount,
      rec.start_period,
      rec.end_period
    FROM recurring_expense_clients rec
    LEFT JOIN recurring_expense_teams ret ON ret.id = rec.recurring_expense_team_id
    LEFT JOIN teams t ON t.id = COALESCE(rec.team_id, ret.team_id)
    LEFT JOIN clients c ON c.id = rec.client_id
    LEFT JOIN teams rv ON rv.id = rec.reviewer_id
    WHERE COALESCE(rec.recurring_expense_id, ret.recurring_expense_id) = $1
    ORDER BY rec.end_period NULLS FIRST, t.name, c.name, rec.start_period
  `, [recurringExpenseId]);
  return result.rows.map(r => ({
    ...r,
    amount: parseFloat(r.amount) || 0,
    is_live: !r.end_period,
  }));
};

// Upsert a recurring_expense_teams row and return its id (select-then-insert to avoid constraint dependency)
const upsertRecurringTeam = async (dbClient, recurringExpenseId, teamId) => {
  const existing = await dbClient.query(
    `SELECT id FROM recurring_expense_teams WHERE recurring_expense_id = $1 AND team_id = $2 LIMIT 1`,
    [recurringExpenseId, teamId]
  );
  if (existing.rows.length > 0) return existing.rows[0].id;
  const result = await dbClient.query(
    `INSERT INTO recurring_expense_teams (recurring_expense_id, team_id) VALUES ($1, $2) RETURNING id`,
    [recurringExpenseId, teamId]
  );
  return result.rows[0].id;
};

// Get all recurring expenses with flat allocations
router.get('/', auth, async (req, res) => {
  try {
    await ensureRecurringExpenseSchema();
    const { includeInactive } = req.query;
    let query = `
      SELECT re.*, eh.name AS expense_head_name, v.name AS vendor_name
      FROM recurring_expenses re
      JOIN expense_heads eh ON re.expense_head_id = eh.id
      LEFT JOIN vendors v ON re.vendor_id = v.id
      WHERE 1=1
    `;
    if (includeInactive !== 'true' && includeInactive !== true) {
      query += ' AND re.is_active = true';
    }
    query += ' ORDER BY eh.name, v.name NULLS LAST, re.start_period ASC';
    const result = await pool.query(query);
    const data = await Promise.all(result.rows.map(async (expense) => ({
      ...expense,
      amount: parseFloat(expense.amount) || 0,
      is_admin: expense.is_admin || false,
      allocations: expense.is_admin ? [] : await getRecurringExpenseAllocations(expense.id),
    })));
    res.json({ success: true, data });
  } catch (error) {
    console.error('Get recurring expenses error:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch recurring expenses' });
  }
});

// Get single recurring expense with flat allocations
router.get('/:id', auth, async (req, res) => {
  try {
    await ensureRecurringExpenseSchema();
    const result = await pool.query(`
      SELECT re.*, eh.name AS expense_head_name, v.name AS vendor_name
      FROM recurring_expenses re
      JOIN expense_heads eh ON re.expense_head_id = eh.id
      LEFT JOIN vendors v ON re.vendor_id = v.id
      WHERE re.id = $1
    `, [req.params.id]);
    if (!result.rows.length) {
      return res.status(404).json({ success: false, message: 'Recurring expense not found' });
    }
    const expense = result.rows[0];
    const allocations = expense.is_admin ? [] : await getRecurringExpenseAllocations(expense.id);
    res.json({
      success: true,
      data: { ...expense, amount: parseFloat(expense.amount) || 0, is_admin: expense.is_admin || false, allocations },
    });
  } catch (error) {
    console.error('Get recurring expense error:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch recurring expense' });
  }
});

// Create recurring expense
router.post('/', auth, [
  body('expense_head_id').isUUID().withMessage('Expense head is required'),
  body('vendor_id').optional({ values: 'falsy' }).isUUID().withMessage('Invalid vendor'),
  body('amount').isFloat({ min: 0 }).withMessage('Amount must be >= 0'),
  body('start_period').matches(PERIOD_REGEX).withMessage('Start period must be in MMM-YY format'),
  body('end_period').optional({ values: 'falsy' }).matches(PERIOD_REGEX).withMessage('End period must be in MMM-YY format'),
  body('is_admin').optional().isBoolean(),
  body('allocations').optional().isArray(),
], async (req, res) => {
  const client = await pool.connect();
  try {
    await ensureRecurringExpenseSchema();
    const errors = validationResult(req);
    if (!errors.isEmpty()) return res.status(400).json({ success: false, errors: errors.array() });

    const { expense_head_id, vendor_id, amount, start_period, end_period, is_admin, allocations } = req.body;

    // Validate unique expense_head + vendor
    const dupCheck = await pool.query(
      `SELECT id FROM recurring_expenses
       WHERE expense_head_id = $1
         AND vendor_id IS NOT DISTINCT FROM $2::uuid
         AND is_active = true`,
      [expense_head_id, vendor_id || null]
    );
    if (dupCheck.rows.length > 0) {
      return res.status(400).json({ success: false, message: 'A recurring expense with this expense head and vendor combination already exists' });
    }

    // Validate unique team+group within submitted live allocations
    const liveAllocs = (allocations || []).filter(a => !a.end_period);
    const liveKeys = new Set();
    for (const a of liveAllocs) {
      if (!a.team_id || !a.client_id) continue;
      const k = `${a.team_id}--${a.client_id}`;
      if (liveKeys.has(k)) {
        return res.status(400).json({ success: false, message: 'Duplicate team+group combination in allocations' });
      }
      liveKeys.add(k);
    }

    // Cross-record unique check: expense_head + vendor + team + group must be unique across all live recurring allocations
    for (const a of liveAllocs) {
      if (!a.team_id || !a.client_id) continue;
      const crossDup = await pool.query(`
        SELECT re.id FROM recurring_expense_clients rec
        JOIN recurring_expenses re ON re.id = COALESCE(rec.recurring_expense_id,
          (SELECT ret.recurring_expense_id FROM recurring_expense_teams ret WHERE ret.id = rec.recurring_expense_team_id))
        WHERE COALESCE(rec.team_id, (SELECT ret.team_id FROM recurring_expense_teams ret WHERE ret.id = rec.recurring_expense_team_id)) = $1
          AND rec.client_id = $2
          AND re.expense_head_id = $3
          AND re.vendor_id IS NOT DISTINCT FROM $4::uuid
          AND rec.end_period IS NULL
          AND re.is_active = true
      `, [a.team_id, a.client_id, expense_head_id, vendor_id || null]);
      if (crossDup.rows.length > 0) {
        return res.status(400).json({ success: false, message: `A live recurring expense already exists for this expense head, vendor, team, and group combination` });
      }
    }

    await client.query('BEGIN');

    const result = await client.query(`
      INSERT INTO recurring_expenses (expense_head_id, vendor_id, amount, start_period, end_period, is_admin, created_by, updated_by)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $7) RETURNING *
    `, [expense_head_id, vendor_id || null, amount, start_period, end_period || null, is_admin || false, req.user.id]);

    const recurringExpenseId = result.rows[0].id;

    if (!is_admin) {
      for (const alloc of (allocations || [])) {
        if (!alloc.team_id || !alloc.client_id) continue;
        const teamEntryId = await upsertRecurringTeam(client, recurringExpenseId, alloc.team_id);
        await client.query(`
          INSERT INTO recurring_expense_clients
            (recurring_expense_team_id, team_id, recurring_expense_id, client_id, reviewer_id, start_period, end_period, amount)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        `, [teamEntryId, alloc.team_id, recurringExpenseId, alloc.client_id, alloc.reviewer_id || null,
            alloc.start_period || start_period, alloc.end_period || null, alloc.amount || 0]);
      }
    }

    await client.query('COMMIT');
    await logAudit('recurring_expenses', recurringExpenseId, 'CREATE', null, result.rows[0], req.user.id);

    res.status(201).json({ success: true, message: 'Recurring expense created successfully', data: result.rows[0] });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Create recurring expense error:', error);
    res.status(500).json({ success: false, message: 'Failed to create recurring expense' });
  } finally {
    client.release();
  }
});

// Update recurring expense
router.put('/:id', auth, [
  body('expense_head_id').optional().isUUID().withMessage('Invalid expense head'),
  body('vendor_id').optional({ values: 'falsy' }).isUUID().withMessage('Invalid vendor'),
  body('amount').optional().isFloat({ min: 0 }).withMessage('Amount must be >= 0'),
  body('start_period').optional().matches(PERIOD_REGEX).withMessage('Start period must be in MMM-YY format'),
  body('end_period').optional({ values: 'falsy' }).matches(PERIOD_REGEX).withMessage('End period must be in MMM-YY format'),
  body('allocations').optional().isArray(),
  body('is_admin').optional().isBoolean(),
  body('is_active').optional().isBoolean(),
], async (req, res) => {
  const dbClient = await pool.connect();
  try {
    await ensureRecurringExpenseSchema();
    const errors = validationResult(req);
    if (!errors.isEmpty()) return res.status(400).json({ success: false, errors: errors.array() });

    const { id } = req.params;
    const current = await dbClient.query('SELECT * FROM recurring_expenses WHERE id = $1', [id]);
    if (!current.rows.length) return res.status(404).json({ success: false, message: 'Recurring expense not found' });

    const { expense_head_id, vendor_id, amount, start_period, end_period, allocations, is_admin, is_active } = req.body;
    const currentExpense = current.rows[0];

    // Validate unique expense_head + vendor if changing
    if (expense_head_id !== undefined || vendor_id !== undefined) {
      const checkHeadId = expense_head_id !== undefined ? expense_head_id : currentExpense.expense_head_id;
      const checkVendorId = vendor_id !== undefined ? (vendor_id || null) : currentExpense.vendor_id;
      const dupCheck = await dbClient.query(
        `SELECT id FROM recurring_expenses
         WHERE expense_head_id = $1
           AND vendor_id IS NOT DISTINCT FROM $2::uuid
           AND is_active = true AND id != $3`,
        [checkHeadId, checkVendorId, id]
      );
      if (dupCheck.rows.length > 0) {
        return res.status(400).json({ success: false, message: 'A recurring expense with this expense head and vendor combination already exists' });
      }
    }

    const nextStartPeriod = start_period !== undefined ? start_period : currentExpense.start_period;
    const nextEndPeriod = end_period !== undefined ? (end_period || null) : currentExpense.end_period;
    if (nextEndPeriod && nextStartPeriod && comparePeriods(nextEndPeriod, nextStartPeriod) < 0) {
      return res.status(400).json({ success: false, message: 'End period cannot be before start period' });
    }

    await dbClient.query('BEGIN');

    // Update main expense fields
    const updates = [];
    const values = [];
    let p = 1;
    if (expense_head_id !== undefined) { updates.push(`expense_head_id = $${p++}`); values.push(expense_head_id); }
    if (vendor_id !== undefined) { updates.push(`vendor_id = $${p++}`); values.push(vendor_id || null); }
    if (amount !== undefined) { updates.push(`amount = $${p++}`); values.push(amount); }
    if (start_period !== undefined) { updates.push(`start_period = $${p++}`); values.push(start_period); }
    if (end_period !== undefined) { updates.push(`end_period = $${p++}`); values.push(end_period || null); }
    if (is_admin !== undefined) { updates.push(`is_admin = $${p++}`); values.push(is_admin); }
    // Auto-set inactive when end_period is provided
    const effectiveIsActive = (end_period !== undefined && end_period) ? false : is_active;
    if (effectiveIsActive !== undefined) { updates.push(`is_active = $${p++}`); values.push(effectiveIsActive); }
    updates.push(`updated_by = $${p++}`);
    values.push(req.user.id);
    values.push(id);
    await dbClient.query(`UPDATE recurring_expenses SET ${updates.join(', ')} WHERE id = $${p}`, values);

    // Update allocations if provided — diff live allocations
    if (allocations !== undefined) {
      // Validate unique team+group within submitted allocations
      const liveAllocs = allocations.filter(a => !a.end_period);
      const liveKeys = new Set();
      for (const a of liveAllocs) {
        if (!a.team_id || !a.client_id) continue;
        const k = `${a.team_id}--${a.client_id}`;
        if (liveKeys.has(k)) {
          await dbClient.query('ROLLBACK');
          return res.status(400).json({ success: false, message: 'Duplicate team+group combination in allocations' });
        }
        liveKeys.add(k);
      }

      // Cross-record unique check: expense_head + vendor + team + group must be unique across all live recurring allocations (exclude self)
      const effectiveHeadId = expense_head_id !== undefined ? expense_head_id : currentExpense.expense_head_id;
      const effectiveVendorId = vendor_id !== undefined ? (vendor_id || null) : currentExpense.vendor_id;
      for (const a of liveAllocs) {
        if (!a.team_id || !a.client_id) continue;
        const crossDup = await dbClient.query(`
          SELECT re.id FROM recurring_expense_clients rec
          JOIN recurring_expenses re ON re.id = COALESCE(rec.recurring_expense_id,
            (SELECT ret.recurring_expense_id FROM recurring_expense_teams ret WHERE ret.id = rec.recurring_expense_team_id))
          WHERE COALESCE(rec.team_id, (SELECT ret.team_id FROM recurring_expense_teams ret WHERE ret.id = rec.recurring_expense_team_id)) = $1
            AND rec.client_id = $2
            AND re.expense_head_id = $3
            AND re.vendor_id IS NOT DISTINCT FROM $4::uuid
            AND rec.end_period IS NULL
            AND re.is_active = true
            AND re.id != $5
        `, [a.team_id, a.client_id, effectiveHeadId, effectiveVendorId, id]);
        if (crossDup.rows.length > 0) {
          await dbClient.query('ROLLBACK');
          return res.status(400).json({ success: false, message: `A live recurring expense already exists for this expense head, vendor, team, and group combination` });
        }
      }

      // Get current live allocations
      const existingLiveResult = await dbClient.query(`
        SELECT rec.id, COALESCE(rec.team_id, ret.team_id) AS team_id, rec.client_id, rec.amount
        FROM recurring_expense_clients rec
        LEFT JOIN recurring_expense_teams ret ON ret.id = rec.recurring_expense_team_id
        WHERE COALESCE(rec.recurring_expense_id, ret.recurring_expense_id) = $1
          AND rec.end_period IS NULL
      `, [id]);
      const existingLive = existingLiveResult.rows;

      const submittedIds = new Set(allocations.filter(a => a.id).map(a => a.id));

      // Remove live allocations not in submitted list, migrating expense references
      for (const ex of existingLive) {
        if (!submittedIds.has(ex.id)) {
          await dbClient.query(`UPDATE expenses SET is_active = false, updated_at = NOW() WHERE recurring_expense_client_id = $1 AND is_active = true AND source_type = 'recurring'`, [ex.id]);
          await dbClient.query(`DELETE FROM recurring_expense_clients WHERE id = $1`, [ex.id]);
        }
      }

      // Update existing live allocations
      for (const alloc of allocations.filter(a => a.id)) {
        const teamEntryId = await upsertRecurringTeam(dbClient, id, alloc.team_id);
        const oldAmount = existingLive.find(e => e.id === alloc.id)?.amount;
        await dbClient.query(`
          UPDATE recurring_expense_clients
          SET team_id = $1, recurring_expense_id = $2, recurring_expense_team_id = $3,
              client_id = $4, reviewer_id = $5, start_period = $6, amount = $7, updated_at = NOW()
          WHERE id = $8
        `, [alloc.team_id, id, teamEntryId, alloc.client_id, alloc.reviewer_id || null,
          alloc.start_period || (start_period ?? currentExpense.start_period), alloc.amount || 0, alloc.id]);
        // Cascade amount change for projected expense rows
        if (oldAmount !== undefined && parseFloat(oldAmount) !== parseFloat(alloc.amount)) {
          await dbClient.query(`
            UPDATE expenses
            SET amount = $1, total_amount = $1 + COALESCE(igst, 0) + COALESCE(cgst, 0) + COALESCE(sgst, 0) + COALESCE(other_charges, 0) + COALESCE(round_off, 0), updated_at = NOW()
            WHERE recurring_expense_client_id = $2 AND is_active = true AND source_type = 'recurring' AND amount = $3
          `, [alloc.amount || 0, alloc.id, parseFloat(oldAmount)]);
        }
      }

      // Create new allocations
      for (const alloc of allocations.filter(a => !a.id)) {
        if (!alloc.team_id || !alloc.client_id) continue;
        const teamEntryId = await upsertRecurringTeam(dbClient, id, alloc.team_id);
        await dbClient.query(`
          INSERT INTO recurring_expense_clients
            (recurring_expense_team_id, team_id, recurring_expense_id, client_id, reviewer_id, start_period, end_period, amount)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        `, [teamEntryId, alloc.team_id, id, alloc.client_id, alloc.reviewer_id || null,
            alloc.start_period || (start_period ?? currentExpense.start_period), alloc.end_period || null, alloc.amount || 0]);
      }
    }

    if (end_period !== undefined && end_period) {
      await dbClient.query(`
        UPDATE recurring_expense_clients
        SET end_period = $1, updated_at = NOW()
        WHERE recurring_expense_id = $2 AND end_period IS NULL
      `, [end_period, id]);
    }

    await dbClient.query('COMMIT');

    // Cleanup stale recurring source records
    try {
      const cleanupClient = await pool.connect();
      try {
        await cleanupClient.query('BEGIN');
        await cleanupStaleRecurringSourceRecords(cleanupClient, req.user.id);
        await cleanupClient.query('COMMIT');
      } catch (err) {
        await cleanupClient.query('ROLLBACK').catch(() => {});
        console.error('Post-update cleanup warning:', err.message);
      } finally {
        cleanupClient.release();
      }
    } catch (err) {
      console.error('Cleanup connection warning:', err.message);
    }

    const updated = await pool.query('SELECT * FROM recurring_expenses WHERE id = $1', [id]);
    await logAudit('recurring_expenses', id, 'UPDATE', current.rows[0], updated.rows[0], req.user.id);

    res.json({ success: true, message: 'Recurring expense updated successfully', data: updated.rows[0] });
  } catch (error) {
    await dbClient.query('ROLLBACK');
    console.error('Update recurring expense error:', error);
    res.status(500).json({ success: false, message: 'Failed to update recurring expense' });
  } finally {
    dbClient.release();
  }
});

// Update a single allocation (for history editing)
router.patch('/:id/allocations/:allocationId', auth, [
  body('team_id').optional().isUUID().withMessage('Invalid team'),
  body('client_id').optional().isUUID().withMessage('Invalid client/group'),
  body('reviewer_id').optional({ values: 'falsy' }).isUUID().withMessage('Invalid reviewer'),
  body('amount').optional().isFloat({ min: 0 }).withMessage('Amount must be >= 0'),
  body('start_period').optional().matches(PERIOD_REGEX).withMessage('Start period must be in MMM-YY format'),
  body('end_period').optional({ values: 'falsy' }).matches(PERIOD_REGEX).withMessage('End period must be in MMM-YY format'),
], async (req, res) => {
  try {
    await ensureRecurringExpenseSchema();
    const errors = validationResult(req);
    if (!errors.isEmpty()) return res.status(400).json({ success: false, errors: errors.array() });

    const { id, allocationId } = req.params;
    const { team_id, client_id, reviewer_id, amount, start_period, end_period } = req.body;

    // Verify allocation belongs to this recurring expense
    const allocResult = await pool.query(`
      SELECT rec.*, COALESCE(rec.recurring_expense_id, ret.recurring_expense_id) AS resolved_re_id
      FROM recurring_expense_clients rec
      LEFT JOIN recurring_expense_teams ret ON ret.id = rec.recurring_expense_team_id
      WHERE rec.id = $1
    `, [allocationId]);
    if (!allocResult.rows.length) {
      return res.status(404).json({ success: false, message: 'Allocation not found' });
    }
    if (allocResult.rows[0].resolved_re_id !== id) {
      return res.status(400).json({ success: false, message: 'Allocation does not belong to this recurring expense' });
    }

    const nextStartPeriod = start_period !== undefined ? start_period : allocResult.rows[0].start_period;
    const nextEndPeriod = end_period !== undefined ? (end_period || null) : allocResult.rows[0].end_period;
    if (nextEndPeriod && nextStartPeriod && comparePeriods(nextEndPeriod, nextStartPeriod) < 0) {
      return res.status(400).json({ success: false, message: 'End period cannot be before start period' });
    }

    const updates = [];
    const values = [];
    let p = 1;
    if (team_id !== undefined) { updates.push(`team_id = $${p++}`); values.push(team_id); }
    if (client_id !== undefined) { updates.push(`client_id = $${p++}`); values.push(client_id); }
    if (reviewer_id !== undefined) { updates.push(`reviewer_id = $${p++}`); values.push(reviewer_id || null); }
    if (amount !== undefined) { updates.push(`amount = $${p++}`); values.push(amount); }
    if (start_period !== undefined) { updates.push(`start_period = $${p++}`); values.push(start_period); }
    if (end_period !== undefined) { updates.push(`end_period = $${p++}`); values.push(end_period || null); }
    if (updates.length === 0) {
      return res.status(400).json({ success: false, message: 'No fields to update' });
    }
    updates.push(`updated_at = NOW()`);
    values.push(allocationId);

    await pool.query(`UPDATE recurring_expense_clients SET ${updates.join(', ')} WHERE id = $${p}`, values);

    // If team changed, also update the recurring_expense_teams link
    if (team_id !== undefined) {
      const dbClient = await pool.connect();
      try {
        const teamEntryId = await upsertRecurringTeam(dbClient, id, team_id);
        await dbClient.query(`UPDATE recurring_expense_clients SET recurring_expense_team_id = $1 WHERE id = $2`, [teamEntryId, allocationId]);
      } finally {
        dbClient.release();
      }
    }

    await logAudit('recurring_expense_clients', allocationId, 'UPDATE_ALLOCATION', null, req.body, req.user.id);
    res.json({ success: true, message: 'Allocation updated successfully' });
  } catch (error) {
    console.error('Update allocation error:', error);
    res.status(500).json({ success: false, message: 'Failed to update allocation' });
  }
});

// End allocation period and create a new live allocation (history feature)
router.put('/:id/allocations/:allocationId/end-period', auth, [
  body('end_period').matches(PERIOD_REGEX).withMessage('End period is required in MMM-YY format'),
  body('new_amount').optional({ values: 'falsy' }).isFloat({ min: 0 }).withMessage('New amount must be >= 0'),
  body('new_reviewer_id').optional({ values: 'falsy' }).isUUID().withMessage('Invalid reviewer'),
], async (req, res) => {
  const dbClient = await pool.connect();
  try {
    await ensureRecurringExpenseSchema();
    const errors = validationResult(req);
    if (!errors.isEmpty()) return res.status(400).json({ success: false, errors: errors.array() });

    const { id, allocationId } = req.params;
    const { end_period, new_amount, new_reviewer_id } = req.body;

    // Fetch allocation
    const allocResult = await dbClient.query(`
      SELECT rec.*,
        COALESCE(rec.team_id, ret.team_id) AS resolved_team_id,
        COALESCE(rec.recurring_expense_id, ret.recurring_expense_id) AS resolved_recurring_expense_id
      FROM recurring_expense_clients rec
      LEFT JOIN recurring_expense_teams ret ON ret.id = rec.recurring_expense_team_id
      WHERE rec.id = $1
    `, [allocationId]);

    if (!allocResult.rows.length) {
      return res.status(404).json({ success: false, message: 'Allocation not found' });
    }
    const alloc = allocResult.rows[0];

    if (alloc.resolved_recurring_expense_id !== id) {
      return res.status(400).json({ success: false, message: 'Allocation does not belong to this recurring expense' });
    }
    if (alloc.end_period) {
      return res.status(400).json({ success: false, message: 'Allocation already has an end period set' });
    }
    if (alloc.start_period && comparePeriods(end_period, alloc.start_period) < 0) {
      return res.status(400).json({ success: false, message: 'End period must be on or after start period' });
    }

    const newStartPeriod = nextPeriod(end_period);
    const newAmount = new_amount !== undefined && new_amount !== null ? parseFloat(new_amount) : parseFloat(alloc.amount);
    const newReviewerId = new_reviewer_id !== undefined ? (new_reviewer_id || null) : alloc.reviewer_id;

    await dbClient.query('BEGIN');

    // Set end_period on existing allocation
    await dbClient.query(`
      UPDATE recurring_expense_clients SET end_period = $1, updated_at = NOW() WHERE id = $2
    `, [end_period, allocationId]);

    // Create new live allocation
    const teamEntryId = await upsertRecurringTeam(dbClient, id, alloc.resolved_team_id);
    const newAllocResult = await dbClient.query(`
      INSERT INTO recurring_expense_clients
        (recurring_expense_team_id, team_id, recurring_expense_id, client_id, reviewer_id, start_period, amount)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING id
    `, [teamEntryId, alloc.resolved_team_id, id, alloc.client_id, newReviewerId, newStartPeriod, newAmount]);

    await dbClient.query('COMMIT');
    await logAudit('recurring_expense_clients', allocationId, 'END_PERIOD',
      { end_period: null }, { end_period, new_allocation_id: newAllocResult.rows[0].id }, req.user.id);

    res.json({
      success: true,
      message: `Period ended at ${end_period}. New allocation starts ${newStartPeriod}.`,
      data: { ended_allocation_id: allocationId, new_allocation_id: newAllocResult.rows[0].id, new_start_period: newStartPeriod },
    });
  } catch (error) {
    await dbClient.query('ROLLBACK');
    console.error('End allocation period error:', error);
    res.status(500).json({ success: false, message: 'Failed to end allocation period' });
  } finally {
    dbClient.release();
  }
});

// Check dependencies for a recurring expense
const getRecurringExpenseDependencies = async (dbClient, id) => {
  await ensureRecurringExpenseSchema();

  const allocResult = await dbClient.query(`
    SELECT rec.id
    FROM recurring_expense_clients rec
    LEFT JOIN recurring_expense_teams ret ON ret.id = rec.recurring_expense_team_id
    WHERE COALESCE(rec.recurring_expense_id, ret.recurring_expense_id) = $1
  `, [id]);
  const allocIds = allocResult.rows.map((row) => row.id).filter(Boolean);

  let expenses = [];
  if (allocIds.length > 0) {
    const expenseResult = await queryLinkedRows(dbClient, `
      SELECT
        e.id,
        COUNT(*) OVER() AS total_count,
        e.amount,
        e.total_amount,
        e.date,
        e.created_at,
        e.updated_at,
        e.is_active,
        sp.display_name AS service_period_name,
        t.name AS team_name,
        c.name AS client_name,
        eh.name AS expense_head_name
      FROM expenses e
      LEFT JOIN service_periods sp ON sp.id = e.service_period_id
      LEFT JOIN teams t ON t.id = e.team_id
      LEFT JOIN clients c ON c.id = e.client_id
      LEFT JOIN expense_heads eh ON eh.id = e.expense_head_id
      WHERE e.source_type = 'recurring'
        AND e.recurring_expense_client_id = ANY($1::uuid[])
        AND e.is_active = true
        AND e.updated_at > e.created_at + interval '2 seconds'
      ORDER BY sp.start_date NULLS LAST, t.name NULLS LAST, c.name NULLS LAST
      LIMIT $2
    `, [allocIds]);

    expenses = expenseResult.rows.map((row) => ({
      ...row,
      amount: parseFloat(row.amount) || 0,
      total_amount: parseFloat(row.total_amount) || 0,
      is_active: row.is_active !== false,
    }));

    const dependencies = [
      buildDependencyEntry({
        type: 'Linked Expense',
        module: 'expenses',
        count: expenseResult.count,
        rows: expenses,
        mapRow: (row) => buildLinkedItem({
          id: row.id,
          label: `${safeText(row.service_period_name, 'No period')} | ${safeText(row.team_name, 'No team')}`,
          line: `Group: ${safeText(row.client_name, 'Not assigned')} | Modified`,
          module: 'expenses',
          type: 'Linked Expense',
          status: 'modified',
        }),
      }),
    ].filter(Boolean);

    return {
      dependencies,
      expenses,
      modified_count: expenses.filter((expense) => expense.is_modified).length,
      unmodified_count: expenses.filter((expense) => !expense.is_modified).length,
      total: dependencies.reduce((sum, dependency) => sum + dependency.count, 0),
      message: buildDeleteBlockedMessage('recurring master', dependencies),
    };
  }

  return {
    dependencies: [],
    expenses: [],
    modified_count: 0,
    unmodified_count: 0,
    total: 0,
    message: '',
  };
};

router.get('/:id/dependencies', auth, async (req, res) => {
  try {
    const { id } = req.params;
    const data = await getRecurringExpenseDependencies(pool, id);
    res.json({ success: true, data });
  } catch (error) {
    console.error('Check recurring expense dependencies error:', error);
    res.status(500).json({ success: false, message: 'Failed to check dependencies' });
  }
});

// Delete recurring expense
router.delete('/:id', auth, requireDelete, async (req, res) => {
  const dbClient = await pool.connect();
  try {
    await ensureRecurringExpenseSchema();
    const { id } = req.params;

    const currentResult = await dbClient.query('SELECT * FROM recurring_expenses WHERE id = $1', [id]);
    if (!currentResult.rows.length) {
      return res.status(404).json({ success: false, message: 'Recurring expense not found' });
    }

    const current = currentResult.rows[0];

    const dependencyData = await getRecurringExpenseDependencies(dbClient, id);
    if (dependencyData.total > 0) {
      return res.status(409).json({
        success: false,
        message: dependencyData.message,
        data: dependencyData,
      });
    }

    await dbClient.query('BEGIN');

    if (current.is_active !== false) {
      // Soft-delete: mark inactive
      const updatedResult = await dbClient.query(`
        UPDATE recurring_expenses SET is_active = false, updated_by = $1, updated_at = CURRENT_TIMESTAMP
        WHERE id = $2 RETURNING *
      `, [req.user.id, id]);
      await dbClient.query('COMMIT');
      await logAudit('recurring_expenses', id, 'DELETE', current, updatedResult.rows[0], req.user.id);
      return res.json({
        success: true,
        message: 'Recurring expense deactivated successfully',
        data: {}
      });
    }

    // Hard delete: already inactive
    // Explicitly delete child allocations first, then the recurring expense
    await dbClient.query('DELETE FROM recurring_expense_clients WHERE recurring_expense_id = $1 OR recurring_expense_team_id IN (SELECT id FROM recurring_expense_teams WHERE recurring_expense_id = $1)', [id]);
    await dbClient.query('DELETE FROM recurring_expense_teams WHERE recurring_expense_id = $1', [id]);
    await dbClient.query('DELETE FROM recurring_expenses WHERE id = $1', [id]);
    await dbClient.query('COMMIT');
    await logAudit('recurring_expenses', id, 'DELETE', current, {
      deleted: true, hard_delete: true
    }, req.user.id);

    res.json({
      success: true,
      message: 'Recurring expense permanently deleted',
      data: {}
    });
  } catch (error) {
    await dbClient.query('ROLLBACK').catch(() => {});
    console.error('Delete recurring expense error:', error);
    res.status(500).json({ success: false, message: 'Failed to delete recurring expense' });
  } finally {
    dbClient.release();
  }
});

// Validate allocation amounts
router.post('/validate', auth, async (req, res) => {
  try {
    const { amount, allocations } = req.body;
    if (!allocations || allocations.length === 0) {
      return res.json({ success: true, valid: true });
    }
    const liveAllocs = allocations.filter(a => !a.end_period);
    const totalAllocated = liveAllocs.reduce((sum, a) => sum + (parseFloat(a.amount) || 0), 0);
    const isValid = Math.abs(totalAllocated - parseFloat(amount)) < 0.01;
    res.json({ success: true, valid: isValid, totalAllocated, expectedAmount: parseFloat(amount), difference: Math.abs(totalAllocated - parseFloat(amount)) });
  } catch (error) {
    console.error('Validate recurring expense error:', error);
    res.status(500).json({ success: false, message: 'Validation failed' });
  }
});

module.exports = router;
