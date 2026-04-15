const express = require('express');
const router = express.Router();
const { body, validationResult } = require('express-validator');
const { pool, logAudit } = require('../database/db');
const { auth, requireDelete } = require('../middleware/auth');
const {
  normalizeClientPeriodHistory,
  summarizeClientPeriodRecord,
} = require('../utils/masterPeriodSummaries');
const {
  deactivateRecurringRevenuesForBillingRows,
  cleanupStaleRecurringSourceRecords,
} = require('../utils/recurringSourceCleanup');
const {
  buildDeleteBlockedMessage,
  buildDependencyEntry,
  buildLinkedItem,
  formatPeriodRange,
  queryLinkedRows,
  safeText,
} = require('../utils/deleteDependencyHelpers');

let clientSchemaPromise = null;
const PERIOD_REGEX = /^[A-Z][a-z]{2}-\d{2}$/;

async function ensureClientSchema() {
  if (!clientSchemaPromise) {
    clientSchemaPromise = (async () => {
      // Client billing rows table (billing name, bill from, reviewer per period)
      await pool.query(`
        CREATE TABLE IF NOT EXISTS client_billing_rows (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          client_id UUID REFERENCES clients(id) ON DELETE CASCADE,
          billing_name VARCHAR(255) NOT NULL,
          pan VARCHAR(10),
          gstin VARCHAR(15),
          service_type_id UUID REFERENCES service_types(id),
          bill_from_id UUID,
          reviewer_id UUID REFERENCES teams(id),
          start_period VARCHAR(10) NOT NULL,
          end_period VARCHAR(10),
          amount DECIMAL(15, 2) NOT NULL DEFAULT 0,
          is_active BOOLEAN DEFAULT true,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);

      await pool.query(`
        ALTER TABLE client_billing_rows
        ADD COLUMN IF NOT EXISTS service_type_id UUID REFERENCES service_types(id);
      `);

      // Refresh unique constraint to include service type
      await pool.query(`
        DO $$ BEGIN
          IF EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'unique_client_billing_combination'
          ) THEN
            ALTER TABLE client_billing_rows DROP CONSTRAINT unique_client_billing_combination;
          END IF;

          ALTER TABLE client_billing_rows
          ADD CONSTRAINT unique_client_billing_combination
          UNIQUE(client_id, billing_name, service_type_id, bill_from_id, reviewer_id, start_period);
        EXCEPTION
          WHEN duplicate_object THEN NULL;
          WHEN undefined_table THEN NULL;
        END $$;
      `).catch(() => {});

      await pool.query('CREATE INDEX IF NOT EXISTS idx_client_billing_rows_client ON client_billing_rows(client_id)');
      await pool.query('CREATE INDEX IF NOT EXISTS idx_client_billing_rows_period ON client_billing_rows(start_period, end_period)');
      await pool.query('CREATE INDEX IF NOT EXISTS idx_client_billing_rows_active ON client_billing_rows(is_active)');
      await pool.query('CREATE INDEX IF NOT EXISTS idx_client_billing_rows_service_type ON client_billing_rows(service_type_id)');

      // Enforce unique client name (case-insensitive)
      await pool.query(`
        CREATE UNIQUE INDEX IF NOT EXISTS idx_clients_name_unique ON clients(LOWER(TRIM(name)));
      `).catch(() => {/* index may already exist with conflicts – ignore */});

      // Client billing history for tracking all period changes
      await pool.query(`
        CREATE TABLE IF NOT EXISTS client_billing_row_history (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          client_billing_row_id UUID REFERENCES client_billing_rows(id) ON DELETE CASCADE,
          billing_name VARCHAR(255) NOT NULL,
          pan VARCHAR(10),
          gstin VARCHAR(15),
          service_type_id UUID REFERENCES service_types(id),
          bill_from_id UUID,
          reviewer_id UUID REFERENCES teams(id),
          start_period VARCHAR(10) NOT NULL,
          end_period VARCHAR(10),
          amount DECIMAL(15, 2) NOT NULL DEFAULT 0,
          changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          changed_by UUID REFERENCES users(id),
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);

      await pool.query(`
        ALTER TABLE client_billing_row_history
        ADD COLUMN IF NOT EXISTS service_type_id UUID REFERENCES service_types(id);
      `);

      await pool.query(`
        ALTER TABLE client_billing_row_history
        ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
      `);

      await pool.query('CREATE INDEX IF NOT EXISTS idx_client_billing_row_history_row ON client_billing_row_history(client_billing_row_id)');

      // One-time sync: correct any stale is_active on clients based on current billing rows
      await pool.query(`
        UPDATE clients c
        SET is_active = EXISTS(
          SELECT 1 FROM client_billing_rows cbr
          WHERE cbr.client_id = c.id AND cbr.is_active = true AND cbr.end_period IS NULL
        )
        WHERE c.is_active IS DISTINCT FROM EXISTS(
          SELECT 1 FROM client_billing_rows cbr
          WHERE cbr.client_id = c.id AND cbr.is_active = true AND cbr.end_period IS NULL
        )
      `);
    })().catch((error) => {
      clientSchemaPromise = null;
      throw error;
    });
  }
  return clientSchemaPromise;
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

const getNextPeriod = (period) => {
  const date = parsePeriod(period);
  if (!date) return null;
  date.setMonth(date.getMonth() + 1);
  return date.toLocaleString('en-US', { month: 'short' }) + `-${date.getFullYear().toString().slice(-2)}`;
};

const comparePeriods = (a, b) => {
  const dateA = parsePeriod(a);
  const dateB = parsePeriod(b);
  if (!dateA && !dateB) return 0;
  if (!dateA) return -1;
  if (!dateB) return 1;
  return dateA - dateB;
};

const isPeriodInRange = (period, startPeriod, endPeriod) => {
  if (!period || !startPeriod) return false;
  if (comparePeriods(period, startPeriod) < 0) return false;
  if (endPeriod && comparePeriods(period, endPeriod) > 0) return false;
  return true;
};

// Verify client name is unique (case-insensitive)
const ensureUniqueClientName = async (dbClient, name, excludeId = null) => {
  const result = await dbClient.query(`
    SELECT id FROM clients
    WHERE LOWER(TRIM(name)) = LOWER(TRIM($1))
      AND ($2::uuid IS NULL OR id <> $2)
    LIMIT 1
  `, [name, excludeId]);
  if (result.rows.length > 0) {
    const error = new Error('A client with this name already exists');
    error.code = 'DUPLICATE_CLIENT_NAME';
    throw error;
  }
};

// Verify billing name is unique within a client (same billing_name cannot have two different billing entities/PAN combos)
const ensureUniqueBillingNameWithinClient = async (dbClient, clientId, billingName, pan, gstin, excludeRowIds = []) => {
  const result = await dbClient.query(`
    SELECT DISTINCT LOWER(TRIM(billing_name)) as bn, COALESCE(pan,'') as pan, COALESCE(gstin,'') as gstin
    FROM client_billing_rows
    WHERE client_id = $1
      AND LOWER(TRIM(billing_name)) = LOWER(TRIM($2))
      AND is_active = true
      AND ($3::uuid[] = '{}'::uuid[] OR id <> ALL($3::uuid[]))
    LIMIT 1
  `, [clientId, billingName, excludeRowIds.length > 0 ? excludeRowIds : []]);

  if (result.rows.length > 0) {
    const existing = result.rows[0];
    const incomingPan = (pan || '').trim().toLowerCase();
    const existingPan = existing.pan.toLowerCase();
    const incomingGstin = (gstin || '').trim().toLowerCase();
    const existingGstin = existing.gstin.toLowerCase();
    // Only conflict if PAN/GSTIN differ — same PAN/GSTIN means same billing entity (allowed — different service types)
    if ((incomingPan && existingPan && incomingPan !== existingPan) ||
        (incomingGstin && existingGstin && incomingGstin !== existingGstin)) {
      const error = new Error('Billing name already exists within this client with different PAN/GSTIN');
      error.code = 'DUPLICATE_BILLING_NAME';
      throw error;
    }
  }
};

const ensureUniqueBillingCombination = async ({
  dbClient,
  clientId,
  billingName,
  serviceTypeId,
  billFromId,
  reviewerId,
  startPeriod,
  excludeRowId = null,
}) => {
  const result = await dbClient.query(`
    SELECT id
    FROM client_billing_rows
    WHERE client_id = $1
      AND LOWER(TRIM(billing_name)) = LOWER(TRIM($2))
      AND COALESCE(service_type_id::text, '') = COALESCE($3::text, '')
      AND COALESCE(bill_from_id::text, '') = COALESCE($4::text, '')
      AND COALESCE(reviewer_id::text, '') = COALESCE($5::text, '')
      AND start_period = $6
      AND is_active = true
      AND ($7::uuid IS NULL OR id <> $7)
    LIMIT 1
  `, [
    clientId,
    billingName,
    serviceTypeId || null,
    billFromId || null,
    reviewerId || null,
    startPeriod,
    excludeRowId,
  ]);

  if (result.rows.length > 0) {
    const error = new Error('Duplicate combination of billing name, service type, bill from, and reviewer for this period');
    error.code = 'DUPLICATE_BILLING_COMBINATION';
    throw error;
  }
};

const getBillFromNameById = async (dbClient, billFromId) => {
  if (!billFromId) return '';

  const result = await dbClient.query(
    'SELECT name FROM bill_from_masters WHERE id = $1',
    [billFromId]
  );

  return result.rows[0]?.name || '';
};

const roundCurrencyAmount = (value) => Math.round((parseFloat(value) || 0) * 100) / 100;

const formatLinkedPeriods = (rows = []) => {
  const periods = [...new Set((Array.isArray(rows) ? rows : []).map((row) => row.period_name).filter(Boolean))];
  if (!periods.length) return '';
  if (periods.length <= 5) return periods.join(', ');
  return `${periods.slice(0, 5).join(', ')} +${periods.length - 5} more`;
};

const getRecurringRevenueOverrideConflicts = async ({
  dbClient,
  billingRowId,
  currentRow,
  nextRow = null,
}) => {
  const overrideResult = await dbClient.query(`
    SELECT r.id, sp.display_name AS period_name
    FROM revenues r
    JOIN service_periods sp ON sp.id = r.service_period_id
    WHERE r.is_active = true
      AND r.source_type = 'recurring'
      AND r.client_billing_row_id = $1
      AND sp.start_date >= DATE_TRUNC('month', CURRENT_DATE)::date
    ORDER BY sp.start_date, r.created_at, r.id
  `, [billingRowId]);

  if (!overrideResult.rows.length) return [];

  const currentBillFromName = await getBillFromNameById(dbClient, currentRow.bill_from_id || null);
  const nextBillFromName = nextRow
    ? await getBillFromNameById(dbClient, nextRow.bill_from_id || null)
    : currentBillFromName;

  return overrideResult.rows.filter((row) => {
    const periodName = row.period_name;
    const currentInRange = isPeriodInRange(periodName, currentRow.start_period, currentRow.end_period);
    if (!currentInRange) return true;
    if (!nextRow) return true;

    const nextInRange = isPeriodInRange(periodName, nextRow.start_period, nextRow.end_period);
    if (!nextInRange) return true;

    return (
      roundCurrencyAmount(currentRow.amount) !== roundCurrencyAmount(nextRow.amount) ||
      (currentRow.service_type_id || null) !== (nextRow.service_type_id || null) ||
      (currentRow.reviewer_id || null) !== (nextRow.reviewer_id || null) ||
      currentBillFromName !== nextBillFromName
    );
  });
};

const assertNoBlockingRecurringRevenueOverrides = async (args) => {
  const conflicts = await getRecurringRevenueOverrideConflicts(args);
  if (!conflicts.length) return;

  const error = new Error(
    `This billing master is linked to overridden projected revenue item(s) for ${formatLinkedPeriods(conflicts)}. Revert those revenue rows back to projected state before modifying the client master.`
  );
  error.code = 'LINKED_PROJECTED_REVENUE';
  throw error;
};

const cascadeRecurringRevenueFieldUpdates = async ({
  dbClient,
  billingRowId,
  currentRow,
  nextRow,
}) => {
  const oldServiceTypeId = currentRow.service_type_id || null;
  const newServiceTypeId = nextRow.service_type_id || null;
  if (oldServiceTypeId !== newServiceTypeId) {
    await dbClient.query(`
      UPDATE revenues
      SET service_type_id = $1,
          updated_at = NOW()
      WHERE client_billing_row_id = $2
        AND is_active = true
        AND source_type = 'recurring'
        AND service_type_id IS NOT DISTINCT FROM $3
    `, [newServiceTypeId, billingRowId, oldServiceTypeId]);
  }

  const oldBillFromName = await getBillFromNameById(dbClient, currentRow.bill_from_id || null);
  const newBillFromName = await getBillFromNameById(dbClient, nextRow.bill_from_id || null);
  if (oldBillFromName !== newBillFromName) {
    await dbClient.query(`
      UPDATE revenues
      SET bill_from = NULLIF($1, ''),
          updated_at = NOW()
      WHERE client_billing_row_id = $2
        AND is_active = true
        AND source_type = 'recurring'
        AND COALESCE(bill_from, '') = $3
    `, [newBillFromName, billingRowId, oldBillFromName]);
  }
};

const getExistingTables = async (dbClient, tableNames) => {
  const result = await dbClient.query(`
    SELECT table_name, to_regclass(table_name) IS NOT NULL AS exists
    FROM unnest($1::text[]) AS tables(table_name)
  `, [tableNames]);

  return Object.fromEntries(
    result.rows.map((row) => [row.table_name.replace(/^public\./, ''), row.exists])
  );
};

const getClientDeleteDependencies = async (dbClient, id) => {
  const existingTables = await getExistingTables(dbClient, [
    'public.expenses',
    'public.revenues',
    'public.fee_masters',
    'public.billing_names',
    'public.recurring_expense_clients',
    'public.team_client_allocations'
  ]);

  const billingRowIdsResult = await dbClient.query(`
    SELECT id
    FROM client_billing_rows
    WHERE client_id = $1
  `, [id]);
  const billingRowIds = billingRowIdsResult.rows.map((row) => row.id);

  const recurringClientIdsResult = existingTables.recurring_expense_clients
    ? await dbClient.query(`
        SELECT id
        FROM recurring_expense_clients
        WHERE client_id = $1
      `, [id])
    : { rows: [] };
  const recurringClientIds = recurringClientIdsResult.rows.map((row) => row.id);

  const revenueResult = existingTables.revenues
    ? await queryLinkedRows(dbClient, `
        SELECT
          r.id,
          COUNT(*) OVER() AS total_count,
          COALESCE(sp.display_name, TO_CHAR(r.date, 'Mon-YY')) AS period_name,
          COALESCE(bn.name, cbr.billing_name) AS client_name,
          st.name AS service_type_name
        FROM revenues r
        LEFT JOIN service_periods sp ON sp.id = r.service_period_id
        LEFT JOIN billing_names bn ON bn.id = r.billing_name_id
        LEFT JOIN client_billing_rows cbr ON cbr.id = r.client_billing_row_id
        LEFT JOIN service_types st ON st.id = COALESCE(r.service_type_id, cbr.service_type_id)
        WHERE (
            r.client_id = $1
            OR ($2::uuid[] <> '{}'::uuid[] AND r.client_billing_row_id = ANY($2::uuid[]))
          ) AND r.updated_at > r.created_at + interval '2 seconds'
        ORDER BY sp.start_date NULLS LAST, r.date NULLS LAST, bn.name NULLS LAST, cbr.billing_name NULLS LAST
        LIMIT $3
      `, [id, billingRowIds])
    : { count: 0, rows: [] };

  const expenseResult = existingTables.expenses
    ? await queryLinkedRows(dbClient, `
        SELECT
          e.id,
          COUNT(*) OVER() AS total_count,
          COALESCE(sp.display_name, TO_CHAR(e.date, 'Mon-YY')) AS period_name,
          t.name AS team_name,
          eh.name AS expense_head_name
        FROM expenses e
        LEFT JOIN service_periods sp ON sp.id = e.service_period_id
        LEFT JOIN teams t ON t.id = e.team_id
        LEFT JOIN expense_heads eh ON eh.id = e.expense_head_id
        WHERE (
            e.client_id = $1
            OR ($2::uuid[] <> '{}'::uuid[] AND e.recurring_expense_client_id = ANY($2::uuid[]))
          ) AND e.updated_at > e.created_at + interval '2 seconds'
        ORDER BY sp.start_date NULLS LAST, e.date NULLS LAST, t.name NULLS LAST, eh.name NULLS LAST
        LIMIT $3
      `, [id, recurringClientIds])
    : { count: 0, rows: [] };

  const feeMasterResult = existingTables.fee_masters
    ? await queryLinkedRows(dbClient, `
        SELECT
          fm.id,
          COUNT(*) OVER() AS total_count,
          bn.name AS client_name,
          st.name AS service_type_name,
          fm.start_period,
          fm.end_period
        FROM fee_masters fm
        LEFT JOIN billing_names bn ON bn.id = fm.billing_name_id
        LEFT JOIN service_types st ON st.id = fm.service_type_id
        WHERE fm.client_id = $1
        ORDER BY fm.start_period NULLS LAST, bn.name NULLS LAST, st.name NULLS LAST
        LIMIT $2
      `, [id])
    : { count: 0, rows: [] };

  const billingNameResult = existingTables.billing_names
    ? await queryLinkedRows(dbClient, `
        SELECT
          bn.id,
          COUNT(*) OVER() AS total_count,
          bn.name,
          bn.gstin,
          bn.pan
        FROM billing_names bn
        WHERE bn.client_id = $1
        ORDER BY bn.name NULLS LAST
        LIMIT $2
      `, [id])
    : { count: 0, rows: [] };

  const recurringResult = existingTables.recurring_expense_clients
    ? await queryLinkedRows(dbClient, `
        SELECT
          rec.id,
          COUNT(*) OVER() AS total_count,
          eh.name AS expense_head_name,
          team_member.name AS team_name,
          reviewer.name AS reviewer_name,
          rec.start_period,
          rec.end_period
        FROM recurring_expense_clients rec
        LEFT JOIN recurring_expense_teams ret ON ret.id = rec.recurring_expense_team_id
        LEFT JOIN recurring_expenses re ON re.id = COALESCE(rec.recurring_expense_id, ret.recurring_expense_id)
        LEFT JOIN expense_heads eh ON eh.id = re.expense_head_id
        LEFT JOIN teams team_member ON team_member.id = COALESCE(rec.team_id, ret.team_id)
        LEFT JOIN teams reviewer ON reviewer.id = rec.reviewer_id
        WHERE rec.client_id = $1
        ORDER BY rec.start_period NULLS LAST, team_member.name NULLS LAST, eh.name NULLS LAST
        LIMIT $2
      `, [id])
    : { count: 0, rows: [] };

  const teamAllocationResult = existingTables.team_client_allocations
    ? await queryLinkedRows(dbClient, `
        SELECT
          tca.id,
          COUNT(*) OVER() AS total_count,
          t.name AS team_name,
          eh.name AS expense_head_name,
          tca.start_period,
          tca.end_period
        FROM team_client_allocations tca
        LEFT JOIN teams t ON t.id = tca.team_id
        LEFT JOIN expense_heads eh ON eh.id = tca.expense_head_id
        WHERE tca.client_id = $1
        ORDER BY t.name NULLS LAST, tca.start_period NULLS LAST, eh.name NULLS LAST
        LIMIT $2
      `, [id])
    : { count: 0, rows: [] };

  const dependencies = [
    buildDependencyEntry({
      type: 'Revenue',
      module: 'revenue',
      count: revenueResult.count,
      rows: revenueResult.rows,
      mapRow: (row) => buildLinkedItem({
        id: row.id,
        label: `${safeText(row.period_name, 'No period')} | ${safeText(row.client_name, 'No client label')}`,
        line: `Service type: ${safeText(row.service_type_name, 'Not assigned')}`,
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
        label: `${safeText(row.period_name, 'No period')} | ${safeText(row.team_name, 'No team')}`,
        line: `Expense head: ${safeText(row.expense_head_name, 'Not assigned')}`,
        module: 'expenses',
        type: 'Expense',
      }),
    }),
    buildDependencyEntry({
      type: 'Fee Master',
      module: 'revenue',
      count: feeMasterResult.count,
      rows: feeMasterResult.rows,
      mapRow: (row) => buildLinkedItem({
        id: row.id,
        label: `${safeText(row.client_name, 'No client label')} | ${safeText(row.service_type_name, 'No service type')}`,
        line: `Fee period: ${formatPeriodRange(row.start_period, row.end_period)}`,
        module: 'revenue',
        type: 'Fee Master',
      }),
    }),
    buildDependencyEntry({
      type: 'Client Master',
      module: 'clients',
      count: billingNameResult.count,
      rows: billingNameResult.rows,
      mapRow: (row) => buildLinkedItem({
        id: row.id,
        label: safeText(row.name, 'Unnamed client'),
        line: [row.gstin, row.pan].filter(Boolean).join(' | ') || 'Linked client master',
        module: 'clients',
        type: 'Client Master',
      }),
    }),
    buildDependencyEntry({
      type: 'Recurring Allocation',
      module: 'recurring',
      count: recurringResult.count,
      rows: recurringResult.rows,
      mapRow: (row) => buildLinkedItem({
        id: row.id,
        label: `${safeText(row.team_name, 'No team')} | ${safeText(row.expense_head_name, 'No expense head')}`,
        line: `Reviewer: ${safeText(row.reviewer_name, 'Not assigned')} | ${formatPeriodRange(row.start_period, row.end_period)}`,
        module: 'recurring',
        type: 'Recurring Allocation',
      }),
    }),
    buildDependencyEntry({
      type: 'Team Allocation',
      module: 'teams',
      count: teamAllocationResult.count,
      rows: teamAllocationResult.rows,
      mapRow: (row) => buildLinkedItem({
        id: row.id,
        label: `${safeText(row.team_name, 'No team')} | ${safeText(row.expense_head_name, 'No expense head')}`,
        line: `Allocation period: ${formatPeriodRange(row.start_period, row.end_period)}`,
        module: 'teams',
        type: 'Team Allocation',
      }),
    }),
  ].filter(Boolean);

  return {
    dependencies,
    total: dependencies.reduce((sum, dependency) => sum + dependency.count, 0),
    message: buildDeleteBlockedMessage('group master', dependencies),
  };
};

// Get all clients with their billing rows
router.get('/', auth, async (req, res) => {
  try {
    await ensureClientSchema();
    const { includeInactive, statusFilter, period } = req.query;

    let query = 'SELECT * FROM clients WHERE 1=1';
    if (statusFilter === 'inactive') {
      query += ' AND is_active = false';
    } else if (statusFilter !== 'all' && !includeInactive) {
      query += ' AND is_active = true';
    }
    query += ' ORDER BY name';

    const result = await pool.query(query);

    // Get billing rows for each client
    const clientsWithBilling = await Promise.all(result.rows.map(async (client) => {
      const billingResult = await pool.query(`
        SELECT cbr.*, st.name as service_type_name, bf.name as bill_from_name, t.name as reviewer_name
        FROM client_billing_rows cbr
        LEFT JOIN service_types st ON cbr.service_type_id = st.id
        LEFT JOIN bill_from_masters bf ON cbr.bill_from_id = bf.id
        LEFT JOIN teams t ON cbr.reviewer_id = t.id
        WHERE cbr.client_id = $1
        ORDER BY cbr.billing_name, st.name NULLS LAST, cbr.start_period ASC
      `, [client.id]);

      // Filter billing rows by period if specified
      let billingRows = billingResult.rows;
      if (period) {
        billingRows = billingRows.filter(row => isPeriodInRange(period, row.start_period, row.end_period));
      }

      // Calculate total amount for active billing rows
      const activeRows = billingRows.filter(row => row.is_active && (!row.end_period || comparePeriods(row.end_period, period || row.start_period) >= 0));
      const totalAmount = activeRows.reduce((sum, row) => sum + parseFloat(row.amount || 0), 0);

      return {
        ...client,
        billing_rows: billingRows,
        total_amount: totalAmount,
        active_billing_count: activeRows.length
      };
    }));

    res.json({ success: true, data: clientsWithBilling });
  } catch (error) {
    console.error('Get clients error:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch clients' });
  }
});

// Get single client with billing details and history
router.get('/:id', auth, async (req, res) => {
  try {
    await ensureClientSchema();
    const result = await pool.query('SELECT * FROM clients WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, message: 'Client not found' });
    }

    const billingResult = await pool.query(`
      SELECT cbr.*, st.name as service_type_name, bf.name as bill_from_name, t.name as reviewer_name
      FROM client_billing_rows cbr
      LEFT JOIN service_types st ON cbr.service_type_id = st.id
      LEFT JOIN bill_from_masters bf ON cbr.bill_from_id = bf.id
      LEFT JOIN teams t ON cbr.reviewer_id = t.id
      WHERE cbr.client_id = $1
      ORDER BY cbr.billing_name, st.name NULLS LAST, cbr.start_period ASC
    `, [req.params.id]);

    // Get history for each billing row
    const billingRowsWithHistory = await Promise.all(billingResult.rows.map(async (row) => {
      const historyResult = await pool.query(`
        SELECT cbh.*, st.name as service_type_name, bf.name as bill_from_name, t.name as reviewer_name
        FROM client_billing_row_history cbh
        LEFT JOIN service_types st ON cbh.service_type_id = st.id
        LEFT JOIN bill_from_masters bf ON cbh.bill_from_id = bf.id
        LEFT JOIN teams t ON cbh.reviewer_id = t.id
        WHERE cbh.client_billing_row_id = $1
        ORDER BY cbh.changed_at DESC
      `, [row.id]);

      const periodHistory = await Promise.all(
        normalizeClientPeriodHistory(row, historyResult.rows).map((record) =>
          summarizeClientPeriodRecord(pool, record)
        )
      );

      return { ...row, history: periodHistory };
    }));

    const activeRows = billingResult.rows.filter(row => row.is_active);
    const totalAmount = activeRows.reduce((sum, row) => sum + parseFloat(row.amount || 0), 0);

    res.json({
      success: true,
      data: {
        ...result.rows[0],
        billing_rows: billingRowsWithHistory,
        total_amount: totalAmount,
        active_billing_count: activeRows.length
      }
    });
  } catch (error) {
    console.error('Get client error:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch client' });
  }
});

// Create client
router.post('/', auth, [
  body('name').trim().notEmpty().withMessage('Name is required'),
  body('billing_rows').optional().isArray(),
  body('is_active').optional().isBoolean(),
], async (req, res) => {
  const dbClient = await pool.connect();
  try {
    await ensureClientSchema();
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ success: false, errors: errors.array() });
    }

    const { name, billing_rows, is_active } = req.body;
    const validBillingRows = (billing_rows || []).filter((row) => row.billing_name && row.start_period);

    await ensureUniqueClientName(dbClient, name);

    if (is_active === false && validBillingRows.some((row) => !row.end_period)) {
      return res.status(400).json({
        success: false,
        message: 'End period must be updated for all billing rows before creating an inactive client'
      });
    }

    await dbClient.query('BEGIN');

    const result = await dbClient.query(
      'INSERT INTO clients (name, is_active) VALUES ($1, $2) RETURNING *',
      [name.trim(), is_active !== false]
    );

    const clientId = result.rows[0].id;

    // Insert billing rows if provided
    if (billing_rows && billing_rows.length > 0) {
      for (const row of billing_rows) {
        if (!row.billing_name || !row.start_period) continue;

        // Validate period format
        if (!PERIOD_REGEX.test(row.start_period)) {
          await dbClient.query('ROLLBACK');
          return res.status(400).json({
            success: false,
            message: `Invalid start period format: ${row.start_period}. Use MMM-YY format (e.g., Jan-24)`
          });
        }

        if (row.end_period && !PERIOD_REGEX.test(row.end_period)) {
          await dbClient.query('ROLLBACK');
          return res.status(400).json({
            success: false,
            message: `Invalid end period format: ${row.end_period}. Use MMM-YY format (e.g., Dec-24)`
          });
        }

        if (!row.service_type_id) {
          await dbClient.query('ROLLBACK');
          return res.status(400).json({
            success: false,
            message: 'Service type is required for each billing row'
          });
        }

        await ensureUniqueBillingCombination({
          dbClient,
          clientId,
          billingName: row.billing_name.trim(),
          serviceTypeId: row.service_type_id,
          billFromId: row.bill_from_id,
          reviewerId: row.reviewer_id,
          startPeriod: row.start_period,
        });

        await dbClient.query(`
          INSERT INTO client_billing_rows 
          (client_id, billing_name, pan, gstin, service_type_id, bill_from_id, reviewer_id, start_period, end_period, amount)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        `, [
          clientId,
          row.billing_name.trim(),
          row.pan || null,
          row.gstin || null,
          row.service_type_id || null,
          row.bill_from_id || null,
          row.reviewer_id || null,
          row.start_period,
          row.end_period || null,
          row.amount || 0
        ]);
      }
    }

    await dbClient.query('COMMIT');
    await logAudit('clients', clientId, 'CREATE', null, result.rows[0], req.user.id);

    res.status(201).json({ success: true, message: 'Client created successfully', data: result.rows[0] });
  } catch (error) {
    await dbClient.query('ROLLBACK');
    console.error('Create client error:', error);
    if (error.code === 'DUPLICATE_CLIENT_NAME') {
      return res.status(400).json({ success: false, message: error.message });
    }
    if (error.code === '23505') {
      return res.status(400).json({
        success: false,
        message: 'Duplicate combination of billing name, service type, bill from, and reviewer for this period'
      });
    }
    if (error.code === 'DUPLICATE_BILLING_COMBINATION') {
      return res.status(400).json({
        success: false,
        message: error.message
      });
    }
    res.status(500).json({ success: false, message: 'Failed to create client' });
  } finally {
    dbClient.release();
  }
});

// Update client
router.put('/:id', auth, [
  body('name').optional().trim().notEmpty().withMessage('Name cannot be empty'),
  body('billing_rows').optional().isArray(),
  body('is_active').optional().isBoolean(),
], async (req, res) => {
  const dbClient = await pool.connect();
  try {
    await ensureClientSchema();
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ success: false, errors: errors.array() });
    }

    const current = await dbClient.query('SELECT * FROM clients WHERE id = $1', [req.params.id]);
    if (current.rows.length === 0) {
      return res.status(404).json({ success: false, message: 'Client not found' });
    }

    const { name, billing_rows, is_active } = req.body;

    await dbClient.query('BEGIN');

    // Update client name if provided
    if (name !== undefined) {
      await ensureUniqueClientName(dbClient, name.trim(), req.params.id);
      await dbClient.query('UPDATE clients SET name = $1 WHERE id = $2', [name.trim(), req.params.id]);
    }

    // Sync billing rows if provided
    if (billing_rows !== undefined) {
      // Get current billing rows
      const currentBillingRows = await dbClient.query(
        'SELECT * FROM client_billing_rows WHERE client_id = $1',
        [req.params.id]
      );

      // Track which rows to keep
      const updatedRowIds = new Set();

      for (const row of billing_rows) {
        if (!row.billing_name || !row.start_period) continue;

        // Validate period format
        if (!PERIOD_REGEX.test(row.start_period)) {
          await dbClient.query('ROLLBACK');
          return res.status(400).json({
            success: false,
            message: `Invalid start period format: ${row.start_period}. Use MMM-YY format (e.g., Jan-24)`
          });
        }

        if (row.end_period && !PERIOD_REGEX.test(row.end_period)) {
          await dbClient.query('ROLLBACK');
          return res.status(400).json({
            success: false,
            message: `Invalid end period format: ${row.end_period}. Use MMM-YY format (e.g., Dec-24)`
          });
        }

        if (!row.service_type_id) {
          await dbClient.query('ROLLBACK');
          return res.status(400).json({
            success: false,
            message: 'Service type is required for each billing row'
          });
        }

        await ensureUniqueBillingCombination({
          dbClient,
          clientId: req.params.id,
          billingName: row.billing_name.trim(),
          serviceTypeId: row.service_type_id,
          billFromId: row.bill_from_id,
          reviewerId: row.reviewer_id,
          startPeriod: row.start_period,
          excludeRowId: row.id || null,
        });

        if (row.id) {
          // Update existing row
          const currentRow = currentBillingRows.rows.find(r => r.id === row.id);
          if (currentRow) {
            const nextRowState = {
              ...currentRow,
              service_type_id: row.service_type_id || null,
              bill_from_id: row.bill_from_id || null,
              reviewer_id: row.reviewer_id || null,
              start_period: row.start_period,
              end_period: row.end_period || null,
              amount: row.amount || 0,
              is_active: row.is_active !== false,
            };
            const billingProjectionChanged =
              currentRow.start_period !== nextRowState.start_period ||
              (currentRow.end_period || null) !== (nextRowState.end_period || null) ||
              roundCurrencyAmount(currentRow.amount) !== roundCurrencyAmount(nextRowState.amount) ||
              (currentRow.service_type_id || null) !== (nextRowState.service_type_id || null) ||
              (currentRow.bill_from_id || null) !== (nextRowState.bill_from_id || null) ||
              (currentRow.reviewer_id || null) !== (nextRowState.reviewer_id || null) ||
              currentRow.is_active !== nextRowState.is_active;

            if (billingProjectionChanged) {
              await assertNoBlockingRecurringRevenueOverrides({
                dbClient,
                billingRowId: row.id,
                currentRow,
                nextRow: nextRowState,
              });
            }

            // Save to history before updating
            await dbClient.query(`
              INSERT INTO client_billing_row_history 
              (client_billing_row_id, billing_name, pan, gstin, service_type_id, bill_from_id, reviewer_id, start_period, end_period, amount, changed_by)
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            `, [
              row.id,
              currentRow.billing_name,
              currentRow.pan,
              currentRow.gstin,
              currentRow.service_type_id,
              currentRow.bill_from_id,
              currentRow.reviewer_id,
              currentRow.start_period,
              currentRow.end_period,
              currentRow.amount,
              req.user.id
            ]);

            await dbClient.query(`
              UPDATE client_billing_rows 
              SET billing_name = $1, pan = $2, gstin = $3, service_type_id = $4, bill_from_id = $5, reviewer_id = $6,
                  start_period = $7, end_period = $8, amount = $9, is_active = $10
              WHERE id = $11
            `, [
              row.billing_name.trim(),
              row.pan || null,
              row.gstin || null,
              row.service_type_id || null,
              row.bill_from_id || null,
              row.reviewer_id || null,
              row.start_period,
              row.end_period || null,
              row.amount || 0,
              row.is_active !== false,
              row.id
            ]);

            // Cascade amount changes to actual revenue records
            const oldAmount = parseFloat(currentRow.amount) || 0;
            const newAmount = parseFloat(row.amount) || 0;
            if (oldAmount !== newAmount) {
              await dbClient.query(`
                UPDATE revenues
                SET revenue_amount = $1,
                    total_amount = $1 + COALESCE(igst, 0) + COALESCE(cgst, 0) + COALESCE(sgst, 0) + COALESCE(other_charges, 0) + COALESCE(round_off, 0),
                    updated_at = NOW()
                WHERE client_billing_row_id = $2
                  AND is_active = true
                  AND source_type = 'recurring'
                  AND revenue_amount = $3
              `, [newAmount, row.id, oldAmount]);
            }
            // Cascade reviewer changes to actual revenue records
            if ((currentRow.reviewer_id || null) !== (row.reviewer_id || null)) {
              await dbClient.query(`
                UPDATE revenues
                SET reviewer_id = $1, updated_at = NOW()
                WHERE client_billing_row_id = $2
                  AND is_active = true
                  AND source_type = 'recurring'
                  AND reviewer_id IS NOT DISTINCT FROM $3
              `, [row.reviewer_id || null, row.id, currentRow.reviewer_id || null]);
            }

            await cascadeRecurringRevenueFieldUpdates({
              dbClient,
              billingRowId: row.id,
              currentRow,
              nextRow: {
                ...currentRow,
                service_type_id: row.service_type_id || null,
                bill_from_id: row.bill_from_id || null,
              },
            });

            updatedRowIds.add(row.id);
          }
        } else {
          // Insert new row
          const insertResult = await dbClient.query(`
            INSERT INTO client_billing_rows 
            (client_id, billing_name, pan, gstin, service_type_id, bill_from_id, reviewer_id, start_period, end_period, amount)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING id
          `, [
            req.params.id,
            row.billing_name.trim(),
            row.pan || null,
            row.gstin || null,
            row.service_type_id || null,
            row.bill_from_id || null,
            row.reviewer_id || null,
            row.start_period,
            row.end_period || null,
            row.amount || 0
          ]);
          updatedRowIds.add(insertResult.rows[0].id);
        }
      }

      // Mark rows not in the update as inactive (soft delete)
      const retiredBillingRowIds = [];
      for (const existingRow of currentBillingRows.rows) {
        if (!updatedRowIds.has(existingRow.id)) {
          await assertNoBlockingRecurringRevenueOverrides({
            dbClient,
            billingRowId: existingRow.id,
            currentRow: existingRow,
            nextRow: null,
          });

          await dbClient.query(`
            INSERT INTO client_billing_row_history
            (client_billing_row_id, billing_name, pan, gstin, service_type_id, bill_from_id, reviewer_id, start_period, end_period, amount, changed_by)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
          `, [
            existingRow.id,
            existingRow.billing_name,
            existingRow.pan,
            existingRow.gstin,
            existingRow.service_type_id,
            existingRow.bill_from_id,
            existingRow.reviewer_id,
            existingRow.start_period,
            existingRow.end_period,
            existingRow.amount,
            req.user.id
          ]);

          await dbClient.query(
            'UPDATE client_billing_rows SET is_active = false, updated_at = NOW() WHERE id = $1',
            [existingRow.id]
          );
          retiredBillingRowIds.push(existingRow.id);
        }
      }

      await deactivateRecurringRevenuesForBillingRows(dbClient, retiredBillingRowIds, req.user.id);
    }

    // Auto-manage client is_active based on whether any open billing rows exist
    await dbClient.query(`
      UPDATE clients
      SET is_active = EXISTS(
        SELECT 1 FROM client_billing_rows
        WHERE client_id = $1 AND is_active = true AND end_period IS NULL
      )
      WHERE id = $1
    `, [req.params.id]);

    await dbClient.query('COMMIT');

    // Cleanup any orphaned recurring source records after client modification
    try {
      const cleanupClient = await pool.connect();
      try {
        await cleanupClient.query('BEGIN');
        await cleanupStaleRecurringSourceRecords(cleanupClient, req.user.id);
        await cleanupClient.query('COMMIT');
      } catch (cleanupErr) {
        await cleanupClient.query('ROLLBACK').catch(() => {});
        console.error('Post-client-update cleanup warning:', cleanupErr.message);
      } finally {
        cleanupClient.release();
      }
    } catch (cleanupErr) {
      console.error('Post-client-update cleanup connection warning:', cleanupErr.message);
    }

    const updated = await pool.query('SELECT * FROM clients WHERE id = $1', [req.params.id]);
    await logAudit('clients', req.params.id, 'UPDATE', current.rows[0], updated.rows[0], req.user.id);

    res.json({ success: true, message: 'Client updated successfully', data: updated.rows[0] });
  } catch (error) {
    await dbClient.query('ROLLBACK');
    console.error('Update client error:', error);
    if (error.code === 'DUPLICATE_CLIENT_NAME') {
      return res.status(400).json({ success: false, message: error.message });
    }
    if (error.code === '23505') {
      return res.status(400).json({
        success: false,
        message: 'Duplicate combination of billing name, service type, bill from, and reviewer for this period'
      });
    }
    if (error.code === 'DUPLICATE_BILLING_COMBINATION') {
      return res.status(400).json({
        success: false,
        message: error.message
      });
    }
    if (error.code === 'LINKED_PROJECTED_REVENUE') {
      return res.status(400).json({
        success: false,
        message: error.message
      });
    }
    res.status(500).json({ success: false, message: 'Failed to update client' });
  } finally {
    dbClient.release();
  }
});

// Add billing row to client
router.post('/:id/billing-rows', auth, [
  body('billing_name').trim().notEmpty().withMessage('Billing name is required'),
  body('service_type_id').isUUID().withMessage('Service type is required'),
  body('start_period').matches(PERIOD_REGEX).withMessage('Start period must be in MMM-YY format'),
  body('amount').isFloat({ min: 0 }).withMessage('Amount must be a positive number'),
], async (req, res) => {
  try {
    await ensureClientSchema();
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ success: false, errors: errors.array() });
    }

    const { billing_name, pan, gstin, service_type_id, bill_from_id, reviewer_id, start_period, end_period, amount } = req.body;

    // Check client exists
    const client = await pool.query('SELECT id FROM clients WHERE id = $1', [req.params.id]);
    if (client.rows.length === 0) {
      return res.status(404).json({ success: false, message: 'Client not found' });
    }

    await ensureUniqueBillingCombination({
      dbClient: pool,
      clientId: req.params.id,
      billingName: billing_name.trim(),
      serviceTypeId: service_type_id,
      billFromId: bill_from_id,
      reviewerId: reviewer_id,
      startPeriod: start_period,
    });

    const result = await pool.query(`
      INSERT INTO client_billing_rows 
      (client_id, billing_name, pan, gstin, service_type_id, bill_from_id, reviewer_id, start_period, end_period, amount)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      RETURNING *
    `, [
      req.params.id,
      billing_name.trim(),
      pan || null,
      gstin || null,
      service_type_id || null,
      bill_from_id || null,
      reviewer_id || null,
      start_period,
      end_period || null,
      amount || 0
    ]);

    // Auto-manage client is_active based on open billing rows
    await pool.query(`
      UPDATE clients
      SET is_active = EXISTS(
        SELECT 1 FROM client_billing_rows
        WHERE client_id = $1 AND is_active = true AND end_period IS NULL
      )
      WHERE id = $1
    `, [req.params.id]);

    res.status(201).json({ success: true, message: 'Billing row added successfully', data: result.rows[0] });
  } catch (error) {
    console.error('Add billing row error:', error);
    if (error.code === '23505') {
      return res.status(400).json({
        success: false,
        message: 'This combination of billing name, service type, bill from, and reviewer already exists for this period'
      });
    }
    if (error.code === 'DUPLICATE_BILLING_COMBINATION') {
      return res.status(400).json({
        success: false,
        message: error.message
      });
    }
    res.status(500).json({ success: false, message: 'Failed to add billing row' });
  }
});

// Add multiple billing rows at once (for billing group creation)
router.post('/:id/billing-rows/batch', auth, async (req, res) => {
  const dbClient = await pool.connect();
  try {
    await ensureClientSchema();
    const { rows } = req.body;
    if (!rows || !Array.isArray(rows) || rows.length === 0) {
      return res.status(400).json({ success: false, message: 'Rows array is required' });
    }

    const client = await dbClient.query('SELECT id FROM clients WHERE id = $1', [req.params.id]);
    if (client.rows.length === 0) {
      return res.status(404).json({ success: false, message: 'Client not found' });
    }

    await dbClient.query('BEGIN');
    const insertedRows = [];

    for (const row of rows) {
      if (!row.billing_name || !row.start_period) continue;
      if (!PERIOD_REGEX.test(row.start_period)) {
        await dbClient.query('ROLLBACK');
        return res.status(400).json({ success: false, message: `Invalid start period format: ${row.start_period}` });
      }
      if (!row.service_type_id) {
        await dbClient.query('ROLLBACK');
        return res.status(400).json({ success: false, message: 'Service type is required for each billing row' });
      }
      if (row.end_period && !PERIOD_REGEX.test(row.end_period)) {
        await dbClient.query('ROLLBACK');
        return res.status(400).json({ success: false, message: `Invalid end period format: ${row.end_period}` });
      }

      await ensureUniqueBillingCombination({
        dbClient,
        clientId: req.params.id,
        billingName: row.billing_name.trim(),
        serviceTypeId: row.service_type_id,
        billFromId: row.bill_from_id,
        reviewerId: row.reviewer_id,
        startPeriod: row.start_period,
      });

      const result = await dbClient.query(`
        INSERT INTO client_billing_rows 
        (client_id, billing_name, pan, gstin, service_type_id, bill_from_id, reviewer_id, start_period, end_period, amount)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        RETURNING *
      `, [
        req.params.id,
        row.billing_name.trim(),
        row.pan || null,
        row.gstin || null,
        row.service_type_id || null,
        row.bill_from_id || null,
        row.reviewer_id || null,
        row.start_period,
        row.end_period || null,
        row.amount || 0
      ]);
      insertedRows.push(result.rows[0]);
    }

    // Auto-manage client is_active — goes active if any open rows were just added
    await dbClient.query(`
      UPDATE clients
      SET is_active = EXISTS(
        SELECT 1 FROM client_billing_rows
        WHERE client_id = $1 AND is_active = true AND end_period IS NULL
      )
      WHERE id = $1
    `, [req.params.id]);

    await dbClient.query('COMMIT');
    res.status(201).json({ success: true, message: `${insertedRows.length} billing rows added`, data: insertedRows });
  } catch (error) {
    await dbClient.query('ROLLBACK');
    console.error('Batch add billing rows error:', error);
    if (error.code === '23505' || error.code === 'DUPLICATE_BILLING_COMBINATION') {
      return res.status(400).json({ success: false, message: error.message || 'Duplicate billing combination found' });
    }
    res.status(500).json({ success: false, message: 'Failed to add billing rows: ' + (error.detail || error.message || String(error)) });
  } finally {
    dbClient.release();
  }
});

// Update billing row
router.put('/:id/billing-rows/:rowId', auth, [
  body('billing_name').optional().trim().notEmpty().withMessage('Billing name cannot be empty'),
  body('service_type_id').optional({ values: 'falsy' }).isUUID().withMessage('Service type must be valid'),
  body('start_period').optional().matches(PERIOD_REGEX).withMessage('Start period must be in MMM-YY format'),
  body('end_period').optional({ values: 'falsy' }).matches(PERIOD_REGEX).withMessage('End period must be in MMM-YY format'),
  body('amount').optional().isFloat({ min: 0 }).withMessage('Amount must be a positive number'),
], async (req, res) => {
  const dbClient = await pool.connect();
  try {
    await ensureClientSchema();
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ success: false, errors: errors.array() });
    }

    const current = await dbClient.query(
      'SELECT * FROM client_billing_rows WHERE id = $1 AND client_id = $2',
      [req.params.rowId, req.params.id]
    );

    if (current.rows.length === 0) {
      return res.status(404).json({ success: false, message: 'Billing row not found' });
    }

    const currentRow = current.rows[0];
    const { billing_name, pan, gstin, service_type_id, bill_from_id, reviewer_id, start_period, end_period, amount, is_active } = req.body;
    const nextStartPeriod = start_period !== undefined ? start_period : currentRow.start_period;
    const nextEndPeriod = end_period !== undefined ? (end_period || null) : currentRow.end_period;
    const nextIsActive = nextEndPeriod ? false : (is_active !== undefined ? is_active : currentRow.is_active);

    if (nextEndPeriod && comparePeriods(nextEndPeriod, nextStartPeriod) < 0) {
      return res.status(400).json({
        success: false,
        message: 'End period cannot be before start period'
      });
    }

    const isReactivating = nextIsActive && (currentRow.is_active === false || !!currentRow.end_period);
    if (isReactivating) {
      let reactivationFloor = currentRow.end_period;
      if (!reactivationFloor) {
        const historyFloorResult = await dbClient.query(`
          SELECT end_period
          FROM client_billing_row_history
          WHERE client_billing_row_id = $1
            AND end_period IS NOT NULL
          ORDER BY changed_at DESC
          LIMIT 1
        `, [req.params.rowId]);
        reactivationFloor = historyFloorResult.rows[0]?.end_period || null;
      }

      const reactivationStartFloor = reactivationFloor ? getNextPeriod(reactivationFloor) : null;
      if (reactivationStartFloor && comparePeriods(nextStartPeriod, reactivationStartFloor) < 0) {
        return res.status(400).json({
          success: false,
          message: `Start period can only be selected from ${reactivationStartFloor} onward when reactivating`
        });
      }
    }

    await ensureUniqueBillingCombination({
      dbClient,
      clientId: req.params.id,
      billingName: (billing_name !== undefined ? billing_name : currentRow.billing_name).trim(),
      serviceTypeId: service_type_id !== undefined ? service_type_id : currentRow.service_type_id,
      billFromId: bill_from_id !== undefined ? bill_from_id : currentRow.bill_from_id,
      reviewerId: reviewer_id !== undefined ? reviewer_id : currentRow.reviewer_id,
      startPeriod: start_period !== undefined ? start_period : currentRow.start_period,
      excludeRowId: req.params.rowId,
    });

    const nextRowState = {
      ...currentRow,
      service_type_id: service_type_id !== undefined ? service_type_id || null : currentRow.service_type_id,
      bill_from_id: bill_from_id !== undefined ? bill_from_id || null : currentRow.bill_from_id,
      reviewer_id: reviewer_id !== undefined ? reviewer_id || null : currentRow.reviewer_id,
      start_period: start_period !== undefined ? start_period : currentRow.start_period,
      end_period: nextEndPeriod,
      amount: amount !== undefined ? amount : currentRow.amount,
      is_active: nextIsActive,
    };
    const billingProjectionChanged =
      currentRow.start_period !== nextRowState.start_period ||
      (currentRow.end_period || null) !== (nextRowState.end_period || null) ||
      roundCurrencyAmount(currentRow.amount) !== roundCurrencyAmount(nextRowState.amount) ||
      (currentRow.service_type_id || null) !== (nextRowState.service_type_id || null) ||
      (currentRow.bill_from_id || null) !== (nextRowState.bill_from_id || null) ||
      (currentRow.reviewer_id || null) !== (nextRowState.reviewer_id || null) ||
      currentRow.is_active !== nextRowState.is_active;

    if (billingProjectionChanged) {
      await assertNoBlockingRecurringRevenueOverrides({
        dbClient,
        billingRowId: req.params.rowId,
        currentRow,
        nextRow: nextRowState,
      });
    }

    await dbClient.query('BEGIN');

    // Save current state to history
    await dbClient.query(`
      INSERT INTO client_billing_row_history 
      (client_billing_row_id, billing_name, pan, gstin, service_type_id, bill_from_id, reviewer_id, start_period, end_period, amount, changed_by)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    `, [
      req.params.rowId,
      currentRow.billing_name,
      currentRow.pan,
      currentRow.gstin,
      currentRow.service_type_id,
      currentRow.bill_from_id,
      currentRow.reviewer_id,
      currentRow.start_period,
      currentRow.end_period,
      currentRow.amount,
      req.user.id
    ]);

    // Build update query
    const updates = [];
    const values = [];
    let paramIndex = 1;

    if (billing_name !== undefined) {
      updates.push(`billing_name = $${paramIndex++}`);
      values.push(billing_name.trim());
    }
    if (pan !== undefined) {
      updates.push(`pan = $${paramIndex++}`);
      values.push(pan || null);
    }
    if (gstin !== undefined) {
      updates.push(`gstin = $${paramIndex++}`);
      values.push(gstin || null);
    }
    if (service_type_id !== undefined) {
      updates.push(`service_type_id = $${paramIndex++}`);
      values.push(service_type_id || null);
    }
    if (bill_from_id !== undefined) {
      updates.push(`bill_from_id = $${paramIndex++}`);
      values.push(bill_from_id || null);
    }
    if (reviewer_id !== undefined) {
      updates.push(`reviewer_id = $${paramIndex++}`);
      values.push(reviewer_id || null);
    }
    if (start_period !== undefined) {
      updates.push(`start_period = $${paramIndex++}`);
      values.push(start_period);
    }
    if (end_period !== undefined) {
      updates.push(`end_period = $${paramIndex++}`);
      values.push(nextEndPeriod);
    }
    if (amount !== undefined) {
      updates.push(`amount = $${paramIndex++}`);
      values.push(amount);
    }
    if (is_active !== undefined || end_period !== undefined) {
      updates.push(`is_active = $${paramIndex++}`);
      values.push(nextIsActive);
    }

    if (updates.length === 0) {
      await dbClient.query('ROLLBACK');
      return res.status(400).json({ success: false, message: 'No fields to update' });
    }

    values.push(req.params.rowId);
    const result = await dbClient.query(
      `UPDATE client_billing_rows SET ${updates.join(', ')} WHERE id = $${paramIndex} RETURNING *`,
      values
    );

    // Cascade amount changes to actual revenue records
    const oldAmountVal = parseFloat(currentRow.amount) || 0;
    const newAmountVal = parseFloat(amount !== undefined ? amount : currentRow.amount) || 0;
    if (oldAmountVal !== newAmountVal) {
      await dbClient.query(`
        UPDATE revenues
        SET revenue_amount = $1,
            total_amount = $1 + COALESCE(igst, 0) + COALESCE(cgst, 0) + COALESCE(sgst, 0) + COALESCE(other_charges, 0) + COALESCE(round_off, 0),
            updated_at = NOW()
        WHERE client_billing_row_id = $2
          AND is_active = true
          AND source_type = 'recurring'
          AND revenue_amount = $3
      `, [newAmountVal, req.params.rowId, oldAmountVal]);
    }
    // Cascade reviewer changes to actual revenue records
    const oldRevId = currentRow.reviewer_id || null;
    const newRevId = (reviewer_id !== undefined ? reviewer_id : currentRow.reviewer_id) || null;
    if (oldRevId !== newRevId) {
      await dbClient.query(`
        UPDATE revenues
        SET reviewer_id = $1, updated_at = NOW()
        WHERE client_billing_row_id = $2
          AND is_active = true
          AND source_type = 'recurring'
          AND reviewer_id IS NOT DISTINCT FROM $3
      `, [newRevId, req.params.rowId, oldRevId]);
    }

    await cascadeRecurringRevenueFieldUpdates({
      dbClient,
      billingRowId: req.params.rowId,
      currentRow,
      nextRow: {
        ...currentRow,
        service_type_id: service_type_id !== undefined ? service_type_id || null : currentRow.service_type_id,
        bill_from_id: bill_from_id !== undefined ? bill_from_id || null : currentRow.bill_from_id,
      },
    });

    // Fully retire linked recurring revenues only when the billing row is removed.
    // Closed rows with an end period keep their in-range projected revenue history.
    if (nextIsActive === false && !nextEndPeriod && currentRow.is_active !== false) {
      await deactivateRecurringRevenuesForBillingRows(dbClient, [req.params.rowId], req.user.id);
    }

    // Auto-manage parent client is_active based on remaining open billing rows
    await dbClient.query(`
      UPDATE clients
      SET is_active = EXISTS(
        SELECT 1 FROM client_billing_rows
        WHERE client_id = $1 AND is_active = true AND end_period IS NULL
      )
      WHERE id = $1
    `, [req.params.id]);

    await dbClient.query('COMMIT');

    // Cleanup any orphaned recurring source records
    try {
      const cleanupClient2 = await pool.connect();
      try {
        await cleanupClient2.query('BEGIN');
        await cleanupStaleRecurringSourceRecords(cleanupClient2, req.user.id);
        await cleanupClient2.query('COMMIT');
      } catch (cleanupErr) {
        await cleanupClient2.query('ROLLBACK').catch(() => {});
        console.error('Post-billing-row-update cleanup warning:', cleanupErr.message);
      } finally {
        cleanupClient2.release();
      }
    } catch (cleanupErr) {
      console.error('Post-billing-row-update cleanup connection warning:', cleanupErr.message);
    }

    res.json({ success: true, message: 'Billing row updated successfully', data: result.rows[0] });
  } catch (error) {
    await dbClient.query('ROLLBACK').catch(() => {});
    console.error('Update billing row error:', error);
    if (error.code === '23505') {
      return res.status(400).json({
        success: false,
        message: 'This combination already exists for another period'
      });
    }
    if (error.code === 'DUPLICATE_BILLING_COMBINATION') {
      return res.status(400).json({
        success: false,
        message: error.message
      });
    }
    if (error.code === 'LINKED_PROJECTED_REVENUE') {
      return res.status(400).json({
        success: false,
        message: error.message
      });
    }
    res.status(500).json({ success: false, message: 'Failed to update billing row' });
  } finally {
    dbClient.release();
  }
});

// Update a closed billing period in one transaction
router.put('/:id/billing-period', auth, [
  body('original_start_period').matches(PERIOD_REGEX).withMessage('Original start period must be in MMM-YY format'),
  body('original_end_period').matches(PERIOD_REGEX).withMessage('Original end period must be in MMM-YY format'),
  body('rows').isArray({ min: 1 }).withMessage('At least one history row is required'),
  body('rows.*.row_id').isUUID().withMessage('History row id must be valid'),
  body('rows.*.amount').isFloat({ min: 0 }).withMessage('Amount must be a positive number'),
  body('rows.*.bill_from_id').optional({ values: 'falsy' }).isUUID().withMessage('Bill from must be valid'),
  body('rows.*.reviewer_id').optional({ values: 'falsy' }).isUUID().withMessage('Reviewer must be valid'),
], async (req, res) => {
  const dbClient = await pool.connect();
  try {
    await ensureClientSchema();
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ success: false, errors: errors.array() });
    }

    const { id } = req.params;
    const { original_start_period, original_end_period, rows = [] } = req.body;
    const rowIds = [...new Set(rows.map((row) => row?.row_id).filter(Boolean))];

    if (!rowIds.length || rowIds.length !== rows.length) {
      return res.status(400).json({
        success: false,
        message: 'Each history row must be supplied once'
      });
    }

    const currentRowsResult = await dbClient.query(`
      SELECT *
      FROM client_billing_rows
      WHERE client_id = $1
        AND id = ANY($2::uuid[])
    `, [id, rowIds]);

    if (currentRowsResult.rows.length !== rowIds.length) {
      return res.status(404).json({
        success: false,
        message: 'One or more billing rows were not found'
      });
    }

    const currentRowsById = new Map(currentRowsResult.rows.map((row) => [row.id, row]));
    const nextRows = rows.map((row) => {
      const currentRow = currentRowsById.get(row.row_id);
      return {
        currentRow,
        nextRow: {
          ...currentRow,
          bill_from_id: row.bill_from_id || null,
          reviewer_id: row.reviewer_id || null,
          amount: parseFloat(row.amount) || 0,
        },
      };
    });

    const periodMismatch = nextRows.some(({ currentRow }) =>
      currentRow.start_period !== original_start_period ||
      (currentRow.end_period || null) !== (original_end_period || null)
    );

    if (periodMismatch) {
      return res.status(400).json({
        success: false,
        message: 'History rows must belong to the selected closed period'
      });
    }

    const hasOpenRow = nextRows.some(({ currentRow }) => !currentRow.end_period);
    if (hasOpenRow) {
      return res.status(400).json({
        success: false,
        message: 'Only closed billing periods can be edited from history'
      });
    }

    const duplicateHistoryRows = new Set();
    for (const { nextRow } of nextRows) {
      const key = [
        (nextRow.billing_name || '').trim().toLowerCase(),
        nextRow.service_type_id || '',
        nextRow.bill_from_id || '',
        nextRow.reviewer_id || '',
        nextRow.start_period || '',
      ].join('||');
      if (duplicateHistoryRows.has(key)) {
        return res.status(400).json({
          success: false,
          message: 'Duplicate bill from and reviewer combinations are not allowed within the same history period'
        });
      }
      duplicateHistoryRows.add(key);
    }

    for (const { currentRow, nextRow } of nextRows) {
      const billingProjectionChanged =
        roundCurrencyAmount(currentRow.amount) !== roundCurrencyAmount(nextRow.amount) ||
        (currentRow.bill_from_id || null) !== (nextRow.bill_from_id || null) ||
        (currentRow.reviewer_id || null) !== (nextRow.reviewer_id || null);

      if (billingProjectionChanged) {
        await assertNoBlockingRecurringRevenueOverrides({
          dbClient,
          billingRowId: currentRow.id,
          currentRow,
          nextRow,
        });
      }
    }

    await dbClient.query('BEGIN');

    for (const { currentRow, nextRow } of nextRows) {
      await dbClient.query(`
        INSERT INTO client_billing_row_history
        (client_billing_row_id, billing_name, pan, gstin, service_type_id, bill_from_id, reviewer_id, start_period, end_period, amount, changed_by)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
      `, [
        currentRow.id,
        currentRow.billing_name,
        currentRow.pan,
        currentRow.gstin,
        currentRow.service_type_id,
        currentRow.bill_from_id,
        currentRow.reviewer_id,
        currentRow.start_period,
        currentRow.end_period,
        currentRow.amount,
        req.user.id
      ]);

      await dbClient.query(`
        UPDATE client_billing_rows
        SET bill_from_id = $1,
            reviewer_id = $2,
            amount = $3,
            updated_at = NOW()
        WHERE id = $4
          AND client_id = $5
      `, [
        nextRow.bill_from_id,
        nextRow.reviewer_id,
        nextRow.amount,
        currentRow.id,
        id,
      ]);

      const oldAmountVal = parseFloat(currentRow.amount) || 0;
      const newAmountVal = parseFloat(nextRow.amount) || 0;
      if (oldAmountVal !== newAmountVal) {
        await dbClient.query(`
          UPDATE revenues
          SET revenue_amount = $1,
              total_amount = $1 + COALESCE(igst, 0) + COALESCE(cgst, 0) + COALESCE(sgst, 0) + COALESCE(other_charges, 0) + COALESCE(round_off, 0),
              updated_at = NOW()
          WHERE client_billing_row_id = $2
            AND is_active = true
            AND source_type = 'recurring'
            AND revenue_amount = $3
        `, [newAmountVal, currentRow.id, oldAmountVal]);
      }

      const oldReviewerId = currentRow.reviewer_id || null;
      const newReviewerId = nextRow.reviewer_id || null;
      if (oldReviewerId !== newReviewerId) {
        await dbClient.query(`
          UPDATE revenues
          SET reviewer_id = $1,
              updated_at = NOW()
          WHERE client_billing_row_id = $2
            AND is_active = true
            AND source_type = 'recurring'
            AND reviewer_id IS NOT DISTINCT FROM $3
        `, [newReviewerId, currentRow.id, oldReviewerId]);
      }

      await cascadeRecurringRevenueFieldUpdates({
        dbClient,
        billingRowId: currentRow.id,
        currentRow,
        nextRow,
      });
    }

    await dbClient.query('COMMIT');
    await logAudit('clients', id, 'UPDATE_PERIOD', { original_start_period, original_end_period }, { rows }, req.user.id);

    res.json({
      success: true,
      message: 'Billing history period updated successfully'
    });
  } catch (error) {
    await dbClient.query('ROLLBACK').catch(() => {});
    console.error('Update billing period error:', error);
    if (error.code === 'LINKED_PROJECTED_REVENUE') {
      return res.status(400).json({
        success: false,
        message: error.message
      });
    }
    res.status(500).json({ success: false, message: 'Failed to update billing history period' });
  } finally {
    dbClient.release();
  }
});

// Delete (soft) billing row
router.delete('/:id/billing-rows/:rowId', auth, requireDelete, async (req, res) => {
  const dbClient = await pool.connect();
  try {
    await ensureClientSchema();

    await dbClient.query('BEGIN');

    const current = await dbClient.query(
      'SELECT * FROM client_billing_rows WHERE id = $1 AND client_id = $2',
      [req.params.rowId, req.params.id]
    );

    if (current.rows.length === 0) {
      await dbClient.query('ROLLBACK');
      return res.status(404).json({ success: false, message: 'Billing row not found' });
    }

    const currentRow = current.rows[0];

    await dbClient.query(`
      INSERT INTO client_billing_row_history
      (client_billing_row_id, billing_name, pan, gstin, service_type_id, bill_from_id, reviewer_id, start_period, end_period, amount, changed_by)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    `, [
      currentRow.id,
      currentRow.billing_name,
      currentRow.pan,
      currentRow.gstin,
      currentRow.service_type_id,
      currentRow.bill_from_id,
      currentRow.reviewer_id,
      currentRow.start_period,
      currentRow.end_period,
      currentRow.amount,
      req.user.id
    ]);

    await dbClient.query(
      'UPDATE client_billing_rows SET is_active = false, updated_at = NOW() WHERE id = $1 AND client_id = $2',
      [req.params.rowId, req.params.id]
    );

    await deactivateRecurringRevenuesForBillingRows(dbClient, [req.params.rowId], req.user.id);

    await dbClient.query('COMMIT');

    res.json({ success: true, message: 'Billing row deactivated successfully' });
  } catch (error) {
    await dbClient.query('ROLLBACK').catch(() => {});
    console.error('Delete billing row error:', error);
    res.status(500).json({ success: false, message: 'Failed to delete billing row' });
  } finally {
    dbClient.release();
  }
});

router.get('/:id/dependencies', auth, async (req, res) => {
  try {
    await ensureClientSchema();
    const data = await getClientDeleteDependencies(pool, req.params.id);
    res.json({ success: true, data });
  } catch (error) {
    console.error('Check client dependencies error:', error);
    res.status(500).json({ success: false, message: 'Failed to check dependencies' });
  }
});

// Delete client
router.delete('/:id', auth, requireDelete, async (req, res) => {
  const dbClient = await pool.connect();
  try {
    await ensureClientSchema();
    const { id } = req.params;

    const current = await dbClient.query('SELECT * FROM clients WHERE id = $1', [id]);
    if (current.rows.length === 0) {
      return res.status(404).json({ success: false, message: 'Client not found' });
    }

    const dependencyData = await getClientDeleteDependencies(dbClient, id);
    if (dependencyData.total > 0) {
      return res.status(409).json({
        success: false,
        message: dependencyData.message,
        data: dependencyData,
      });
    }

    await dbClient.query('BEGIN');
    const billingRowCountResult = await dbClient.query(`
      SELECT COUNT(*) AS count
      FROM client_billing_rows
      WHERE client_id = $1
    `, [id]);

    await dbClient.query('DELETE FROM clients WHERE id = $1', [id]);
    await dbClient.query('COMMIT');

    const deletedSummary = {
      billing_rows: parseInt(billingRowCountResult.rows[0]?.count, 10) || 0,
    };

    try {
      await logAudit('clients', id, 'DELETE', current.rows[0], { deleted: true, cascade_summary: deletedSummary }, req.user.id);
    } catch (auditError) {
      console.error('Client delete audit log error:', auditError);
    }

    res.json({
      success: true,
      message: 'Group deleted successfully',
      data: deletedSummary
    });
  } catch (error) {
    await dbClient.query('ROLLBACK').catch(() => {});
    console.error('Delete client error:', error);
    res.status(500).json({ success: false, message: 'Failed to delete client' });
  } finally {
    dbClient.release();
  }
});

// Get billing rows summary for dashboard
router.get('/summary/monthly-total', auth, async (req, res) => {
  try {
    await ensureClientSchema();
    const { period } = req.query;

    let query = `
      SELECT 
        COALESCE(SUM(cbr.amount), 0) as total_amount,
        COUNT(DISTINCT cbr.client_id) as client_count,
        COUNT(cbr.id) as billing_row_count
      FROM client_billing_rows cbr
      JOIN clients c ON cbr.client_id = c.id
      WHERE cbr.is_active = true AND c.is_active = true
    `;

    const params = [];
    if (period && PERIOD_REGEX.test(period)) {
      params.push(period, period);
      query += ` AND cbr.start_period <= $1 AND (cbr.end_period IS NULL OR cbr.end_period >= $2)`;
    }

    const result = await pool.query(query, params);

    res.json({
      success: true,
      data: {
        total_amount: parseFloat(result.rows[0].total_amount || 0),
        client_count: parseInt(result.rows[0].client_count || 0),
        billing_row_count: parseInt(result.rows[0].billing_row_count || 0)
      }
    });
  } catch (error) {
    console.error('Get monthly total error:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch monthly total' });
  }
});

module.exports = router;
