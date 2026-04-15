const express = require('express');
const router = express.Router();
const { body, validationResult } = require('express-validator');
const { pool, logAudit } = require('../database/db');
const { auth, requireDelete } = require('../middleware/auth');
const { ensureTeamAllocationPeriodColumns } = require('../utils/teamAllocationPeriods');

let expenseSchemaPromise = null;

async function ensureExpenseSchema() {
  if (!expenseSchemaPromise) {
    expenseSchemaPromise = (async () => {
      await pool.query(`
        ALTER TABLE expenses
          ADD COLUMN IF NOT EXISTS source_type VARCHAR(20) DEFAULT 'non-recurring',
          ADD COLUMN IF NOT EXISTS recurring_expense_client_id UUID;
      `);

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

      await pool.query(`
        CREATE TABLE IF NOT EXISTS recurring_expense_teams (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          recurring_expense_id UUID REFERENCES recurring_expenses(id) ON DELETE CASCADE,
          team_id UUID REFERENCES teams(id) NOT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);

      await pool.query(`
        CREATE TABLE IF NOT EXISTS recurring_expense_clients (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          recurring_expense_team_id UUID REFERENCES recurring_expense_teams(id) ON DELETE CASCADE,
          client_id UUID REFERENCES clients(id),
          start_period VARCHAR(10) NOT NULL,
          end_period VARCHAR(10),
          amount DECIMAL(15, 2) NOT NULL DEFAULT 0,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);

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
        ALTER TABLE recurring_expenses
          ADD COLUMN IF NOT EXISTS vendor_id UUID REFERENCES vendors(id);
      `).catch(() => {});

      await pool.query(`
        ALTER TABLE expenses
          ADD COLUMN IF NOT EXISTS team_client_allocation_id UUID;
      `).catch(() => {});

      await pool.query(`
        ALTER TABLE expenses
          ADD COLUMN IF NOT EXISTS is_admin BOOLEAN DEFAULT false;
      `).catch(() => {});

      await ensureTeamAllocationPeriodColumns(pool);

      await pool.query('CREATE INDEX IF NOT EXISTS idx_expenses_source_type ON expenses(source_type)');
      await pool.query('CREATE INDEX IF NOT EXISTS idx_expenses_recurring_client_period ON expenses(recurring_expense_client_id, service_period_id)');
      await pool.query('CREATE INDEX IF NOT EXISTS idx_expenses_team_alloc_period ON expenses(team_client_allocation_id, service_period_id)');
    })().catch((error) => {
      expenseSchemaPromise = null;
      throw error;
    });
  }

  return expenseSchemaPromise;
}

const parseFilterArray = (value) => {
  if (!value) return null;
  if (Array.isArray(value)) return value.length > 0 ? value : null;
  const arr = String(value).split(',').filter(Boolean);
  return arr.length > 0 ? arr : null;
};

const LIVE_MONTH_START_SQL = "DATE_TRUNC('month', CURRENT_DATE)::date";
const buildRecurringProjectedExpr = (alias = 'e') => `(${alias}.id IS NULL AND sp.start_date >= ${LIVE_MONTH_START_SQL})`;
const buildRecurringUnbilledExpr = (alias = 'e') => `
  (CASE
    WHEN ${alias}.id IS NULL THEN sp.start_date < ${LIVE_MONTH_START_SQL}
    ELSE COALESCE(${alias}.is_unbilled, true)
  END)
`;
const buildTeamCompensationAmountExpr = (teamAlias = 't', periodAlias = 'sp') => `COALESCE(
  (
    SELECT tih.amount
    FROM team_increment_history tih
    WHERE tih.team_id = ${teamAlias}.id
      AND TO_DATE('01-' || tih.start_period, 'DD-Mon-YY') <= ${periodAlias}.start_date
      AND (
        tih.end_period IS NULL
        OR TO_DATE('01-' || tih.end_period, 'DD-Mon-YY') >= ${periodAlias}.start_date
      )
    ORDER BY TO_DATE('01-' || tih.start_period, 'DD-Mon-YY') DESC
    LIMIT 1
  ),
  ${teamAlias}.amount
)`;
const buildTeamAllocationAmountExpr = (allocationAlias = 'tca', teamAlias = 't', periodAlias = 'sp') => `ROUND(
  COALESCE(
    ${allocationAlias}.allocation_amount,
    (${allocationAlias}.allocation_percentage / 100.0) * ${buildTeamCompensationAmountExpr(teamAlias, periodAlias)}
  ),
  2
)`;
const buildTeamAllocationExistsExpr = (allocationAlias = 'tca') =>
  `(COALESCE(${allocationAlias}.allocation_amount, 0) > 0 OR COALESCE(${allocationAlias}.allocation_percentage, 0) > 0)`;

function generateUniqueKey(clientName, expenseHeadName, servicePeriodName) {
  return `${clientName || 'general'}|${expenseHeadName || 'expense'}|${servicePeriodName || 'periodless'}`
    .toLowerCase()
    .replace(/\s+/g, '-');
}

const buildNonRecurringWhere = (filters = {}, alias = 'e', periodAlias = 'sp') => {
  const params = [];
  let paramIndex = 1;
  let whereClause = `WHERE ${alias}.is_active = true AND COALESCE(${alias}.source_type, 'non-recurring') = 'non-recurring'`;

  const clientIds = parseFilterArray(filters.clientId);
  if (clientIds) {
    params.push(clientIds);
    whereClause += ` AND ${alias}.client_id = ANY($${paramIndex++}::uuid[])`;
  }
  const spIds = parseFilterArray(filters.servicePeriodId);
  if (spIds) {
    params.push(spIds);
    whereClause += ` AND ${alias}.service_period_id = ANY($${paramIndex++}::uuid[])`;
  }
  const teamIds = parseFilterArray(filters.teamId);
  if (teamIds) {
    params.push(teamIds);
    whereClause += ` AND ${alias}.team_id = ANY($${paramIndex++}::uuid[])`;
  }
  const ehIds = parseFilterArray(filters.expenseHeadId);
  if (ehIds) {
    params.push(ehIds);
    whereClause += ` AND ${alias}.expense_head_id = ANY($${paramIndex++}::uuid[])`;
  }
  if (filters.financialYear) {
    params.push(filters.financialYear);
    whereClause += ` AND ${periodAlias}.financial_year = $${paramIndex++}`;
  }
  if (filters.isUnbilled !== undefined) {
    params.push(filters.isUnbilled === 'true' || filters.isUnbilled === true);
    whereClause += ` AND ${alias}.is_unbilled = $${paramIndex++}`;
  }
  if (filters.startDate) {
    params.push(filters.startDate);
    whereClause += ` AND ${alias}.date >= $${paramIndex++}`;
  }
  if (filters.endDate) {
    params.push(filters.endDate);
    whereClause += ` AND ${alias}.date <= $${paramIndex++}`;
  }

  return { whereClause, params };
};

const getNonRecurringRows = async (filters = {}) => {
  const { whereClause, params } = buildNonRecurringWhere(filters);
  const result = await pool.query(`
    SELECT
      e.id::text as id,
      e.recurring_expense_client_id,
      e.unique_key,
      e.client_id,
      e.service_period_id,
      e.date,
      e.is_entered_in_books,
      e.is_unbilled,
      e.team_id,
      e.expense_head_id,
      e.description,
      e.amount,
      e.gst_rate,
      e.igst,
      e.cgst,
      e.sgst,
      e.other_charges,
      e.round_off,
      e.total_amount,
      e.bill_from,
      e.notes,
      e.created_at,
      e.updated_at,
      c.name as client_name,
      sp.display_name as service_period_name,
      sp.financial_year,
      sp.start_date as period_start_date,
      t.name as team_name,
      eh.name as expense_head_name,
      'non-recurring' as source_type
    FROM expenses e
    LEFT JOIN clients c ON e.client_id = c.id
    LEFT JOIN service_periods sp ON e.service_period_id = sp.id
    LEFT JOIN teams t ON e.team_id = t.id
    LEFT JOIN expense_heads eh ON e.expense_head_id = eh.id
    ${whereClause}
  `, params);

  return result.rows.map((row) => ({
    ...row,
    amount: parseFloat(row.amount) || 0,
    igst: parseFloat(row.igst) || 0,
    cgst: parseFloat(row.cgst) || 0,
    sgst: parseFloat(row.sgst) || 0,
    other_charges: parseFloat(row.other_charges) || 0,
    round_off: parseFloat(row.round_off) || 0,
    total_amount: parseFloat(row.total_amount) || 0,
  }));
};

const getRecurringRows = async (filters = {}) => {
  const params = [];
  let paramIndex = 1;
  let whereClause = 'WHERE (re.is_active = true OR re.end_period IS NOT NULL OR e.id IS NOT NULL)';

  const clientIds = parseFilterArray(filters.clientId);
  if (clientIds) {
    params.push(clientIds);
    whereClause += ` AND rec.client_id = ANY($${paramIndex++}::uuid[])`;
  }
  const spIds = parseFilterArray(filters.servicePeriodId);
  if (spIds) {
    params.push(spIds);
    whereClause += ` AND sp.id = ANY($${paramIndex++}::uuid[])`;
  }
  const teamIds = parseFilterArray(filters.teamId);
  if (teamIds) {
    params.push(teamIds);
    whereClause += ` AND ret.team_id = ANY($${paramIndex++}::uuid[])`;
  }
  if (filters.expenseHeadId) {
    const ehIds = parseFilterArray(filters.expenseHeadId);
    if (ehIds) {
      params.push(ehIds);
      whereClause += ` AND re.expense_head_id = ANY($${paramIndex++}::uuid[])`;
    }
  }
  if (filters.financialYear) {
    params.push(filters.financialYear);
    whereClause += ` AND sp.financial_year = $${paramIndex++}`;
  }
  if (filters.isUnbilled !== undefined) {
    params.push(filters.isUnbilled === 'true' || filters.isUnbilled === true);
    whereClause += ` AND ${buildRecurringUnbilledExpr('e')} = $${paramIndex++}`;
  }
  if (filters.startDate) {
    params.push(filters.startDate);
    whereClause += ` AND COALESCE(e.date, sp.start_date) >= $${paramIndex++}`;
  }
  if (filters.endDate) {
    params.push(filters.endDate);
    whereClause += ` AND COALESCE(e.date, sp.start_date) <= $${paramIndex++}`;
  }

  const result = await pool.query(`
    SELECT
      COALESCE(e.id::text, CONCAT('recurring-', rec.id::text, '-', sp.id::text)) as id,
      e.id as expense_record_id,
      rec.id as recurring_expense_client_id,
      re.id as recurring_expense_id,
      ret.id as recurring_expense_team_id,
      COALESCE(e.unique_key, LOWER(REPLACE(CONCAT(COALESCE(c.name, 'general'), '|', eh.name, '|', sp.display_name), ' ', '-'))) as unique_key,
      rec.client_id,
      sp.id as service_period_id,
      COALESCE(e.date, sp.start_date) as date,
      COALESCE(e.is_entered_in_books, false) as is_entered_in_books,
      ${buildRecurringUnbilledExpr('e')} as is_unbilled,
      ${buildRecurringProjectedExpr('e')} as is_projected,
      ret.team_id as team_id,
      re.expense_head_id,
      COALESCE(e.description, CONCAT('Recurring ', eh.name)) as description,
      COALESCE(e.amount, rec.amount) as amount,
      rec.amount as projected_amount,
      COALESCE(e.gst_rate, 0) as gst_rate,
      COALESCE(e.igst, 0) as igst,
      COALESCE(e.cgst, 0) as cgst,
      COALESCE(e.sgst, 0) as sgst,
      COALESCE(e.other_charges, 0) as other_charges,
      COALESCE(e.round_off, 0) as round_off,
      COALESCE(
        e.total_amount,
        COALESCE(e.amount, rec.amount) + COALESCE(e.igst, 0) + COALESCE(e.cgst, 0) + COALESCE(e.sgst, 0) + COALESCE(e.other_charges, 0) + COALESCE(e.round_off, 0)
      ) as total_amount,
      e.bill_from,
      e.notes,
      COALESCE(e.created_at, rec.created_at) as created_at,
      COALESCE(e.updated_at, rec.updated_at) as updated_at,
      c.name as client_name,
      sp.display_name as service_period_name,
      sp.financial_year,
      sp.start_date as period_start_date,
      t.name as team_name,
      eh.name as expense_head_name,
      v.name as vendor_name,
      re.vendor_id,
      'recurring' as source_type
    FROM recurring_expenses re
    JOIN recurring_expense_teams ret ON ret.recurring_expense_id = re.id
    JOIN recurring_expense_clients rec ON rec.recurring_expense_team_id = ret.id
    JOIN service_periods sp
      ON sp.is_active = true
     AND sp.start_date >= TO_DATE('01-' || COALESCE(rec.start_period, re.start_period), 'DD-Mon-YY')
     AND (
       COALESCE(rec.end_period, re.end_period) IS NULL
       OR sp.start_date <= TO_DATE('01-' || COALESCE(rec.end_period, re.end_period), 'DD-Mon-YY')
     )
    LEFT JOIN LATERAL (
      SELECT ex.*
      FROM expenses ex
      WHERE ex.is_active = true
        AND ex.recurring_expense_client_id = rec.id
        AND ex.service_period_id = sp.id
        AND (
          ex.source_type = 'recurring'
          OR (ex.source_type IS NULL AND ex.recurring_expense_client_id IS NOT NULL)
        )
      ORDER BY ex.updated_at DESC NULLS LAST, ex.created_at DESC NULLS LAST, ex.id DESC
      LIMIT 1
    ) e ON true
    LEFT JOIN clients c ON rec.client_id = c.id
    LEFT JOIN teams t ON ret.team_id = t.id
    JOIN expense_heads eh ON re.expense_head_id = eh.id
    LEFT JOIN vendors v ON re.vendor_id = v.id
    ${whereClause}
  `, params);

  return result.rows.map((row) => {
    const isProjected = row.is_projected === true || row.is_projected === 't';
    return {
      ...row,
      amount: parseFloat(row.amount) || 0,
      projected_amount: parseFloat(row.projected_amount) || 0,
      igst: parseFloat(row.igst) || 0,
      cgst: parseFloat(row.cgst) || 0,
      sgst: parseFloat(row.sgst) || 0,
      other_charges: parseFloat(row.other_charges) || 0,
      round_off: parseFloat(row.round_off) || 0,
      total_amount: parseFloat(row.total_amount) || 0,
      is_projected: isProjected,
      date: isProjected ? null : row.date,
    };
  });
};

const getTeamExpenseRows = async (filters = {}) => {
  const params = [];
  let paramIndex = 1;
  let whereClause = `WHERE t.is_reviewer = true
    AND t.start_period IS NOT NULL
    AND t.amount IS NOT NULL
    AND (
      t.is_active = true
      OR COALESCE(tca.end_period, t.end_period) IS NOT NULL
      OR e.id IS NOT NULL
    )
    AND (${buildTeamAllocationExistsExpr('tca')} OR e.id IS NOT NULL)`;

  const clientIds = parseFilterArray(filters.clientId);
  if (clientIds) {
    params.push(clientIds);
    whereClause += ` AND tca.client_id = ANY($${paramIndex++}::uuid[])`;
  }
  const spIds = parseFilterArray(filters.servicePeriodId);
  if (spIds) {
    params.push(spIds);
    whereClause += ` AND sp.id = ANY($${paramIndex++}::uuid[])`;
  }
  const teamIds = parseFilterArray(filters.teamId);
  if (teamIds) {
    params.push(teamIds);
    whereClause += ` AND t.id = ANY($${paramIndex++}::uuid[])`;
  }
  const ehIds = parseFilterArray(filters.expenseHeadId);
  if (ehIds) {
    params.push(ehIds);
    whereClause += ` AND tca.expense_head_id = ANY($${paramIndex++}::uuid[])`;
  }
  if (filters.financialYear) {
    params.push(filters.financialYear);
    whereClause += ` AND sp.financial_year = $${paramIndex++}`;
  }
  if (filters.isUnbilled !== undefined) {
    params.push(filters.isUnbilled === 'true' || filters.isUnbilled === true);
    whereClause += ` AND ${buildRecurringUnbilledExpr('e')} = $${paramIndex++}`;
  }

  const result = await pool.query(`
    SELECT
      COALESCE(e.id::text, CONCAT('team-', tca.id::text, '-', sp.id::text)) as id,
      e.id as expense_record_id,
      tca.id as team_client_allocation_id,
      tca.client_id,
      sp.id as service_period_id,
      COALESCE(e.date, sp.start_date) as date,
      COALESCE(e.is_entered_in_books, false) as is_entered_in_books,
      ${buildRecurringUnbilledExpr('e')} as is_unbilled,
      ${buildRecurringProjectedExpr('e')} as is_projected,
      t.id as team_id,
      tca.expense_head_id as expense_head_id,
      COALESCE(e.description, CONCAT(t.name, ' - ', eh.name)) as description,
      COALESCE(e.amount, ${buildTeamAllocationAmountExpr('tca', 't', 'sp')}) as amount,
      ${buildTeamAllocationAmountExpr('tca', 't', 'sp')} as projected_amount,
      COALESCE(e.gst_rate, 0) as gst_rate,
      COALESCE(e.igst, 0) as igst,
      COALESCE(e.cgst, 0) as cgst,
      COALESCE(e.sgst, 0) as sgst,
      COALESCE(e.other_charges, 0) as other_charges,
      COALESCE(e.round_off, 0) as round_off,
      COALESCE(
        e.total_amount,
        ${buildTeamAllocationAmountExpr('tca', 't', 'sp')}
      ) as total_amount,
      e.bill_from,
      e.notes,
      COALESCE(e.unique_key, CONCAT('team-', tca.id::text, '-', sp.id::text)) as unique_key,
      COALESCE(e.created_at, sp.start_date) as created_at,
      COALESCE(e.updated_at, sp.start_date) as updated_at,
      c.name as client_name,
      sp.display_name as service_period_name,
      sp.financial_year,
      sp.start_date as period_start_date,
      t.name as team_name,
      eh.name as expense_head_name,
      'team-recurring' as source_type
    FROM teams t
    JOIN team_client_allocations tca ON tca.team_id = t.id
    JOIN expense_heads eh ON tca.expense_head_id = eh.id
    JOIN service_periods sp
      ON sp.is_active = true
     AND sp.start_date >= TO_DATE('01-' || COALESCE(tca.start_period, t.start_period), 'DD-Mon-YY')
     AND (
       COALESCE(tca.end_period, t.end_period) IS NULL
       OR sp.start_date <= TO_DATE('01-' || COALESCE(tca.end_period, t.end_period), 'DD-Mon-YY')
     )
    LEFT JOIN LATERAL (
      SELECT ${buildTeamCompensationAmountExpr('t', 'sp')} as base_amount
    ) eff ON true
    LEFT JOIN LATERAL (
      SELECT ex.*
      FROM expenses ex
      WHERE ex.is_active = true
        AND ex.team_client_allocation_id = tca.id
        AND ex.service_period_id = sp.id
        AND (
          ex.source_type = 'team-recurring'
          OR (ex.source_type IS NULL AND ex.team_client_allocation_id IS NOT NULL)
        )
      ORDER BY ex.updated_at DESC NULLS LAST, ex.created_at DESC NULLS LAST, ex.id DESC
      LIMIT 1
    ) e ON true
    LEFT JOIN clients c ON tca.client_id = c.id
    ${whereClause}
  `, params);

  return result.rows.map((row) => {
    const isProjected = row.is_projected === true || row.is_projected === 't';
    return {
      ...row,
      amount: parseFloat(row.amount) || 0,
      projected_amount: parseFloat(row.projected_amount) || 0,
      igst: parseFloat(row.igst) || 0,
      cgst: parseFloat(row.cgst) || 0,
      sgst: parseFloat(row.sgst) || 0,
      other_charges: parseFloat(row.other_charges) || 0,
      round_off: parseFloat(row.round_off) || 0,
      total_amount: parseFloat(row.total_amount) || 0,
      is_projected: isProjected,
      date: isProjected ? null : row.date,
    };
  });
};

const getAdminRecurringRows = async (filters = {}) => {
  const params = [];
  let paramIndex = 1;
  let whereClause = 'WHERE (re.is_active = true OR re.end_period IS NOT NULL) AND re.is_admin = true';

  const spIds = parseFilterArray(filters.servicePeriodId);
  if (spIds) {
    params.push(spIds);
    whereClause += ` AND sp.id = ANY($${paramIndex++}::uuid[])`;
  }
  if (filters.expenseHeadId) {
    const ehIds = parseFilterArray(filters.expenseHeadId);
    if (ehIds) {
      params.push(ehIds);
      whereClause += ` AND re.expense_head_id = ANY($${paramIndex++}::uuid[])`;
    }
  }
  if (filters.financialYear) {
    params.push(filters.financialYear);
    whereClause += ` AND sp.financial_year = $${paramIndex++}`;
  }

  const result = await pool.query(`
    SELECT
      CONCAT('admin-', re.id::text, '-', sp.id::text) as id,
      NULL as expense_record_id,
      NULL as recurring_expense_client_id,
      re.id as recurring_expense_id,
      NULL as recurring_expense_team_id,
      LOWER(REPLACE(CONCAT('admin|', eh.name, '|', sp.display_name), ' ', '-')) as unique_key,
      NULL as client_id,
      sp.id as service_period_id,
      sp.start_date as date,
      false as is_entered_in_books,
      true as is_unbilled,
      true as is_projected,
      NULL as team_id,
      re.expense_head_id,
      CONCAT('Admin - ', eh.name) as description,
      re.amount,
      re.amount as projected_amount,
      0 as gst_rate,
      0 as igst,
      0 as cgst,
      0 as sgst,
      0 as other_charges,
      0 as round_off,
      re.amount as total_amount,
      NULL as bill_from,
      NULL as notes,
      re.created_at,
      re.updated_at,
      NULL as client_name,
      sp.display_name as service_period_name,
      sp.financial_year,
      sp.start_date as period_start_date,
      'Admin' as team_name,
      eh.name as expense_head_name,
      v.name as vendor_name,
      re.vendor_id,
      'recurring' as source_type
    FROM recurring_expenses re
    JOIN expense_heads eh ON re.expense_head_id = eh.id
    LEFT JOIN vendors v ON re.vendor_id = v.id
    JOIN service_periods sp
      ON sp.is_active = true
     AND sp.start_date >= TO_DATE('01-' || re.start_period, 'DD-Mon-YY')
     AND (
       re.end_period IS NULL
       OR sp.start_date <= TO_DATE('01-' || re.end_period, 'DD-Mon-YY')
     )
    ${whereClause}
  `, params);

  return result.rows.map((row) => ({
    ...row,
    amount: parseFloat(row.amount) || 0,
    projected_amount: parseFloat(row.projected_amount) || 0,
    igst: 0,
    cgst: 0,
    sgst: 0,
    other_charges: 0,
    round_off: 0,
    total_amount: parseFloat(row.total_amount) || 0,
    is_projected: true,
    date: null,
  }));
};

const sortRows = (rows) => rows.sort((a, b) => {
  const clientCmp = (a.client_name || '').localeCompare(b.client_name || '');
  if (clientCmp !== 0) return clientCmp;
  const dateA = a.period_start_date ? new Date(a.period_start_date) : new Date(a.date);
  const dateB = b.period_start_date ? new Date(b.period_start_date) : new Date(b.date);
  return dateA - dateB || new Date(a.created_at || 0) - new Date(b.created_at || 0);
});

const getCombinedRows = async (filters = {}) => {
  const sourceType = filters.sourceType || 'all';
  if (sourceType === 'recurring') {
    const [rec, adminRec] = await Promise.all([getRecurringRows(filters), getAdminRecurringRows(filters)]);
    return sortRows([...rec, ...adminRec]);
  }
  if (sourceType === 'non-recurring') {
    return sortRows(await getNonRecurringRows(filters));
  }
  if (sourceType === 'team-recurring') {
    return sortRows(await getTeamExpenseRows(filters));
  }

  const [nonRecurring, recurring, teamExpenses, adminRecurring] = await Promise.all([
    getNonRecurringRows(filters),
    getRecurringRows(filters),
    getTeamExpenseRows(filters),
    getAdminRecurringRows(filters),
  ]);

  return sortRows([...nonRecurring, ...recurring, ...teamExpenses, ...adminRecurring]);
};

router.get('/', auth, async (req, res) => {
  try {
    await ensureExpenseSchema();
    const {
      clientId, servicePeriodId, teamId, expenseHeadId,
      financialYear, isUnbilled, startDate, endDate,
      sourceType = 'all', page = 1, limit = 100
    } = req.query;

    const rows = await getCombinedRows({
      clientId,
      servicePeriodId,
      teamId,
      expenseHeadId,
      financialYear,
      isUnbilled,
      startDate,
      endDate,
      sourceType,
    });

    const pageNumber = parseInt(page, 10) || 1;
    const pageSize = parseInt(limit, 10) || 100;
    const offset = (pageNumber - 1) * pageSize;

    res.json({
      success: true,
      data: rows.slice(offset, offset + pageSize),
      pagination: {
        page: pageNumber,
        limit: pageSize,
        total: rows.length,
        totalPages: Math.ceil(rows.length / pageSize)
      }
    });
  } catch (error) {
    console.error('Get expenses error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch expenses'
    });
  }
});

router.get('/summary', auth, async (req, res) => {
  try {
    await ensureExpenseSchema();
    const rows = await getCombinedRows({
      clientId: req.query.clientId,
      servicePeriodId: req.query.servicePeriodId,
      teamId: req.query.teamId,
      expenseHeadId: req.query.expenseHeadId,
      financialYear: req.query.financialYear,
      isUnbilled: req.query.isUnbilled,
      startDate: req.query.startDate,
      endDate: req.query.endDate,
      sourceType: req.query.sourceType || 'all',
    });

    const summary = rows.reduce((acc, row) => {
      acc.total_records += 1;
      const amount = parseFloat(row.amount) || 0;
      acc.total_expense += amount;
      acc.total_with_gst += parseFloat(row.total_amount) || 0;
      acc.total_gst += (parseFloat(row.igst) || 0) + (parseFloat(row.cgst) || 0) + (parseFloat(row.sgst) || 0);
      if (row.is_projected) {
        acc.projected_expense += amount;
      } else if (row.is_unbilled) {
        acc.unbilled_expense += amount;
      } else {
        acc.billed_expense += amount;
      }
      return acc;
    }, {
      total_records: 0,
      total_expense: 0,
      total_with_gst: 0,
      projected_expense: 0,
      unbilled_expense: 0,
      billed_expense: 0,
      total_gst: 0,
    });

    res.json({
      success: true,
      data: summary
    });
  } catch (error) {
    console.error('Get expense summary error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch expense summary'
    });
  }
});

router.get('/:id', auth, async (req, res) => {
  try {
    await ensureExpenseSchema();
    const result = await pool.query(`
      SELECT e.*,
             c.name as client_name,
             sp.display_name as service_period_name,
             sp.financial_year,
             t.name as team_name,
             eh.name as expense_head_name
      FROM expenses e
      LEFT JOIN clients c ON e.client_id = c.id
      LEFT JOIN service_periods sp ON e.service_period_id = sp.id
      LEFT JOIN teams t ON e.team_id = t.id
      LEFT JOIN expense_heads eh ON e.expense_head_id = eh.id
      WHERE e.id = $1
    `, [req.params.id]);

    if (result.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Expense not found'
      });
    }

    res.json({
      success: true,
      data: result.rows[0]
    });
  } catch (error) {
    console.error('Get expense error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch expense'
    });
  }
});

router.post('/', auth, [
  body('source_type').optional().isIn(['recurring', 'non-recurring', 'team-recurring']),
  body('recurring_expense_client_id').optional({ values: 'falsy' }).isUUID().withMessage('Valid recurring expense reference is required'),
  body('team_client_allocation_id').optional({ values: 'falsy' }).isUUID().withMessage('Valid team allocation reference is required'),
  body('service_period_id').isUUID().withMessage('Valid service period is required'),
  body('expense_head_id').isUUID().withMessage('Valid expense head is required'),
  body('date').isISO8601().withMessage('Valid book date is required'),
  body('amount').isFloat({ min: 0 }).withMessage('Amount must be zero or greater'),
  body('team_id').optional({ values: 'falsy' }).isUUID(),
  body('client_id').optional({ values: 'falsy' }).isUUID(),
  body('bill_from').optional().trim(),
  body('description').optional().trim(),
  body('igst').optional().isFloat(),
  body('cgst').optional().isFloat(),
  body('sgst').optional().isFloat(),
  body('other_charges').optional().isFloat(),
  body('round_off').optional().isFloat(),
  body('total_amount').optional().isFloat(),
  body('gst_rate').optional().isFloat({ min: 0, max: 100 }),
  body('is_unbilled').optional().isBoolean(),
  body('is_entered_in_books').optional().isBoolean(),
  body('is_admin').optional().isBoolean(),
], async (req, res) => {
  const dbClient = await pool.connect();

  try {
    await ensureExpenseSchema();
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        errors: errors.array()
      });
    }

    const {
      source_type = 'non-recurring',
      recurring_expense_client_id,
      team_client_allocation_id,
      client_id,
      service_period_id,
      date,
      team_id,
      expense_head_id,
      description,
      amount,
      gst_rate,
      igst = 0,
      cgst = 0,
      sgst = 0,
      other_charges = 0,
      round_off = 0,
      total_amount,
      is_unbilled = false,
      is_entered_in_books = false,
      bill_from,
      notes,
      is_admin = false
    } = req.body;

    if (source_type === 'recurring' && !recurring_expense_client_id) {
      return res.status(400).json({
        success: false,
        message: 'Recurring expense requires a recurring expense client reference'
      });
    }

    if (source_type === 'team-recurring' && !team_client_allocation_id) {
      return res.status(400).json({
        success: false,
        message: 'Team expense requires a team client allocation reference'
      });
    }

    await dbClient.query('BEGIN');

    const refs = await dbClient.query(`
      SELECT
        c.name as client_name,
        eh.name as expense_head_name,
        sp.display_name as service_period_name
      FROM service_periods sp
      LEFT JOIN clients c ON c.id = $1
      JOIN expense_heads eh ON eh.id = $2
      WHERE sp.id = $3
    `, [client_id || null, expense_head_id, service_period_id]);

    if (refs.rows.length === 0) {
      await dbClient.query('ROLLBACK');
      return res.status(400).json({
        success: false,
        message: 'Invalid service period or expense head'
      });
    }

    const ref = refs.rows[0];
    const finalTotal = total_amount !== undefined
      ? parseFloat(total_amount)
      : (parseFloat(amount) || 0) + (parseFloat(igst) || 0) + (parseFloat(cgst) || 0) + (parseFloat(sgst) || 0) + (parseFloat(other_charges) || 0) + (parseFloat(round_off) || 0);

    let existingExpense = null;

    if (source_type === 'recurring') {
      const existingResult = await dbClient.query(`
        SELECT *
        FROM expenses
        WHERE is_active = true
          AND service_period_id = $1
          AND recurring_expense_client_id = $2
          AND (
            source_type = 'recurring'
            OR (source_type IS NULL AND recurring_expense_client_id IS NOT NULL)
          )
        ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST, id DESC
        LIMIT 1
      `, [service_period_id, recurring_expense_client_id]);
      existingExpense = existingResult.rows[0] || null;
    } else if (source_type === 'team-recurring') {
      const existingResult = await dbClient.query(`
        SELECT *
        FROM expenses
        WHERE is_active = true
          AND service_period_id = $1
          AND team_client_allocation_id = $2
          AND (
            source_type = 'team-recurring'
            OR (source_type IS NULL AND team_client_allocation_id IS NOT NULL)
          )
        ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST, id DESC
        LIMIT 1
      `, [service_period_id, team_client_allocation_id]);
      existingExpense = existingResult.rows[0] || null;
    }

    if (existingExpense) {
      const result = await dbClient.query(`
        UPDATE expenses
        SET
          unique_key = $1,
          client_id = $2,
          service_period_id = $3,
          date = $4,
          is_entered_in_books = $5,
          is_unbilled = $6,
          team_id = $7,
          expense_head_id = $8,
          description = $9,
          amount = $10,
          gst_rate = $11,
          igst = $12,
          cgst = $13,
          sgst = $14,
          other_charges = $15,
          round_off = $16,
          total_amount = $17,
          bill_from = $18,
          notes = $19,
          updated_by = $20,
          updated_at = CURRENT_TIMESTAMP,
          source_type = $21,
          recurring_expense_client_id = $22,
          team_client_allocation_id = $23,
          is_admin = $24
        WHERE id = $25
        RETURNING *
      `, [
        generateUniqueKey(ref.client_name, ref.expense_head_name, ref.service_period_name),
        client_id || null,
        service_period_id,
        date,
        is_entered_in_books,
        is_unbilled,
        team_id || null,
        expense_head_id,
        description || null,
        amount,
        gst_rate || 0,
        igst || 0,
        cgst || 0,
        sgst || 0,
        other_charges || 0,
        round_off || 0,
        finalTotal,
        bill_from || null,
        notes || null,
        req.user.id,
        source_type,
        recurring_expense_client_id || null,
        team_client_allocation_id || null,
        is_admin || false,
        existingExpense.id,
      ]);

      await dbClient.query('COMMIT');
      await logAudit('expenses', existingExpense.id, 'UPDATE', existingExpense, result.rows[0], req.user.id);

      return res.json({
        success: true,
        message: 'Expense updated successfully',
        data: result.rows[0]
      });
    }

    const result = await dbClient.query(`
      INSERT INTO expenses (
        unique_key, client_id, service_period_id, date, is_entered_in_books, is_unbilled,
        team_id, expense_head_id, description, amount, gst_rate, igst, cgst, sgst,
        other_charges, round_off, total_amount, bill_from, notes, created_by, updated_by,
        source_type, recurring_expense_client_id, team_client_allocation_id, is_admin
      ) VALUES (
        $1, $2, $3, $4, $5, $6,
        $7, $8, $9, $10, $11, $12, $13, $14,
        $15, $16, $17, $18, $19, $20, $20,
        $21, $22, $23, $24
      ) RETURNING *
    `, [
      generateUniqueKey(ref.client_name, ref.expense_head_name, ref.service_period_name),
      client_id || null,
      service_period_id,
      date,
      is_entered_in_books,
      is_unbilled,
      team_id || null,
      expense_head_id,
      description || null,
      amount,
      gst_rate || 0,
      igst || 0,
      cgst || 0,
      sgst || 0,
      other_charges || 0,
      round_off || 0,
      finalTotal,
      bill_from || null,
      notes || null,
      req.user.id,
      source_type,
      recurring_expense_client_id || null,
      team_client_allocation_id || null,
      is_admin || false,
    ]);

    await dbClient.query('COMMIT');
    await logAudit('expenses', result.rows[0].id, 'CREATE', null, result.rows[0], req.user.id);

    res.status(201).json({
      success: true,
      message: 'Expense saved successfully',
      data: result.rows[0]
    });
  } catch (error) {
    await dbClient.query('ROLLBACK');
    console.error('Create expense error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to save expense'
    });
  } finally {
    dbClient.release();
  }
});

router.put('/:id', auth, [
  body('source_type').optional().isIn(['recurring', 'non-recurring', 'team-recurring']),
  body('date').optional().isISO8601(),
  body('service_period_id').optional().isUUID(),
  body('team_id').optional({ values: 'falsy' }).isUUID(),
  body('client_id').optional({ values: 'falsy' }).isUUID(),
  body('expense_head_id').optional().isUUID(),
  body('amount').optional().isFloat({ min: 0 }),
  body('gst_rate').optional().isFloat({ min: 0, max: 100 }),
  body('igst').optional().isFloat(),
  body('cgst').optional().isFloat(),
  body('sgst').optional().isFloat(),
  body('other_charges').optional().isFloat(),
  body('round_off').optional().isFloat(),
  body('total_amount').optional().isFloat(),
  body('recurring_expense_client_id').optional({ values: 'falsy' }).isUUID(),
  body('is_unbilled').optional().isBoolean(),
  body('is_entered_in_books').optional().isBoolean(),
  body('is_admin').optional().isBoolean(),
], async (req, res) => {
  const dbClient = await pool.connect();

  try {
    await ensureExpenseSchema();
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        errors: errors.array()
      });
    }

    const current = await dbClient.query('SELECT * FROM expenses WHERE id = $1', [req.params.id]);
    if (current.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Expense not found'
      });
    }

    const currentData = current.rows[0];
    const updates = { ...req.body };

    if (updates.total_amount === undefined && (
      updates.amount !== undefined ||
      updates.igst !== undefined ||
      updates.cgst !== undefined ||
      updates.sgst !== undefined ||
      updates.other_charges !== undefined ||
      updates.round_off !== undefined
    )) {
      updates.total_amount =
        parseFloat(updates.amount ?? currentData.amount ?? 0) +
        parseFloat(updates.igst ?? currentData.igst ?? 0) +
        parseFloat(updates.cgst ?? currentData.cgst ?? 0) +
        parseFloat(updates.sgst ?? currentData.sgst ?? 0) +
        parseFloat(updates.other_charges ?? currentData.other_charges ?? 0) +
        parseFloat(updates.round_off ?? currentData.round_off ?? 0);
    }

    await dbClient.query('BEGIN');

    const allowedFields = [
      'source_type', 'client_id', 'service_period_id', 'date', 'is_entered_in_books', 'is_unbilled',
      'team_id', 'expense_head_id', 'description', 'amount', 'gst_rate', 'igst', 'cgst',
      'sgst', 'other_charges', 'round_off', 'total_amount', 'bill_from', 'notes',
      'recurring_expense_client_id', 'team_client_allocation_id', 'is_admin'
    ];

    const setClauses = [];
    const values = [];
    let paramIndex = 1;

    for (const field of allowedFields) {
      if (updates[field] !== undefined) {
        setClauses.push(`${field} = $${paramIndex++}`);
        values.push(updates[field]);
      }
    }

    if (setClauses.length === 0) {
      await dbClient.query('ROLLBACK');
      return res.status(400).json({
        success: false,
        message: 'No fields to update'
      });
    }

    setClauses.push('updated_at = CURRENT_TIMESTAMP');
    setClauses.push(`updated_by = $${paramIndex++}`);
    values.push(req.user.id);
    values.push(req.params.id);

    const result = await dbClient.query(
      `UPDATE expenses SET ${setClauses.join(', ')} WHERE id = $${paramIndex} RETURNING *`,
      values
    );

    await dbClient.query('COMMIT');
    await logAudit('expenses', req.params.id, 'UPDATE', currentData, result.rows[0], req.user.id);

    res.json({
      success: true,
      message: 'Expense updated successfully',
      data: result.rows[0]
    });
  } catch (error) {
    await dbClient.query('ROLLBACK');
    console.error('Update expense error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to update expense'
    });
  } finally {
    dbClient.release();
  }
});

router.delete('/:id', auth, requireDelete, async (req, res) => {
  try {
    await ensureExpenseSchema();
    const current = await pool.query('SELECT * FROM expenses WHERE id = $1', [req.params.id]);
    if (current.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Expense not found'
      });
    }

    const currentData = current.rows[0];
    if (
      currentData.source_type === 'recurring' ||
      currentData.source_type === 'team-recurring' ||
      currentData.recurring_expense_client_id ||
      currentData.team_client_allocation_id
    ) {
      return res.status(400).json({
        success: false,
        message: 'Recurring expense entries cannot be deleted. Update the amount to 0 instead.'
      });
    }

    await pool.query('UPDATE expenses SET is_active = false, updated_by = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2', [req.user.id, req.params.id]);
    await logAudit('expenses', req.params.id, 'DELETE', current.rows[0], null, req.user.id);

    res.json({
      success: true,
      message: 'Expense deleted successfully'
    });
  } catch (error) {
    console.error('Delete expense error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to delete expense'
    });
  }
});

module.exports = router;
