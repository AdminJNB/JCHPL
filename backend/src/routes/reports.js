const express = require('express');
const router = express.Router();
const { pool } = require('../database/db');
const { auth } = require('../middleware/auth');
const { ensureTeamAllocationPeriodColumns } = require('../utils/teamAllocationPeriods');
const { cleanupStaleRecurringSourceRecords } = require('../utils/recurringSourceCleanup');

let reportsSchemaPromise = null;
let staleCleanupDone = false;
const LIVE_MONTH_START_SQL = "DATE_TRUNC('month', CURRENT_DATE)::date";
const buildSyntheticProjectedExpr = (alias = 'rr') => `(${alias}.id IS NULL AND sp.start_date >= ${LIVE_MONTH_START_SQL})`;
const buildSyntheticUnbilledExpr = (alias = 'rr') => `
  (CASE
    WHEN ${alias}.id IS NULL THEN sp.start_date < ${LIVE_MONTH_START_SQL}
    ELSE COALESCE(${alias}.is_unbilled, true)
  END)
`;
const buildRecurringClientRowVisibilityExpr = (billingAlias = 'cbr', revenueAlias = 'rr') =>
  `(${billingAlias}.is_active = true OR ${billingAlias}.end_period IS NOT NULL OR ${revenueAlias}.id IS NOT NULL)`;
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
const buildTeamAllocationAmountExpr = (allocationAlias = 'tca', teamAlias = 't', periodAlias = 'sp') =>
  `ROUND(
    COALESCE(
      ${allocationAlias}.allocation_amount,
      (${allocationAlias}.allocation_percentage / 100.0) * ${buildTeamCompensationAmountExpr(teamAlias, periodAlias)}
    ),
    2
  )`;
const buildTeamAllocationExistsExpr = (allocationAlias = 'tca') =>
  `(COALESCE(${allocationAlias}.allocation_amount, 0) > 0 OR COALESCE(${allocationAlias}.allocation_percentage, 0) > 0)`;

async function ensureReportsSchema() {
  if (!reportsSchemaPromise) {
    reportsSchemaPromise = ensureTeamAllocationPeriodColumns(pool).catch((error) => {
      reportsSchemaPromise = null;
      throw error;
    });
  }

  return reportsSchemaPromise;
}

async function runStaleCleanupOnce() {
  if (staleCleanupDone) return;
  staleCleanupDone = true;
  const dbClient = await pool.connect();
  try {
    await dbClient.query('BEGIN');
    await cleanupStaleRecurringSourceRecords(dbClient);
    await dbClient.query('COMMIT');
  } catch (err) {
    await dbClient.query('ROLLBACK').catch(() => {});
    console.error('Startup stale cleanup warning:', err.message);
  } finally {
    dbClient.release();
  }
}

const buildRecurringRevenueWhere = ({ financialYear, clientId } = {}) => {
  const params = [];
  let paramIndex = 1;
  let whereClause = `WHERE ${buildRecurringClientRowVisibilityExpr('cbr', 'rr')}`;

  if (financialYear) {
    params.push(financialYear);
    whereClause += ` AND sp.financial_year = $${paramIndex++}`;
  }
  if (clientId) {
    params.push(clientId);
    whereClause += ` AND c.id = $${paramIndex++}`;
  }

  return { whereClause, params };
};

const getRecurringRevenueStats = async ({ financialYear, clientId } = {}) => {
  const { whereClause, params } = buildRecurringRevenueWhere({ financialYear, clientId });
  const result = await pool.query(`
    SELECT
      COUNT(*) as revenue_count,
      COALESCE(SUM(COALESCE(rr.revenue_amount, cbr.amount)), 0) as total_revenue,
      COALESCE(SUM(CASE WHEN ${buildSyntheticUnbilledExpr('rr')} = false THEN COALESCE(rr.revenue_amount, cbr.amount) ELSE 0 END), 0) as billed_revenue,
      COALESCE(SUM(CASE WHEN ${buildSyntheticUnbilledExpr('rr')} = true THEN COALESCE(rr.revenue_amount, cbr.amount) ELSE 0 END), 0) as unbilled_revenue
    FROM client_billing_rows cbr
    JOIN clients c ON cbr.client_id = c.id
    JOIN service_periods sp
      ON sp.is_active = true
     AND sp.start_date >= TO_DATE('01-' || cbr.start_period, 'DD-Mon-YY')
     AND (
       cbr.end_period IS NULL
       OR sp.start_date <= TO_DATE('01-' || cbr.end_period, 'DD-Mon-YY')
     )
    LEFT JOIN LATERAL (
      SELECT r.*
      FROM revenues r
      WHERE r.is_active = true
        AND r.source_type = 'recurring'
        AND r.client_billing_row_id = cbr.id
        AND r.service_period_id = sp.id
      ORDER BY r.updated_at DESC NULLS LAST, r.created_at DESC NULLS LAST
      LIMIT 1
    ) rr ON true
    ${whereClause}
  `, params);

  return result.rows[0];
};

router.use(async (_req, _res, next) => {
  try {
    await ensureReportsSchema();
    runStaleCleanupOnce().catch(() => {});
    next();
  } catch (error) {
    next(error);
  }
});

const getNonRecurringRevenueStats = async ({ financialYear, clientId } = {}) => {
  let fyCondition = '';
  const params = [];
  let paramIndex = 1;

  if (financialYear) {
    params.push(financialYear);
    fyCondition += ` AND sp.financial_year = $${paramIndex++}`;
  }
  if (clientId) {
    params.push(clientId);
    fyCondition += ` AND r.client_id = $${paramIndex++}`;
  }

  const result = await pool.query(`
    SELECT
      COALESCE(SUM(r.revenue_amount), 0) as total_revenue,
      COALESCE(SUM(CASE WHEN r.is_unbilled = false THEN r.revenue_amount ELSE 0 END), 0) as billed_revenue,
      COALESCE(SUM(CASE WHEN r.is_unbilled = true THEN r.revenue_amount ELSE 0 END), 0) as unbilled_revenue,
      COUNT(*) as revenue_count
    FROM revenues r
    LEFT JOIN service_periods sp ON r.service_period_id = sp.id
    WHERE r.is_active = true
      AND COALESCE(r.source_type, 'non-recurring') = 'non-recurring'
      ${fyCondition}
  `, params);

  return result.rows[0];
};

const buildRecurringExpenseWhere = ({ financialYear, clientId } = {}) => {
  const params = [];
  let paramIndex = 1;
  let whereClause = 'WHERE (re.is_active = true OR re.end_period IS NOT NULL OR e.id IS NOT NULL)';

  if (financialYear) {
    params.push(financialYear);
    whereClause += ` AND sp.financial_year = $${paramIndex++}`;
  }
  if (clientId) {
    params.push(clientId);
    whereClause += ` AND rec.client_id = $${paramIndex++}`;
  }

  return { whereClause, params };
};

const getTeamRecurringExpenseStats = async ({ financialYear, clientId } = {}) => {
  const params = [];
  let paramIndex = 1;
  let whereClause = `WHERE t.is_reviewer = true
    AND t.start_period IS NOT NULL
    AND t.amount IS NOT NULL
    AND (${buildTeamAllocationExistsExpr('tca')} OR e.id IS NOT NULL)
    AND (
      t.is_active = true
      OR COALESCE(tca.end_period, t.end_period) IS NOT NULL
      OR e.id IS NOT NULL
    )`;
  if (financialYear) { params.push(financialYear); whereClause += ` AND sp.financial_year = $${paramIndex++}`; }
  if (clientId) { params.push(clientId); whereClause += ` AND tca.client_id = $${paramIndex++}`; }
  const result = await pool.query(`
    SELECT
      COUNT(*) as expense_count,
      COALESCE(SUM(COALESCE(e.amount, ${buildTeamAllocationAmountExpr('tca', 't', 'sp')})), 0) as total_expense,
      COALESCE(SUM(CASE WHEN ${buildSyntheticUnbilledExpr('e')} = false THEN e.amount ELSE 0 END), 0) as billed_expense,
      COALESCE(SUM(CASE WHEN ${buildSyntheticUnbilledExpr('e')} = true THEN COALESCE(e.amount, ${buildTeamAllocationAmountExpr('tca', 't', 'sp')}) ELSE 0 END), 0) as unbilled_expense
    FROM teams t
    JOIN team_client_allocations tca ON tca.team_id = t.id
    JOIN service_periods sp
      ON sp.is_active = true
     AND sp.start_date >= TO_DATE('01-' || COALESCE(tca.start_period, t.start_period), 'DD-Mon-YY')
     AND (
       COALESCE(tca.end_period, t.end_period) IS NULL
       OR sp.start_date <= TO_DATE('01-' || COALESCE(tca.end_period, t.end_period), 'DD-Mon-YY')
     )
    LEFT JOIN expenses e
      ON e.is_active = true
     AND e.source_type = 'team-recurring'
     AND e.team_client_allocation_id = tca.id
     AND e.service_period_id = sp.id
    ${whereClause}
  `, params);
  return result.rows[0];
};

const getRecurringExpenseStats = async ({ financialYear, clientId } = {}) => {
  const { whereClause, params } = buildRecurringExpenseWhere({ financialYear, clientId });
  const result = await pool.query(`
    SELECT
      COUNT(*) as expense_count,
      COALESCE(SUM(COALESCE(e.amount, rec.amount)), 0) as total_expense,
      COALESCE(SUM(CASE WHEN ${buildSyntheticUnbilledExpr('e')} = false THEN COALESCE(e.amount, rec.amount) ELSE 0 END), 0) as billed_expense,
      COALESCE(SUM(CASE WHEN ${buildSyntheticUnbilledExpr('e')} = true THEN COALESCE(e.amount, rec.amount) ELSE 0 END), 0) as unbilled_expense
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
    LEFT JOIN expenses e
      ON e.is_active = true
     AND e.source_type = 'recurring'
     AND e.recurring_expense_client_id = rec.id
     AND e.service_period_id = sp.id
    ${whereClause}
  `, params);

  return result.rows[0];
};

const getNonRecurringExpenseStats = async ({ financialYear, clientId } = {}) => {
  let whereClause = `WHERE e.is_active = true AND COALESCE(e.source_type, 'non-recurring') = 'non-recurring'`;
  const params = [];
  let paramIndex = 1;

  if (financialYear) {
    params.push(financialYear);
    whereClause += ` AND sp.financial_year = $${paramIndex++}`;
  }
  if (clientId) {
    params.push(clientId);
    whereClause += ` AND e.client_id = $${paramIndex++}`;
  }

  const result = await pool.query(`
    SELECT
      COALESCE(SUM(e.amount), 0) as total_expense,
      COALESCE(SUM(CASE WHEN e.is_unbilled = false THEN e.amount ELSE 0 END), 0) as billed_expense,
      COALESCE(SUM(CASE WHEN e.is_unbilled = true THEN e.amount ELSE 0 END), 0) as unbilled_expense,
      COUNT(*) as expense_count
    FROM expenses e
    LEFT JOIN service_periods sp ON e.service_period_id = sp.id
    ${whereClause}
  `, params);

  return result.rows[0];
};

const getAdminRecurringExpenseStats = async ({ financialYear } = {}) => {
  const params = [];
  let paramIndex = 1;
  let whereClause = 'WHERE (re.is_active = true OR re.end_period IS NOT NULL) AND re.is_admin = true';
  if (financialYear) { params.push(financialYear); whereClause += ` AND sp.financial_year = $${paramIndex++}`; }
  const result = await pool.query(`
    SELECT
      COUNT(*) as expense_count,
      COALESCE(SUM(re.amount), 0) as total_expense,
      0 as billed_expense,
      0 as unbilled_expense
    FROM recurring_expenses re
    JOIN service_periods sp
      ON sp.is_active = true
     AND sp.start_date >= TO_DATE('01-' || re.start_period, 'DD-Mon-YY')
     AND (
       re.end_period IS NULL
       OR sp.start_date <= TO_DATE('01-' || re.end_period, 'DD-Mon-YY')
     )
    ${whereClause}
  `, params);
  return result.rows[0];
};

// Dashboard KPIs
router.get('/dashboard', auth, async (req, res) => {
  try {
    const { financialYear, sourceType = 'all' } = req.query;

    let revenue;
    if (sourceType === 'recurring') {
      revenue = await getRecurringRevenueStats({ financialYear });
    } else if (sourceType === 'non-recurring') {
      revenue = await getNonRecurringRevenueStats({ financialYear });
    } else {
      const [recurringRevenue, nonRecurringRevenue] = await Promise.all([
        getRecurringRevenueStats({ financialYear }),
        getNonRecurringRevenueStats({ financialYear }),
      ]);
      revenue = {
        total_revenue: (parseFloat(recurringRevenue.total_revenue) || 0) + (parseFloat(nonRecurringRevenue.total_revenue) || 0),
        billed_revenue: (parseFloat(recurringRevenue.billed_revenue) || 0) + (parseFloat(nonRecurringRevenue.billed_revenue) || 0),
        unbilled_revenue: (parseFloat(recurringRevenue.unbilled_revenue) || 0) + (parseFloat(nonRecurringRevenue.unbilled_revenue) || 0),
        revenue_count: (parseInt(recurringRevenue.revenue_count, 10) || 0) + (parseInt(nonRecurringRevenue.revenue_count, 10) || 0),
      };
    }

    const [recurringExpense, nonRecurringExpense, teamRecurringExpense, adminRecurringExpense] = await Promise.all([
      getRecurringExpenseStats({ financialYear }),
      getNonRecurringExpenseStats({ financialYear }),
      getTeamRecurringExpenseStats({ financialYear }),
      getAdminRecurringExpenseStats({ financialYear }),
    ]);
    
    const clientStats = await pool.query(`
      SELECT COUNT(DISTINCT c.id) as total_clients
      FROM clients c
      WHERE c.is_active = true
    `);
    
    const expense = {
      total_expense: (parseFloat(recurringExpense.total_expense) || 0) + (parseFloat(nonRecurringExpense.total_expense) || 0) + (parseFloat(teamRecurringExpense.total_expense) || 0) + (parseFloat(adminRecurringExpense.total_expense) || 0),
      billed_expense: (parseFloat(recurringExpense.billed_expense) || 0) + (parseFloat(nonRecurringExpense.billed_expense) || 0) + (parseFloat(teamRecurringExpense.billed_expense) || 0) + (parseFloat(adminRecurringExpense.billed_expense) || 0),
      unbilled_expense: (parseFloat(recurringExpense.unbilled_expense) || 0) + (parseFloat(nonRecurringExpense.unbilled_expense) || 0) + (parseFloat(teamRecurringExpense.unbilled_expense) || 0) + (parseFloat(adminRecurringExpense.unbilled_expense) || 0),
      expense_count: (parseInt(recurringExpense.expense_count, 10) || 0) + (parseInt(nonRecurringExpense.expense_count, 10) || 0) + (parseInt(teamRecurringExpense.expense_count, 10) || 0) + (parseInt(adminRecurringExpense.expense_count, 10) || 0),
    };
    
    res.json({
      success: true,
      data: {
        totalRevenue: parseFloat(revenue.total_revenue) || 0,
        billedRevenue: parseFloat(revenue.billed_revenue) || 0,
        unbilledRevenue: parseFloat(revenue.unbilled_revenue) || 0,
        totalExpense: parseFloat(expense.total_expense) || 0,
        billedExpense: parseFloat(expense.billed_expense) || 0,
        unbilledExpense: parseFloat(expense.unbilled_expense) || 0,
        netProfit: (parseFloat(revenue.total_revenue) || 0) - (parseFloat(expense.total_expense) || 0),
        totalClients: parseInt(clientStats.rows[0].total_clients) || 0,
        revenueCount: parseInt(revenue.revenue_count) || 0,
        expenseCount: parseInt(expense.expense_count) || 0
      }
    });
  } catch (error) {
    console.error('Dashboard error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch dashboard data'
    });
  }
});

// Revenue report - client wise
router.get('/revenue/client-wise', auth, async (req, res) => {
  try {
    const { financialYear, reviewerId, billingNameId, category } = req.query;
    
    let whereClause = 'WHERE r.is_active = true';
    const params = [];
    let paramIndex = 1;
    
    if (financialYear) {
      params.push(financialYear);
      whereClause += ` AND sp.financial_year = $${paramIndex++}`;
    }
    if (reviewerId) {
      params.push(reviewerId);
      whereClause += ` AND r.reviewer_id = $${paramIndex++}`;
    }
    if (billingNameId) {
      params.push(billingNameId);
      whereClause += ` AND r.billing_name_id = $${paramIndex++}`;
    }
    if (category === 'billed') {
      whereClause += ' AND r.is_unbilled = false';
    } else if (category === 'unbilled') {
      whereClause += ' AND r.is_unbilled = true';
    }
    
    const result = await pool.query(`
      SELECT 
        c.id as client_id,
        c.name as client_name,
        COUNT(*) as transaction_count,
        SUM(r.revenue_amount) as total_revenue,
        SUM(r.total_amount) as total_with_gst,
        SUM(CASE WHEN r.is_unbilled = false THEN r.revenue_amount ELSE 0 END) as billed,
        SUM(CASE WHEN r.is_unbilled = true THEN r.revenue_amount ELSE 0 END) as unbilled
      FROM revenues r
      JOIN clients c ON r.client_id = c.id
      LEFT JOIN service_periods sp ON r.service_period_id = sp.id
      ${whereClause}
      GROUP BY c.id, c.name
      ORDER BY c.name
    `, params);
    
    res.json({
      success: true,
      data: result.rows
    });
  } catch (error) {
    console.error('Client-wise revenue error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch client-wise revenue report'
    });
  }
});

// Revenue report - reviewer wise
router.get('/revenue/reviewer-wise', auth, async (req, res) => {
  try {
    const { financialYear, clientId, category } = req.query;
    
    let whereClause = 'WHERE r.is_active = true AND r.reviewer_id IS NOT NULL';
    const params = [];
    let paramIndex = 1;
    
    if (financialYear) {
      params.push(financialYear);
      whereClause += ` AND sp.financial_year = $${paramIndex++}`;
    }
    if (clientId) {
      params.push(clientId);
      whereClause += ` AND r.client_id = $${paramIndex++}`;
    }
    if (category === 'billed') {
      whereClause += ' AND r.is_unbilled = false';
    } else if (category === 'unbilled') {
      whereClause += ' AND r.is_unbilled = true';
    }
    
    const result = await pool.query(`
      SELECT 
        t.id as reviewer_id,
        t.name as reviewer_name,
        COUNT(*) as transaction_count,
        SUM(r.revenue_amount) as total_revenue,
        SUM(r.total_amount) as total_with_gst,
        SUM(CASE WHEN r.is_unbilled = false THEN r.revenue_amount ELSE 0 END) as billed,
        SUM(CASE WHEN r.is_unbilled = true THEN r.revenue_amount ELSE 0 END) as unbilled
      FROM revenues r
      JOIN teams t ON r.reviewer_id = t.id
      LEFT JOIN service_periods sp ON r.service_period_id = sp.id
      ${whereClause}
      GROUP BY t.id, t.name
      ORDER BY t.name
    `, params);
    
    res.json({
      success: true,
      data: result.rows
    });
  } catch (error) {
    console.error('Reviewer-wise revenue error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch reviewer-wise revenue report'
    });
  }
});

// Revenue report - billing wise
router.get('/revenue/billing-wise', auth, async (req, res) => {
  try {
    const { financialYear, clientId, category } = req.query;
    
    let whereClause = 'WHERE r.is_active = true AND r.billing_name_id IS NOT NULL';
    const params = [];
    let paramIndex = 1;
    
    if (financialYear) {
      params.push(financialYear);
      whereClause += ` AND sp.financial_year = $${paramIndex++}`;
    }
    if (clientId) {
      params.push(clientId);
      whereClause += ` AND r.client_id = $${paramIndex++}`;
    }
    if (category === 'billed') {
      whereClause += ' AND r.is_unbilled = false';
    } else if (category === 'unbilled') {
      whereClause += ' AND r.is_unbilled = true';
    }
    
    const result = await pool.query(`
      SELECT 
        bn.id as billing_name_id,
        bn.name as billing_name,
        bn.gstin,
        COUNT(*) as transaction_count,
        SUM(r.revenue_amount) as total_revenue,
        SUM(r.total_amount) as total_with_gst,
        SUM(CASE WHEN r.is_unbilled = false THEN r.revenue_amount ELSE 0 END) as billed,
        SUM(CASE WHEN r.is_unbilled = true THEN r.revenue_amount ELSE 0 END) as unbilled
      FROM revenues r
      JOIN billing_names bn ON r.billing_name_id = bn.id
      LEFT JOIN service_periods sp ON r.service_period_id = sp.id
      ${whereClause}
      GROUP BY bn.id, bn.name, bn.gstin
      ORDER BY bn.name
    `, params);
    
    res.json({
      success: true,
      data: result.rows
    });
  } catch (error) {
    console.error('Billing-wise revenue error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch billing-wise revenue report'
    });
  }
});

// Expense report - client wise
router.get('/expense/client-wise', auth, async (req, res) => {
  try {
    const { financialYear, reviewerId, category } = req.query;
    
    let whereClause = 'WHERE e.is_active = true';
    const params = [];
    let paramIndex = 1;
    
    if (financialYear) {
      params.push(financialYear);
      whereClause += ` AND sp.financial_year = $${paramIndex++}`;
    }
    if (reviewerId) {
      params.push(reviewerId);
      whereClause += ` AND e.reviewer_id = $${paramIndex++}`;
    }
    if (category === 'billed') {
      whereClause += ' AND e.is_unbilled = false';
    } else if (category === 'unbilled') {
      whereClause += ' AND e.is_unbilled = true';
    }
    
    const result = await pool.query(`
      SELECT 
        c.id as client_id,
        c.name as client_name,
        COUNT(*) as transaction_count,
        SUM(e.amount) as total_expense,
        SUM(e.total_amount) as total_with_gst,
        SUM(CASE WHEN e.is_unbilled = false THEN e.amount ELSE 0 END) as billed,
        SUM(CASE WHEN e.is_unbilled = true THEN e.amount ELSE 0 END) as unbilled
      FROM expenses e
      JOIN clients c ON e.client_id = c.id
      LEFT JOIN service_periods sp ON e.service_period_id = sp.id
      ${whereClause}
      GROUP BY c.id, c.name
      ORDER BY c.name
    `, params);
    
    res.json({
      success: true,
      data: result.rows
    });
  } catch (error) {
    console.error('Client-wise expense error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch client-wise expense report'
    });
  }
});

// Monthly trend report
router.get('/trend/monthly', auth, async (req, res) => {
  try {
    const { financialYear, clientId, billingNameId, reviewerId, sourceType = 'all' } = req.query;

    let revenueRows = [];
    if (sourceType !== 'recurring') {
      let whereClause = `WHERE r.is_active = true
        AND COALESCE(r.source_type, 'non-recurring') = 'non-recurring'`;
      const revenueParams = [];
      let paramIndex = 1;

      if (financialYear) { revenueParams.push(financialYear); whereClause += ` AND sp.financial_year = $${paramIndex++}`; }
      if (clientId)      { revenueParams.push(clientId);      whereClause += ` AND r.client_id = $${paramIndex++}`; }
      if (billingNameId) { revenueParams.push(billingNameId); whereClause += ` AND r.billing_name_id = $${paramIndex++}`; }
      if (reviewerId)    { revenueParams.push(reviewerId);    whereClause += ` AND r.reviewer_id = $${paramIndex++}`; }

      const revenueResult = await pool.query(`
        SELECT
          sp.display_name as period,
          sp.start_date,
          SUM(r.revenue_amount) as revenue
        FROM revenues r
        JOIN service_periods sp ON r.service_period_id = sp.id
        ${whereClause}
        GROUP BY sp.id, sp.display_name, sp.start_date
        ORDER BY sp.start_date
      `, revenueParams);

      revenueRows = revenueResult.rows;
    }

    if (sourceType !== 'non-recurring' && !billingNameId) {
      const { whereClause: recurringRevenueBaseWhereClause, params } = buildRecurringRevenueWhere({ financialYear, clientId });
      let whereClause = recurringRevenueBaseWhereClause;
      if (reviewerId) {
        params.push(reviewerId);
        whereClause += ` AND COALESCE(rr.reviewer_id, cbr.reviewer_id) = $${params.length}`;
      }
      const recurringResult = await pool.query(`
        SELECT
          sp.display_name as period,
          sp.start_date,
          SUM(COALESCE(rr.revenue_amount, cbr.amount)) as revenue
        FROM client_billing_rows cbr
        JOIN clients c ON cbr.client_id = c.id
        JOIN service_periods sp
          ON sp.is_active = true
         AND sp.start_date >= TO_DATE('01-' || cbr.start_period, 'DD-Mon-YY')
         AND (
           cbr.end_period IS NULL
           OR sp.start_date <= TO_DATE('01-' || cbr.end_period, 'DD-Mon-YY')
         )
        LEFT JOIN LATERAL (
          SELECT r.*
          FROM revenues r
          WHERE r.is_active = true
            AND r.source_type = 'recurring'
            AND r.client_billing_row_id = cbr.id
            AND r.service_period_id = sp.id
          ORDER BY r.updated_at DESC NULLS LAST, r.created_at DESC NULLS LAST
          LIMIT 1
        ) rr ON true
        ${whereClause}
        GROUP BY sp.id, sp.display_name, sp.start_date
        ORDER BY sp.start_date
      `, params);

      revenueRows = [...revenueRows, ...recurringResult.rows];
    }

    const expenseMap = {};

    const nonRecurringExpenseParams = [];
    let nonRecurringExpenseWhereClause = `WHERE e.is_active = true AND COALESCE(e.source_type, 'non-recurring') = 'non-recurring'`;
    let nonRecurringExpenseParamIndex = 1;

    if (financialYear) { nonRecurringExpenseParams.push(financialYear); nonRecurringExpenseWhereClause += ` AND sp.financial_year = $${nonRecurringExpenseParamIndex++}`; }
    if (clientId)      { nonRecurringExpenseParams.push(clientId);      nonRecurringExpenseWhereClause += ` AND e.client_id = $${nonRecurringExpenseParamIndex++}`; }
    if (reviewerId)    { nonRecurringExpenseParams.push(reviewerId);    nonRecurringExpenseWhereClause += ` AND e.team_id = $${nonRecurringExpenseParamIndex++}`; }

    const nonRecurringExpenseResult = await pool.query(`
      SELECT 
        sp.display_name as period,
        sp.start_date,
        SUM(e.amount) as expense
      FROM expenses e
      JOIN service_periods sp ON e.service_period_id = sp.id
      ${nonRecurringExpenseWhereClause}
      GROUP BY sp.id, sp.display_name, sp.start_date
      ORDER BY sp.start_date
    `, nonRecurringExpenseParams);

    nonRecurringExpenseResult.rows.forEach((row) => {
      if (!expenseMap[row.period]) {
        expenseMap[row.period] = {
          period: row.period,
          expense: 0,
          start_date: row.start_date
        };
      }
      expenseMap[row.period].expense += parseFloat(row.expense) || 0;
    });

    const { whereClause: recurringExpenseBaseWhereClause, params: recurringExpenseParams } = buildRecurringExpenseWhere({ financialYear, clientId });
    let recurringExpenseWhereClause = recurringExpenseBaseWhereClause;
    if (reviewerId) {
      recurringExpenseParams.push(reviewerId);
      recurringExpenseWhereClause += ` AND COALESCE(e.team_id, ret.team_id) = $${recurringExpenseParams.length}`;
    }
    const recurringExpenseResult = await pool.query(`
      SELECT
        sp.display_name as period,
        sp.start_date,
        SUM(COALESCE(e.amount, rec.amount)) as expense
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
      LEFT JOIN expenses e
        ON e.is_active = true
       AND e.source_type = 'recurring'
       AND e.recurring_expense_client_id = rec.id
       AND e.service_period_id = sp.id
      ${recurringExpenseWhereClause}
      GROUP BY sp.id, sp.display_name, sp.start_date
      ORDER BY sp.start_date
    `, recurringExpenseParams);

    recurringExpenseResult.rows.forEach((row) => {
      if (!expenseMap[row.period]) {
        expenseMap[row.period] = {
          period: row.period,
          expense: 0,
          start_date: row.start_date
        };
      }
      expenseMap[row.period].expense += parseFloat(row.expense) || 0;
    });

    // Team-recurring expenses
    const teamExpenseParams = [];
    let teamExpenseWhereStart = `WHERE t.is_reviewer = true
      AND t.start_period IS NOT NULL
      AND t.amount IS NOT NULL
      AND (${buildTeamAllocationExistsExpr('tca')} OR e.id IS NOT NULL)
      AND (
        t.is_active = true
        OR COALESCE(tca.end_period, t.end_period) IS NOT NULL
        OR e.id IS NOT NULL
      )`;
    let teamExpPI = 1;
    if (financialYear) { teamExpenseParams.push(financialYear); teamExpenseWhereStart += ` AND sp.financial_year = $${teamExpPI++}`; }
    if (clientId)      { teamExpenseParams.push(clientId);      teamExpenseWhereStart += ` AND tca.client_id = $${teamExpPI++}`; }
    if (reviewerId)    { teamExpenseParams.push(reviewerId);    teamExpenseWhereStart += ` AND t.id = $${teamExpPI++}`; }

    const teamRecurringExpenseResult = await pool.query(`
      SELECT
        sp.display_name as period,
        sp.start_date,
        SUM(COALESCE(e.amount, ${buildTeamAllocationAmountExpr('tca', 't', 'sp')})) as expense
      FROM teams t
      JOIN team_client_allocations tca ON tca.team_id = t.id
      JOIN service_periods sp
        ON sp.is_active = true
       AND sp.start_date >= TO_DATE('01-' || COALESCE(tca.start_period, t.start_period), 'DD-Mon-YY')
       AND (
         COALESCE(tca.end_period, t.end_period) IS NULL
         OR sp.start_date <= TO_DATE('01-' || COALESCE(tca.end_period, t.end_period), 'DD-Mon-YY')
       )
      LEFT JOIN expenses e
        ON e.is_active = true
       AND e.source_type = 'team-recurring'
       AND e.team_client_allocation_id = tca.id
       AND e.service_period_id = sp.id
      ${teamExpenseWhereStart}
      GROUP BY sp.id, sp.display_name, sp.start_date
      ORDER BY sp.start_date
    `, teamExpenseParams);

    teamRecurringExpenseResult.rows.forEach((row) => {
      if (!expenseMap[row.period]) {
        expenseMap[row.period] = {
          period: row.period,
          expense: 0,
          start_date: row.start_date
        };
      }
      expenseMap[row.period].expense += parseFloat(row.expense) || 0;
    });
    
    // Merge revenue and expense data
    const mergedData = {};
    
    revenueRows.forEach(row => {
      if (!mergedData[row.period]) {
        mergedData[row.period] = {
          period: row.period,
          revenue: 0,
          expense: 0,
          start_date: row.start_date
        };
      }
      mergedData[row.period].revenue += parseFloat(row.revenue) || 0;
    });
    
    Object.values(expenseMap).forEach((row) => {
      if (mergedData[row.period]) {
        mergedData[row.period].expense = parseFloat(row.expense) || 0;
      } else {
        mergedData[row.period] = {
          period: row.period,
          revenue: 0,
          expense: parseFloat(row.expense) || 0,
          start_date: row.start_date
        };
      }
    });
    
    // Calculate profit for each period
    Object.values(mergedData).forEach(item => {
      item.profit = item.revenue - item.expense;
    });
    
    res.json({
      success: true,
      data: Object.values(mergedData).sort((a, b) => new Date(a.start_date) - new Date(b.start_date))
    });
  } catch (error) {
    console.error('Monthly trend error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch monthly trend report'
    });
  }
});

// Profitability report - client wise
router.get('/profitability/client-wise', auth, async (req, res) => {
  try {
    const { financialYear } = req.query;
    
    let fyCondition = '';
    const params = [];
    
    if (financialYear) {
      params.push(financialYear);
      fyCondition = 'AND sp.financial_year = $1';
    }
    
    const result = await pool.query(`
      WITH revenue_data AS (
        SELECT 
          r.client_id,
          SUM(r.revenue_amount) as total_revenue
        FROM revenues r
        LEFT JOIN service_periods sp ON r.service_period_id = sp.id
        WHERE r.is_active = true ${fyCondition}
        GROUP BY r.client_id
      ),
      expense_data AS (
        SELECT 
          e.client_id,
          SUM(e.amount) as total_expense
        FROM expenses e
        LEFT JOIN service_periods sp ON e.service_period_id = sp.id
        WHERE e.is_active = true ${fyCondition}
        GROUP BY e.client_id
      )
      SELECT 
        c.id as client_id,
        c.name as client_name,
        COALESCE(rd.total_revenue, 0) as total_revenue,
        COALESCE(ed.total_expense, 0) as total_expense,
        COALESCE(rd.total_revenue, 0) - COALESCE(ed.total_expense, 0) as profit,
        CASE 
          WHEN COALESCE(rd.total_revenue, 0) > 0 
          THEN ((COALESCE(rd.total_revenue, 0) - COALESCE(ed.total_expense, 0)) / COALESCE(rd.total_revenue, 0)) * 100
          ELSE 0 
        END as margin_percentage
      FROM clients c
      LEFT JOIN revenue_data rd ON c.id = rd.client_id
      LEFT JOIN expense_data ed ON c.id = ed.client_id
      WHERE (rd.total_revenue > 0 OR ed.total_expense > 0)
      ORDER BY c.name
    `, params);
    
    res.json({
      success: true,
      data: result.rows
    });
  } catch (error) {
    console.error('Profitability report error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch profitability report'
    });
  }
});

// Overall summary report
router.get('/overall', auth, async (req, res) => {
  try {
    const { financialYear, clientId, reviewerId, billingNameId } = req.query;
    
    let revenueWhere = 'WHERE r.is_active = true';
    let expenseWhere = 'WHERE e.is_active = true';
    const params = [];
    let paramIndex = 1;
    
    if (financialYear) {
      params.push(financialYear);
      revenueWhere += ` AND sp.financial_year = $${paramIndex}`;
      expenseWhere += ` AND sp.financial_year = $${paramIndex}`;
      paramIndex++;
    }
    if (clientId) {
      params.push(clientId);
      revenueWhere += ` AND r.client_id = $${paramIndex}`;
      expenseWhere += ` AND e.client_id = $${paramIndex}`;
      paramIndex++;
    }
    if (reviewerId) {
      params.push(reviewerId);
      revenueWhere += ` AND r.reviewer_id = $${paramIndex}`;
      expenseWhere += ` AND e.reviewer_id = $${paramIndex}`;
      paramIndex++;
    }
    if (billingNameId) {
      params.push(billingNameId);
      revenueWhere += ` AND r.billing_name_id = $${paramIndex}`;
      paramIndex++;
    }
    
    const revenueResult = await pool.query(`
      SELECT 
        COUNT(*) as count,
        COALESCE(SUM(r.revenue_amount), 0) as total,
        COALESCE(SUM(CASE WHEN r.is_unbilled = false THEN r.revenue_amount ELSE 0 END), 0) as billed,
        COALESCE(SUM(CASE WHEN r.is_unbilled = true THEN r.revenue_amount ELSE 0 END), 0) as unbilled,
        COALESCE(SUM(r.igst + r.cgst + r.sgst), 0) as gst
      FROM revenues r
      LEFT JOIN service_periods sp ON r.service_period_id = sp.id
      ${revenueWhere}
    `, params.slice(0, revenueWhere.split('$').length - 1));
    
    const expenseParams = [];
    if (financialYear) expenseParams.push(financialYear);
    if (clientId) expenseParams.push(clientId);
    if (reviewerId) expenseParams.push(reviewerId);
    
    const expenseResult = await pool.query(`
      SELECT 
        COUNT(*) as count,
        COALESCE(SUM(e.amount), 0) as total,
        COALESCE(SUM(CASE WHEN e.is_unbilled = false THEN e.amount ELSE 0 END), 0) as billed,
        COALESCE(SUM(CASE WHEN e.is_unbilled = true THEN e.amount ELSE 0 END), 0) as unbilled,
        COALESCE(SUM(e.igst + e.cgst + e.sgst), 0) as gst
      FROM expenses e
      LEFT JOIN service_periods sp ON e.service_period_id = sp.id
      ${expenseWhere}
    `, expenseParams);
    
    const revenue = revenueResult.rows[0];
    const expense = expenseResult.rows[0];
    
    res.json({
      success: true,
      data: {
        revenue: {
          count: parseInt(revenue.count),
          total: parseFloat(revenue.total),
          billed: parseFloat(revenue.billed),
          unbilled: parseFloat(revenue.unbilled),
          gst: parseFloat(revenue.gst)
        },
        expense: {
          count: parseInt(expense.count),
          total: parseFloat(expense.total),
          billed: parseFloat(expense.billed),
          unbilled: parseFloat(expense.unbilled),
          gst: parseFloat(expense.gst)
        },
        summary: {
          netRevenue: parseFloat(revenue.total) - parseFloat(expense.total),
          netBilled: parseFloat(revenue.billed) - parseFloat(expense.billed),
          netUnbilled: parseFloat(revenue.unbilled) - parseFloat(expense.unbilled)
        }
      }
    });
  } catch (error) {
    console.error('Overall report error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch overall report'
    });
  }
});

// Drill-down report for a specific client
router.get('/drilldown/client/:clientId', auth, async (req, res) => {
  try {
    const { clientId } = req.params;
    const { financialYear } = req.query;
    
    let fyCondition = '';
    const params = [clientId];
    
    if (financialYear) {
      params.push(financialYear);
      fyCondition = 'AND sp.financial_year = $2';
    }
    
    // Get client info
    const clientInfo = await pool.query(
      'SELECT * FROM clients WHERE id = $1',
      [clientId]
    );
    
    if (clientInfo.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Client not found'
      });
    }
    
    // Get revenue breakdown by service type
    const revenueByService = await pool.query(`
      SELECT 
        st.name as service_type,
        COUNT(*) as count,
        SUM(r.revenue_amount) as total
      FROM revenues r
      JOIN service_types st ON r.service_type_id = st.id
      LEFT JOIN service_periods sp ON r.service_period_id = sp.id
      WHERE r.client_id = $1 AND r.is_active = true ${fyCondition}
      GROUP BY st.id, st.name
      ORDER BY st.name NULLS LAST
    `, params);
    
    // Get expense breakdown by expense head
    const expenseByHead = await pool.query(`
      SELECT 
        eh.name as expense_head,
        eh.is_recurring,
        COUNT(*) as count,
        SUM(e.amount) as total
      FROM expenses e
      JOIN expense_heads eh ON e.expense_head_id = eh.id
      LEFT JOIN service_periods sp ON e.service_period_id = sp.id
      WHERE e.client_id = $1 AND e.is_active = true ${fyCondition}
      GROUP BY eh.id, eh.name, eh.is_recurring
      ORDER BY eh.name
    `, params);
    
    // Get monthly breakdown
    const monthlyBreakdown = await pool.query(`
      SELECT 
        sp.display_name as period,
        COALESCE(SUM(r.revenue_amount), 0) as revenue,
        0 as expense
      FROM revenues r
      JOIN service_periods sp ON r.service_period_id = sp.id
      WHERE r.client_id = $1 AND r.is_active = true ${fyCondition}
      GROUP BY sp.id, sp.display_name, sp.start_date
      ORDER BY sp.start_date
    `, params);
    
    res.json({
      success: true,
      data: {
        client: clientInfo.rows[0],
        revenueByService: revenueByService.rows,
        expenseByHead: expenseByHead.rows,
        monthlyBreakdown: monthlyBreakdown.rows
      }
    });
  } catch (error) {
    console.error('Drilldown report error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch drilldown report'
    });
  }
});

// Head-wise: revenue by service type + expense by expense head
router.get('/summary/head-wise', auth, async (req, res) => {
  try {
    const { financialYear, clientId, billingNameId, reviewerId } = req.query;

    const revParams = [];
    let revWhere = 'WHERE r.is_active = true';
    let ri = 1;
    if (financialYear) { revParams.push(financialYear); revWhere += ` AND sp.financial_year = $${ri++}`; }
    if (clientId)      { revParams.push(clientId);      revWhere += ` AND r.client_id = $${ri++}`; }
    if (billingNameId) { revParams.push(billingNameId); revWhere += ` AND r.billing_name_id = $${ri++}`; }
    if (reviewerId)    { revParams.push(reviewerId);    revWhere += ` AND r.reviewer_id = $${ri++}`; }

    const revenueByHead = await pool.query(`
      SELECT
        COALESCE(st.name, 'Unclassified') as head_name,
        COALESCE(SUM(r.revenue_amount), 0) as total,
        COALESCE(SUM(CASE WHEN r.is_unbilled = false THEN r.revenue_amount ELSE 0 END), 0) as billed,
        COALESCE(SUM(CASE WHEN r.is_unbilled IS DISTINCT FROM false THEN r.revenue_amount ELSE 0 END), 0) as unbilled
      FROM revenues r
      LEFT JOIN service_types st ON r.service_type_id = st.id
      LEFT JOIN service_periods sp ON r.service_period_id = sp.id
      ${revWhere}
      GROUP BY st.id, st.name
      ORDER BY st.name NULLS LAST
    `, revParams);

    const expParams = [];
    let expWhere = 'WHERE e.is_active = true';
    let ei = 1;
    if (financialYear) { expParams.push(financialYear); expWhere += ` AND sp.financial_year = $${ei++}`; }
    if (clientId)      { expParams.push(clientId);      expWhere += ` AND e.client_id = $${ei++}`; }
    if (reviewerId)    { expParams.push(reviewerId);    expWhere += ` AND e.team_id = $${ei++}`; }

    const expenseByHead = await pool.query(`
      SELECT
        COALESCE(eh.name, 'Unclassified') as head_name,
        COALESCE(SUM(e.amount), 0) as total,
        COALESCE(SUM(CASE WHEN e.is_unbilled = false THEN e.amount ELSE 0 END), 0) as billed,
        COALESCE(SUM(CASE WHEN e.is_unbilled IS DISTINCT FROM false THEN e.amount ELSE 0 END), 0) as unbilled
      FROM expenses e
      LEFT JOIN expense_heads eh ON e.expense_head_id = eh.id
      LEFT JOIN service_periods sp ON e.service_period_id = sp.id
      ${expWhere}
      GROUP BY eh.id, eh.name
      ORDER BY eh.name NULLS LAST
    `, expParams);

    res.json({
      success: true,
      data: {
        revenue: revenueByHead.rows.map(r => ({
          ...r,
          total: parseFloat(r.total) || 0,
          billed: parseFloat(r.billed) || 0,
          unbilled: parseFloat(r.unbilled) || 0,
        })),
        expense: expenseByHead.rows.map(r => ({
          ...r,
          total: parseFloat(r.total) || 0,
          billed: parseFloat(r.billed) || 0,
          unbilled: parseFloat(r.unbilled) || 0,
        })),
      },
    });
  } catch (error) {
    console.error('Head-wise report error:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch head-wise report' });
  }
});

// FY-wise: revenue + expense grouped by financial year
router.get('/summary/fy-wise', auth, async (req, res) => {
  try {
    const { clientId, billingNameId, reviewerId } = req.query;

    const revParams = [];
    let revWhere = 'WHERE r.is_active = true AND sp.id IS NOT NULL';
    let ri = 1;
    if (clientId)      { revParams.push(clientId);      revWhere += ` AND r.client_id = $${ri++}`; }
    if (billingNameId) { revParams.push(billingNameId); revWhere += ` AND r.billing_name_id = $${ri++}`; }
    if (reviewerId)    { revParams.push(reviewerId);    revWhere += ` AND r.reviewer_id = $${ri++}`; }

    const revenueByFY = await pool.query(`
      SELECT
        sp.financial_year,
        COALESCE(SUM(r.revenue_amount), 0) as total,
        COALESCE(SUM(CASE WHEN r.is_unbilled = false THEN r.revenue_amount ELSE 0 END), 0) as billed,
        COALESCE(SUM(CASE WHEN r.is_unbilled IS DISTINCT FROM false THEN r.revenue_amount ELSE 0 END), 0) as unbilled
      FROM revenues r
      JOIN service_periods sp ON r.service_period_id = sp.id
      ${revWhere}
      GROUP BY sp.financial_year
      ORDER BY sp.financial_year
    `, revParams);

    const expParams = [];
    let expWhere = 'WHERE e.is_active = true AND sp.id IS NOT NULL';
    let ei = 1;
    if (clientId)   { expParams.push(clientId);   expWhere += ` AND e.client_id = $${ei++}`; }
    if (reviewerId) { expParams.push(reviewerId); expWhere += ` AND e.team_id = $${ei++}`; }

    const expenseByFY = await pool.query(`
      SELECT
        sp.financial_year,
        COALESCE(SUM(e.amount), 0) as total,
        COALESCE(SUM(CASE WHEN e.is_unbilled = false THEN e.amount ELSE 0 END), 0) as billed,
        COALESCE(SUM(CASE WHEN e.is_unbilled IS DISTINCT FROM false THEN e.amount ELSE 0 END), 0) as unbilled
      FROM expenses e
      JOIN service_periods sp ON e.service_period_id = sp.id
      ${expWhere}
      GROUP BY sp.financial_year
      ORDER BY sp.financial_year
    `, expParams);

    const fyMap = {};
    revenueByFY.rows.forEach((row) => {
      fyMap[row.financial_year] = {
        financial_year: row.financial_year,
        revenue: parseFloat(row.total) || 0,
        revenue_billed: parseFloat(row.billed) || 0,
        revenue_unbilled: parseFloat(row.unbilled) || 0,
        expense: 0,
        expense_billed: 0,
        expense_unbilled: 0,
      };
    });
    expenseByFY.rows.forEach((row) => {
      if (!fyMap[row.financial_year]) {
        fyMap[row.financial_year] = {
          financial_year: row.financial_year,
          revenue: 0, revenue_billed: 0, revenue_unbilled: 0,
          expense: 0, expense_billed: 0, expense_unbilled: 0,
        };
      }
      fyMap[row.financial_year].expense = parseFloat(row.total) || 0;
      fyMap[row.financial_year].expense_billed = parseFloat(row.billed) || 0;
      fyMap[row.financial_year].expense_unbilled = parseFloat(row.unbilled) || 0;
    });

    const rows = Object.values(fyMap)
      .map((row) => ({ ...row, net: row.revenue - row.expense }))
      .sort((a, b) => a.financial_year.localeCompare(b.financial_year));

    res.json({ success: true, data: rows });
  } catch (error) {
    console.error('FY-wise report error:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch FY-wise report' });
  }
});

// Matrix report: revenue by service type and expense by expense head, amounts per month for a FY
router.get('/matrix', auth, async (req, res) => {
  try {
    const { financialYear, clientIds, billingNameIds, reviewerIds, billingStatus } = req.query;

    if (!financialYear) {
      return res.status(400).json({ success: false, message: 'financialYear is required' });
    }

    // Parse multi-select IDs (comma-separated)
    const clientIdArr = clientIds ? clientIds.split(',').filter(Boolean) : [];
    const billingNameIdArr = billingNameIds ? billingNameIds.split(',').filter(Boolean) : [];
    const reviewerIdArr = reviewerIds ? reviewerIds.split(',').filter(Boolean) : [];
    const statusArr = billingStatus ? billingStatus.split(',').filter(Boolean) : [];
    const wantBilled = !statusArr.length || statusArr.includes('billed');
    const wantUnbilled = !statusArr.length || statusArr.includes('unbilled');
    const wantProjected = !statusArr.length || statusArr.includes('projected');

    // Get the 12 periods for this FY
    const periodsResult = await pool.query(
      `SELECT id, display_name, start_date FROM service_periods
       WHERE financial_year = $1 AND is_active = true
       ORDER BY start_date`,
      [financialYear]
    );
    const periods = periodsResult.rows;

    // Build revenue query (non-recurring only for filtered)
    const revParams = [financialYear];
    let revWhere = `WHERE r.is_active = true
      AND COALESCE(r.source_type, 'non-recurring') = 'non-recurring'
      AND sp.financial_year = $1`;
    let ri = 2;
    if (clientIdArr.length) { revParams.push(clientIdArr); revWhere += ` AND r.client_id = ANY($${ri++}::uuid[])`; }
    if (billingNameIdArr.length) { revParams.push(billingNameIdArr); revWhere += ` AND r.billing_name_id = ANY($${ri++}::uuid[])`; }
    if (reviewerIdArr.length) { revParams.push(reviewerIdArr); revWhere += ` AND r.reviewer_id = ANY($${ri++}::uuid[])`; }
    // Apply billing status filter to non-recurring revenue
    if (statusArr.length && !(wantBilled && wantUnbilled)) {
      if (wantBilled && !wantUnbilled) revWhere += ' AND r.is_unbilled = false';
      else if (wantUnbilled && !wantBilled) revWhere += ' AND r.is_unbilled = true';
    }

    let revenueRows = [];
    if (wantBilled || wantUnbilled) {
      const revenueResult = await pool.query(`
        SELECT
          COALESCE(st.name, 'Unclassified') as head_name,
          COALESCE(st.id::text, 'unclassified') as head_id,
          sp.display_name as period,
          COALESCE(SUM(r.revenue_amount), 0) as amount
        FROM revenues r
        LEFT JOIN service_types st ON r.service_type_id = st.id
        LEFT JOIN service_periods sp ON r.service_period_id = sp.id
        ${revWhere}
        GROUP BY st.id, st.name, sp.display_name, sp.start_date
        ORDER BY st.name, sp.start_date
      `, revParams);
      revenueRows = revenueResult.rows;
    }

    // Also get recurring revenue if no billing-name filter
    let recurringRevenueRows = [];
    if (!billingNameIdArr.length) {
      const recRevParams = [financialYear];
      let recRevWhere = ' AND sp.financial_year = $1';
      let rri = 2;
      if (clientIdArr.length) { recRevParams.push(clientIdArr); recRevWhere += ` AND c.id = ANY($${rri++}::uuid[])`; }
      if (reviewerIdArr.length) {
        recRevParams.push(reviewerIdArr);
        recRevWhere += ` AND COALESCE(rr.reviewer_id, cbr.reviewer_id) = ANY($${rri++}::uuid[])`;
      }

      // Build CASE for billing status filtering on recurring revenue
      const recRevAmountExpr = (() => {
        if (!statusArr.length) return 'COALESCE(rr.revenue_amount, cbr.amount)';
        const parts = [];
        if (wantBilled) parts.push('CASE WHEN rr.id IS NOT NULL AND rr.is_unbilled = false THEN COALESCE(rr.revenue_amount, cbr.amount) ELSE 0 END');
        if (wantUnbilled) parts.push(`CASE WHEN ${buildSyntheticUnbilledExpr('rr')} = true THEN COALESCE(rr.revenue_amount, cbr.amount) ELSE 0 END`);
        if (wantProjected) parts.push(`CASE WHEN ${buildSyntheticProjectedExpr('rr')} THEN cbr.amount ELSE 0 END`);
        return parts.length ? parts.join(' + ') : '0';
      })();

      const recurringResult = await pool.query(`
        SELECT
          COALESCE(st.name, 'Unclassified') as head_name,
          COALESCE(st.id::text, 'unclassified') as head_id,
          sp.display_name as period,
          COALESCE(SUM(${recRevAmountExpr}), 0) as amount
        FROM client_billing_rows cbr
        JOIN clients c ON cbr.client_id = c.id
        LEFT JOIN service_types st ON cbr.service_type_id = st.id
        JOIN service_periods sp
          ON sp.is_active = true
         AND sp.start_date >= TO_DATE('01-' || cbr.start_period, 'DD-Mon-YY')
         AND (
           cbr.end_period IS NULL
           OR sp.start_date <= TO_DATE('01-' || cbr.end_period, 'DD-Mon-YY')
         )
        LEFT JOIN LATERAL (
          SELECT r.*
          FROM revenues r
          WHERE r.is_active = true
            AND r.source_type = 'recurring'
            AND r.client_billing_row_id = cbr.id
            AND r.service_period_id = sp.id
          ORDER BY r.updated_at DESC NULLS LAST, r.created_at DESC NULLS LAST
          LIMIT 1
        ) rr ON true
        WHERE ${buildRecurringClientRowVisibilityExpr('cbr', 'rr')}
        ${recRevWhere}
        GROUP BY st.id, st.name, sp.display_name, sp.start_date
        ORDER BY st.name, sp.start_date
      `, recRevParams);
      recurringRevenueRows = recurringResult.rows;
    }

    // Build expense query (non-recurring)
    const expParams = [financialYear];
    let expWhere = `WHERE e.is_active = true
      AND COALESCE(e.source_type, 'non-recurring') = 'non-recurring'
      AND sp.financial_year = $1`;
    let ei = 2;
    if (clientIdArr.length) { expParams.push(clientIdArr); expWhere += ` AND e.client_id = ANY($${ei++}::uuid[])`; }
    if (reviewerIdArr.length) { expParams.push(reviewerIdArr); expWhere += ` AND e.team_id = ANY($${ei++}::uuid[])`; }
    // Apply billing status filter to non-recurring expense
    if (statusArr.length && !(wantBilled && wantUnbilled)) {
      if (wantBilled && !wantUnbilled) expWhere += ' AND e.is_unbilled = false';
      else if (wantUnbilled && !wantBilled) expWhere += ' AND e.is_unbilled = true';
    }

    let expenseRows = [];
    if (wantBilled || wantUnbilled) {
      const expenseResult = await pool.query(`
        SELECT
          COALESCE(eh.name, 'Unclassified') as head_name,
          COALESCE(eh.id::text, 'unclassified') as head_id,
          sp.display_name as period,
          COALESCE(SUM(e.amount), 0) as amount
        FROM expenses e
        LEFT JOIN expense_heads eh ON e.expense_head_id = eh.id
        LEFT JOIN service_periods sp ON e.service_period_id = sp.id
        ${expWhere}
        GROUP BY eh.id, eh.name, sp.display_name, sp.start_date
        ORDER BY eh.name, sp.start_date
      `, expParams);
      expenseRows = expenseResult.rows;
    }

    // Get recurring expense
    // Build CASE for billing status filtering on recurring expense
    const recExpAmountExpr = (() => {
      if (!statusArr.length) return 'COALESCE(e.amount, rec.amount)';
      const parts = [];
      if (wantBilled) parts.push('CASE WHEN e.id IS NOT NULL AND e.is_unbilled = false THEN COALESCE(e.amount, rec.amount) ELSE 0 END');
      if (wantUnbilled) parts.push(`CASE WHEN ${buildSyntheticUnbilledExpr('e')} = true THEN COALESCE(e.amount, rec.amount) ELSE 0 END`);
      if (wantProjected) parts.push(`CASE WHEN ${buildSyntheticProjectedExpr('e')} THEN rec.amount ELSE 0 END`);
      return parts.length ? parts.join(' + ') : '0';
    })();

    const recExpParams = [financialYear];
    let recExpWhere = ' AND sp.financial_year = $1';
    let rei = 2;
    if (clientIdArr.length) { recExpParams.push(clientIdArr); recExpWhere += ` AND rec.client_id = ANY($${rei++}::uuid[])`; }
    if (reviewerIdArr.length) { recExpParams.push(reviewerIdArr); recExpWhere += ` AND COALESCE(e.team_id, ret.team_id) = ANY($${rei++}::uuid[])`; }

    const recurringExpenseResult = await pool.query(`
      SELECT
        COALESCE(eh.name, 'Unclassified') as head_name,
        COALESCE(eh.id::text, 'unclassified') as head_id,
        sp.display_name as period,
        COALESCE(SUM(${recExpAmountExpr}), 0) as amount
      FROM recurring_expenses re
      JOIN recurring_expense_teams ret ON ret.recurring_expense_id = re.id
      JOIN recurring_expense_clients rec ON rec.recurring_expense_team_id = ret.id
      LEFT JOIN expense_heads eh ON re.expense_head_id = eh.id
      JOIN service_periods sp
        ON sp.is_active = true
       AND sp.start_date >= TO_DATE('01-' || COALESCE(rec.start_period, re.start_period), 'DD-Mon-YY')
       AND (
         COALESCE(rec.end_period, re.end_period) IS NULL
         OR sp.start_date <= TO_DATE('01-' || COALESCE(rec.end_period, re.end_period), 'DD-Mon-YY')
       )
      LEFT JOIN expenses e
        ON e.is_active = true
       AND e.source_type = 'recurring'
       AND e.recurring_expense_client_id = rec.id
       AND e.service_period_id = sp.id
      WHERE (re.is_active = true OR re.end_period IS NOT NULL OR e.id IS NOT NULL) AND re.is_admin IS NOT TRUE ${recExpWhere}
      GROUP BY eh.id, eh.name, sp.display_name, sp.start_date
      ORDER BY eh.name, sp.start_date
    `, recExpParams);

    // Get admin recurring expenses (no allocations, directly from recurring_expenses)
    const adminExpAmountExpr = (() => {
      if (!statusArr.length) return 're.amount';
      const parts = [];
      // Admin expenses are always projected (no actual expense rows)
      if (wantProjected) parts.push('re.amount');
      return parts.length ? parts.join(' + ') : '0';
    })();

    const adminExpParams = [financialYear];
    let adminExpWhere = ' AND sp.financial_year = $1';
    const adminResult = await pool.query(`
      SELECT
        COALESCE(eh.name, 'Unclassified') as head_name,
        COALESCE(eh.id::text, 'unclassified') as head_id,
        sp.display_name as period,
        COALESCE(SUM(${adminExpAmountExpr}), 0) as amount
      FROM recurring_expenses re
      LEFT JOIN expense_heads eh ON re.expense_head_id = eh.id
      JOIN service_periods sp
        ON sp.is_active = true
       AND sp.start_date >= TO_DATE('01-' || re.start_period, 'DD-Mon-YY')
       AND (
         re.end_period IS NULL
         OR sp.start_date <= TO_DATE('01-' || re.end_period, 'DD-Mon-YY')
       )
      WHERE (re.is_active = true OR re.end_period IS NOT NULL) AND re.is_admin = true ${adminExpWhere}
      GROUP BY eh.id, eh.name, sp.display_name, sp.start_date
      ORDER BY eh.name, sp.start_date
    `, adminExpParams);

    // Get team-recurring expenses (from team_client_allocations)
    // Build CASE for billing status filtering on team-recurring expense
    const allocExpr = buildTeamAllocationAmountExpr('tca', 't', 'sp');
    const teamExpAmountExpr = (() => {
      if (!statusArr.length) return `COALESCE(e.amount, ${allocExpr})`;
      const parts = [];
      if (wantBilled) parts.push(`CASE WHEN e.id IS NOT NULL AND e.is_unbilled = false THEN COALESCE(e.amount, ${allocExpr}) ELSE 0 END`);
      if (wantUnbilled) parts.push(`CASE WHEN ${buildSyntheticUnbilledExpr('e')} = true THEN COALESCE(e.amount, ${allocExpr}) ELSE 0 END`);
      if (wantProjected) parts.push(`CASE WHEN ${buildSyntheticProjectedExpr('e')} THEN ${allocExpr} ELSE 0 END`);
      return parts.length ? parts.join(' + ') : '0';
    })();

    const teamExpParams = [financialYear];
    let teamExpWhere = ' AND sp.financial_year = $1';
    let tei = 2;
    if (clientIdArr.length) { teamExpParams.push(clientIdArr); teamExpWhere += ` AND tca.client_id = ANY($${tei++}::uuid[])`; }
    if (reviewerIdArr.length) { teamExpParams.push(reviewerIdArr); teamExpWhere += ` AND t.id = ANY($${tei++}::uuid[])`; }

    const teamExpenseResult = await pool.query(`
      SELECT
        COALESCE(eh.name, 'Unclassified') as head_name,
        COALESCE(eh.id::text, 'unclassified') as head_id,
        sp.display_name as period,
        COALESCE(SUM(${teamExpAmountExpr}), 0) as amount
      FROM teams t
      JOIN team_client_allocations tca ON tca.team_id = t.id
      LEFT JOIN expense_heads eh ON COALESCE(tca.expense_head_id, t.expense_head_id) = eh.id
      JOIN service_periods sp
        ON sp.is_active = true
       AND sp.start_date >= TO_DATE('01-' || COALESCE(tca.start_period, t.start_period), 'DD-Mon-YY')
       AND (
         COALESCE(tca.end_period, t.end_period) IS NULL
         OR sp.start_date <= TO_DATE('01-' || COALESCE(tca.end_period, t.end_period), 'DD-Mon-YY')
       )
      LEFT JOIN expenses e
        ON e.is_active = true
       AND e.source_type = 'team-recurring'
       AND e.team_client_allocation_id = tca.id
       AND e.service_period_id = sp.id
      WHERE t.is_reviewer = true
        AND t.start_period IS NOT NULL
        AND t.amount IS NOT NULL
        AND (${buildTeamAllocationExistsExpr('tca')} OR e.id IS NOT NULL)
        AND (
          t.is_active = true
          OR COALESCE(tca.end_period, t.end_period) IS NOT NULL
          OR e.id IS NOT NULL
        ) ${teamExpWhere}
      GROUP BY eh.id, eh.name, sp.display_name, sp.start_date
      ORDER BY eh.name, sp.start_date
    `, teamExpParams);

    // Build period list
    const periodNames = periods.map(p => p.display_name);

    // Aggregate revenue into { head_name -> { period -> amount } }
    const revenueMap = {};
    const addRevRow = (row) => {
      if (!revenueMap[row.head_name]) {
        revenueMap[row.head_name] = {};
        periodNames.forEach(p => { revenueMap[row.head_name][p] = 0; });
      }
      revenueMap[row.head_name][row.period] = (revenueMap[row.head_name][row.period] || 0) + (parseFloat(row.amount) || 0);
    };
    revenueRows.forEach(addRevRow);
    recurringRevenueRows.forEach(addRevRow);

    // Aggregate expense
    const expenseMap = {};
    const addExpRow = (row) => {
      if (!expenseMap[row.head_name]) {
        expenseMap[row.head_name] = {};
        periodNames.forEach(p => { expenseMap[row.head_name][p] = 0; });
      }
      expenseMap[row.head_name][row.period] = (expenseMap[row.head_name][row.period] || 0) + (parseFloat(row.amount) || 0);
    };
    expenseRows.forEach(addExpRow);
    recurringExpenseResult.rows.forEach(addExpRow);
    teamExpenseResult.rows.forEach(addExpRow);

    // Aggregate admin expenses separately
    const adminExpenseMap = {};
    const addAdminExpRow = (row) => {
      if (!adminExpenseMap[row.head_name]) {
        adminExpenseMap[row.head_name] = {};
        periodNames.forEach(p => { adminExpenseMap[row.head_name][p] = 0; });
      }
      adminExpenseMap[row.head_name][row.period] = (adminExpenseMap[row.head_name][row.period] || 0) + (parseFloat(row.amount) || 0);
    };
    adminResult.rows.forEach(addAdminExpRow);

    // Convert to rows
    const revenueFinalRows = Object.entries(revenueMap).map(([head, months]) => {
      const total = periodNames.reduce((s, p) => s + (months[p] || 0), 0);
      return { head_name: head, months, total };
    }).sort((a, b) => b.total - a.total);

    const expenseFinalRows = Object.entries(expenseMap).map(([head, months]) => {
      const total = periodNames.reduce((s, p) => s + (months[p] || 0), 0);
      return { head_name: head, months, total };
    }).sort((a, b) => b.total - a.total);

    const adminExpenseFinalRows = Object.entries(adminExpenseMap).map(([head, months]) => {
      const total = periodNames.reduce((s, p) => s + (months[p] || 0), 0);
      return { head_name: head, months, total };
    }).sort((a, b) => b.total - a.total);

    res.json({
      success: true,
      data: {
        periods: periodNames,
        revenue: revenueFinalRows,
        expense: expenseFinalRows,
        admin_expense: adminExpenseFinalRows,
      },
    });
  } catch (error) {
    console.error('Matrix report error:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch matrix report' });
  }
});

// Dashboard variance: projected (recurring) vs actuals
router.get('/variance', auth, async (req, res) => {
  try {
    const { financialYear, clientIds, billingNameIds, reviewerIds } = req.query;
    if (!financialYear) {
      return res.status(400).json({ success: false, message: 'financialYear is required' });
    }

    const clientIdArr = clientIds ? clientIds.split(',').filter(Boolean) : [];
    const billingNameIdArr = billingNameIds ? billingNameIds.split(',').filter(Boolean) : [];
    const reviewerIdArr = reviewerIds ? reviewerIds.split(',').filter(Boolean) : [];

    const periodsResult = await pool.query(
      `SELECT id, display_name, start_date FROM service_periods
       WHERE financial_year = $1 AND is_active = true ORDER BY start_date`,
      [financialYear]
    );
    const periods = periodsResult.rows;
    const periodIndex = new Map(periods.map((period, index) => [period.display_name, index]));

    const revenueParams = [financialYear];
    let revenueWhere = `WHERE ${buildRecurringClientRowVisibilityExpr('cbr', 'rr')} AND sp.financial_year = $1`;
    let revenueParamIndex = 2;
    if (clientIdArr.length) {
      revenueParams.push(clientIdArr);
      revenueWhere += ` AND c.id = ANY($${revenueParamIndex++}::uuid[])`;
    }
    if (billingNameIdArr.length) {
      revenueParams.push(billingNameIdArr);
      revenueWhere += ` AND rr.billing_name_id = ANY($${revenueParamIndex++}::uuid[])`;
    }
    if (reviewerIdArr.length) {
      revenueParams.push(reviewerIdArr);
      revenueWhere += ` AND COALESCE(rr.reviewer_id, cbr.reviewer_id) = ANY($${revenueParamIndex++}::uuid[])`;
    }

    const revenueVarianceResult = await pool.query(`
      SELECT
        sp.display_name as period,
        COALESCE(SUM(cbr.amount), 0) as projected_amount,
        COALESCE(SUM(CASE WHEN rr.is_unbilled = false THEN COALESCE(rr.revenue_amount, cbr.amount) ELSE 0 END), 0) as billed_amount,
        COALESCE(SUM(CASE WHEN rr.is_unbilled = true THEN COALESCE(rr.revenue_amount, cbr.amount) ELSE 0 END), 0) as unbilled_amount
      FROM client_billing_rows cbr
      JOIN clients c ON cbr.client_id = c.id AND c.is_active = true
      JOIN service_periods sp
        ON sp.is_active = true
       AND sp.start_date >= TO_DATE('01-' || cbr.start_period, 'DD-Mon-YY')
       AND (
         cbr.end_period IS NULL
         OR sp.start_date <= TO_DATE('01-' || cbr.end_period, 'DD-Mon-YY')
       )
      JOIN LATERAL (
        SELECT r.*
        FROM revenues r
        WHERE r.is_active = true
          AND r.source_type = 'recurring'
          AND r.client_billing_row_id = cbr.id
          AND r.service_period_id = sp.id
        ORDER BY r.updated_at DESC NULLS LAST, r.created_at DESC NULLS LAST
        LIMIT 1
      ) rr ON true
      ${revenueWhere}
      GROUP BY sp.display_name, sp.start_date
      ORDER BY sp.start_date
    `, revenueParams);

    const recurringExpenseParams = [financialYear];
    let recurringExpenseWhere = 'WHERE (re.is_active = true OR re.end_period IS NOT NULL) AND sp.financial_year = $1';
    let recurringExpenseParamIndex = 2;
    if (clientIdArr.length) {
      recurringExpenseParams.push(clientIdArr);
      recurringExpenseWhere += ` AND rec.client_id = ANY($${recurringExpenseParamIndex++}::uuid[])`;
    }
    if (reviewerIdArr.length) {
      recurringExpenseParams.push(reviewerIdArr);
      recurringExpenseWhere += ` AND COALESCE(e.team_id, ret.team_id) = ANY($${recurringExpenseParamIndex++}::uuid[])`;
    }

    const recurringExpenseVarianceResult = await pool.query(`
      SELECT
        sp.display_name as period,
        COALESCE(SUM(rec.amount), 0) as projected_amount,
        COALESCE(SUM(CASE WHEN e.is_unbilled = false THEN COALESCE(e.amount, rec.amount) ELSE 0 END), 0) as billed_amount,
        COALESCE(SUM(CASE WHEN e.is_unbilled = true THEN COALESCE(e.amount, rec.amount) ELSE 0 END), 0) as unbilled_amount
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
      JOIN expenses e
        ON e.is_active = true
       AND e.source_type = 'recurring'
       AND e.recurring_expense_client_id = rec.id
       AND e.service_period_id = sp.id
      ${recurringExpenseWhere}
      GROUP BY sp.display_name, sp.start_date
      ORDER BY sp.start_date
    `, recurringExpenseParams);

    const teamExpenseParams = [financialYear];
    let teamExpenseWhere = `WHERE ${buildTeamAllocationExistsExpr('tca')}
      AND t.is_reviewer = true
      AND t.start_period IS NOT NULL
      AND t.amount IS NOT NULL
      AND sp.financial_year = $1
      AND (
        t.is_active = true
        OR COALESCE(tca.end_period, t.end_period) IS NOT NULL
        OR e.id IS NOT NULL
      )`;
    let teamExpenseParamIndex = 2;
    if (clientIdArr.length) {
      teamExpenseParams.push(clientIdArr);
      teamExpenseWhere += ` AND tca.client_id = ANY($${teamExpenseParamIndex++}::uuid[])`;
    }
    if (reviewerIdArr.length) {
      teamExpenseParams.push(reviewerIdArr);
      teamExpenseWhere += ` AND COALESCE(e.team_id, t.id) = ANY($${teamExpenseParamIndex++}::uuid[])`;
    }

    const teamExpenseVarianceResult = await pool.query(`
      SELECT
        sp.display_name as period,
        COALESCE(SUM(${buildTeamAllocationAmountExpr('tca', 't', 'sp')}), 0) as projected_amount,
        COALESCE(SUM(CASE WHEN e.is_unbilled = false THEN COALESCE(e.amount, ${buildTeamAllocationAmountExpr('tca', 't', 'sp')}) ELSE 0 END), 0) as billed_amount,
        COALESCE(SUM(CASE WHEN e.is_unbilled = true THEN COALESCE(e.amount, ${buildTeamAllocationAmountExpr('tca', 't', 'sp')}) ELSE 0 END), 0) as unbilled_amount
      FROM teams t
      JOIN team_client_allocations tca ON tca.team_id = t.id
      JOIN service_periods sp
        ON sp.is_active = true
       AND sp.start_date >= TO_DATE('01-' || COALESCE(tca.start_period, t.start_period), 'DD-Mon-YY')
       AND (
         COALESCE(tca.end_period, t.end_period) IS NULL
         OR sp.start_date <= TO_DATE('01-' || COALESCE(tca.end_period, t.end_period), 'DD-Mon-YY')
       )
      JOIN expenses e
        ON e.is_active = true
       AND e.source_type = 'team-recurring'
       AND e.team_client_allocation_id = tca.id
       AND e.service_period_id = sp.id
      ${teamExpenseWhere}
      GROUP BY sp.display_name, sp.start_date
      ORDER BY sp.start_date
    `, teamExpenseParams);

    const revenueMap = new Map(
      revenueVarianceResult.rows.map((row) => [
        row.period,
        {
          projected: parseFloat(row.projected_amount) || 0,
          billed: parseFloat(row.billed_amount) || 0,
          unbilled: parseFloat(row.unbilled_amount) || 0,
        },
      ]),
    );
    const recurringExpenseMap = new Map(
      recurringExpenseVarianceResult.rows.map((row) => [
        row.period,
        {
          projected: parseFloat(row.projected_amount) || 0,
          billed: parseFloat(row.billed_amount) || 0,
          unbilled: parseFloat(row.unbilled_amount) || 0,
        },
      ]),
    );
    const teamExpenseMap = new Map(
      teamExpenseVarianceResult.rows.map((row) => [
        row.period,
        {
          projected: parseFloat(row.projected_amount) || 0,
          billed: parseFloat(row.billed_amount) || 0,
          unbilled: parseFloat(row.unbilled_amount) || 0,
        },
      ]),
    );

    const monthPeriods = Array.from(new Set([
      ...revenueMap.keys(),
      ...recurringExpenseMap.keys(),
      ...teamExpenseMap.keys(),
    ])).sort((left, right) => (periodIndex.get(left) ?? Number.MAX_SAFE_INTEGER) - (periodIndex.get(right) ?? Number.MAX_SAFE_INTEGER));

    const monthData = monthPeriods.map((period) => {
      const revenueRow = revenueMap.get(period) || { projected: 0, billed: 0, unbilled: 0 };
      const recurringExpenseRow = recurringExpenseMap.get(period) || { projected: 0, billed: 0, unbilled: 0 };
      const teamExpenseRow = teamExpenseMap.get(period) || { projected: 0, billed: 0, unbilled: 0 };

      const projectedRevenue = revenueRow.projected;
      const billedRevenue = revenueRow.billed;
      const unbilledRevenue = revenueRow.unbilled;
      const actualRevenue = billedRevenue + unbilledRevenue;

      const projectedExpense = recurringExpenseRow.projected + teamExpenseRow.projected;
      const billedExpense = recurringExpenseRow.billed + teamExpenseRow.billed;
      const unbilledExpense = recurringExpenseRow.unbilled + teamExpenseRow.unbilled;
      const actualExpense = billedExpense + unbilledExpense;

      return {
        period,
        projected_revenue: projectedRevenue,
        billed_revenue: billedRevenue,
        unbilled_revenue: unbilledRevenue,
        actual_revenue: actualRevenue,
        revenue_variance: actualRevenue - projectedRevenue,
        projected_expense: projectedExpense,
        billed_expense: billedExpense,
        unbilled_expense: unbilledExpense,
        actual_expense: actualExpense,
        expense_variance: actualExpense - projectedExpense,
        projected_net: projectedRevenue - projectedExpense,
        actual_net: actualRevenue - actualExpense,
      };
    }).filter((month) => (
      month.projected_revenue !== 0 ||
      month.billed_revenue !== 0 ||
      month.unbilled_revenue !== 0 ||
      month.projected_expense !== 0 ||
      month.billed_expense !== 0 ||
      month.unbilled_expense !== 0
    ));

    res.json({ success: true, data: { periods: monthData.map((month) => month.period), months: monthData } });
  } catch (error) {
    console.error('Variance report error:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch variance report' });
  }
});

module.exports = router;

