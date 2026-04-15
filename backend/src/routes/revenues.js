const express = require('express');
const router = express.Router();
const { body, validationResult } = require('express-validator');
const { pool, logAudit } = require('../database/db');
const { auth, requireDelete } = require('../middleware/auth');

let revenueSchemaPromise = null;

async function ensureRevenueSchema() {
  if (!revenueSchemaPromise) {
    revenueSchemaPromise = (async () => {
      await pool.query(`
        ALTER TABLE revenues
          ADD COLUMN IF NOT EXISTS source_type VARCHAR(20) DEFAULT 'non-recurring';
      `);
      await pool.query(`
        ALTER TABLE revenues
          ADD COLUMN IF NOT EXISTS client_billing_row_id UUID REFERENCES client_billing_rows(id);
      `);
      await pool.query(`
        ALTER TABLE client_billing_rows
          ADD COLUMN IF NOT EXISTS service_type_id UUID REFERENCES service_types(id);
      `);
      
      // Ensure client_billing_rows table exists for recurring revenue
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
      
      await pool.query('CREATE INDEX IF NOT EXISTS idx_client_billing_rows_client ON client_billing_rows(client_id)');
      await pool.query('CREATE INDEX IF NOT EXISTS idx_client_billing_rows_period ON client_billing_rows(start_period, end_period)');
      await pool.query('CREATE INDEX IF NOT EXISTS idx_client_billing_rows_active ON client_billing_rows(is_active)');
      await pool.query('CREATE INDEX IF NOT EXISTS idx_client_billing_rows_service_type ON client_billing_rows(service_type_id)');
      await pool.query('CREATE INDEX IF NOT EXISTS idx_revenues_client_billing_row_period ON revenues(client_billing_row_id, service_period_id)');
    })().catch((error) => {
      revenueSchemaPromise = null;
      throw error;
    });
  }

  return revenueSchemaPromise;
}

function generateUniqueKey(clientName, serviceType, servicePeriod) {
  return `${clientName}|${serviceType}|${servicePeriod}`.toLowerCase().replace(/\s+/g, '-');
}

function calculateGST(amount, gstRate, isInterState = true) {
  const gstAmount = (amount * gstRate) / 100;
  if (isInterState) {
    return { igst: gstAmount, cgst: 0, sgst: 0 };
  }
  return { igst: 0, cgst: gstAmount / 2, sgst: gstAmount / 2 };
}

const parseFilterArray = (value) => {
  if (!value) return null;
  if (Array.isArray(value)) return value.length > 0 ? value : null;
  const arr = String(value).split(',').filter(Boolean);
  return arr.length > 0 ? arr : null;
};

const PERIOD_REGEX = /^[A-Z][a-z]{2}-\d{2}$/;
const LIVE_MONTH_START_SQL = "DATE_TRUNC('month', CURRENT_DATE)::date";
const buildRecurringProjectedExpr = (alias = 'rr') => `(${alias}.id IS NULL AND sp.start_date >= ${LIVE_MONTH_START_SQL})`;
const buildRecurringUnbilledExpr = (alias = 'rr') => `
  (CASE
    WHEN ${alias}.id IS NULL THEN sp.start_date < ${LIVE_MONTH_START_SQL}
    ELSE COALESCE(${alias}.is_unbilled, true)
  END)
`;

const parsePeriod = (period) => {
  if (!period || !PERIOD_REGEX.test(period)) return null;
  const monthMap = {
    Jan: 0, Feb: 1, Mar: 2, Apr: 3, May: 4, Jun: 5,
    Jul: 6, Aug: 7, Sep: 8, Oct: 9, Nov: 10, Dec: 11
  };
  const [mon, yy] = period.split('-');
  return new Date(2000 + parseInt(yy, 10), monthMap[mon], 1);
};

const buildNonRecurringWhere = (filters = {}, alias = 'r', periodAlias = 'sp') => {
  const params = [];
  let paramIndex = 1;
  let whereClause = `WHERE ${alias}.is_active = true`;

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
  const stIds = parseFilterArray(filters.serviceTypeId);
  if (stIds) {
    params.push(stIds);
    whereClause += ` AND ${alias}.service_type_id = ANY($${paramIndex++}::uuid[])`;
  }
  const revIds = parseFilterArray(filters.reviewerId);
  if (revIds) {
    params.push(revIds);
    whereClause += ` AND ${alias}.reviewer_id = ANY($${paramIndex++}::uuid[])`;
  }
  if (filters.financialYear) {
    params.push(filters.financialYear);
    whereClause += ` AND ${periodAlias}.financial_year = $${paramIndex++}`;
  }
  if (filters.isUnbilled !== undefined) {
    params.push(filters.isUnbilled === 'true' || filters.isUnbilled === true);
    whereClause += ` AND ${alias}.is_unbilled = $${paramIndex++}`;
  }
  const bnIds = parseFilterArray(filters.billingNameId);
  if (bnIds) {
    params.push(bnIds);
    whereClause += ` AND ${alias}.billing_name_id = ANY($${paramIndex++}::uuid[])`;
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
      r.id::text as id,
      r.unique_key,
      r.client_id,
      r.service_type_id,
      r.service_period_id,
      r.date,
      r.invoice_no,
      r.is_unbilled,
      r.billing_name_id,
      r.nature_of_service,
      r.hsn_code,
      r.gst_rate,
      r.revenue_amount,
      r.currency,
      r.igst,
      r.cgst,
      r.sgst,
      r.other_charges,
      r.round_off,
      r.total_amount,
      r.reviewer_id,
      r.reviewer_allocation_percentage,
      r.bill_from,
      r.notes,
      r.created_at,
      r.updated_at,
      c.name as client_name,
      st.name as service_type_name,
      sp.display_name as service_period_name,
      sp.financial_year,
      sp.start_date as period_start_date,
      bn.name as billing_name,
      t.name as reviewer_name,
      'non-recurring' as source_type
    FROM revenues r
    LEFT JOIN clients c ON r.client_id = c.id
    LEFT JOIN service_types st ON r.service_type_id = st.id
    LEFT JOIN service_periods sp ON r.service_period_id = sp.id
    LEFT JOIN billing_names bn ON r.billing_name_id = bn.id
    LEFT JOIN teams t ON r.reviewer_id = t.id
    ${whereClause} AND COALESCE(r.source_type, 'non-recurring') = 'non-recurring'
  `, params);

  return result.rows.map((row) => ({
    ...row,
    revenue_amount: parseFloat(row.revenue_amount) || 0,
    total_amount: parseFloat(row.total_amount) || 0,
    igst: parseFloat(row.igst) || 0,
    cgst: parseFloat(row.cgst) || 0,
    sgst: parseFloat(row.sgst) || 0,
  }));
};

const getRecurringRows = async (filters = {}) => {
  const params = [];
  let paramIndex = 1;
  let whereClause = 'WHERE (cbr.is_active = true OR cbr.end_period IS NOT NULL OR rr.id IS NOT NULL)';

  const clientIds = parseFilterArray(filters.clientId);
  if (clientIds) {
    params.push(clientIds);
    whereClause += ` AND c.id = ANY($${paramIndex++}::uuid[])`;
  }
  if (filters.financialYear) {
    params.push(filters.financialYear);
    whereClause += ` AND sp.financial_year = $${paramIndex++}`;
  }
  const spIds = parseFilterArray(filters.servicePeriodId);
  if (spIds) {
    params.push(spIds);
    whereClause += ` AND sp.id = ANY($${paramIndex++}::uuid[])`;
  }
  const stIds = parseFilterArray(filters.serviceTypeId);
  if (stIds) {
    params.push(stIds);
    whereClause += ` AND COALESCE(rr.service_type_id, cbr.service_type_id) = ANY($${paramIndex++}::uuid[])`;
  }
  const revIds = parseFilterArray(filters.reviewerId);
  if (revIds) {
    params.push(revIds);
    whereClause += ` AND COALESCE(rr.reviewer_id, cbr.reviewer_id) = ANY($${paramIndex++}::uuid[])`;
  }
  if (filters.isUnbilled !== undefined) {
    params.push(filters.isUnbilled === 'true' || filters.isUnbilled === true);
    whereClause += ` AND ${buildRecurringUnbilledExpr('rr')} = $${paramIndex++}`;
  }
  if (filters.startDate) {
    params.push(filters.startDate);
    whereClause += ` AND COALESCE(rr.date, sp.start_date) >= $${paramIndex++}`;
  }
  if (filters.endDate) {
    params.push(filters.endDate);
    whereClause += ` AND COALESCE(rr.date, sp.start_date) <= $${paramIndex++}`;
  }

  const result = await pool.query(`
    SELECT
      COALESCE(
        rr.id::text,
        CONCAT('recurring-', cbr.id::text, '-', sp.id::text)
      ) as id,
      rr.id as rr_id,
      COALESCE(
        rr.unique_key,
        LOWER(REPLACE(CONCAT(c.name, '|', COALESCE(st.name, cbr.billing_name), '|', sp.display_name), ' ', '-'))
      ) as unique_key,
      c.id as client_id,
      cbr.id as client_billing_row_id,
      COALESCE(rr.service_type_id, cbr.service_type_id) as service_type_id,
      sp.id as service_period_id,
      COALESCE(rr.date, sp.start_date) as date,
      rr.invoice_no,
      ${buildRecurringUnbilledExpr('rr')} as is_unbilled,
      ${buildRecurringProjectedExpr('rr')} as is_projected,
      rr.billing_name_id,
      rr.nature_of_service,
      rr.hsn_code,
      COALESCE(rr.gst_rate, 0) as gst_rate,
      COALESCE(rr.revenue_amount, cbr.amount) as revenue_amount,
      cbr.amount as projected_amount,
      COALESCE(rr.currency, 'INR') as currency,
      COALESCE(rr.igst, 0) as igst,
      COALESCE(rr.cgst, 0) as cgst,
      COALESCE(rr.sgst, 0) as sgst,
      COALESCE(rr.other_charges, 0) as other_charges,
      COALESCE(rr.round_off, 0) as round_off,
      COALESCE(
        rr.total_amount,
        rr.revenue_amount + COALESCE(rr.igst, 0) + COALESCE(rr.cgst, 0) + COALESCE(rr.sgst, 0) + COALESCE(rr.other_charges, 0) + COALESCE(rr.round_off, 0),
        cbr.amount
      ) as total_amount,
      COALESCE(rr.reviewer_id, cbr.reviewer_id) as reviewer_id,
      rr.reviewer_allocation_percentage,
      COALESCE(rr.bill_from, bill_from.name) as bill_from,
      rr.notes,
      COALESCE(rr.created_at, cbr.created_at) as created_at,
      COALESCE(rr.updated_at, cbr.updated_at) as updated_at,
      c.name as client_name,
      st.name as service_type_name,
      sp.display_name as service_period_name,
      sp.financial_year,
      sp.start_date as period_start_date,
      cbr.billing_name as billing_name,
      COALESCE(reviewer.name, cbr_reviewer.name) as reviewer_name,
      'recurring' as source_type
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
        AND r.client_billing_row_id = cbr.id
        AND r.service_period_id = sp.id
        AND (
          r.source_type = 'recurring'
          OR (r.source_type IS NULL AND r.client_billing_row_id IS NOT NULL)
        )
      ORDER BY r.updated_at DESC NULLS LAST, r.created_at DESC NULLS LAST, r.id DESC
      LIMIT 1
    ) rr ON true
    LEFT JOIN service_types st ON COALESCE(rr.service_type_id, cbr.service_type_id) = st.id
    LEFT JOIN bill_from_masters bill_from ON cbr.bill_from_id = bill_from.id
    LEFT JOIN teams reviewer ON rr.reviewer_id = reviewer.id
    LEFT JOIN teams cbr_reviewer ON cbr.reviewer_id = cbr_reviewer.id
    ${whereClause}
  `, params);

  return result.rows.map((row) => {
    const isProjected = row.is_projected === true || row.is_projected === 't';
    return {
      ...row,
      revenue_amount: parseFloat(row.revenue_amount) || 0,
      projected_amount: parseFloat(row.projected_amount) || 0,
      total_amount: parseFloat(row.total_amount) || 0,
      igst: parseFloat(row.igst) || 0,
      cgst: parseFloat(row.cgst) || 0,
      sgst: parseFloat(row.sgst) || 0,
      is_projected: isProjected,
      date: isProjected ? null : row.date,
      invoice_no: isProjected ? null : row.invoice_no,
    };
  });
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
  let rows = [];

  if (sourceType === 'non-recurring') {
    rows = await getNonRecurringRows(filters);
  } else if (sourceType === 'recurring') {
    rows = await getRecurringRows(filters);
  } else {
    const [nonRecurring, recurring] = await Promise.all([
      getNonRecurringRows(filters),
      getRecurringRows(filters),
    ]);
    rows = [...nonRecurring, ...recurring];
  }

  return sortRows(rows);
};

router.get('/', auth, async (req, res) => {
  try {
    await ensureRevenueSchema();
    const {
      clientId, servicePeriodId, serviceTypeId, reviewerId,
      financialYear, isUnbilled, billingNameId,
      startDate, endDate, sourceType = 'all', page, limit
    } = req.query;

    const rows = await getCombinedRows({
      clientId,
      servicePeriodId,
      serviceTypeId,
      reviewerId,
      financialYear,
      isUnbilled,
      billingNameId,
      startDate,
      endDate,
      sourceType,
    });

    const hasExplicitPagination = page !== undefined || limit !== undefined;
    if (!hasExplicitPagination) {
      return res.json({
        success: true,
        data: rows,
        pagination: {
          page: 1,
          limit: rows.length,
          total: rows.length,
          totalPages: rows.length ? 1 : 0
        }
      });
    }

    const pageNumber = parseInt(page, 10) || 1;
    const pageSize = parseInt(limit, 10) || 50;
    const offset = (pageNumber - 1) * pageSize;
    const pagedRows = rows.slice(offset, offset + pageSize);

    res.json({
      success: true,
      data: pagedRows,
      pagination: {
        page: pageNumber,
        limit: pageSize,
        total: rows.length,
        totalPages: Math.ceil(rows.length / pageSize)
      }
    });
  } catch (error) {
    console.error('Get revenues error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch revenues'
    });
  }
});

router.get('/summary', auth, async (req, res) => {
  try {
    await ensureRevenueSchema();
    const { financialYear, clientId, sourceType = 'all' } = req.query;
    const rows = await getCombinedRows({
      financialYear,
      clientId,
      sourceType,
    });

    const summary = rows.reduce((acc, row) => {
      acc.total_records += 1;
      const amount = parseFloat(row.revenue_amount) || 0;
      acc.total_revenue += amount;
      acc.total_with_gst += parseFloat(row.total_amount) || 0;
      if (row.is_projected) {
        acc.projected_revenue += amount;
      } else if (row.is_unbilled) {
        acc.unbilled_revenue += amount;
      } else {
        acc.billed_revenue += amount;
      }
      acc.total_gst += (parseFloat(row.igst) || 0) + (parseFloat(row.cgst) || 0) + (parseFloat(row.sgst) || 0);
      return acc;
    }, {
      total_records: 0,
      total_revenue: 0,
      total_with_gst: 0,
      projected_revenue: 0,
      unbilled_revenue: 0,
      billed_revenue: 0,
      total_gst: 0,
    });

    res.json({
      success: true,
      data: summary
    });
  } catch (error) {
    console.error('Get revenue summary error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch revenue summary'
    });
  }
});

router.get('/:id/history', auth, async (req, res) => {
  try {
    await ensureRevenueSchema();
    const result = await pool.query(`
      SELECT
        al.*,
        u.name as changed_by_name
      FROM audit_logs al
      LEFT JOIN users u ON al.user_id = u.id
      WHERE al.table_name = 'revenues'
        AND al.record_id = $1
      ORDER BY al.created_at DESC
    `, [req.params.id]);

    res.json({
      success: true,
      data: result.rows
    });
  } catch (error) {
    console.error('Get revenue history error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch revenue history'
    });
  }
});

router.get('/:id', auth, async (req, res) => {
  try {
    await ensureRevenueSchema();
    const result = await pool.query(`
      SELECT r.*,
             c.name as client_name,
             st.name as service_type_name,
             sp.display_name as service_period_name,
             sp.financial_year,
             bn.name as billing_name,
             t.name as reviewer_name
      FROM revenues r
      LEFT JOIN clients c ON r.client_id = c.id
      LEFT JOIN service_types st ON r.service_type_id = st.id
      LEFT JOIN service_periods sp ON r.service_period_id = sp.id
      LEFT JOIN billing_names bn ON r.billing_name_id = bn.id
      LEFT JOIN teams t ON r.reviewer_id = t.id
      WHERE r.id = $1
    `, [req.params.id]);

    if (result.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Revenue not found'
      });
    }

    const allocations = await pool.query(`
      SELECT rra.*, t.name as reviewer_name
      FROM revenue_reviewer_allocations rra
      JOIN teams t ON rra.reviewer_id = t.id
      WHERE rra.revenue_id = $1
    `, [req.params.id]);

    const audits = await pool.query(`
      SELECT al.*, u.name as user_name
      FROM audit_logs al
      LEFT JOIN users u ON al.user_id = u.id
      WHERE al.table_name = 'revenues' AND al.record_id = $1
      ORDER BY al.created_at DESC
      LIMIT 20
    `, [req.params.id]);

    res.json({
      success: true,
      data: {
        ...result.rows[0],
        source_type: result.rows[0].source_type || 'non-recurring',
        reviewer_allocations: allocations.rows,
        audit_trail: audits.rows
      }
    });
  } catch (error) {
    console.error('Get revenue error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch revenue'
    });
  }
});

router.post('/', auth, [
  body('source_type').optional().isIn(['recurring', 'non-recurring']),
  body('client_id').isUUID().withMessage('Valid client ID is required'),
  body('client_billing_row_id').optional({ values: 'falsy' }).isUUID().withMessage('Valid client billing row ID is required'),
  body('service_type_id').optional({ values: 'falsy' }).isUUID().withMessage('Valid service type ID is required'),
  body('service_period_id').isUUID().withMessage('Valid service period ID is required'),
  body('date').isISO8601().withMessage('Valid date is required'),
  body('revenue_amount').isFloat({ min: 0 }).withMessage('Revenue amount must be zero or greater'),
  body('invoice_no').optional().trim(),
  body('is_unbilled').optional().isBoolean(),
  body('billing_name_id').optional({ values: 'falsy' }).isUUID(),
  body('nature_of_service').optional().trim(),
  body('hsn_code').optional().trim(),
  body('gst_rate').optional().isFloat({ min: 0, max: 28 }),
  body('currency').optional().isIn(['INR', 'USD', 'EUR', 'GBP', 'AED', 'SGD', 'AUD', 'CAD', 'JPY', 'CHF']),
  body('reviewer_id').optional({ values: 'falsy' }).isUUID(),
  body('reviewer_allocation_percentage').optional().isFloat({ min: 0, max: 100 }),
  body('bill_from').optional().trim(),
  body('round_off').optional().isFloat(),
  body('is_interstate').optional().isBoolean(),
], async (req, res) => {
  const dbClient = await pool.connect();

  try {
    await ensureRevenueSchema();
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        errors: errors.array()
      });
    }

    const {
      source_type, client_billing_row_id,
      client_id, service_type_id, service_period_id, date, revenue_amount,
      invoice_no, is_unbilled, billing_name_id, nature_of_service, hsn_code,
      gst_rate, currency, reviewer_id, reviewer_allocation_percentage,
      bill_from, round_off, is_interstate, other_charges, notes
    } = req.body;

    await dbClient.query('BEGIN');

    const refs = await dbClient.query(`
      SELECT
        c.name as client_name,
        st.name as service_type_name,
        bn.name as billing_name,
        sp.display_name as period_name,
        st.gst_rate as default_gst_rate,
        st.hsn_code as default_hsn
      FROM clients c
      JOIN service_periods sp ON sp.id = $2
      LEFT JOIN service_types st ON st.id = $3
      LEFT JOIN billing_names bn ON bn.id = $4
      WHERE c.id = $1
    `, [client_id, service_period_id, service_type_id || null, billing_name_id || null]);

    if (refs.rows.length === 0) {
      await dbClient.query('ROLLBACK');
      return res.status(400).json({
        success: false,
        message: 'Invalid client, service type, or service period'
      });
    }

    const finalSourceType = source_type || 'non-recurring';
    if (finalSourceType === 'recurring' && !client_billing_row_id) {
      await dbClient.query('ROLLBACK');
      return res.status(400).json({
        success: false,
        message: 'Recurring revenue requires a client billing row reference'
      });
    }
    const {
      client_name,
      service_type_name,
      billing_name,
      period_name,
      default_gst_rate,
      default_hsn
    } = refs.rows[0];
    const unique_key = generateUniqueKey(
      client_name,
      service_type_name || billing_name || finalSourceType,
      period_name
    );
    const finalGstRate = gst_rate !== undefined ? gst_rate : (default_gst_rate || 0);
    const finalHsn = hsn_code || default_hsn;
    const { igst, cgst, sgst } = calculateGST(revenue_amount, finalGstRate, is_interstate !== false);
    const total_amount = parseFloat(revenue_amount) + igst + cgst + sgst + (parseFloat(other_charges) || 0) + (parseFloat(round_off) || 0);

    let existingRevenue = null;
    if (finalSourceType === 'recurring') {
      const existingResult = await dbClient.query(`
        SELECT *
        FROM revenues
        WHERE is_active = true
          AND service_period_id = $1
          AND client_billing_row_id = $2
          AND (
            source_type = 'recurring'
            OR (source_type IS NULL AND client_billing_row_id IS NOT NULL)
          )
        ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST, id DESC
        LIMIT 1
      `, [service_period_id, client_billing_row_id]);
      existingRevenue = existingResult.rows[0] || null;
    }

    if (existingRevenue) {
      const result = await dbClient.query(`
        UPDATE revenues
        SET
          unique_key = $1,
          client_id = $2,
          service_type_id = $3,
          service_period_id = $4,
          date = $5,
          invoice_no = $6,
          is_unbilled = $7,
          billing_name_id = $8,
          nature_of_service = $9,
          hsn_code = $10,
          gst_rate = $11,
          revenue_amount = $12,
          currency = $13,
          igst = $14,
          cgst = $15,
          sgst = $16,
          other_charges = $17,
          round_off = $18,
          total_amount = $19,
          reviewer_id = $20,
          reviewer_allocation_percentage = $21,
          bill_from = $22,
          notes = $23,
          updated_by = $24,
          updated_at = CURRENT_TIMESTAMP,
          source_type = $25,
          client_billing_row_id = $26
        WHERE id = $27
        RETURNING *
      `, [
        unique_key,
        client_id,
        service_type_id || null,
        service_period_id,
        date,
        invoice_no || null,
        is_unbilled || false,
        billing_name_id || null,
        nature_of_service || null,
        finalHsn,
        finalGstRate,
        revenue_amount,
        currency || 'INR',
        igst,
        cgst,
        sgst,
        other_charges || 0,
        round_off || 0,
        total_amount,
        reviewer_id || null,
        reviewer_allocation_percentage || null,
        bill_from || null,
        notes || null,
        req.user.id,
        finalSourceType,
        client_billing_row_id || null,
        existingRevenue.id
      ]);

      await dbClient.query('COMMIT');
      await logAudit('revenues', existingRevenue.id, 'UPDATE', existingRevenue, result.rows[0], req.user.id);

      return res.json({
        success: true,
        message: 'Revenue updated successfully',
        data: result.rows[0]
      });
    }

    const result = await dbClient.query(`
      INSERT INTO revenues (
        unique_key, client_id, service_type_id, service_period_id, date, invoice_no,
        is_unbilled, billing_name_id, nature_of_service, hsn_code, gst_rate,
        revenue_amount, currency, igst, cgst, sgst, other_charges, round_off,
        total_amount, reviewer_id, reviewer_allocation_percentage, bill_from, notes,
        created_by, updated_by, source_type, client_billing_row_id
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $24, $25, $26
      ) RETURNING *
    `, [
      unique_key, client_id, service_type_id, service_period_id, date, invoice_no || null,
      is_unbilled || false, billing_name_id || null, nature_of_service || null, finalHsn, finalGstRate,
      revenue_amount, currency || 'INR', igst, cgst, sgst, other_charges || 0, round_off || 0,
      total_amount, reviewer_id || null, reviewer_allocation_percentage || null, bill_from || null,
      notes || null, req.user.id, finalSourceType, client_billing_row_id || null
    ]);

    await dbClient.query('COMMIT');
    await logAudit('revenues', result.rows[0].id, 'CREATE', null, result.rows[0], req.user.id);

    res.status(201).json({
      success: true,
      message: 'Revenue created successfully',
      data: result.rows[0]
    });
  } catch (error) {
    await dbClient.query('ROLLBACK');
    console.error('Create revenue error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to create revenue'
    });
  } finally {
    dbClient.release();
  }
});

router.put('/:id', auth, [
  body('source_type').optional().isIn(['recurring', 'non-recurring']),
  body('service_type_id').optional({ values: 'falsy' }).isUUID(),
  body('client_billing_row_id').optional({ values: 'falsy' }).isUUID(),
  body('service_period_id').optional().isUUID(),
  body('date').optional().isISO8601(),
  body('revenue_amount').optional().isFloat({ min: 0 }),
  body('invoice_no').optional().trim(),
  body('is_unbilled').optional().isBoolean(),
  body('billing_name_id').optional({ values: 'falsy' }).isUUID(),
  body('gst_rate').optional().isFloat({ min: 0, max: 28 }),
  body('round_off').optional().isFloat(),
], async (req, res) => {
  const dbClient = await pool.connect();

  try {
    await ensureRevenueSchema();
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        errors: errors.array()
      });
    }

    const { id } = req.params;
    const current = await dbClient.query('SELECT * FROM revenues WHERE id = $1', [id]);
    if (current.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Revenue not found'
      });
    }

    const currentData = current.rows[0];
    const updates = { ...req.body };

    if (updates.revenue_amount !== undefined || updates.gst_rate !== undefined) {
      const amount = updates.revenue_amount ?? currentData.revenue_amount;
      const rate = updates.gst_rate ?? currentData.gst_rate;
      const isInterState = parseFloat(currentData.igst) > 0;

      const { igst, cgst, sgst } = calculateGST(parseFloat(amount), parseFloat(rate), isInterState);
      updates.igst = igst;
      updates.cgst = cgst;
      updates.sgst = sgst;
      updates.total_amount = parseFloat(amount) + igst + cgst + sgst +
        parseFloat(updates.other_charges ?? currentData.other_charges ?? 0) +
        parseFloat(updates.round_off ?? currentData.round_off ?? 0);
    }

    await dbClient.query('BEGIN');

    const allowedFields = [
      'source_type', 'service_period_id', 'date', 'invoice_no', 'is_unbilled', 'billing_name_id', 'nature_of_service',
      'service_type_id', 'client_billing_row_id', 'hsn_code', 'gst_rate', 'revenue_amount', 'currency', 'igst', 'cgst', 'sgst',
      'other_charges', 'round_off', 'total_amount', 'reviewer_id',
      'reviewer_allocation_percentage', 'bill_from', 'notes'
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
    values.push(id);

    const query = `UPDATE revenues SET ${setClauses.join(', ')} WHERE id = $${paramIndex} RETURNING *`;
    const result = await dbClient.query(query, values);

    await dbClient.query('COMMIT');
    await logAudit('revenues', id, 'UPDATE', currentData, result.rows[0], req.user.id);

    res.json({
      success: true,
      message: 'Revenue updated successfully',
      data: result.rows[0]
    });
  } catch (error) {
    await dbClient.query('ROLLBACK');
    console.error('Update revenue error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to update revenue'
    });
  } finally {
    dbClient.release();
  }
});

router.delete('/:id', auth, requireDelete, async (req, res) => {
  try {
    await ensureRevenueSchema();
    const { id } = req.params;
    const current = await pool.query('SELECT * FROM revenues WHERE id = $1', [id]);
    if (current.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Revenue not found'
      });
    }

    const currentData = current.rows[0];
    if (currentData.source_type === 'recurring' || currentData.client_billing_row_id) {
      return res.status(400).json({
        success: false,
        message: 'Recurring revenue entries cannot be deleted. Update the amount to 0 instead.'
      });
    }

    await pool.query('UPDATE revenues SET is_active = false, updated_by = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2', [req.user.id, id]);
    await logAudit('revenues', id, 'DELETE', current.rows[0], null, req.user.id);

    res.json({
      success: true,
      message: 'Revenue deleted successfully'
    });
  } catch (error) {
    console.error('Delete revenue error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to delete revenue'
    });
  }
});

module.exports = router;
