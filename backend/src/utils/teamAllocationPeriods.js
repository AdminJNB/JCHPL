const PERIOD_REGEX = /^[A-Z][a-z]{2}-\d{2}$/;

const parsePeriod = (period) => {
  if (!period || !PERIOD_REGEX.test(period)) return null;
  const monthMap = {
    Jan: 0,
    Feb: 1,
    Mar: 2,
    Apr: 3,
    May: 4,
    Jun: 5,
    Jul: 6,
    Aug: 7,
    Sep: 8,
    Oct: 9,
    Nov: 10,
    Dec: 11,
  };
  const [month, year] = period.split('-');
  return new Date(2000 + parseInt(year, 10), monthMap[month], 1);
};

const comparePeriods = (left, right) => {
  const leftDate = parsePeriod(left);
  const rightDate = parsePeriod(right);
  if (!leftDate && !rightDate) return 0;
  if (!leftDate) return -1;
  if (!rightDate) return 1;
  return leftDate - rightDate;
};

const samePeriodRange = (left = {}, right = {}) =>
  (left?.start_period || null) === (right?.start_period || null) &&
  (left?.end_period || null) === (right?.end_period || null);

const roundCurrency = (value) => Math.round((parseFloat(value) || 0) * 100) / 100;

const roundPercentage = (value) => Math.round((parseFloat(value) || 0) * 10000) / 10000;

const normalizeAllocationMethod = (value) => (value === 'manual' ? 'manual' : 'percentage');

const getStoredTeamAllocationAmount = (row = {}, fallbackAmount = 0) => {
  if (row?.allocation_amount !== undefined && row?.allocation_amount !== null && row?.allocation_amount !== '') {
    return roundCurrency(row.allocation_amount);
  }

  return roundCurrency(((parseFloat(row?.allocation_percentage) || 0) / 100) * (parseFloat(fallbackAmount) || 0));
};

const normalizeTeamAllocationRows = (rows = []) =>
  (Array.isArray(rows) ? rows : [])
    .filter(Boolean)
    .map((row) => ({
      ...row,
      allocation_percentage: roundPercentage(row.allocation_percentage),
      allocation_amount:
        row?.allocation_amount === undefined || row?.allocation_amount === null || row?.allocation_amount === ''
          ? null
          : roundCurrency(row.allocation_amount),
      allocation_method: normalizeAllocationMethod(row?.allocation_method),
    }))
    .sort((a, b) =>
      comparePeriods(a.start_period, b.start_period) ||
      comparePeriods(a.end_period, b.end_period) ||
      String(a.client_name || '').localeCompare(String(b.client_name || '')) ||
      String(a.id || '').localeCompare(String(b.id || ''))
    );

const filterAllocationsForPeriod = (rows = [], periodRow) => {
  if (!periodRow?.start_period) return [];
  return normalizeTeamAllocationRows(rows).filter((row) => samePeriodRange(row, periodRow));
};

const getLatestPeriodRow = (rows = []) => {
  const normalizedRows = (Array.isArray(rows) ? rows : [])
    .filter((row) => row?.start_period)
    .sort((a, b) =>
      comparePeriods(a.start_period, b.start_period) ||
      comparePeriods(a.end_period, b.end_period)
    );

  return normalizedRows[normalizedRows.length - 1] || null;
};

let teamAllocationSchemaPromise = null;

const ensureTeamAllocationPeriodColumns = async (pool) => {
  const tableExistsResult = await pool.query(`
    SELECT to_regclass('public.team_client_allocations') AS table_name
  `);

  if (!tableExistsResult.rows[0]?.table_name) {
    teamAllocationSchemaPromise = null;
    return;
  }

  if (!teamAllocationSchemaPromise) {
    teamAllocationSchemaPromise = (async () => {
      await pool.query(`
        ALTER TABLE team_client_allocations
          ADD COLUMN IF NOT EXISTS start_period VARCHAR(10),
          ADD COLUMN IF NOT EXISTS end_period VARCHAR(10),
          ADD COLUMN IF NOT EXISTS allocation_amount DECIMAL(15, 2),
          ADD COLUMN IF NOT EXISTS allocation_method VARCHAR(20) DEFAULT 'percentage',
          ADD COLUMN IF NOT EXISTS reviewer_id UUID REFERENCES teams(id);
      `);

      await pool.query(`
        ALTER TABLE team_client_allocations
          DROP CONSTRAINT IF EXISTS team_client_allocations_team_id_client_id_key;
      `);

      await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_team_client_allocations_team_period
        ON team_client_allocations(team_id, start_period, end_period);
      `);

      await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_team_client_allocations_reviewer
        ON team_client_allocations(reviewer_id);
      `);

      await pool.query(`
        UPDATE team_client_allocations tca
        SET
          start_period = COALESCE(
            tca.start_period,
            (
              SELECT tih.start_period
              FROM team_increment_history tih
              WHERE tih.team_id = tca.team_id
              ORDER BY TO_DATE('01-' || tih.start_period, 'DD-Mon-YY') DESC
              LIMIT 1
            ),
            t.start_period
          ),
          end_period = COALESCE(tca.end_period, t.end_period)
        FROM teams t
        WHERE t.id = tca.team_id
          AND (tca.start_period IS NULL OR tca.end_period IS NULL);
      `);

      await pool.query(`
        UPDATE team_client_allocations tca
        SET
          allocation_method = COALESCE(NULLIF(tca.allocation_method, ''), 'percentage'),
          allocation_amount = COALESCE(
            tca.allocation_amount,
            ROUND(
              (COALESCE(tca.allocation_percentage, 0) / 100.0) * COALESCE(
                (
                  SELECT tih.amount
                  FROM team_increment_history tih
                  WHERE tih.team_id = tca.team_id
                    AND tih.start_period = COALESCE(tca.start_period, t.start_period)
                    AND tih.end_period IS NOT DISTINCT FROM COALESCE(tca.end_period, t.end_period)
                  ORDER BY TO_DATE('01-' || tih.start_period, 'DD-Mon-YY') DESC
                  LIMIT 1
                ),
                t.amount,
                0
              ),
              2
            )
          )
        FROM teams t
        WHERE t.id = tca.team_id
          AND (
            tca.allocation_amount IS NULL
            OR tca.allocation_method IS NULL
            OR tca.allocation_method = ''
          );
      `);

      await pool.query(`
        UPDATE team_client_allocations tca
        SET reviewer_id = t.id
        FROM teams t
        WHERE t.id = tca.team_id
          AND tca.reviewer_id IS NULL
          AND t.is_reviewer = true;
      `).catch(() => {});
    })().catch((error) => {
      teamAllocationSchemaPromise = null;
      throw error;
    });
  }

  return teamAllocationSchemaPromise;
};

module.exports = {
  comparePeriods,
  ensureTeamAllocationPeriodColumns,
  filterAllocationsForPeriod,
  getLatestPeriodRow,
  getStoredTeamAllocationAmount,
  normalizeTeamAllocationRows,
  normalizeAllocationMethod,
  roundCurrency,
  roundPercentage,
  samePeriodRange,
};
