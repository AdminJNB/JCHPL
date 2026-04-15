const express = require('express');
const router = express.Router();
const { body, validationResult } = require('express-validator');
const { pool, logAudit } = require('../database/db');
const { auth, requireDelete } = require('../middleware/auth');
const { summarizeTeamCompensationRows } = require('../utils/masterPeriodSummaries');
const {
  ensureTeamAllocationPeriodColumns,
  filterAllocationsForPeriod,
  getLatestPeriodRow,
  getStoredTeamAllocationAmount,
  normalizeAllocationMethod,
  normalizeTeamAllocationRows,
  roundCurrency,
  roundPercentage,
  samePeriodRange,
} = require('../utils/teamAllocationPeriods');
const {
  deactivateTeamRecurringExpensesForAllocations,
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

let teamSchemaPromise = null;

async function ensureTeamSchema() {
  if (!teamSchemaPromise) {
    teamSchemaPromise = (async () => {
      await pool.query(`
        ALTER TABLE teams
          ADD COLUMN IF NOT EXISTS amount DECIMAL(15, 2),
          ADD COLUMN IF NOT EXISTS start_period VARCHAR(10),
          ADD COLUMN IF NOT EXISTS increment_period VARCHAR(10),
          ADD COLUMN IF NOT EXISTS end_period VARCHAR(10),
          ADD COLUMN IF NOT EXISTS expense_head_id UUID REFERENCES expense_heads(id),
          ADD COLUMN IF NOT EXISTS is_admin BOOLEAN DEFAULT false;
      `);

      await pool.query(`
        CREATE TABLE IF NOT EXISTS team_increment_history (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          team_id UUID REFERENCES teams(id) ON DELETE CASCADE,
          start_period VARCHAR(10) NOT NULL,
          end_period VARCHAR(10),
          amount DECIMAL(15, 2) NOT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);

      await pool.query('CREATE INDEX IF NOT EXISTS idx_team_increment_history_team ON team_increment_history(team_id)');

      // Add expense_head_id per client allocation
      await pool.query(`
        CREATE TABLE IF NOT EXISTS team_client_allocations (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          team_id UUID REFERENCES teams(id) ON DELETE CASCADE,
          client_id UUID REFERENCES clients(id) ON DELETE CASCADE,
          allocation_percentage DECIMAL(8,4) NOT NULL DEFAULT 0,
          allocation_amount DECIMAL(15,2),
          allocation_method VARCHAR(20) DEFAULT 'percentage',
          expense_head_id UUID REFERENCES expense_heads(id),
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);
      await pool.query(`
        ALTER TABLE team_client_allocations
          ADD COLUMN IF NOT EXISTS expense_head_id UUID REFERENCES expense_heads(id);
      `).catch(() => {});

      await ensureTeamAllocationPeriodColumns(pool);
    })().catch((error) => {
      teamSchemaPromise = null;
      throw error;
    });
  }

  return teamSchemaPromise;
}

const PERIOD_REGEX = /^[A-Z][a-z]{2}-\d{2}$/;

const parsePeriod = (period) => {
  if (!period || !PERIOD_REGEX.test(period)) return null;
  const monthMap = {
    Jan: 0, Feb: 1, Mar: 2, Apr: 3, May: 4, Jun: 5,
    Jul: 6, Aug: 7, Sep: 8, Oct: 9, Nov: 10, Dec: 11
  };
  const [mon, yy] = period.split('-');
  return new Date(2000 + parseInt(yy, 10), monthMap[mon], 1);
};

const formatPeriod = (date) => {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) return null;
  const mon = date.toLocaleString('en-US', { month: 'short' });
  const yy = date.getFullYear().toString().slice(-2);
  return `${mon}-${yy}`;
};

const getPreviousPeriod = (period) => {
  const date = parsePeriod(period);
  if (!date) return null;
  date.setMonth(date.getMonth() - 1);
  return formatPeriod(date);
};

const comparePeriods = (a, b) => {
  const dateA = parsePeriod(a);
  const dateB = parsePeriod(b);
  if (!dateA && !dateB) return 0;
  if (!dateA) return -1;
  if (!dateB) return 1;
  return dateA - dateB;
};

const normalizeStoredCompensationRows = (rows = []) =>
  rows
    .filter((row) => row && row.start_period && row.amount !== undefined && row.amount !== null && row.amount !== '')
    .map((row) => ({
      id: row.id,
      start_period: row.start_period,
      end_period: row.end_period || null,
      amount: parseFloat(row.amount),
    }))
    .sort((a, b) =>
      comparePeriods(a.start_period, b.start_period) ||
      comparePeriods(a.end_period, b.end_period)
    );

const normalizeIncrementEvents = (rows = []) =>
  normalizeStoredCompensationRows(rows).map((row) => ({
    id: row.id,
    effective_period: row.start_period,
    end_period: row.end_period,
    amount: row.amount,
  }));

const deriveLegacyCompensationHistory = (team, incrementRows = []) => {
  if (!team?.start_period || team.amount === null || team.amount === undefined || team.amount === '') {
    return [];
  }

  const normalizedEvents = normalizeIncrementEvents(incrementRows);
  const segments = [];
  let currentStart = team.start_period;
  let currentAmount = parseFloat(team.amount);

  for (const event of normalizedEvents) {
    if (comparePeriods(event.effective_period, currentStart) <= 0) {
      currentStart = event.effective_period;
      currentAmount = event.amount;
      continue;
    }

    segments.push({
      from_period: currentStart,
      to_period: getPreviousPeriod(event.effective_period),
      amount: currentAmount,
      source: segments.length === 0 ? 'base' : 'increment'
    });

    currentStart = event.effective_period;
    currentAmount = event.amount;
  }

  segments.push({
    from_period: currentStart,
    to_period: team.end_period || null,
    amount: currentAmount,
    source: normalizedEvents.length === 0 ? 'base' : 'increment'
  });

  return segments;
};

const hasExplicitCompensationHistory = (team, rows = []) => {
  const normalizedRows = normalizeStoredCompensationRows(rows);
  if (!normalizedRows.length) return false;

  return normalizedRows.some((row) => row.end_period) ||
    normalizedRows[0].start_period === team?.start_period;
};

const deriveCompensationHistory = (team, incrementRows = []) => {
  const normalizedRows = normalizeStoredCompensationRows(incrementRows);

  if (!normalizedRows.length) {
    if (!team?.start_period || team.amount === null || team.amount === undefined || team.amount === '') {
      return [];
    }

    return [{
      from_period: team.start_period,
      to_period: team.end_period || null,
      amount: parseFloat(team.amount),
      source: 'base',
    }];
  }

  if (!hasExplicitCompensationHistory(team, normalizedRows)) {
    return deriveLegacyCompensationHistory(team, normalizedRows);
  }

  return normalizedRows.map((row, index) => ({
    from_period: row.start_period,
    to_period: row.end_period || null,
    amount: row.amount,
    source: index === 0 ? 'base' : 'increment',
  }));
};

const toCompensationRows = (team, incrementRows = []) =>
  deriveCompensationHistory(team, incrementRows).map((segment) => ({
    start_period: segment.from_period,
    end_period: segment.to_period,
    amount: segment.amount
  }));

const normalizeCompensationRows = (rows = []) =>
  rows
    .filter((row) => row && row.start_period && row.amount !== undefined && row.amount !== null && row.amount !== '')
    .map((row) => ({
      start_period: row.start_period,
      end_period: row.end_period || null,
      amount: parseFloat(row.amount)
    }))
    .sort((a, b) => comparePeriods(a.start_period, b.start_period));

const validateCompensationRows = (rows) => {
  if (!rows.length) return 'At least one compensation row is required';

  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i];
    if (!PERIOD_REGEX.test(row.start_period)) {
      return 'Compensation row start period must be in MMM-YY format';
    }
    if (row.end_period && !PERIOD_REGEX.test(row.end_period)) {
      return 'Compensation row end period must be in MMM-YY format';
    }
    if (row.end_period && comparePeriods(row.end_period, row.start_period) < 0) {
      return 'Compensation row end period cannot be before start period';
    }
    if (Number.isNaN(row.amount) || row.amount < 0) {
      return 'Compensation row amount must be a positive number';
    }

    const nextRow = rows[i + 1];
    if (nextRow) {
      if (comparePeriods(nextRow.start_period, row.start_period) <= 0) {
        return 'Compensation rows must be in increasing period order';
      }
      if (!row.end_period) {
        return 'Close the current compensation row before adding a later period';
      }
      if (comparePeriods(nextRow.start_period, row.end_period) <= 0) {
        return 'Compensation periods cannot overlap';
      }
    }
  }

  const openRows = rows.filter((row) => !row.end_period);
  if (openRows.length > 1) {
    return 'Only one compensation row can be open-ended';
  }
  if (openRows.length === 1 && rows[rows.length - 1].end_period) {
    return 'Only the latest compensation row can be open-ended';
  }

  return null;
};

const compensationRowsToPayload = (rows) => {
  const normalizedRows = normalizeCompensationRows(rows);
  const firstRow = normalizedRows[0];
  const incrementEvents = normalizedRows.map((row) => ({
    effective_period: row.start_period,
    end_period: row.end_period || null,
    amount: row.amount
  }));

  return {
    normalizedRows,
    start_period: firstRow.start_period,
    amount: firstRow.amount,
    end_period: normalizedRows[normalizedRows.length - 1].end_period || null,
    increment_period: incrementEvents[1]?.effective_period || null,
    incrementEvents
  };
};

const getTeamActiveStatus = (endPeriod) => !endPeriod;

const loadTeamAllocationRows = async (dbClient, teamId) => {
  const result = await dbClient.query(`
    SELECT
      tca.id,
      tca.client_id,
      tca.allocation_percentage,
      tca.allocation_amount,
      tca.allocation_method,
      tca.expense_head_id,
      tca.start_period,
      tca.end_period,
      c.name as client_name,
      eh.name as expense_head_name
    FROM team_client_allocations tca
    LEFT JOIN clients c ON tca.client_id = c.id
    LEFT JOIN expense_heads eh ON tca.expense_head_id = eh.id
    WHERE tca.team_id = $1
    ORDER BY c.name, tca.start_period, tca.id
  `, [teamId]);

  return normalizeTeamAllocationRows(result.rows);
};

const getActiveClientAllocations = (allocationRows = [], compensationRows = []) => {
  const liveRow = getLatestPeriodRow(compensationRows);
  return filterAllocationsForPeriod(allocationRows, liveRow).filter(
    (allocation) =>
      getStoredTeamAllocationAmount(allocation, liveRow?.amount) > 0 ||
      (parseFloat(allocation.allocation_percentage) || 0) > 0
  );
};

const toIncrementHistoryRows = (events = []) =>
  (Array.isArray(events) ? events : []).map((row) => ({
    start_period: row.effective_period,
    end_period: row.end_period || null,
    amount: row.amount,
  }));

const getAllocationKey = (allocation) => `${allocation.client_id}||${allocation.expense_head_id || ''}`;

const roundAmount = roundCurrency;
const roundAllocationPercentage = roundPercentage;

const normalizeDesiredAllocations = (allocations = [], periodAmount = 0) => {
  const cleanedAllocations = (Array.isArray(allocations) ? allocations : [])
    .filter((allocation) => allocation && allocation.client_id)
    .map((allocation) => ({
      client_id: allocation.client_id,
      expense_head_id: allocation.expense_head_id || null,
      allocation_percentage: roundAllocationPercentage(allocation.allocation_percentage),
      allocation_amount:
        allocation.allocation_amount === undefined || allocation.allocation_amount === null || allocation.allocation_amount === ''
          ? null
          : roundAmount(allocation.allocation_amount),
      allocation_method: normalizeAllocationMethod(allocation.allocation_method),
    }));

  if (!cleanedAllocations.length) {
    return {
      allocations: [],
      mode: 'percentage',
      hasMixedModes: false,
      totalAmount: 0,
      totalPercentage: 0,
    };
  }

  const modes = [...new Set(cleanedAllocations.map((allocation) => allocation.allocation_method))];
  const mode = modes[0] || 'percentage';
  const hasMixedModes = modes.length > 1;

  if (mode === 'manual') {
    const manualAllocations = cleanedAllocations.map((allocation) => {
      const allocationAmount = roundAmount(allocation.allocation_amount);
      return {
        ...allocation,
        allocation_amount: allocationAmount,
        allocation_percentage:
          periodAmount > 0 ? roundAllocationPercentage((allocationAmount / periodAmount) * 100) : 0,
      };
    });

    return {
      allocations: manualAllocations,
      mode,
      hasMixedModes,
      totalAmount: roundAmount(
        manualAllocations.reduce((sum, allocation) => sum + allocation.allocation_amount, 0)
      ),
      totalPercentage: roundAllocationPercentage(
        manualAllocations.reduce((sum, allocation) => sum + allocation.allocation_percentage, 0)
      ),
    };
  }

  const percentageAllocations = cleanedAllocations.map((allocation) => ({
    ...allocation,
    allocation_percentage: roundAllocationPercentage(allocation.allocation_percentage),
    allocation_amount: roundAmount(
      ((parseFloat(allocation.allocation_percentage) || 0) / 100) * (parseFloat(periodAmount) || 0)
    ),
  }));

  const roundedTotal = roundAmount(
    percentageAllocations.reduce((sum, allocation) => sum + allocation.allocation_amount, 0)
  );
  const remainder = roundAmount((parseFloat(periodAmount) || 0) - roundedTotal);

  if (percentageAllocations.length > 0 && Math.abs(remainder) >= 0.01) {
    const adjustmentIndex = percentageAllocations.reduce((bestIndex, allocation, index, rows) => (
      allocation.allocation_amount > rows[bestIndex].allocation_amount ? index : bestIndex
    ), 0);
    percentageAllocations[adjustmentIndex] = {
      ...percentageAllocations[adjustmentIndex],
      allocation_amount: roundAmount(percentageAllocations[adjustmentIndex].allocation_amount + remainder),
    };
  }

  return {
    allocations: percentageAllocations,
    mode,
    hasMixedModes,
    totalAmount: roundAmount(
      percentageAllocations.reduce((sum, allocation) => sum + allocation.allocation_amount, 0)
    ),
    totalPercentage: roundAllocationPercentage(
      percentageAllocations.reduce((sum, allocation) => sum + allocation.allocation_percentage, 0)
    ),
  };
};

const validateDesiredAllocations = ({ allocations, periodAmount, label = 'Client allocations' }) => {
  const cleanedAllocations = Array.isArray(allocations)
    ? allocations.filter((allocation) => allocation && allocation.client_id)
    : [];

  if (!cleanedAllocations.length) {
    return { allocations: [] };
  }

  const allocationKeys = cleanedAllocations.map(getAllocationKey);
  if (new Set(allocationKeys).size !== allocationKeys.length) {
    return { error: 'Duplicate client + expense head combination found in allocations' };
  }

  const normalized = normalizeDesiredAllocations(cleanedAllocations, periodAmount);
  if (normalized.hasMixedModes) {
    return { error: `${label} must use either percentage mode or manual amount mode for all rows` };
  }

  if (normalized.mode === 'manual') {
    if ((parseFloat(periodAmount) || 0) <= 0) {
      return { error: `Enter a valid amount before saving ${label.toLowerCase()}` };
    }
    if (Math.abs(normalized.totalAmount - roundAmount(periodAmount)) > 0.01) {
      return {
        error: `${label} must total INR ${roundAmount(periodAmount).toFixed(2)} (current: INR ${normalized.totalAmount.toFixed(2)})`,
      };
    }
    return normalized;
  }

  if (Math.abs(normalized.totalPercentage - 100) > 0.01) {
    return {
      error: `${label} must total 100% (current: ${normalized.totalPercentage.toFixed(2)}%)`,
    };
  }

  if ((parseFloat(periodAmount) || 0) <= 0) {
    return { error: `Enter a valid amount before saving ${label.toLowerCase()}` };
  }

  return normalized;
};

const isPeriodInRange = (periodName, startPeriod, endPeriod) => {
  if (!periodName || !startPeriod) return false;
  if (comparePeriods(periodName, startPeriod) < 0) return false;
  if (endPeriod && comparePeriods(periodName, endPeriod) > 0) return false;
  return true;
};

const hasCompensationCoverageForPeriod = (compensationRows = [], periodName) =>
  (Array.isArray(compensationRows) ? compensationRows : []).some((row) =>
    isPeriodInRange(periodName, row.start_period, row.end_period)
  );

const normalizeAllocationSignature = (allocations = []) =>
  (Array.isArray(allocations) ? allocations : [])
    .filter((allocation) => allocation && allocation.client_id)
    .map((allocation) => ({
      client_id: allocation.client_id,
      expense_head_id: allocation.expense_head_id || null,
      allocation_amount: roundAmount(getStoredTeamAllocationAmount(allocation)),
    }))
    .sort((left, right) =>
      `${left.client_id}||${left.expense_head_id || ''}`.localeCompare(
        `${right.client_id}||${right.expense_head_id || ''}`
      )
    );

const sameAllocationSet = (left = [], right = []) => {
  const normalizedLeft = normalizeAllocationSignature(left);
  const normalizedRight = normalizeAllocationSignature(right);
  if (normalizedLeft.length !== normalizedRight.length) return false;

  return normalizedLeft.every((allocation, index) => {
    const nextAllocation = normalizedRight[index];
    return allocation.client_id === nextAllocation.client_id &&
      allocation.expense_head_id === nextAllocation.expense_head_id &&
      allocation.allocation_amount === nextAllocation.allocation_amount;
  });
};

const sameCompensationRows = (left = [], right = []) => {
  const normalizedLeft = normalizeCompensationRows(left);
  const normalizedRight = normalizeCompensationRows(right);
  if (normalizedLeft.length !== normalizedRight.length) return false;

  return normalizedLeft.every((row, index) => {
    const nextRow = normalizedRight[index];
    return row.start_period === nextRow.start_period &&
      (row.end_period || null) === (nextRow.end_period || null) &&
      roundAmount(row.amount) === roundAmount(nextRow.amount);
  });
};

const getEffectiveExpenseHeadId = (allocation, team) => allocation?.expense_head_id || team?.expense_head_id || null;

const formatLinkedPeriods = (rows = []) => {
  const periods = [...new Set((Array.isArray(rows) ? rows : []).map((row) => row.period_name).filter(Boolean))];
  if (!periods.length) return '';
  if (periods.length <= 5) return periods.join(', ');
  return `${periods.slice(0, 5).join(', ')} +${periods.length - 5} more`;
};

const getCompensationAmountForPeriod = (compensationRows = [], periodName) => {
  if (!periodName) return 0;

  const match = (Array.isArray(compensationRows) ? compensationRows : []).find((row) => {
    if (!row?.start_period) return false;
    if (comparePeriods(periodName, row.start_period) < 0) return false;
    if (row.end_period && comparePeriods(periodName, row.end_period) > 0) return false;
    return true;
  });

  return roundAmount(match?.amount || 0);
};

const getTeamRecurringExpenseOverrideConflicts = async ({
  dbClient,
  currentTeam,
  nextTeam,
  currentCompensationRows,
  nextCompensationRows,
  currentAllocationRows,
  desiredActiveAllocations,
}) => {
  const currentById = new Map(
    (Array.isArray(currentAllocationRows) ? currentAllocationRows : [])
      .filter((row) => row?.id)
      .map((row) => [row.id, row])
  );
  const allocationIds = [...currentById.keys()];
  if (!allocationIds.length) return [];

  const desiredByKey = new Map(
    (Array.isArray(desiredActiveAllocations) ? desiredActiveAllocations : [])
      .filter((allocation) => allocation && allocation.client_id)
      .map((allocation) => [getAllocationKey(allocation), allocation])
  );

  const expenseResult = await dbClient.query(`
    SELECT e.id, e.team_client_allocation_id, sp.display_name AS period_name
    FROM expenses e
    JOIN service_periods sp ON sp.id = e.service_period_id
    WHERE e.is_active = true
      AND e.source_type = 'team-recurring'
      AND e.team_client_allocation_id = ANY($1::uuid[])
      AND sp.start_date >= DATE_TRUNC('month', CURRENT_DATE)::date
    ORDER BY sp.start_date, e.created_at, e.id
  `, [allocationIds]);

  return expenseResult.rows.filter((expense) => {
    const currentAllocation = currentById.get(expense.team_client_allocation_id);
    if (!currentAllocation) return true;

    const periodName = expense.period_name;
    const currentCovered = isPeriodInRange(periodName, currentAllocation.start_period, currentAllocation.end_period) &&
      hasCompensationCoverageForPeriod(currentCompensationRows, periodName);
    if (!currentCovered) return true;

    const desiredAllocation = desiredByKey.get(getAllocationKey(currentAllocation));
    const nextCovered = Boolean(desiredAllocation) && hasCompensationCoverageForPeriod(nextCompensationRows, periodName);
    if (!nextCovered) return true;

    const currentProjectedAmount = roundAmount(
      getStoredTeamAllocationAmount(currentAllocation, getCompensationAmountForPeriod(currentCompensationRows, periodName))
    );
    const nextProjectedAmount = roundAmount(
      getStoredTeamAllocationAmount(desiredAllocation, getCompensationAmountForPeriod(nextCompensationRows, periodName))
    );

    return currentProjectedAmount !== nextProjectedAmount ||
      getEffectiveExpenseHeadId(currentAllocation, currentTeam) !== getEffectiveExpenseHeadId(desiredAllocation, nextTeam);
  });
};

const assertNoBlockingTeamRecurringExpenseOverrides = async (args) => {
  const conflicts = await getTeamRecurringExpenseOverrideConflicts(args);
  if (!conflicts.length) return;

  const error = new Error(
    `This team master is linked to overridden projected expense item(s) for ${formatLinkedPeriods(conflicts)}. Revert those expense rows back to projected state before modifying the team master.`
  );
  error.code = 'LINKED_PROJECTED_EXPENSE';
  throw error;
};

const syncTeamRecurringExpenseAmounts = async ({
  dbClient,
  currentCompensationRows,
  nextCompensationRows,
  currentAllocationRows,
  nextAllocationRows,
  updatedBy = null,
}) => {
  const currentById = new Map(
    (Array.isArray(currentAllocationRows) ? currentAllocationRows : [])
      .filter((row) => row?.id)
      .map((row) => [row.id, row])
  );
  const nextById = new Map(
    (Array.isArray(nextAllocationRows) ? nextAllocationRows : [])
      .filter((row) => row?.id)
      .map((row) => [row.id, row])
  );
  const sharedAllocationIds = [...nextById.keys()].filter((id) => currentById.has(id));
  if (!sharedAllocationIds.length) return 0;

  const expenseResult = await dbClient.query(`
    SELECT e.id, e.amount, e.team_client_allocation_id, sp.display_name AS period_name
    FROM expenses e
    JOIN service_periods sp ON sp.id = e.service_period_id
    WHERE e.is_active = true
      AND e.source_type = 'team-recurring'
      AND e.team_client_allocation_id = ANY($1::uuid[])
      AND sp.start_date >= DATE_TRUNC('month', CURRENT_DATE)::date
  `, [sharedAllocationIds]);

  let updatedCount = 0;

  for (const expense of expenseResult.rows) {
    const currentAllocation = currentById.get(expense.team_client_allocation_id);
    const nextAllocation = nextById.get(expense.team_client_allocation_id);
    if (!currentAllocation || !nextAllocation) continue;

    const oldProjectedAmount = roundAmount(
      getStoredTeamAllocationAmount(currentAllocation, getCompensationAmountForPeriod(currentCompensationRows, expense.period_name))
    );
    const newProjectedAmount = roundAmount(
      getStoredTeamAllocationAmount(nextAllocation, getCompensationAmountForPeriod(nextCompensationRows, expense.period_name))
    );
    const savedAmount = roundAmount(expense.amount);

    if (savedAmount !== oldProjectedAmount || oldProjectedAmount === newProjectedAmount) {
      continue;
    }

    await dbClient.query(`
      UPDATE expenses
      SET amount = $1,
          total_amount = $1 + COALESCE(igst, 0) + COALESCE(cgst, 0) + COALESCE(sgst, 0) + COALESCE(other_charges, 0) + COALESCE(round_off, 0),
          updated_at = NOW(),
          updated_by = COALESCE($2::uuid, updated_by)
      WHERE id = $3
    `, [newProjectedAmount, updatedBy, expense.id]);

    updatedCount += 1;
  }

  return updatedCount;
};

const retireOrphanedAllocationRows = async (dbClient, teamId, compensationRows, updatedBy) => {
  const validPeriods = (compensationRows || []).map((row) => ({
    start: row.start_period,
    end: row.end_period || null,
  }));

  const allRows = await dbClient.query(`
    SELECT id, start_period, end_period, allocation_percentage, allocation_amount
    FROM team_client_allocations
    WHERE team_id = $1
  `, [teamId]);

  const toRetire = [];
  for (const row of allRows.rows) {
    const matches = validPeriods.some(
      (p) => p.start === row.start_period && (p.end || null) === (row.end_period || null)
    );
    if (!matches && (parseFloat(row.allocation_percentage) > 0 || roundAmount(row.allocation_amount) > 0)) {
      toRetire.push(row.id);
    }
  }

  if (toRetire.length > 0) {
    await dbClient.query(`
      UPDATE team_client_allocations
      SET allocation_percentage = 0,
          allocation_amount = 0
      WHERE id = ANY($1::uuid[])
    `, [toRetire]);
    await deactivateTeamRecurringExpensesForAllocations(dbClient, toRetire, updatedBy);
  }

  return toRetire.length;
};

const syncTeamAllocationRowsForPeriod = async ({
  dbClient,
  teamId,
  periodRow,
  allocations,
  updatedBy = null,
}) => {
  if (!periodRow?.start_period) return;

  const existingResult = await dbClient.query(`
    SELECT id, client_id, allocation_percentage, allocation_amount, allocation_method, expense_head_id, start_period, end_period
    FROM team_client_allocations
    WHERE team_id = $1
      AND start_period = $2
      AND end_period IS NOT DISTINCT FROM $3
    ORDER BY id
  `, [teamId, periodRow.start_period, periodRow.end_period || null]);

  const existingRows = normalizeTeamAllocationRows(existingResult.rows);
  const existingByKey = new Map(existingRows.map((row) => [getAllocationKey(row), row]));
  const desiredAllocations = (Array.isArray(allocations) ? allocations : [])
    .filter((allocation) => allocation && allocation.client_id)
    .map((allocation) => ({
      client_id: allocation.client_id,
      allocation_percentage: parseFloat(allocation.allocation_percentage) || 0,
      allocation_amount: roundAmount(allocation.allocation_amount),
      allocation_method: normalizeAllocationMethod(allocation.allocation_method),
      expense_head_id: allocation.expense_head_id || null,
    }));

  const syncedKeys = new Set();
  const retiredAllocationIds = [];

  for (const allocation of desiredAllocations) {
    const allocationKey = getAllocationKey(allocation);
    const existing = existingByKey.get(allocationKey);

    if (existing) {
      await dbClient.query(`
        UPDATE team_client_allocations
        SET allocation_percentage = $1,
            allocation_amount = $2,
            allocation_method = $3,
            expense_head_id = $4,
            start_period = $5,
            end_period = $6
        WHERE id = $7
      `, [
        allocation.allocation_percentage,
        allocation.allocation_amount,
        allocation.allocation_method,
        allocation.expense_head_id,
        periodRow.start_period,
        periodRow.end_period || null,
        existing.id,
      ]);
    } else {
      await dbClient.query(`
        INSERT INTO team_client_allocations (
          team_id, client_id, allocation_percentage, allocation_amount, allocation_method, expense_head_id, start_period, end_period
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      `, [
        teamId,
        allocation.client_id,
        allocation.allocation_percentage,
        allocation.allocation_amount,
        allocation.allocation_method,
        allocation.expense_head_id,
        periodRow.start_period,
        periodRow.end_period || null,
      ]);
    }

    syncedKeys.add(allocationKey);
  }

  for (const existing of existingRows) {
    if (syncedKeys.has(getAllocationKey(existing))) continue;

    await dbClient.query(`
      UPDATE team_client_allocations
      SET allocation_percentage = 0,
          allocation_amount = 0,
          start_period = $1,
          end_period = $2
      WHERE id = $3
    `, [
      periodRow.start_period,
      periodRow.end_period || null,
      existing.id,
    ]);
    retiredAllocationIds.push(existing.id);
  }

  await deactivateTeamRecurringExpensesForAllocations(dbClient, retiredAllocationIds, updatedBy);
};

// Get all teams
router.get('/', auth, async (req, res) => {
  try {
    await ensureTeamSchema();
    const { includeInactive, reviewersOnly } = req.query;
    
    let query = `
      SELECT t.*, 
             eh.name as expense_head_name,
             COALESCE((
               SELECT json_agg(
                 json_build_object(
                 'id', tca.id,
                 'client_id', tca.client_id,
                 'client_name', c.name,
                 'allocation_percentage', tca.allocation_percentage,
                 'allocation_amount', tca.allocation_amount,
                 'allocation_method', tca.allocation_method,
                 'expense_head_id', tca.expense_head_id,
                 'expense_head_name', eh2.name,
                 'start_period', tca.start_period,
                 'end_period', tca.end_period
                 ) ORDER BY c.name
               )
               FROM team_client_allocations tca
               LEFT JOIN clients c ON tca.client_id = c.id
               LEFT JOIN expense_heads eh2 ON tca.expense_head_id = eh2.id
               WHERE tca.team_id = t.id
             ), '[]') as client_allocations,
             COALESCE((
               SELECT json_agg(
                 json_build_object(
                 'id', tih.id,
                 'start_period', tih.start_period,
                 'end_period', tih.end_period,
                 'amount', tih.amount
                 ) ORDER BY tih.start_period
               )
               FROM team_increment_history tih
               WHERE tih.team_id = t.id
             ), '[]') as increment_history
      FROM teams t
      LEFT JOIN expense_heads eh ON t.expense_head_id = eh.id
      WHERE 1=1
    `;
    
    if (!includeInactive) {
      query += ' AND t.is_active = true';
    }
    
    if (reviewersOnly === 'true') {
      query += ' AND t.is_reviewer = true';
    }
    
    query += ' ORDER BY t.name';
    
    const result = await pool.query(query);
    
    const data = result.rows.map((row) => {
      const compensationHistory = deriveCompensationHistory(row, row.increment_history);
      const compensationRows = toCompensationRows(row, row.increment_history);

      return {
        ...row,
        client_allocations: getActiveClientAllocations(row.client_allocations, compensationRows),
        increment_events: normalizeIncrementEvents(row.increment_history),
        compensation_history: compensationHistory,
        compensation_rows: compensationRows,
      };
    });

    res.json({
      success: true,
      data
    });
  } catch (error) {
    console.error('Get teams error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch teams'
    });
  }
});

// Get reviewers only (for dropdown)
router.get('/reviewers', auth, async (req, res) => {
  try {
    await ensureTeamSchema();
    const result = await pool.query(`
      SELECT id, name, mobile, email, amount, start_period, increment_period, end_period
      FROM teams 
      WHERE is_reviewer = true AND is_active = true 
      ORDER BY name
    `);
    
    res.json({
      success: true,
      data: result.rows
    });
  } catch (error) {
    console.error('Get reviewers error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch reviewers'
    });
  }
});

// Get single team
router.get('/:id', auth, async (req, res) => {
  try {
    await ensureTeamSchema();
    const teamResult = await pool.query(
      'SELECT * FROM teams WHERE id = $1',
      [req.params.id]
    );
    
    if (teamResult.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Team member not found'
      });
    }

    // Get allocations
    const allocationsResult = await pool.query(`
      SELECT
        tca.id,
        tca.client_id,
        tca.allocation_percentage,
        tca.allocation_amount,
        tca.allocation_method,
        tca.expense_head_id,
        tca.start_period,
        tca.end_period,
        c.name as client_name,
        eh.name as expense_head_name
      FROM team_client_allocations tca
      JOIN clients c ON tca.client_id = c.id
      LEFT JOIN expense_heads eh ON tca.expense_head_id = eh.id
      WHERE tca.team_id = $1
      ORDER BY c.name, tca.start_period, tca.id
    `, [req.params.id]);

    const historyResult = await pool.query(`
      SELECT id, start_period, end_period, amount
      FROM team_increment_history
      WHERE team_id = $1
      ORDER BY start_period
    `, [req.params.id]);

    const compensationRows = toCompensationRows(teamResult.rows[0], historyResult.rows);
    const summarizedCompensationRows = await summarizeTeamCompensationRows(pool, req.params.id, compensationRows);
    const activeClientAllocations = getActiveClientAllocations(allocationsResult.rows, compensationRows);
    
    res.json({
      success: true,
      data: {
        ...teamResult.rows[0],
        client_allocations: activeClientAllocations,
        increment_events: normalizeIncrementEvents(historyResult.rows),
        compensation_history: deriveCompensationHistory(teamResult.rows[0], historyResult.rows),
        compensation_rows: summarizedCompensationRows
      }
    });
  } catch (error) {
    console.error('Get team error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch team member'
    });
  }
});

// Create team member
router.post('/', auth, [
  body('name').trim().notEmpty().withMessage('Name is required'),
  body('mobile').optional({ values: 'falsy' }).matches(/^[0-9]{10}$/).withMessage('Invalid mobile number'),
  body('email').optional({ values: 'falsy' }).isEmail().withMessage('Invalid email format'),
  body('is_reviewer').optional().isBoolean(),
  body('is_admin').optional().isBoolean(),
  body('expense_head_id').optional({ values: 'falsy' }).isUUID().withMessage('Invalid expense head'),
  body('amount').optional({ values: 'falsy' }).isFloat({ min: 0 }).withMessage('Amount must be a positive number'),
  body('start_period').optional({ values: 'falsy' }).matches(PERIOD_REGEX).withMessage('Start period must be in MMM-YY format'),
  body('increment_period').optional({ values: 'falsy' }).matches(PERIOD_REGEX).withMessage('Increment period must be in MMM-YY format'),
  body('end_period').optional({ values: 'falsy' }).matches(PERIOD_REGEX).withMessage('End period must be in MMM-YY format'),
  body('client_allocations').optional().isArray(),
  body('increment_events').optional().isArray(),
  body('compensation_rows').optional().isArray(),
], async (req, res) => {
  const client = await pool.connect();
  
  try {
    await ensureTeamSchema();
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        errors: errors.array()
      });
    }

    const {
      name, mobile, email, is_reviewer, is_admin, expense_head_id, amount, start_period,
      increment_period, end_period, client_allocations, increment_events, compensation_rows
    } = req.body;
    const cleanedAllocations = Array.isArray(client_allocations)
      ? client_allocations.filter((alloc) => alloc && alloc.client_id)
      : [];
    const cleanedHistory = Array.isArray(increment_events)
      ? increment_events.filter((row) => row && row.effective_period && row.amount !== undefined && row.amount !== null && row.amount !== '')
      : [];
    let finalAmount = amount;
    let finalStartPeriod = start_period;
    let finalEndPeriod = end_period;
    let finalIncrementPeriod = increment_period;
    let finalHistory = cleanedHistory;

    if (Array.isArray(compensation_rows) && compensation_rows.length > 0) {
      const normalizedRows = normalizeCompensationRows(compensation_rows);
      const rowError = validateCompensationRows(normalizedRows);
      if (rowError) {
        return res.status(400).json({ success: false, message: rowError });
      }

      const payload = compensationRowsToPayload(normalizedRows);
      finalAmount = payload.amount;
      finalStartPeriod = payload.start_period;
      finalEndPeriod = payload.end_period;
      finalIncrementPeriod = payload.increment_period;
      finalHistory = payload.incrementEvents;
    }

    if ((finalHistory.length > 0 || finalAmount !== undefined || finalStartPeriod) && (!finalStartPeriod || finalAmount === undefined || finalAmount === null || finalAmount === '')) {
      return res.status(400).json({
        success: false,
        message: 'Start period and amount are required to maintain compensation history'
      });
    }

    for (const row of finalHistory) {
      if (!PERIOD_REGEX.test(row.effective_period)) {
        return res.status(400).json({ success: false, message: 'Increment effective period must be in MMM-YY format' });
      }
      if (row.end_period && !PERIOD_REGEX.test(row.end_period)) {
        return res.status(400).json({ success: false, message: 'Increment end period must be in MMM-YY format' });
      }
      if (row.end_period && comparePeriods(row.end_period, row.effective_period) < 0) {
        return res.status(400).json({ success: false, message: 'Increment end period cannot be before start period' });
      }
      if (parseFloat(row.amount) < 0) {
        return res.status(400).json({ success: false, message: 'Increment amount must be a positive number' });
      }
    }

    const finalCompensationRows = toCompensationRows({
      start_period: finalStartPeriod,
      end_period: finalEndPeriod,
      amount: finalAmount,
    }, toIncrementHistoryRows(finalHistory));
    const liveCompensationRow = getLatestPeriodRow(finalCompensationRows);
    const normalizedLiveAllocations = validateDesiredAllocations({
      allocations: cleanedAllocations,
      periodAmount: liveCompensationRow?.amount || 0,
      label: 'Client allocations',
    });
    if (normalizedLiveAllocations.error) {
      return res.status(400).json({ success: false, message: normalizedLiveAllocations.error });
    }

    await client.query('BEGIN');

    // Create team member
    const teamResult = await client.query(
      `INSERT INTO teams (name, mobile, email, is_reviewer, is_admin, expense_type, expense_head_id, amount, start_period, increment_period, end_period, is_active) 
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) RETURNING *`,
      [
        name, mobile || null, email || null, is_reviewer || false, is_admin || false, 'non-recurring',
        expense_head_id || null, finalAmount || null, finalStartPeriod || null, finalIncrementPeriod || null, finalEndPeriod || null,
        getTeamActiveStatus(finalEndPeriod)
      ]
    );

    const team = teamResult.rows[0];

    // Add client allocations
    if (normalizedLiveAllocations.allocations.length > 0 && liveCompensationRow?.start_period) {
      for (const alloc of normalizedLiveAllocations.allocations) {
        await client.query(
          `INSERT INTO team_client_allocations (
             team_id, client_id, allocation_percentage, allocation_amount, allocation_method, expense_head_id, start_period, end_period
           )
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
          [
            team.id,
            alloc.client_id,
            alloc.allocation_percentage,
            alloc.allocation_amount,
            alloc.allocation_method,
            alloc.expense_head_id || null,
            liveCompensationRow.start_period,
            liveCompensationRow.end_period || null,
          ]
        );
      }
    }

    if (finalHistory.length > 0) {
      for (const row of finalHistory) {
        await client.query(
          `INSERT INTO team_increment_history (team_id, start_period, end_period, amount)
           VALUES ($1, $2, $3, $4)`,
          [team.id, row.effective_period, row.end_period || null, row.amount]
        );
      }
    }

    await client.query('COMMIT');

    await logAudit('teams', team.id, 'CREATE', null, { ...team, client_allocations: normalizedLiveAllocations.allocations, increment_events: finalHistory }, req.user.id);

    res.status(201).json({
      success: true,
      message: 'Team member created successfully',
      data: {
        ...team,
        client_allocations: normalizedLiveAllocations.allocations,
        increment_events: finalHistory,
        compensation_history: deriveCompensationHistory(team, finalHistory.map((row) => ({
          start_period: row.effective_period,
          end_period: row.end_period || null,
          amount: row.amount,
        }))),
        compensation_rows: toCompensationRows(team, finalHistory.map((row) => ({
          start_period: row.effective_period,
          end_period: row.end_period || null,
          amount: row.amount,
        })))
      }
    });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Create team error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to create team member'
    });
  } finally {
    client.release();
  }
});

// Update team member
router.put('/:id', auth, [
  body('name').optional().trim().notEmpty().withMessage('Name cannot be empty'),
  body('mobile').optional({ values: 'falsy' }).matches(/^[0-9]{10}$/).withMessage('Invalid mobile number'),
  body('email').optional({ values: 'falsy' }).isEmail().withMessage('Invalid email format'),
  body('is_reviewer').optional().isBoolean(),
  body('is_admin').optional().isBoolean(),
  body('expense_head_id').optional({ values: 'falsy' }).isUUID().withMessage('Invalid expense head'),
  body('amount').optional({ values: 'falsy' }).isFloat({ min: 0 }).withMessage('Amount must be a positive number'),
  body('start_period').optional({ values: 'falsy' }).matches(PERIOD_REGEX).withMessage('Start period must be in MMM-YY format'),
  body('increment_period').optional({ values: 'falsy' }).matches(PERIOD_REGEX).withMessage('Increment period must be in MMM-YY format'),
  body('end_period').optional({ values: 'falsy' }).matches(PERIOD_REGEX).withMessage('End period must be in MMM-YY format'),
  body('client_allocations').optional().isArray(),
  body('increment_events').optional().isArray(),
  body('compensation_rows').optional().isArray(),
  body('is_active').optional().isBoolean(),
], async (req, res) => {
  const dbClient = await pool.connect();
  
  try {
    await ensureTeamSchema();
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        errors: errors.array()
      });
    }

    const {
      name, mobile, email, is_reviewer, is_admin, expense_head_id, amount, start_period,
      increment_period, end_period, client_allocations, increment_events, compensation_rows
    } = req.body;
    const { id } = req.params;
    const cleanedAllocations = Array.isArray(client_allocations)
      ? client_allocations.filter((alloc) => alloc && alloc.client_id)
      : client_allocations;
    const cleanedHistory = Array.isArray(increment_events)
      ? increment_events.filter((row) => row && row.effective_period && row.amount !== undefined && row.amount !== null && row.amount !== '')
      : increment_events;

    const current = await dbClient.query('SELECT * FROM teams WHERE id = $1', [id]);
    if (current.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Team member not found'
      });
    }

    let finalAmount = amount !== undefined ? amount : current.rows[0].amount;
    let finalStartPeriod = start_period !== undefined ? start_period : current.rows[0].start_period;
    let finalEndPeriod = end_period !== undefined ? end_period : current.rows[0].end_period;
    let finalIncrementPeriod = increment_period !== undefined ? increment_period : current.rows[0].increment_period;
    let finalHistory = cleanedHistory;

    if (Array.isArray(compensation_rows) && compensation_rows.length > 0) {
      const normalizedRows = normalizeCompensationRows(compensation_rows);
      const rowError = validateCompensationRows(normalizedRows);
      if (rowError) {
        return res.status(400).json({ success: false, message: rowError });
      }

      const payload = compensationRowsToPayload(normalizedRows);
      finalAmount = payload.amount;
      finalStartPeriod = payload.start_period;
      finalEndPeriod = payload.end_period;
      finalIncrementPeriod = payload.increment_period;
      finalHistory = payload.incrementEvents;
    }

    if ((finalHistory !== undefined || finalStartPeriod || finalAmount !== null) &&
        (!finalStartPeriod || finalAmount === undefined || finalAmount === null || finalAmount === '')) {
      return res.status(400).json({
        success: false,
        message: 'Start period and amount are required to maintain compensation history'
      });
    }

    // Validate allocations sum to 100 if provided
    if (cleanedAllocations && cleanedAllocations.length > 0) {
      const totalAllocation = cleanedAllocations.reduce((sum, a) => sum + parseFloat(a.allocation_percentage || 0), 0);
      if (Math.abs(totalAllocation - 100) > 0.01) {
        return res.status(400).json({
          success: false,
          message: `Client allocations must total 100% (current: ${totalAllocation}%)`
        });
      }
      const allocKeys = cleanedAllocations.map(getAllocationKey);
      if (new Set(allocKeys).size !== allocKeys.length) {
        return res.status(400).json({
          success: false,
          message: 'Duplicate client + expense head combination found in allocations'
        });
      }
    }

    if (finalHistory && finalHistory.length > 0) {
      for (const row of finalHistory) {
        if (!PERIOD_REGEX.test(row.effective_period)) {
          return res.status(400).json({ success: false, message: 'Increment effective period must be in MMM-YY format' });
        }
        if (row.end_period && !PERIOD_REGEX.test(row.end_period)) {
          return res.status(400).json({ success: false, message: 'Increment end period must be in MMM-YY format' });
        }
        if (row.end_period && comparePeriods(row.end_period, row.effective_period) < 0) {
          return res.status(400).json({ success: false, message: 'Increment end period cannot be before start period' });
        }
        if (parseFloat(row.amount) < 0) {
          return res.status(400).json({ success: false, message: 'Increment amount must be a positive number' });
        }
      }
    }

    const currentHistoryResult = await dbClient.query(`
      SELECT id, start_period, end_period, amount
      FROM team_increment_history
      WHERE team_id = $1
      ORDER BY start_period
    `, [id]);
    const currentCompensationRows = toCompensationRows(current.rows[0], currentHistoryResult.rows);
    const currentLiveCompensationRow = getLatestPeriodRow(currentCompensationRows);
    const currentAllocationRows = await loadTeamAllocationRows(dbClient, id);
    const currentActiveAllocations = getActiveClientAllocations(currentAllocationRows, currentCompensationRows).map((allocation) => ({
      client_id: allocation.client_id,
      allocation_percentage: allocation.allocation_percentage,
      expense_head_id: allocation.expense_head_id || null,
    }));
    const effectiveHistoryRows = finalHistory !== undefined
      ? toIncrementHistoryRows(finalHistory)
      : currentHistoryResult.rows;

    const finalCompensationRows = toCompensationRows({
      ...current.rows[0],
      amount: finalAmount,
      start_period: finalStartPeriod,
      end_period: finalEndPeriod,
    }, effectiveHistoryRows);
    const finalLiveCompensationRow = getLatestPeriodRow(finalCompensationRows);

    const desiredActiveAllocations = cleanedAllocations !== undefined
      ? cleanedAllocations.map((allocation) => ({
          client_id: allocation.client_id,
          allocation_percentage: parseFloat(allocation.allocation_percentage) || 0,
          expense_head_id: allocation.expense_head_id || null,
        }))
      : currentActiveAllocations;
    const nextTeamProjectionState = {
      ...current.rows[0],
      expense_head_id: expense_head_id !== undefined ? expense_head_id || null : current.rows[0].expense_head_id,
    };
    const projectedExpenseImpactRequested =
      !sameCompensationRows(currentCompensationRows, finalCompensationRows) ||
      !sameAllocationSet(currentActiveAllocations, desiredActiveAllocations) ||
      getEffectiveExpenseHeadId(null, current.rows[0]) !== getEffectiveExpenseHeadId(null, nextTeamProjectionState);

    if (projectedExpenseImpactRequested) {
      await assertNoBlockingTeamRecurringExpenseOverrides({
        dbClient,
        currentTeam: current.rows[0],
        nextTeam: nextTeamProjectionState,
        currentCompensationRows,
        nextCompensationRows: finalCompensationRows,
        currentAllocationRows,
        desiredActiveAllocations,
      });
    }

    await dbClient.query('BEGIN');

    // Update team fields
    const updates = [];
    const values = [];
    let paramIndex = 1;

    if (name !== undefined) {
      updates.push(`name = $${paramIndex++}`);
      values.push(name);
    }
    if (mobile !== undefined) {
      updates.push(`mobile = $${paramIndex++}`);
      values.push(mobile || null);
    }
    if (email !== undefined) {
      updates.push(`email = $${paramIndex++}`);
      values.push(email || null);
    }
    if (is_reviewer !== undefined) {
      updates.push(`is_reviewer = $${paramIndex++}`);
      values.push(is_reviewer);
    }
    if (is_admin !== undefined) {
      updates.push(`is_admin = $${paramIndex++}`);
      values.push(is_admin);
    }
    if (expense_head_id !== undefined) {
      updates.push(`expense_head_id = $${paramIndex++}`);
      values.push(expense_head_id || null);
    }
    if (finalAmount !== undefined) {
      updates.push(`amount = $${paramIndex++}`);
      values.push(finalAmount || null);
    }
    if (finalStartPeriod !== undefined) {
      updates.push(`start_period = $${paramIndex++}`);
      values.push(finalStartPeriod || null);
    }
    if (finalIncrementPeriod !== undefined) {
      updates.push(`increment_period = $${paramIndex++}`);
      values.push(finalIncrementPeriod || null);
    }
    if (finalEndPeriod !== undefined) {
      updates.push(`end_period = $${paramIndex++}`);
      values.push(finalEndPeriod || null);
    }
    updates.push(`is_active = $${paramIndex++}`);
    values.push(getTeamActiveStatus(finalEndPeriod));

    let teamResult = current;
    if (updates.length > 0) {
      values.push(id);
      const query = `UPDATE teams SET ${updates.join(', ')} WHERE id = $${paramIndex} RETURNING *`;
      teamResult = await dbClient.query(query, values);
    }

    if (finalHistory !== undefined) {
      await dbClient.query('DELETE FROM team_increment_history WHERE team_id = $1', [id]);

      if (finalHistory.length > 0) {
        for (const row of finalHistory) {
          await dbClient.query(
            `INSERT INTO team_increment_history (team_id, start_period, end_period, amount)
             VALUES ($1, $2, $3, $4)`,
            [id, row.effective_period, row.end_period || null, row.amount]
          );
        }
      }
    }

    if (currentLiveCompensationRow?.start_period) {
      const frozenCurrentRow =
        finalCompensationRows.find((row) => row.start_period === currentLiveCompensationRow.start_period) ||
        finalLiveCompensationRow;

      if (frozenCurrentRow?.start_period && !samePeriodRange(currentLiveCompensationRow, frozenCurrentRow)) {
        await dbClient.query(`
          UPDATE team_client_allocations
          SET start_period = $1,
              end_period = $2
          WHERE team_id = $3
            AND start_period = $4
            AND end_period IS NOT DISTINCT FROM $5
        `, [
          frozenCurrentRow.start_period,
          frozenCurrentRow.end_period || null,
          id,
          currentLiveCompensationRow.start_period,
          currentLiveCompensationRow.end_period || null,
        ]);
      }
    }

    if (cleanedAllocations !== undefined) {
      if (finalLiveCompensationRow?.start_period) {
        await syncTeamAllocationRowsForPeriod({
          dbClient,
          teamId: id,
          periodRow: finalLiveCompensationRow,
          allocations: desiredActiveAllocations,
          updatedBy: req.user.id,
        });
      }

      await retireOrphanedAllocationRows(dbClient, id, finalCompensationRows, req.user.id);
    }

    const finalAllocationRows = await loadTeamAllocationRows(dbClient, id);
    await syncTeamRecurringExpenseAmounts({
      dbClient,
      currentCompensationRows,
      nextCompensationRows: finalCompensationRows,
      currentAllocationRows,
      nextAllocationRows: finalAllocationRows,
      updatedBy: req.user.id,
    });

    await dbClient.query('COMMIT');

    // Cleanup any orphaned recurring source records after team modification
    try {
      const cleanupClient = await pool.connect();
      try {
        await cleanupClient.query('BEGIN');
        await cleanupStaleRecurringSourceRecords(cleanupClient, req.user.id);
        await cleanupClient.query('COMMIT');
      } catch (cleanupErr) {
        await cleanupClient.query('ROLLBACK').catch(() => {});
        console.error('Post-team-update cleanup warning:', cleanupErr.message);
      } finally {
        cleanupClient.release();
      }
    } catch (cleanupErr) {
      console.error('Post-team-update cleanup connection warning:', cleanupErr.message);
    }

    await logAudit('teams', id, 'UPDATE', current.rows[0], teamResult.rows[0], req.user.id);

    res.json({
      success: true,
      message: 'Team member updated successfully',
      data: teamResult.rows[0]
    });
  } catch (error) {
    await dbClient.query('ROLLBACK').catch(() => {});
    console.error('Update team error:', error);
    if (error.code === 'LINKED_PROJECTED_EXPENSE') {
      return res.status(400).json({
        success: false,
        message: error.message
      });
    }
    res.status(500).json({
      success: false,
      message: 'Failed to update team member'
    });
  } finally {
    dbClient.release();
  }
});

// Check dependencies for a team
const getTeamDependencies = async (dbClient, id) => {
  const revenueAllocationResult = await queryLinkedRows(dbClient, `
    SELECT
      rra.id,
      COUNT(*) OVER() AS total_count,
      COALESCE(sp.display_name, TO_CHAR(r.date, 'Mon-YY')) AS period_name,
      c.name AS group_name
    FROM revenue_reviewer_allocations rra
    JOIN revenues r ON r.id = rra.revenue_id
    LEFT JOIN service_periods sp ON sp.id = r.service_period_id
    LEFT JOIN clients c ON c.id = r.client_id
    WHERE rra.reviewer_id = $1
    ORDER BY sp.start_date NULLS LAST, r.date NULLS LAST, c.name NULLS LAST
    LIMIT $2
  `, [id]);

  const feeAllocationResult = await queryLinkedRows(dbClient, `
    SELECT
      fra.id,
      COUNT(*) OVER() AS total_count,
      c.name AS group_name,
      st.name AS service_type_name,
      fm.start_period,
      fm.end_period
    FROM fee_reviewer_allocations fra
    JOIN fee_masters fm ON fm.id = fra.fee_master_id
    LEFT JOIN clients c ON c.id = fm.client_id
    LEFT JOIN service_types st ON st.id = fm.service_type_id
    WHERE fra.reviewer_id = $1
    ORDER BY fm.start_period NULLS LAST, c.name NULLS LAST, st.name NULLS LAST
    LIMIT $2
  `, [id]);

  const recurringTeamResult = await queryLinkedRows(dbClient, `
    SELECT
      ret.id,
      COUNT(*) OVER() AS total_count,
      eh.name AS expense_head_name,
      v.name AS vendor_name,
      re.start_period,
      re.end_period
    FROM recurring_expense_teams ret
    JOIN recurring_expenses re ON re.id = ret.recurring_expense_id
    LEFT JOIN expense_heads eh ON eh.id = re.expense_head_id
    LEFT JOIN vendors v ON v.id = re.vendor_id
    WHERE ret.team_id = $1
    ORDER BY re.start_period NULLS LAST, eh.name NULLS LAST, v.name NULLS LAST
    LIMIT $2
  `, [id]);

  const dependencies = [
    buildDependencyEntry({
      type: 'Revenue Allocation',
      module: 'revenue',
      count: revenueAllocationResult.count,
      rows: revenueAllocationResult.rows,
      mapRow: (row) => buildLinkedItem({
        id: row.id,
        label: `${safeText(row.group_name, 'Unassigned group')} | ${safeText(row.period_name, 'No period')}`,
        line: 'Revenue reviewer allocation line',
        module: 'revenue',
        type: 'Revenue Allocation',
      }),
    }),
    buildDependencyEntry({
      type: 'Fee Allocation',
      module: 'revenue',
      count: feeAllocationResult.count,
      rows: feeAllocationResult.rows,
      mapRow: (row) => buildLinkedItem({
        id: row.id,
        label: `${safeText(row.group_name, 'Unassigned group')} | ${safeText(row.service_type_name, 'No service type')}`,
        line: `Fee period: ${formatPeriodRange(row.start_period, row.end_period)}`,
        module: 'revenue',
        type: 'Fee Allocation',
      }),
    }),
    buildDependencyEntry({
      type: 'Recurring Team Line',
      module: 'recurring',
      count: recurringTeamResult.count,
      rows: recurringTeamResult.rows,
      mapRow: (row) => buildLinkedItem({
        id: row.id,
        label: `${safeText(row.expense_head_name, 'No expense head')} | ${safeText(row.vendor_name, 'No vendor')}`,
        line: `Recurring period: ${formatPeriodRange(row.start_period, row.end_period)}`,
        module: 'recurring',
        type: 'Recurring Team Line',
      }),
    }),
  ].filter(Boolean);

  return {
    dependencies,
    total: dependencies.reduce((sum, dependency) => sum + dependency.count, 0),
    message: buildDeleteBlockedMessage('team master', dependencies),
  };
};

router.get('/:id/dependencies', auth, async (req, res) => {
  try {
    await ensureTeamSchema();
    const { id } = req.params;
    const data = await getTeamDependencies(pool, id);
    res.json({ success: true, data });
  } catch (error) {
    console.error('Check team dependencies error:', error);
    res.status(500).json({ success: false, message: 'Failed to check dependencies' });
  }
});

router.delete('/:id', auth, requireDelete, async (req, res) => {
  const dbClient = await pool.connect();
  try {
    await ensureTeamSchema();
    const { id } = req.params;

    const current = await dbClient.query('SELECT * FROM teams WHERE id = $1', [id]);
    if (current.rows.length === 0) {
      return res.status(404).json({ success: false, message: 'Team member not found' });
    }

    const dependencyData = await getTeamDependencies(dbClient, id);
    if (dependencyData.total > 0) {
      return res.status(409).json({
        success: false,
        message: dependencyData.message,
        data: dependencyData,
      });
    }

    // Hard delete: remove all references, then team
    await dbClient.query('BEGIN');
    await dbClient.query('UPDATE client_billing_rows SET reviewer_id = null WHERE reviewer_id = $1', [id]);
    await dbClient.query('UPDATE client_billing_row_history SET reviewer_id = null WHERE reviewer_id = $1', [id]);
    await dbClient.query('UPDATE revenues SET reviewer_id = null WHERE reviewer_id = $1', [id]);
    await dbClient.query('UPDATE expenses SET team_id = null, reviewer_id = null WHERE team_id = $1 OR reviewer_id = $1', [id]);
    await dbClient.query('UPDATE recurring_expense_clients SET reviewer_id = null WHERE reviewer_id = $1', [id]);
    await dbClient.query('DELETE FROM fee_reviewer_allocations WHERE reviewer_id = $1', [id]);
    await dbClient.query('DELETE FROM revenue_reviewer_allocations WHERE reviewer_id = $1', [id]);
    await dbClient.query('DELETE FROM team_client_allocations WHERE team_id = $1', [id]);
    await dbClient.query('DELETE FROM team_increment_history WHERE team_id = $1', [id]);
    await dbClient.query('DELETE FROM teams WHERE id = $1', [id]);
    await dbClient.query('COMMIT');
    
    await logAudit('teams', id, 'DELETE', current.rows[0], null, req.user.id);

    res.json({
      success: true,
      message: 'Team member deleted successfully'
    });
  } catch (error) {
    await dbClient.query('ROLLBACK').catch(() => {});
    console.error('Delete team error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to delete team member: ' + (error.detail || error.message || String(error))
    });
  } finally {
    dbClient.release();
  }
});

// Update a specific compensation period's amount and client allocations
router.put('/:id/compensation-period', auth, async (req, res) => {
  const dbClient = await pool.connect();
  try {
    await ensureTeamSchema();
    const { id } = req.params;
    const { original_start_period, original_end_period, amount, client_allocations } = req.body;

    if (!original_start_period || !PERIOD_REGEX.test(original_start_period)) {
      return res.status(400).json({ success: false, message: 'Valid original start period is required' });
    }
    if (original_end_period && !PERIOD_REGEX.test(original_end_period)) {
      return res.status(400).json({ success: false, message: 'Original end period must be in MMM-YY format' });
    }
    if (amount !== undefined && (amount === '' || amount === null || parseFloat(amount) < 0)) {
      return res.status(400).json({ success: false, message: 'Amount must be a positive number' });
    }

    const current = await dbClient.query('SELECT * FROM teams WHERE id = $1', [id]);
    if (current.rows.length === 0) {
      return res.status(404).json({ success: false, message: 'Team member not found' });
    }

    const team = current.rows[0];
    const historyResult = await dbClient.query(
      'SELECT id, start_period, end_period, amount FROM team_increment_history WHERE team_id = $1 ORDER BY start_period',
      [id]
    );
    const currentCompensationRows = toCompensationRows(team, historyResult.rows);
    const currentAllocationRows = await loadTeamAllocationRows(dbClient, id);
    const explicitHistoryRows = normalizeStoredCompensationRows(historyResult.rows);

    // Find the matching compensation period
    const matchedIndex = currentCompensationRows.findIndex(
      (row) => row.start_period === original_start_period &&
               (row.end_period || null) === (original_end_period || null)
    );
    if (matchedIndex === -1) {
      return res.status(404).json({ success: false, message: 'Compensation period not found' });
    }

    await dbClient.query('BEGIN');

    // Update amount if changed
    if (amount !== undefined) {
      const newAmount = parseFloat(amount);
      const matchedHistoryRow = explicitHistoryRows.find(
        (row) =>
          row.start_period === original_start_period &&
          (row.end_period || null) === (original_end_period || null)
      );
      if (matchedHistoryRow) {
        // First period → update teams.amount (base amount)
        await dbClient.query(
          'UPDATE team_increment_history SET amount = $1 WHERE id = $2',
          [newAmount, matchedHistoryRow.id]
        );
        if (matchedIndex === 0) {
          await dbClient.query('UPDATE teams SET amount = $1 WHERE id = $2', [newAmount, id]);
        }
      } else if (matchedIndex === 0) {
        await dbClient.query('UPDATE teams SET amount = $1 WHERE id = $2', [newAmount, id]);
      } else {
        // Subsequent period → update the corresponding increment_history entry
        const incrementRows = historyResult.rows.sort((a, b) => comparePeriods(a.start_period, b.start_period));
        const incrementEntry = incrementRows[matchedIndex - 1];
        if (incrementEntry) {
          await dbClient.query(
            'UPDATE team_increment_history SET amount = $1 WHERE id = $2',
            [newAmount, incrementEntry.id]
          );
        }
      }
    }

    // Update allocations if provided
    if (Array.isArray(client_allocations)) {
      const cleanedAllocations = client_allocations.filter((a) => a && a.client_id);
      if (cleanedAllocations.length > 0) {
        const totalAllocation = cleanedAllocations.reduce((sum, a) => sum + (parseFloat(a.allocation_percentage) || 0), 0);
        if (Math.abs(totalAllocation - 100) > 0.01) {
          await dbClient.query('ROLLBACK');
          return res.status(400).json({
            success: false,
            message: `Client allocations must total 100% (current: ${totalAllocation.toFixed(2)}%)`
          });
        }
      }

      await syncTeamAllocationRowsForPeriod({
        dbClient,
        teamId: id,
        periodRow: { start_period: original_start_period, end_period: original_end_period || null },
        allocations: cleanedAllocations,
        updatedBy: req.user.id,
      });
    }

    const updatedTeamResult = await dbClient.query('SELECT * FROM teams WHERE id = $1', [id]);
    const updatedHistoryResult = await dbClient.query(
      'SELECT id, start_period, end_period, amount FROM team_increment_history WHERE team_id = $1 ORDER BY start_period',
      [id]
    );
    const finalCompensationRows = toCompensationRows(updatedTeamResult.rows[0], updatedHistoryResult.rows);
    const finalAllocationRows = await loadTeamAllocationRows(dbClient, id);

    await syncTeamRecurringExpenseAmounts({
      dbClient,
      currentCompensationRows,
      nextCompensationRows: finalCompensationRows,
      currentAllocationRows,
      nextAllocationRows: finalAllocationRows,
      updatedBy: req.user.id,
    });

    await dbClient.query('COMMIT');
    await logAudit('teams', id, 'UPDATE_PERIOD', { original_start_period, original_end_period }, { amount, client_allocations }, req.user.id);

    res.json({ success: true, message: 'Compensation period updated successfully' });
  } catch (error) {
    await dbClient.query('ROLLBACK');
    console.error('Update compensation period error:', error);
    res.status(500).json({ success: false, message: 'Failed to update compensation period' });
  } finally {
    dbClient.release();
  }
});

module.exports = router;
