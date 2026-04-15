/**
 * One-time cleanup script to fix team_client_allocation rows for ALL
 * compensation periods.
 *
 * When team allocations were changed alongside a compensation period transition,
 * historical periods either kept stale client entries (wrong combos) or were
 * missing correct entries entirely.
 *
 * This script:
 *  1. For each team, derives compensation periods from increment history.
 *  2. Gets the live period's active allocations (client_id + expense_head_id + %).
 *  3. Syncs every historical compensation period to have the same allocation set.
 *  4. Retires any orphaned allocation rows whose period range doesn't match
 *     any current compensation period.
 *  5. Runs the recurring source cleanup for period-range mismatches.
 */
require('dotenv').config();

const { pool } = require('../database/db');
const {
  cleanupStaleRecurringSourceRecords,
  deactivateTeamRecurringExpensesForAllocations,
} = require('../utils/recurringSourceCleanup');
const {
  normalizeTeamAllocationRows,
  filterAllocationsForPeriod,
  getLatestPeriodRow,
} = require('../utils/teamAllocationPeriods');

const PERIOD_REGEX = /^[A-Z][a-z]{2}-\d{2}$/;

const parsePeriod = (period) => {
  if (!period || !PERIOD_REGEX.test(period)) return null;
  const monthMap = { Jan:0,Feb:1,Mar:2,Apr:3,May:4,Jun:5,Jul:6,Aug:7,Sep:8,Oct:9,Nov:10,Dec:11 };
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
  const dA = parsePeriod(a); const dB = parsePeriod(b);
  if (!dA && !dB) return 0; if (!dA) return -1; if (!dB) return 1;
  return dA - dB;
};

const toCompensationRows = (team, incrementRows = []) => {
  if (!team?.start_period || team.amount === null || team.amount === undefined) return [];
  const events = incrementRows
    .filter(r => r && r.start_period && r.amount !== undefined && r.amount !== null)
    .map(r => ({ effective_period: r.start_period, amount: parseFloat(r.amount) }))
    .sort((a, b) => comparePeriods(a.effective_period, b.effective_period));

  const segments = [];
  let currentStart = team.start_period;
  let currentAmount = parseFloat(team.amount);

  for (const ev of events) {
    if (comparePeriods(ev.effective_period, currentStart) <= 0) {
      currentStart = ev.effective_period;
      currentAmount = ev.amount;
      continue;
    }
    segments.push({ start_period: currentStart, end_period: getPreviousPeriod(ev.effective_period), amount: currentAmount });
    currentStart = ev.effective_period;
    currentAmount = ev.amount;
  }

  segments.push({ start_period: currentStart, end_period: team.end_period || null, amount: currentAmount });
  return segments;
};

const getAllocationKey = (a) => `${a.client_id}||${a.expense_head_id || ''}`;

async function syncAllocationsForPeriod(dbClient, teamId, periodRow, desiredAllocations) {
  const existingResult = await dbClient.query(`
    SELECT id, client_id, allocation_percentage, expense_head_id, start_period, end_period
    FROM team_client_allocations
    WHERE team_id = $1 AND start_period = $2 AND end_period IS NOT DISTINCT FROM $3
    ORDER BY id
  `, [teamId, periodRow.start_period, periodRow.end_period || null]);

  const existingRows = normalizeTeamAllocationRows(existingResult.rows);
  const existingByKey = new Map(existingRows.map(r => [getAllocationKey(r), r]));
  const syncedKeys = new Set();
  const retiredIds = [];

  for (const alloc of desiredAllocations) {
    const key = getAllocationKey(alloc);
    const existing = existingByKey.get(key);
    if (existing) {
      await dbClient.query(`
        UPDATE team_client_allocations
        SET allocation_percentage = $1, expense_head_id = $2, start_period = $3, end_period = $4
        WHERE id = $5
      `, [alloc.allocation_percentage, alloc.expense_head_id, periodRow.start_period, periodRow.end_period || null, existing.id]);
    } else {
      await dbClient.query(`
        INSERT INTO team_client_allocations (team_id, client_id, allocation_percentage, expense_head_id, start_period, end_period)
        VALUES ($1, $2, $3, $4, $5, $6)
      `, [teamId, alloc.client_id, alloc.allocation_percentage, alloc.expense_head_id, periodRow.start_period, periodRow.end_period || null]);
    }
    syncedKeys.add(key);
  }

  for (const existing of existingRows) {
    if (syncedKeys.has(getAllocationKey(existing))) continue;
    await dbClient.query(`
      UPDATE team_client_allocations SET allocation_percentage = 0, start_period = $1, end_period = $2 WHERE id = $3
    `, [periodRow.start_period, periodRow.end_period || null, existing.id]);
    retiredIds.push(existing.id);
  }

  if (retiredIds.length > 0) {
    await deactivateTeamRecurringExpensesForAllocations(dbClient, retiredIds);
  }

  return { created: desiredAllocations.length - [...syncedKeys].filter(k => existingByKey.has(k)).length, retired: retiredIds.length };
}

async function main() {
  const dbClient = await pool.connect();

  try {
    await dbClient.query('BEGIN');

    const teams = await dbClient.query(`
      SELECT t.id, t.name, t.amount, t.start_period, t.end_period
      FROM teams t
      WHERE t.start_period IS NOT NULL AND t.amount IS NOT NULL
      ORDER BY t.name
    `);

    let totalCreated = 0;
    let totalRetired = 0;
    let totalOrphaned = 0;

    for (const team of teams.rows) {
      const histResult = await dbClient.query(
        'SELECT start_period, amount FROM team_increment_history WHERE team_id = $1 ORDER BY start_period',
        [team.id]
      );
      const compensationRows = toCompensationRows(team, histResult.rows);
      if (!compensationRows.length) continue;

      // Load all allocation rows for this team
      const allocResult = await dbClient.query(`
        SELECT id, client_id, allocation_percentage, expense_head_id, start_period, end_period
        FROM team_client_allocations WHERE team_id = $1 ORDER BY id
      `, [team.id]);
      const allRows = normalizeTeamAllocationRows(allocResult.rows);

      // Get the live period's active allocations as the source of truth
      const liveRow = getLatestPeriodRow(compensationRows);
      const liveAllocations = filterAllocationsForPeriod(allRows, liveRow)
        .filter(a => a.allocation_percentage > 0)
        .map(a => ({
          client_id: a.client_id,
          allocation_percentage: a.allocation_percentage,
          expense_head_id: a.expense_head_id || null,
        }));

      if (!liveAllocations.length) {
        console.log(`  ${team.name}: No live allocations, skipping.`);
        continue;
      }

      console.log(`\n  ${team.name}: ${compensationRows.length} compensation period(s), ${liveAllocations.length} live allocation(s)`);

      // Sync each compensation period with the live allocations
      for (const compRow of compensationRows) {
        const result = await syncAllocationsForPeriod(dbClient, team.id, compRow, liveAllocations);
        if (result.created > 0 || result.retired > 0) {
          console.log(`    ${compRow.start_period}→${compRow.end_period || 'open'}: created=${result.created}, retired=${result.retired}`);
        }
        totalCreated += result.created;
        totalRetired += result.retired;
      }

      // Retire orphaned rows whose period range doesn't match any compensation period
      const validPeriods = compensationRows.map(r => ({ start: r.start_period, end: r.end_period || null }));
      const refreshed = await dbClient.query(`
        SELECT id, start_period, end_period, allocation_percentage
        FROM team_client_allocations WHERE team_id = $1
      `, [team.id]);

      const orphanIds = [];
      for (const row of refreshed.rows) {
        const matches = validPeriods.some(
          p => p.start === row.start_period && (p.end || null) === (row.end_period || null)
        );
        if (!matches && parseFloat(row.allocation_percentage) > 0) {
          orphanIds.push(row.id);
        }
      }

      if (orphanIds.length > 0) {
        await dbClient.query(`
          UPDATE team_client_allocations SET allocation_percentage = 0 WHERE id = ANY($1::uuid[])
        `, [orphanIds]);
        await deactivateTeamRecurringExpensesForAllocations(dbClient, orphanIds);
        console.log(`    Retired ${orphanIds.length} orphaned allocation row(s)`);
        totalOrphaned += orphanIds.length;
      }
    }

    // Run recurring source cleanup
    const cleanupSummary = await cleanupStaleRecurringSourceRecords(dbClient);

    await dbClient.query('COMMIT');

    console.log(`\n=== Summary ===`);
    console.log(`  Created allocation rows: ${totalCreated}`);
    console.log(`  Retired stale rows: ${totalRetired}`);
    console.log(`  Retired orphaned rows: ${totalOrphaned}`);
    console.log(`  Recurring cleanup:`, JSON.stringify(cleanupSummary));
    console.log('Done.');
  } catch (error) {
    await dbClient.query('ROLLBACK').catch(() => {});
    console.error('Cleanup failed:', error.message);
    process.exitCode = 1;
  } finally {
    dbClient.release();
    await pool.end();
  }
}

main();
