const PERIOD_REGEX = /^[A-Z][a-z]{2}-\d{2}$/;
const { filterAllocationsForPeriod, getStoredTeamAllocationAmount } = require('./teamAllocationPeriods');

const parsePeriod = (period) => {
  if (!period || !PERIOD_REGEX.test(period)) return null;
  const monthMap = {
    Jan: 0, Feb: 1, Mar: 2, Apr: 3, May: 4, Jun: 5,
    Jul: 6, Aug: 7, Sep: 8, Oct: 9, Nov: 10, Dec: 11,
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

const getRecordTimestamp = (record) =>
  new Date(record?.changed_at || record?.updated_at || record?.created_at || 0).getTime();

const sortPeriodRecordsDesc = (records = [], startKey = 'start_period', endKey = 'end_period') =>
  [...records].sort((a, b) =>
    comparePeriods(b[startKey], a[startKey]) ||
    comparePeriods(b[endKey], a[endKey]) ||
    getRecordTimestamp(b) - getRecordTimestamp(a)
  );

const normalizeClientPeriodHistory = (currentRow, historyRows = []) => {
  if (!currentRow) return [];

  const currentRecord = {
    id: `current-${currentRow.id}`,
    client_billing_row_id: currentRow.id,
    billing_name: currentRow.billing_name,
    pan: currentRow.pan,
    gstin: currentRow.gstin,
    service_type_id: currentRow.service_type_id,
    service_type_name: currentRow.service_type_name,
    bill_from_id: currentRow.bill_from_id,
    bill_from_name: currentRow.bill_from_name,
    reviewer_id: currentRow.reviewer_id,
    reviewer_name: currentRow.reviewer_name,
    start_period: currentRow.start_period,
    end_period: currentRow.end_period,
    amount: parseFloat(currentRow.amount) || 0,
    changed_at: currentRow.updated_at || currentRow.created_at,
    is_current: true,
  };

  const grouped = new Map();

  [...historyRows, currentRecord].forEach((record, index) => {
    if (!record?.start_period) return;

    const key = `${record.start_period || ''}|${record.end_period || ''}`;
    const nextRecord = {
      ...record,
      id: record.id || `history-${currentRow.id}-${index}`,
      client_billing_row_id: record.client_billing_row_id || currentRow.id,
      amount: parseFloat(record.amount) || 0,
      is_current: record.is_current === true,
    };

    const existing = grouped.get(key);
    if (!existing) {
      grouped.set(key, nextRecord);
      return;
    }

    const nextTime = getRecordTimestamp(nextRecord);
    const existingTime = getRecordTimestamp(existing);
    const shouldReplace =
      nextRecord.is_current ||
      (!existing.is_current && nextTime >= existingTime);

    if (shouldReplace) {
      grouped.set(key, nextRecord);
    }
  });

  const cleaned = Array.from(grouped.values()).filter((record) => {
    if (record.end_period) return true;
    const boundedSibling = Array.from(grouped.values()).find(
      (candidate) =>
        candidate.start_period === record.start_period &&
        candidate.end_period &&
        candidate.client_billing_row_id === record.client_billing_row_id
    );
    return !boundedSibling || record.is_current;
  });

  return sortPeriodRecordsDesc(cleaned);
};

const summarizeClientPeriodRecord = async (_dbClient, record) => ({
  ...record,
  amount: parseFloat(record.amount) || 0,
});

const summarizeTeamCompensationRows = async (dbClient, teamId, rows = []) => {
  const allocationsResult = await dbClient.query(`
    SELECT
      tca.id,
      tca.client_id,
      c.name AS client_name,
      tca.allocation_percentage,
      tca.allocation_amount,
      tca.allocation_method,
      tca.expense_head_id,
      eh.name AS expense_head_name,
      tca.start_period,
      tca.end_period
    FROM team_client_allocations tca
    LEFT JOIN clients c ON c.id = tca.client_id
    LEFT JOIN expense_heads eh ON eh.id = tca.expense_head_id
    WHERE tca.team_id = $1
    ORDER BY c.name
  `, [teamId]);

  const allocations = allocationsResult.rows.map((allocation) => ({
    ...allocation,
    allocation_percentage: parseFloat(allocation.allocation_percentage) || 0,
  }));

  return rows.map((row) => {
    const rowAmount = parseFloat(row.amount) || 0;
    const rowAllocations = filterAllocationsForPeriod(allocations, row).filter(
      (allocation) =>
        getStoredTeamAllocationAmount(allocation, rowAmount) > 0 ||
        allocation.allocation_percentage > 0
    );

    return {
      ...row,
      amount: rowAmount,
      client_allocations: rowAllocations.map((allocation) => ({
        ...allocation,
        amount: getStoredTeamAllocationAmount(allocation, rowAmount),
      })),
    };
  });
};

const summarizeRecurringClientRecord = async (_dbClient, record) => ({
  ...record,
  amount: parseFloat(record?.amount) || 0,
});

const summarizeRecurringTeams = async (dbClient, expense, teams = []) =>
  Promise.all(teams.map(async (team) => ({
    ...team,
    clients: await Promise.all((team.clients || []).map((client) =>
      summarizeRecurringClientRecord(dbClient, {
        ...client,
        start_period: client.start_period || expense.start_period,
        end_period: client.end_period || expense.end_period,
      })
    )),
  })));

module.exports = {
  comparePeriods,
  normalizeClientPeriodHistory,
  sortPeriodRecordsDesc,
  summarizeClientPeriodRecord,
  summarizeTeamCompensationRows,
  summarizeRecurringTeams,
};
