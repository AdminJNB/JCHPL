const MAX_LINKED_ITEMS = 12;

const safeText = (value, fallback = '-') => {
  if (value === null || value === undefined) return fallback;
  const text = String(value).trim();
  return text || fallback;
};

const formatPeriodRange = (startPeriod, endPeriod) => {
  const start = safeText(startPeriod, '');
  if (!start) return safeText(endPeriod);
  return endPeriod ? `${start} to ${endPeriod}` : `${start} to Open`;
};

const queryLinkedRows = async (dbClient, sql, params = []) => {
  const result = await dbClient.query(sql, [...params, MAX_LINKED_ITEMS]);
  const totalCount = result.rows.length > 0
    ? parseInt(result.rows[0].total_count, 10) || 0
    : 0;

  return {
    count: totalCount,
    rows: result.rows,
  };
};

const buildLinkedItem = ({
  id,
  label,
  line,
  module,
  type,
  status = null,
}) => ({
  id,
  label: safeText(label),
  line: safeText(line),
  module,
  type,
  status,
});

const buildDependencyEntry = ({
  type,
  module,
  count,
  rows = [],
  mapRow,
}) => {
  if (!count) return null;

  const linkedItems = rows.map((row) => mapRow(row));

  return {
    type,
    count,
    module,
    linked_items: linkedItems,
    shown_count: linkedItems.length,
    remaining_count: Math.max(0, count - linkedItems.length),
  };
};

const summarizeDependencies = (dependencies = []) =>
  dependencies
    .map((dependency) => `${dependency.count} ${dependency.type} record${dependency.count === 1 ? '' : 's'}`)
    .join(', ');

const buildDeleteBlockedMessage = (masterLabel, dependencies = []) => {
  const summary = summarizeDependencies(dependencies);
  return summary
    ? `Delete blocked. This ${masterLabel} is still linked to ${summary}. Update or restore those linked item(s) first, then try again.`
    : `Delete blocked. This ${masterLabel} still has linked records. Update or restore those linked item(s) first, then try again.`;
};

module.exports = {
  MAX_LINKED_ITEMS,
  safeText,
  formatPeriodRange,
  queryLinkedRows,
  buildLinkedItem,
  buildDependencyEntry,
  buildDeleteBlockedMessage,
};
