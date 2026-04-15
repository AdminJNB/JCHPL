const MONTH_MAP = {
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

const MONTHS = Object.keys(MONTH_MAP);
export const DEFAULT_FINANCIAL_YEAR = '2026-27';

export const parsePeriod = (period) => {
  if (!period || typeof period !== 'string') return null;
  const [month, yearPart] = period.split('-');
  if (!(month in MONTH_MAP) || !yearPart) return null;
  return new Date(2000 + Number(yearPart), MONTH_MAP[month], 1);
};

export const comparePeriods = (left, right) => {
  const leftDate = parsePeriod(left);
  const rightDate = parsePeriod(right);
  if (!leftDate && !rightDate) return 0;
  if (!leftDate) return -1;
  if (!rightDate) return 1;
  return leftDate - rightDate;
};

export const sortPeriods = (periods = []) => [...periods].sort(comparePeriods);

export const resolveDefaultFinancialYear = (years = []) => {
  if (!Array.isArray(years) || years.length === 0) return DEFAULT_FINANCIAL_YEAR;
  return (
    years.find((year) => String(year) === DEFAULT_FINANCIAL_YEAR) ||
    years.find((year) => String(year).startsWith('2026-')) ||
    years[0] ||
    DEFAULT_FINANCIAL_YEAR
  );
};

export const getCurrentFinancialYear = () => DEFAULT_FINANCIAL_YEAR;

export const getFinancialYearPeriods = (financialYear) => {
  if (!financialYear) return [];
  const [startYearString, endYearString] = String(financialYear).split('-');
  const startYear = Number(startYearString);
  const endYear = Number(endYearString);
  if (Number.isNaN(startYear) || Number.isNaN(endYear)) return [];

  return [
    `Apr-${String(startYear).slice(-2)}`,
    `May-${String(startYear).slice(-2)}`,
    `Jun-${String(startYear).slice(-2)}`,
    `Jul-${String(startYear).slice(-2)}`,
    `Aug-${String(startYear).slice(-2)}`,
    `Sep-${String(startYear).slice(-2)}`,
    `Oct-${String(startYear).slice(-2)}`,
    `Nov-${String(startYear).slice(-2)}`,
    `Dec-${String(startYear).slice(-2)}`,
    `Jan-${String(endYear).slice(-2)}`,
    `Feb-${String(endYear).slice(-2)}`,
    `Mar-${String(endYear).slice(-2)}`,
  ];
};

export const getPeriodLabel = (period) => {
  const date = parsePeriod(period);
  if (!date) return period;
  return `${MONTHS[date.getMonth()]} ${date.getFullYear()}`;
};
