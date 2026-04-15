import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Box,
  Button,
  CircularProgress,
  Grid,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import { Bar } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  BarElement,
  CategoryScale,
  Legend,
  LinearScale,
  Title,
  Tooltip as ChartTooltip,
} from 'chart.js';
import {
  Deselect,
  MonetizationOn,
  QueryStats,
  Refresh,
  Savings,
  SelectAll,
  Tune,
} from '@mui/icons-material';
import { toast } from 'react-toastify';
import {
  FilterPanel,
  HoverActionButton,
  MetricCard,
  PageHeader,
  SectionCard,
  SelectionField,
} from '../components/FuturisticUI';
import { billingNameAPI, clientAPI, reportAPI, servicePeriodAPI, teamAPI } from '../services/api';
import { getFinancialYearPeriods, resolveDefaultFinancialYear } from '../utils/periods';

ChartJS.register(CategoryScale, LinearScale, BarElement, Title, ChartTooltip, Legend);

const fmt = (value) => {
  const numericValue = Number(value || 0);
  return numericValue.toLocaleString('en-IN', {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  });
};

const percent = (value, base) => {
  if (!base) return '0.0%';
  return `${((value / base) * 100).toFixed(1)}%`;
};

const createEmptyMonth = (period) => ({
  period,
  projected_revenue: 0,
  billed_revenue: 0,
  unbilled_revenue: 0,
  actual_revenue: 0,
  projected_expense: 0,
  billed_expense: 0,
  unbilled_expense: 0,
  actual_expense: 0,
  projected_net: 0,
  actual_net: 0,
  revenue_variance: 0,
  expense_variance: 0,
});

const normaliseMonth = (month) => {
  const projectedRevenue = Number(month.projected_revenue || 0);
  const billedRevenue = Number(month.billed_revenue || 0);
  const unbilledRevenue = Number(month.unbilled_revenue || 0);
  const actualRevenue = Number(month.actual_revenue || 0);
  const projectedExpense = Number(month.projected_expense || 0);
  const billedExpense = Number(month.billed_expense || 0);
  const unbilledExpense = Number(month.unbilled_expense || 0);
  const actualExpense = Number(month.actual_expense || 0);

  return {
    period: month.period,
    projected_revenue: projectedRevenue,
    billed_revenue: billedRevenue,
    unbilled_revenue: unbilledRevenue,
    actual_revenue: actualRevenue,
    projected_expense: projectedExpense,
    billed_expense: billedExpense,
    unbilled_expense: unbilledExpense,
    actual_expense: actualExpense,
    projected_net: Number(month.projected_net ?? projectedRevenue - projectedExpense),
    actual_net: Number(month.actual_net ?? actualRevenue - actualExpense),
    revenue_variance: Number(month.revenue_variance ?? actualRevenue - projectedRevenue),
    expense_variance: Number(month.expense_variance ?? actualExpense - projectedExpense),
  };
};

const mergeVariancePayload = (responses, selectedFinancialYears) => {
  const monthMap = new Map();

  responses.forEach((response) => {
    (response?.months || []).forEach((month) => {
      const current = monthMap.get(month.period) || createEmptyMonth(month.period);
      const next = normaliseMonth(month);
      monthMap.set(month.period, {
        period: month.period,
        projected_revenue: current.projected_revenue + next.projected_revenue,
        billed_revenue: current.billed_revenue + next.billed_revenue,
        unbilled_revenue: current.unbilled_revenue + next.unbilled_revenue,
        actual_revenue: current.actual_revenue + next.actual_revenue,
        projected_expense: current.projected_expense + next.projected_expense,
        billed_expense: current.billed_expense + next.billed_expense,
        unbilled_expense: current.unbilled_expense + next.unbilled_expense,
        actual_expense: current.actual_expense + next.actual_expense,
        projected_net: current.projected_net + next.projected_net,
        actual_net: current.actual_net + next.actual_net,
        revenue_variance: current.revenue_variance + next.revenue_variance,
        expense_variance: current.expense_variance + next.expense_variance,
      });
    });
  });

  const orderedPeriods = selectedFinancialYears.flatMap((financialYear) => getFinancialYearPeriods(financialYear));
  const uniqueOrderedPeriods = [...new Set(orderedPeriods)];

  return Array.from(monthMap.values()).sort(
    (left, right) => (uniqueOrderedPeriods.indexOf(left.period) === -1 ? Number.MAX_SAFE_INTEGER : uniqueOrderedPeriods.indexOf(left.period))
      - (uniqueOrderedPeriods.indexOf(right.period) === -1 ? Number.MAX_SAFE_INTEGER : uniqueOrderedPeriods.indexOf(right.period)),
  );
};

const Dashboard = () => {
  const theme = useTheme();
  const [loading, setLoading] = useState(false);
  const [filterMode, setFilterMode] = useState('dropdown');
  const [filters, setFilters] = useState({
    financialYears: [],
    clientIds: [],
    billingNameIds: [],
    reviewerIds: [],
  });
  const [clients, setClients] = useState([]);
  const [billingNames, setBillingNames] = useState([]);
  const [reviewers, setReviewers] = useState([]);
  const [financialYears, setFinancialYears] = useState([]);
  const [allMonths, setAllMonths] = useState([]);
  const [selectedMonths, setSelectedMonths] = useState([]);

  useEffect(() => {
    Promise.all([
      clientAPI.getAll(),
      billingNameAPI.getAll(),
      teamAPI.getAll(),
      servicePeriodAPI.getFinancialYears(),
    ])
      .then(([clientResponse, billingResponse, teamResponse, financialYearResponse]) => {
        const loadedFinancialYears = financialYearResponse.data.data || [];
        const defaultFinancialYear = resolveDefaultFinancialYear(loadedFinancialYears);

        setClients(clientResponse.data.data || []);
        setBillingNames(billingResponse.data.data || []);
        setReviewers((teamResponse.data.data || []).filter((teamMember) => teamMember.is_reviewer));
        setFinancialYears(loadedFinancialYears);
        setFilters((previous) => ({
          ...previous,
          financialYears: previous.financialYears.length ? previous.financialYears : (defaultFinancialYear ? [defaultFinancialYear] : []),
        }));
      })
      .catch(() => toast.error('Failed to load dashboard filters'));
  }, []);

  const buildParams = useCallback((financialYear) => {
    const params = { financialYear };
    if (filters.clientIds.length) params.clientIds = filters.clientIds.join(',');
    if (filters.billingNameIds.length) params.billingNameIds = filters.billingNameIds.join(',');
    if (filters.reviewerIds.length) params.reviewerIds = filters.reviewerIds.join(',');
    return params;
  }, [filters.billingNameIds, filters.clientIds, filters.reviewerIds]);

  const loadDashboard = useCallback(async () => {
    if (!filters.financialYears.length) {
      setAllMonths([]);
      setSelectedMonths([]);
      return;
    }

    setLoading(true);
    try {
      const responses = await Promise.all(
        filters.financialYears.map((financialYear) => reportAPI.getVariance(buildParams(financialYear))),
      );
      const mergedMonths = mergeVariancePayload(
        responses.map((response) => response.data.data || { months: [] }),
        filters.financialYears,
      );
      setAllMonths(mergedMonths);
      setSelectedMonths(mergedMonths.map((month) => month.period));
    } catch {
      toast.error('Failed to load dashboard');
    } finally {
      setLoading(false);
    }
  }, [buildParams, filters.financialYears]);

  useEffect(() => {
    loadDashboard();
  }, [loadDashboard]);

  const visibleMonths = useMemo(
    () => allMonths.filter((month) => selectedMonths.includes(month.period)),
    [allMonths, selectedMonths],
  );

  const filteredBillingNames = useMemo(
    () => billingNames.filter((billingName) => !filters.clientIds.length || filters.clientIds.includes(billingName.client_id)),
    [billingNames, filters.clientIds],
  );

  const totals = useMemo(() => {
    const base = {
      projectedRevenue: 0,
      billedRevenue: 0,
      unbilledRevenue: 0,
      actualRevenue: 0,
      projectedExpense: 0,
      billedExpense: 0,
      unbilledExpense: 0,
      actualExpense: 0,
    };

    visibleMonths.forEach((month) => {
      base.projectedRevenue += Number(month.projected_revenue || 0);
      base.billedRevenue += Number(month.billed_revenue || 0);
      base.unbilledRevenue += Number(month.unbilled_revenue || 0);
      base.actualRevenue += Number(month.actual_revenue || 0);
      base.projectedExpense += Number(month.projected_expense || 0);
      base.billedExpense += Number(month.billed_expense || 0);
      base.unbilledExpense += Number(month.unbilled_expense || 0);
      base.actualExpense += Number(month.actual_expense || 0);
    });

    return {
      ...base,
      projectedNet: base.projectedRevenue - base.projectedExpense,
      actualNet: base.actualRevenue - base.actualExpense,
      revenueVariance: base.actualRevenue - base.projectedRevenue,
      expenseVariance: base.actualExpense - base.projectedExpense,
      netVariance: (base.actualRevenue - base.actualExpense) - (base.projectedRevenue - base.projectedExpense),
    };
  }, [visibleMonths]);

  const chartData = useMemo(() => ({
    labels: visibleMonths.map((month) => month.period),
    datasets: [
      {
        label: 'Projected Revenue',
        data: visibleMonths.map((month) => month.projected_revenue),
        backgroundColor: 'rgba(111, 157, 137, 0.72)',
        borderRadius: 12,
      },
      {
        label: 'Actual Revenue',
        data: visibleMonths.map((month) => month.actual_revenue),
        backgroundColor: 'rgba(72, 191, 145, 0.82)',
        borderRadius: 12,
      },
      {
        label: 'Projected Expense',
        data: visibleMonths.map((month) => month.projected_expense),
        backgroundColor: 'rgba(215, 182, 128, 0.72)',
        borderRadius: 12,
      },
      {
        label: 'Actual Expense',
        data: visibleMonths.map((month) => month.actual_expense),
        backgroundColor: 'rgba(221, 152, 136, 0.88)',
        borderRadius: 12,
      },
    ],
  }), [visibleMonths]);

  const chartOptions = useMemo(() => ({
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'bottom',
        labels: {
          boxWidth: 14,
          boxHeight: 14,
          color: theme.palette.text.secondary,
          font: {
            family: theme.typography.fontFamily,
            size: 12,
            weight: 700,
          },
          padding: 18,
        },
      },
      tooltip: {
        backgroundColor: alpha(theme.palette.text.primary, 0.92),
        padding: 12,
        cornerRadius: 14,
        titleFont: {
          family: theme.typography.fontFamily,
          weight: '700',
        },
        bodyFont: {
          family: theme.typography.fontFamily,
        },
      },
    },
    scales: {
      x: {
        grid: { display: false },
        ticks: {
          color: theme.palette.text.secondary,
          font: {
            family: theme.typography.fontFamily,
            size: 11,
          },
        },
      },
      y: {
        beginAtZero: true,
        grid: {
          color: alpha(theme.palette.primary.main, 0.08),
        },
        ticks: {
          color: theme.palette.text.secondary,
          font: {
            family: theme.typography.fontFamily,
            size: 11,
          },
        },
      },
    },
  }), [theme]);

  const financialYearOptions = financialYears.map((financialYear) => ({
    value: financialYear,
    label: financialYear,
  }));

  const clientOptions = clients.map((client) => ({ value: client.id, label: client.name }));
  const billingOptions = filteredBillingNames.map((billingName) => ({ value: billingName.id, label: billingName.name }));
  const reviewerOptions = reviewers.map((reviewer) => ({ value: reviewer.id, label: reviewer.name }));

  const setArrayFilter = (key) => (nextValue) => {
    setFilters((previous) => {
      const next = { ...previous, [key]: nextValue };
      if (key === 'clientIds') next.billingNameIds = previous.clientIds === nextValue ? previous.billingNameIds : [];
      return next;
    });
  };

  const headerCellStyles = {
    py: 1,
    px: 1.2,
    whiteSpace: 'nowrap',
    background: `linear-gradient(135deg, ${alpha(theme.palette.primary.light, 0.44)} 0%, ${alpha(theme.palette.secondary.light, 0.44)} 100%)`,
  };

  const bodyCellStyles = {
    py: 0.85,
    px: 1.2,
    whiteSpace: 'nowrap',
    fontSize: '0.82rem',
  };

  const varianceTone = (value) => (value >= 0 ? theme.palette.primary.dark : theme.palette.error.dark);

  return (
    <Box>
      <PageHeader
        eyebrow="Variance Report"
        title="Projected vs actuals variance"
        actions={[
          <HoverActionButton key="refresh" icon={<Refresh fontSize="small" />} label="Refresh" onClick={loadDashboard} tone="mint" />,
        ]}
        chips={[
          { label: `${visibleMonths.length} visible months`, background: alpha(theme.palette.primary.light, 0.38) },
          { label: `${filters.financialYears.length || 0} FY selected`, background: alpha(theme.palette.secondary.light, 0.46) },
        ]}
      />

      <Grid container spacing={2} sx={{ mb: 2.5 }}>
        <Grid item xs={6} sm={4} md={2}>
          <MetricCard eyebrow="Revenue" title="Projected" value={fmt(totals.projectedRevenue)} icon={<MonetizationOn fontSize="small" />} tone="mint" />
        </Grid>
        <Grid item xs={6} sm={4} md={2}>
          <MetricCard eyebrow="Revenue" title="Actual" value={fmt(totals.actualRevenue)} icon={<QueryStats fontSize="small" />} tone="peach" />
        </Grid>
        <Grid item xs={6} sm={4} md={2}>
          <MetricCard eyebrow="Variance" title="Revenue variance" value={fmt(totals.revenueVariance)} helper={percent(totals.revenueVariance, totals.projectedRevenue)} icon={<QueryStats fontSize="small" />} tone="sand" />
        </Grid>
        <Grid item xs={6} sm={4} md={2}>
          <MetricCard eyebrow="Expense" title="Projected" value={fmt(totals.projectedExpense)} icon={<Savings fontSize="small" />} tone="sage" />
        </Grid>
        <Grid item xs={6} sm={4} md={2}>
          <MetricCard eyebrow="Expense" title="Actual" value={fmt(totals.actualExpense)} icon={<Savings fontSize="small" />} tone="rose" />
        </Grid>
        <Grid item xs={6} sm={4} md={2}>
          <MetricCard
            eyebrow="Variance"
            title="Expense variance"
            value={fmt(totals.expenseVariance)}
            helper={percent(totals.expenseVariance, totals.projectedExpense)}
            icon={<Savings fontSize="small" />}
            tone={totals.expenseVariance <= 0 ? 'mint' : 'rose'}
          />
        </Grid>
      </Grid>

      <FilterPanel
        mode={filterMode}
        onModeChange={setFilterMode}
        title="Selection Matrix"
        showModeToggle={false}
        onClear={
          <Box sx={{ display: 'flex', gap: 1.5 }}>
            <HoverActionButton
              icon={<Tune fontSize="small" />}
              label={filterMode === 'dropdown' ? 'Dropdown view' : 'Full selection'}
              onClick={() => setFilterMode((previous) => (previous === 'dropdown' ? 'full' : 'dropdown'))}
              tone="peach"
            />
            <HoverActionButton
              icon={<Deselect fontSize="small" />}
              label="Clear filters"
              onClick={() => {
                const currentFinancialYear = filters.financialYears[0] || resolveDefaultFinancialYear(financialYears);
                setFilters({
                  financialYears: currentFinancialYear ? [currentFinancialYear] : [],
                  clientIds: [],
                  billingNameIds: [],
                  reviewerIds: [],
                });
              }}
              tone="rose"
            />
          </Box>
        }
      >
      </FilterPanel>

      <SectionCard
        title="Fiscal year month selector"
        tone="sand"
        action={
          <Box sx={{ display: 'flex', gap: 1 }}>
            <HoverActionButton
              icon={<SelectAll fontSize="small" />}
              label="Select all"
              onClick={() => setSelectedMonths(allMonths.map((month) => month.period))}
              tone="mint"
            />
            <HoverActionButton
              icon={<Deselect fontSize="small" />}
              label="Clear months"
              onClick={() => setSelectedMonths([])}
              tone="rose"
            />
          </Box>
        }
        contentSx={{ pt: 1.5 }}
      >
        <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
          {allMonths.map((month) => {
            const selected = selectedMonths.includes(month.period);
            return (
              <Button
                key={month.period}
                variant={selected ? 'contained' : 'outlined'}
                onClick={() => {
                  setSelectedMonths((previous) => (
                    previous.includes(month.period)
                      ? previous.filter((period) => period !== month.period)
                      : [...previous, month.period]
                  ));
                }}
                sx={{
                  minWidth: 0,
                  px: 1.4,
                  background: selected
                    ? `linear-gradient(135deg, ${theme.palette.primary.main} 0%, ${theme.palette.secondary.main} 100%)`
                    : alpha('#ffffff', 0.78),
                }}
              >
                {month.period}
              </Button>
            );
          })}
        </Box>
      </SectionCard>

      {loading ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 8 }}>
          <CircularProgress />
        </Box>
      ) : (
        <Grid container spacing={2} sx={{ mt: 0.25 }}>
          <Grid item xs={12} lg={7}>
            <SectionCard
              title="Variance report chart"
              tone="mint"
            >
              <Box sx={{ height: 360 }}>
                <Bar data={chartData} options={chartOptions} />
              </Box>
            </SectionCard>
          </Grid>
          <Grid item xs={12} lg={5}>
            <SectionCard
              title="Variance totals"
              tone="peach"
              contentSx={{ p: 0 }}
            >
              <Box sx={{ px: 2, py: 1.6 }}>
                <Box sx={{ mb: 2, p: 1.4, borderRadius: 4, background: alpha(theme.palette.primary.light, 0.28) }}>
                  <Typography variant="subtitle2" color="text.secondary">Revenue section</Typography>
                  <Typography variant="h5">{fmt(totals.actualRevenue)}</Typography>
                  <Typography variant="body2" color="text.secondary">
                    Projected {fmt(totals.projectedRevenue)} | Actual {fmt(totals.actualRevenue)} | Variance {fmt(totals.revenueVariance)}
                  </Typography>
                </Box>
                <Box sx={{ p: 1.4, borderRadius: 4, background: alpha(theme.palette.warning.light, 0.5) }}>
                  <Typography variant="subtitle2" color="text.secondary">Expense section</Typography>
                  <Typography variant="h5">{fmt(totals.actualExpense)}</Typography>
                  <Typography variant="body2" color="text.secondary">
                    Projected {fmt(totals.projectedExpense)} | Actual {fmt(totals.actualExpense)} | Variance {fmt(totals.expenseVariance)}
                  </Typography>
                </Box>
              </Box>
            </SectionCard>
          </Grid>

          {visibleMonths.length === 0 ? (
            <Grid item xs={12}>
              <SectionCard
                title="Variance report"
                tone="sage"
              >
                <Typography color="text.secondary">
                  No projected revenue or expense items were converted to actuals for the current filters.
                </Typography>
              </SectionCard>
            </Grid>
          ) : (
            <Grid item xs={12}>
            <SectionCard
              title="Month-by-month variance report"
              tone="sage"
              contentSx={{ p: 0 }}
            >
              <TableContainer>
                <Table stickyHeader size="small">
                  <TableHead>
                    <TableRow>
                      <TableCell sx={{ ...headerCellStyles, position: 'sticky', left: 0, zIndex: 5 }}>Metric</TableCell>
                      {visibleMonths.map((month) => (
                        <TableCell key={month.period} align="right" sx={headerCellStyles}>
                          {month.period}
                        </TableCell>
                      ))}
                      <TableCell align="right" sx={headerCellStyles}>Visible total</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    <TableRow>
                      <TableCell sx={{ ...bodyCellStyles, position: 'sticky', left: 0, zIndex: 3, background: alpha('#ffffff', 0.96), fontWeight: 700 }}>Projected revenue</TableCell>
                      {visibleMonths.map((month) => <TableCell key={month.period} align="right" sx={bodyCellStyles}>{fmt(month.projected_revenue)}</TableCell>)}
                      <TableCell align="right" sx={{ ...bodyCellStyles, fontWeight: 700 }}>{fmt(totals.projectedRevenue)}</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell sx={{ ...bodyCellStyles, position: 'sticky', left: 0, zIndex: 3, background: alpha('#ffffff', 0.96), fontWeight: 700 }}>Actual revenue</TableCell>
                      {visibleMonths.map((month) => <TableCell key={month.period} align="right" sx={bodyCellStyles}>{fmt(month.actual_revenue)}</TableCell>)}
                      <TableCell align="right" sx={{ ...bodyCellStyles, fontWeight: 700 }}>{fmt(totals.actualRevenue)}</TableCell>
                    </TableRow>
                    <TableRow sx={{ background: alpha(theme.palette.secondary.light, 0.34) }}>
                      <TableCell sx={{ ...bodyCellStyles, position: 'sticky', left: 0, zIndex: 3, background: alpha(theme.palette.secondary.light, 0.34), fontWeight: 700 }}>Revenue variance</TableCell>
                      {visibleMonths.map((month) => (
                        <TableCell key={month.period} align="right" sx={{ ...bodyCellStyles, color: varianceTone(month.revenue_variance), fontWeight: 700 }}>
                          {fmt(month.revenue_variance)}
                        </TableCell>
                      ))}
                      <TableCell align="right" sx={{ ...bodyCellStyles, color: varianceTone(totals.revenueVariance), fontWeight: 700 }}>{fmt(totals.revenueVariance)}</TableCell>
                    </TableRow>

                    <TableRow>
                      <TableCell colSpan={visibleMonths.length + 2} sx={{ py: 0.5, background: alpha(theme.palette.primary.main, 0.1) }} />
                    </TableRow>

                    <TableRow>
                      <TableCell sx={{ ...bodyCellStyles, position: 'sticky', left: 0, zIndex: 3, background: alpha('#ffffff', 0.96), fontWeight: 700 }}>Projected expense</TableCell>
                      {visibleMonths.map((month) => <TableCell key={month.period} align="right" sx={bodyCellStyles}>{fmt(month.projected_expense)}</TableCell>)}
                      <TableCell align="right" sx={{ ...bodyCellStyles, fontWeight: 700 }}>{fmt(totals.projectedExpense)}</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell sx={{ ...bodyCellStyles, position: 'sticky', left: 0, zIndex: 3, background: alpha('#ffffff', 0.96), fontWeight: 700 }}>Actual expense</TableCell>
                      {visibleMonths.map((month) => <TableCell key={month.period} align="right" sx={bodyCellStyles}>{fmt(month.actual_expense)}</TableCell>)}
                      <TableCell align="right" sx={{ ...bodyCellStyles, fontWeight: 700 }}>{fmt(totals.actualExpense)}</TableCell>
                    </TableRow>
                    <TableRow sx={{ background: alpha(theme.palette.warning.light, 0.42) }}>
                      <TableCell sx={{ ...bodyCellStyles, position: 'sticky', left: 0, zIndex: 3, background: alpha(theme.palette.warning.light, 0.42), fontWeight: 700 }}>Expense variance</TableCell>
                      {visibleMonths.map((month) => (
                        <TableCell key={month.period} align="right" sx={{ ...bodyCellStyles, color: varianceTone(-month.expense_variance), fontWeight: 700 }}>
                          {fmt(month.expense_variance)}
                        </TableCell>
                      ))}
                      <TableCell align="right" sx={{ ...bodyCellStyles, color: varianceTone(-totals.expenseVariance), fontWeight: 700 }}>{fmt(totals.expenseVariance)}</TableCell>
                    </TableRow>
                  </TableBody>
                </Table>
              </TableContainer>
            </SectionCard>
          </Grid>
          )}
        </Grid>
      )}
    </Box>
  );
};

export default Dashboard;
