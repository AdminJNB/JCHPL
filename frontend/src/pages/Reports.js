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
} from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import { Download, Refresh, TableChart, Tune, ViewCompactAlt } from '@mui/icons-material';
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

const fmt = (value) => {
  const numericValue = Number(value || 0);
  return numericValue.toLocaleString('en-IN', {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  });
};

const mergeSectionRows = (groups) => {
  const rowMap = new Map();

  groups.forEach((rows) => {
    rows.forEach((row) => {
      const current = rowMap.get(row.head_name) || { head_name: row.head_name, months: {}, total: 0 };
      const mergedMonths = { ...current.months };

      Object.entries(row.months || {}).forEach(([period, value]) => {
        mergedMonths[period] = Number(mergedMonths[period] || 0) + Number(value || 0);
      });

      rowMap.set(row.head_name, {
        head_name: row.head_name,
        months: mergedMonths,
        total: Number(current.total || 0) + Number(row.total || 0),
      });
    });
  });

  return [...rowMap.values()].sort((left, right) => left.head_name.localeCompare(right.head_name));
};

const Reports = () => {
  const theme = useTheme();
  const [loading, setLoading] = useState(false);
  const [filterMode, setFilterMode] = useState('dropdown');
  const [filters, setFilters] = useState({
    financialYears: [],
    clientIds: [],
    billingNameIds: [],
    reviewerIds: [],
    billingStatus: [],
  });
  const [clients, setClients] = useState([]);
  const [billingNames, setBillingNames] = useState([]);
  const [reviewers, setReviewers] = useState([]);
  const [financialYears, setFinancialYears] = useState([]);
  const [reportData, setReportData] = useState({ periods: [], revenue: [], expense: [], admin_expense: [] });

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
      .catch(() => toast.error('Failed to load report filters'));
  }, []);

  const buildParams = useCallback((financialYear) => {
    const params = { financialYear };
    if (filters.clientIds.length) params.clientIds = filters.clientIds.join(',');
    if (filters.billingNameIds.length) params.billingNameIds = filters.billingNameIds.join(',');
    if (filters.reviewerIds.length) params.reviewerIds = filters.reviewerIds.join(',');
    if (filters.billingStatus.length) params.billingStatus = filters.billingStatus.join(',');
    return params;
  }, [filters.billingNameIds, filters.billingStatus, filters.clientIds, filters.reviewerIds]);

  const loadReport = useCallback(async () => {
    if (!filters.financialYears.length) {
      setReportData({ periods: [], revenue: [], expense: [], admin_expense: [] });
      return;
    }

    setLoading(true);
    try {
      const responses = await Promise.all(
        filters.financialYears.map((financialYear) => reportAPI.getMatrix(buildParams(financialYear))),
      );
      const payloads = responses.map((response) => response.data.data || { periods: [], revenue: [], expense: [], admin_expense: [] });
      const periods = [...new Set(filters.financialYears.flatMap((financialYear) => getFinancialYearPeriods(financialYear)))];

      setReportData({
        periods,
        revenue: mergeSectionRows(payloads.map((payload) => payload.revenue || [])),
        expense: mergeSectionRows(payloads.map((payload) => payload.expense || [])),
        admin_expense: mergeSectionRows(payloads.map((payload) => payload.admin_expense || [])),
      });
    } catch {
      toast.error('Failed to load report');
    } finally {
      setLoading(false);
    }
  }, [buildParams, filters.financialYears]);

  useEffect(() => {
    loadReport();
  }, [loadReport]);

  const filteredBillingNames = useMemo(
    () => billingNames.filter((billingName) => !filters.clientIds.length || filters.clientIds.includes(billingName.client_id)),
    [billingNames, filters.clientIds],
  );

  const revenueTotalByMonth = useMemo(() => {
    const totals = {};
    reportData.periods.forEach((period) => { totals[period] = 0; });
    reportData.revenue.forEach((row) => {
      reportData.periods.forEach((period) => {
        totals[period] += Number(row.months?.[period] || 0);
      });
    });
    return totals;
  }, [reportData.periods, reportData.revenue]);

  const expenseTotalByMonth = useMemo(() => {
    const totals = {};
    reportData.periods.forEach((period) => { totals[period] = 0; });
    reportData.expense.forEach((row) => {
      reportData.periods.forEach((period) => {
        totals[period] += Number(row.months?.[period] || 0);
      });
    });
    return totals;
  }, [reportData.expense, reportData.periods]);

  const totalRevenue = useMemo(
    () => reportData.revenue.reduce((sum, row) => sum + Number(row.total || 0), 0),
    [reportData.revenue],
  );
  const totalExpense = useMemo(
    () => reportData.expense.reduce((sum, row) => sum + Number(row.total || 0), 0),
    [reportData.expense],
  );
  const totalAdminExpense = useMemo(
    () => (reportData.admin_expense || []).reduce((sum, row) => sum + Number(row.total || 0), 0),
    [reportData.admin_expense],
  );
  const adminExpenseTotalByMonth = useMemo(() => {
    const totals = {};
    reportData.periods.forEach((period) => { totals[period] = 0; });
    (reportData.admin_expense || []).forEach((row) => {
      reportData.periods.forEach((period) => {
        totals[period] += Number(row.months?.[period] || 0);
      });
    });
    return totals;
  }, [reportData.admin_expense, reportData.periods]);
  const netTotal = totalRevenue - totalExpense;
  const hasSpecificFilter = filters.clientIds.length > 0 || filters.billingNameIds.length > 0 || filters.reviewerIds.length > 0;
  const showAdminExpenses = !hasSpecificFilter && (reportData.admin_expense || []).length > 0;
  const netAfterAdmin = netTotal - (showAdminExpenses ? totalAdminExpense : 0);

  const exportCsv = () => {
    const headers = ['Section', 'Head', ...reportData.periods, 'Visible Total'];
    const rows = [];

    reportData.revenue.forEach((row) => {
      rows.push(['Revenue', row.head_name, ...reportData.periods.map((period) => row.months?.[period] || 0), row.total || 0]);
    });
    rows.push(['Revenue Total', '', ...reportData.periods.map((period) => revenueTotalByMonth[period] || 0), totalRevenue]);

    reportData.expense.forEach((row) => {
      rows.push(['Expense', row.head_name, ...reportData.periods.map((period) => row.months?.[period] || 0), row.total || 0]);
    });
    rows.push(['Expense Total', '', ...reportData.periods.map((period) => expenseTotalByMonth[period] || 0), totalExpense]);
    rows.push(['Net', '', ...reportData.periods.map((period) => (revenueTotalByMonth[period] || 0) - (expenseTotalByMonth[period] || 0)), netTotal]);

    if (showAdminExpenses) {
      (reportData.admin_expense || []).forEach((row) => {
        rows.push(['Admin Cost', row.head_name, ...reportData.periods.map((period) => row.months?.[period] || 0), row.total || 0]);
      });
      rows.push(['Admin Cost Total', '', ...reportData.periods.map((period) => adminExpenseTotalByMonth[period] || 0), totalAdminExpense]);
      rows.push(['Net After Admin', '', ...reportData.periods.map((period) => (revenueTotalByMonth[period] || 0) - (expenseTotalByMonth[period] || 0) - (adminExpenseTotalByMonth[period] || 0)), netAfterAdmin]);
    }

    const csv = [headers.join(','), ...rows.map((row) => row.join(','))].join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `report-matrix-${filters.financialYears.join('_') || 'current'}.csv`;
    link.click();
    URL.revokeObjectURL(url);
  };

  const financialYearOptions = financialYears.map((financialYear) => ({ value: financialYear, label: financialYear }));
  const clientOptions = clients.map((client) => ({ value: client.id, label: client.name }));
  const billingOptions = filteredBillingNames.map((billingName) => ({ value: billingName.id, label: billingName.name }));
  const reviewerOptions = reviewers.map((reviewer) => ({ value: reviewer.id, label: reviewer.name }));
  const billingStatusOptions = [
    { value: 'billed', label: 'Billed' },
    { value: 'unbilled', label: 'Unbilled' },
    { value: 'projected', label: 'Projected' },
  ];

  const setArrayFilter = (key) => (nextValue) => {
    setFilters((previous) => {
      const next = { ...previous, [key]: nextValue };
      if (key === 'clientIds') next.billingNameIds = [];
      return next;
    });
  };

  const headerCellStyles = {
    py: 1,
    px: 1.2,
    whiteSpace: 'nowrap',
    background: `linear-gradient(135deg, ${alpha(theme.palette.primary.light, 0.44)} 0%, ${alpha(theme.palette.secondary.light, 0.44)} 100%)`,
  };
  const cellStyles = {
    py: 0.8,
    px: 1.2,
    whiteSpace: 'nowrap',
    fontSize: '0.82rem',
  };

  return (
    <Box>
      <PageHeader
        eyebrow="Matrix Report"
        title="Compact reporting matrix"
        actions={[
          <HoverActionButton key="export" icon={<Download fontSize="small" />} label="Export CSV" onClick={exportCsv} tone="peach" />,
          <HoverActionButton key="refresh" icon={<Refresh fontSize="small" />} label="Refresh" onClick={loadReport} tone="mint" />,
          <HoverActionButton
            key="mode"
            icon={<Tune fontSize="small" />}
            label={filterMode === 'dropdown' ? 'Dropdown filters' : 'Full filters'}
            onClick={() => setFilterMode((previous) => (previous === 'dropdown' ? 'full' : 'dropdown'))}
            tone="sand"
          />,
        ]}
        chips={[
          { label: `${reportData.periods.length} periods`, background: alpha(theme.palette.primary.light, 0.38) },
          { label: `${filters.financialYears.length || 0} FY active`, background: alpha(theme.palette.secondary.light, 0.42) },
        ]}
      />

      <Grid container spacing={2} sx={{ mb: 2.5 }}>
        <Grid item xs={12} md={4}>
          <MetricCard eyebrow="Revenue" title="Total revenue" value={fmt(totalRevenue)} icon={<TableChart fontSize="small" />} tone="mint" />
        </Grid>
        <Grid item xs={12} md={4}>
          <MetricCard eyebrow="Expense" title="Total expense" value={fmt(totalExpense)} icon={<ViewCompactAlt fontSize="small" />} tone="sand" />
        </Grid>
        <Grid item xs={12} md={4}>
          <MetricCard eyebrow="Net" title={netTotal >= 0 ? 'Net profit' : 'Net loss'} value={fmt(netTotal)} helper={netTotal >= 0 ? 'Healthy spread' : 'Negative spread'} icon={<Refresh fontSize="small" />} tone={netTotal >= 0 ? 'peach' : 'rose'} />
        </Grid>
      </Grid>

      <FilterPanel
        mode={filterMode}
        onModeChange={setFilterMode}
        title="Report filters"
        onClear={
          <Button
            variant="outlined"
            onClick={() => {
              const fallbackFinancialYear = filters.financialYears[0] || resolveDefaultFinancialYear(financialYears);
              setFilters({
                financialYears: fallbackFinancialYear ? [fallbackFinancialYear] : [],
                clientIds: [],
                billingNameIds: [],
                reviewerIds: [],
                billingStatus: [],
              });
            }}
          >
            Clear filters
          </Button>
        }
      >
        <Grid container spacing={1.5}>
          <Grid item xs={12} md={2.4}>
            <SelectionField
              label="Fiscal Year"
              value={filters.financialYears}
              options={financialYearOptions}
              onChange={setArrayFilter('financialYears')}
              mode={filterMode}
              tone="mint"
              multi
            />
          </Grid>
          <Grid item xs={12} md={2.4}>
            <SelectionField
              label="Group"
              value={filters.clientIds}
              options={clientOptions}
              onChange={setArrayFilter('clientIds')}
              mode={filterMode}
              tone="peach"
              multi
            />
          </Grid>
          <Grid item xs={12} md={2.4}>
            <SelectionField
              label="Client"
              value={filters.billingNameIds}
              options={billingOptions}
              onChange={setArrayFilter('billingNameIds')}
              mode={filterMode}
              tone="sand"
              multi
            />
          </Grid>
          <Grid item xs={12} md={2.4}>
            <SelectionField
              label="Reviewer"
              value={filters.reviewerIds}
              options={reviewerOptions}
              onChange={setArrayFilter('reviewerIds')}
              mode={filterMode}
              tone="rose"
              multi
            />
          </Grid>
          <Grid item xs={12} md={2.4}>
            <SelectionField
              label="Billing Status"
              value={filters.billingStatus}
              options={billingStatusOptions}
              onChange={setArrayFilter('billingStatus')}
              mode={filterMode}
              tone="sage"
              multi
            />
          </Grid>
        </Grid>
      </FilterPanel>

      {loading ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 8 }}>
          <CircularProgress />
        </Box>
      ) : (
        <SectionCard
          title="Color-segregated report ledger"
          tone="sage"
          contentSx={{ p: 0 }}
        >
          <TableContainer>
            <Table stickyHeader size="small">
              <TableHead>
                <TableRow>
                  <TableCell sx={{ ...headerCellStyles, position: 'sticky', left: 0, zIndex: 5 }}>Particular</TableCell>
                  {reportData.periods.map((period) => (
                    <TableCell key={period} align="right" sx={headerCellStyles}>
                      {period}
                    </TableCell>
                  ))}
                  <TableCell align="right" sx={headerCellStyles}>Visible total</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                <TableRow>
                  <TableCell
                    colSpan={reportData.periods.length + 2}
                    sx={{
                      ...cellStyles,
                      fontWeight: 800,
                      color: theme.palette.primary.dark,
                      background: alpha(theme.palette.primary.light, 0.42),
                    }}
                  >
                    Revenue
                  </TableCell>
                </TableRow>
                {reportData.revenue.map((row) => (
                  <TableRow key={row.head_name}>
                    <TableCell sx={{ ...cellStyles, position: 'sticky', left: 0, zIndex: 3, background: alpha('#ffffff', 0.96), fontWeight: 700 }}>
                      {row.head_name}
                    </TableCell>
                    {reportData.periods.map((period) => (
                      <TableCell key={period} align="right" sx={cellStyles}>
                        {fmt(row.months?.[period])}
                      </TableCell>
                    ))}
                    <TableCell align="right" sx={{ ...cellStyles, fontWeight: 700 }}>{fmt(row.total)}</TableCell>
                  </TableRow>
                ))}
                <TableRow sx={{ background: alpha(theme.palette.primary.light, 0.28) }}>
                  <TableCell sx={{ ...cellStyles, position: 'sticky', left: 0, zIndex: 3, background: alpha(theme.palette.primary.light, 0.28), fontWeight: 800 }}>
                    Revenue total
                  </TableCell>
                  {reportData.periods.map((period) => (
                    <TableCell key={period} align="right" sx={{ ...cellStyles, fontWeight: 800 }}>
                      {fmt(revenueTotalByMonth[period])}
                    </TableCell>
                  ))}
                  <TableCell align="right" sx={{ ...cellStyles, fontWeight: 800 }}>{fmt(totalRevenue)}</TableCell>
                </TableRow>

                <TableRow>
                  <TableCell
                    colSpan={reportData.periods.length + 2}
                    sx={{
                      ...cellStyles,
                      fontWeight: 800,
                      color: theme.palette.warning.dark,
                      background: alpha(theme.palette.warning.light, 0.46),
                    }}
                  >
                    Expenses
                  </TableCell>
                </TableRow>
                {reportData.expense.map((row) => (
                  <TableRow key={row.head_name}>
                    <TableCell sx={{ ...cellStyles, position: 'sticky', left: 0, zIndex: 3, background: alpha('#ffffff', 0.96), fontWeight: 700 }}>
                      {row.head_name}
                    </TableCell>
                    {reportData.periods.map((period) => (
                      <TableCell key={period} align="right" sx={cellStyles}>
                        {fmt(row.months?.[period])}
                      </TableCell>
                    ))}
                    <TableCell align="right" sx={{ ...cellStyles, fontWeight: 700 }}>{fmt(row.total)}</TableCell>
                  </TableRow>
                ))}
                <TableRow sx={{ background: alpha(theme.palette.warning.light, 0.34) }}>
                  <TableCell sx={{ ...cellStyles, position: 'sticky', left: 0, zIndex: 3, background: alpha(theme.palette.warning.light, 0.34), fontWeight: 800 }}>
                    Expense total
                  </TableCell>
                  {reportData.periods.map((period) => (
                    <TableCell key={period} align="right" sx={{ ...cellStyles, fontWeight: 800 }}>
                      {fmt(expenseTotalByMonth[period])}
                    </TableCell>
                  ))}
                  <TableCell align="right" sx={{ ...cellStyles, fontWeight: 800 }}>{fmt(totalExpense)}</TableCell>
                </TableRow>

                <TableRow sx={{ background: alpha(theme.palette.secondary.light, 0.42) }}>
                  <TableCell sx={{ ...cellStyles, position: 'sticky', left: 0, zIndex: 3, background: alpha(theme.palette.secondary.light, 0.42), fontWeight: 900 }}>
                    Net profit / (loss)
                  </TableCell>
                  {reportData.periods.map((period) => {
                    const net = Number(revenueTotalByMonth[period] || 0) - Number(expenseTotalByMonth[period] || 0);
                    return (
                      <TableCell
                        key={period}
                        align="right"
                        sx={{
                          ...cellStyles,
                          fontWeight: 900,
                          color: net >= 0 ? theme.palette.primary.dark : theme.palette.error.dark,
                        }}
                      >
                        {fmt(net)}
                      </TableCell>
                    );
                  })}
                  <TableCell
                    align="right"
                    sx={{
                      ...cellStyles,
                      fontWeight: 900,
                      color: netTotal >= 0 ? theme.palette.primary.dark : theme.palette.error.dark,
                    }}
                  >
                    {fmt(netTotal)}
                  </TableCell>
                </TableRow>

                {/* Admin Costs section - shown only when no specific filters applied */}
                {showAdminExpenses && (
                  <>
                <TableRow>
                  <TableCell
                    colSpan={reportData.periods.length + 2}
                    sx={{
                      ...cellStyles,
                      fontWeight: 800,
                      color: theme.palette.info.dark,
                      background: alpha(theme.palette.info.light, 0.36),
                    }}
                  >
                    Admin Costs
                  </TableCell>
                </TableRow>
                {(reportData.admin_expense || []).map((row) => (
                  <TableRow key={`admin-${row.head_name}`}>
                    <TableCell sx={{ ...cellStyles, position: 'sticky', left: 0, zIndex: 3, background: alpha('#ffffff', 0.96), fontWeight: 700 }}>
                      {row.head_name}
                    </TableCell>
                    {reportData.periods.map((period) => (
                      <TableCell key={period} align="right" sx={cellStyles}>
                        {fmt(row.months?.[period])}
                      </TableCell>
                    ))}
                    <TableCell align="right" sx={{ ...cellStyles, fontWeight: 700 }}>{fmt(row.total)}</TableCell>
                  </TableRow>
                ))}
                <TableRow sx={{ background: alpha(theme.palette.info.light, 0.28) }}>
                  <TableCell sx={{ ...cellStyles, position: 'sticky', left: 0, zIndex: 3, background: alpha(theme.palette.info.light, 0.28), fontWeight: 800 }}>
                    Admin cost total
                  </TableCell>
                  {reportData.periods.map((period) => (
                    <TableCell key={period} align="right" sx={{ ...cellStyles, fontWeight: 800 }}>
                      {fmt(adminExpenseTotalByMonth[period])}
                    </TableCell>
                  ))}
                  <TableCell align="right" sx={{ ...cellStyles, fontWeight: 800 }}>{fmt(totalAdminExpense)}</TableCell>
                </TableRow>

                <TableRow sx={{ background: alpha(theme.palette.secondary.dark, 0.18) }}>
                  <TableCell sx={{ ...cellStyles, position: 'sticky', left: 0, zIndex: 3, background: alpha(theme.palette.secondary.dark, 0.18), fontWeight: 900 }}>
                    Net profit after admin cost
                  </TableCell>
                  {reportData.periods.map((period) => {
                    const netAfter = Number(revenueTotalByMonth[period] || 0) - Number(expenseTotalByMonth[period] || 0) - Number(adminExpenseTotalByMonth[period] || 0);
                    return (
                      <TableCell
                        key={period}
                        align="right"
                        sx={{
                          ...cellStyles,
                          fontWeight: 900,
                          color: netAfter >= 0 ? theme.palette.primary.dark : theme.palette.error.dark,
                        }}
                      >
                        {fmt(netAfter)}
                      </TableCell>
                    );
                  })}
                  <TableCell
                    align="right"
                    sx={{
                      ...cellStyles,
                      fontWeight: 900,
                      color: netAfterAdmin >= 0 ? theme.palette.primary.dark : theme.palette.error.dark,
                    }}
                  >
                    {fmt(netAfterAdmin)}
                  </TableCell>
                </TableRow>
                  </>
                )}

              </TableBody>
            </Table>
          </TableContainer>
        </SectionCard>
      )}
    </Box>
  );
};

export default Reports;
