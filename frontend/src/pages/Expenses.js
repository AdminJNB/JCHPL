import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Alert, Box, Button, Checkbox, Chip, Dialog, DialogActions,
  DialogContent, DialogTitle, FormControl, FormControlLabel, Grid, IconButton, InputLabel,
  MenuItem, Select, Tab, Tabs, TextField, Typography
} from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import { DataGrid } from '@mui/x-data-grid';
import { Add, Delete, Edit, MoneyOff, Refresh, Search, Tune, ViewSidebar } from '@mui/icons-material';
import { toast } from 'react-toastify';
import { useAuth } from '../context/AuthContext';
import {
  FilterPanel,
  HoverActionButton,
  MetricCard,
  PageHeader,
  SectionCard,
  SelectionField,
} from '../components/FuturisticUI';
import {
  expenseAPI,
  expenseHeadAPI,
  teamAPI,
  clientAPI,
  servicePeriodAPI,
  billFromAPI,
} from '../services/api';
import { resolveDefaultFinancialYear } from '../utils/periods';

const DEFAULT_FORM = {
  source_type: 'non-recurring',
  recurring_expense_client_id: '',
  team_client_allocation_id: '',
  client_id: '',
  team_id: '',
  expense_head_id: '',
  financial_year: '',
  service_period_id: '',
  date: '',
  billing_status: 'billed',
  description: '',
  projected_amount: '',
  amount: '',
  igst: '',
  cgst: '',
  sgst: '',
  other_charges: '',
  round_off: '',
  total_amount: '',
  bill_from: '',
  is_unbilled: false,
  is_entered_in_books: false,
  is_admin: false,
};

const formatCurrency = (value) =>
  `INR ${Number(value || 0).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;

const getBillingStatus = (row) => {
  if (row?.is_projected) return 'projected';
  return row?.is_unbilled ? 'unbilled' : 'billed';
};

const computeTotal = (formData) =>
  [
    formData.amount,
    formData.igst,
    formData.cgst,
    formData.sgst,
    formData.other_charges,
    formData.round_off,
  ].reduce((sum, value) => sum + (parseFloat(value) || 0), 0);

const Expenses = () => {
  const theme = useTheme();
  const { user } = useAuth();
  const [rows, setRows] = useState([]);
  const [summary, setSummary] = useState({
    total_expense: 0,
    billed_expense: 0,
    projected_expense: 0,
    unbilled_expense: 0,
    total_records: 0,
  });
  const [expenseHeads, setExpenseHeads] = useState([]);
  const [teams, setTeams] = useState([]);
  const [clients, setClients] = useState([]);
  const [servicePeriods, setServicePeriods] = useState([]);
  const [billFroms, setBillFroms] = useState([]);
  const [financialYears, setFinancialYears] = useState([]);
  const [loading, setLoading] = useState(true);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [selected, setSelected] = useState(null);
  const [sourceType, setSourceType] = useState('all');
  const [filterMode, setFilterMode] = useState('dropdown');
  const [filters, setFilters] = useState({
    financialYear: [],
    clientId: [],
    teamId: [],
    expenseHeadId: [],
    servicePeriodId: '',
    search: '',
  });
  const [formData, setFormData] = useState(DEFAULT_FORM);
  const [error, setError] = useState('');

  const activeClients = useMemo(
    () => clients.filter((client) => client.is_active !== false),
    [clients]
  );
  const activeTeams = useMemo(
    () => teams.filter((team) => team.is_active !== false),
    [teams]
  );
  const activeExpenseHeads = useMemo(
    () => expenseHeads.filter((head) => head.is_active !== false),
    [expenseHeads]
  );
  const activeBillFroms = useMemo(
    () => billFroms.filter((billFrom) => billFrom.is_active !== false),
    [billFroms]
  );

  const loadMasters = useCallback(async () => {
    const [expenseHeadRes, teamRes, clientRes, servicePeriodRes, financialYearRes, billFromRes] = await Promise.all([
      expenseHeadAPI.getAll({ includeInactive: true }),
      teamAPI.getAll({ includeInactive: true }),
      clientAPI.getAll({ includeInactive: true }),
      servicePeriodAPI.getAll(),
      servicePeriodAPI.getFinancialYears(),
      billFromAPI.getAll({ includeInactive: true }),
    ]);

    const years = financialYearRes.data.data || [];
    const defaultFY = resolveDefaultFinancialYear(years);

    setExpenseHeads(expenseHeadRes.data.data || []);
    setTeams(teamRes.data.data || []);
    setClients(clientRes.data.data || []);
    setServicePeriods(servicePeriodRes.data.data || []);
    setFinancialYears(years);
    setBillFroms(billFromRes.data.data || []);
    setFilters((prev) => ({
      ...prev,
      financialYear: prev.financialYear.length ? prev.financialYear : (defaultFY ? [defaultFY] : []),
    }));
  }, []);  // eslint-disable-line react-hooks/exhaustive-deps

  const loadExpenses = useCallback(async () => {
    setLoading(true);
    try {
      const yearsToLoad = filters.financialYear.length ? filters.financialYear : [];
      if (!yearsToLoad.length) {
        setRows([]);
        setSummary({
          total_expense: 0,
          billed_expense: 0,
          projected_expense: 0,
          unbilled_expense: 0,
          total_records: 0,
        });
        return;
      }

      const responses = await Promise.all(
        yearsToLoad.map(async (financialYear) => {
          const params = {
            sourceType,
            financialYear,
            clientId: filters.clientId.length ? filters.clientId.join(',') : undefined,
            teamId: filters.teamId.length ? filters.teamId.join(',') : undefined,
            expenseHeadId: filters.expenseHeadId.length ? filters.expenseHeadId.join(',') : undefined,
            servicePeriodId: filters.servicePeriodId || undefined,
          };

          const [listResponse, summaryResponse] = await Promise.all([
            expenseAPI.getAll(params),
            expenseAPI.getSummary(params),
          ]);

          return {
            rows: listResponse.data.data || [],
            summary: summaryResponse.data.data || {
              total_expense: 0,
              billed_expense: 0,
              projected_expense: 0,
              unbilled_expense: 0,
              total_records: 0,
            },
          };
        }),
      );

      setRows(
        responses
          .flatMap((response) => response.rows)
          .sort((left, right) => (left.client_name || '').localeCompare(right.client_name || '') || new Date(left.period_start_date || 0) - new Date(right.period_start_date || 0)),
      );
      setSummary(
        responses.reduce((accumulator, response) => ({
          total_expense: accumulator.total_expense + Number(response.summary.total_expense || 0),
          billed_expense: accumulator.billed_expense + Number(response.summary.billed_expense || 0),
          projected_expense: accumulator.projected_expense + Number(response.summary.projected_expense || 0),
          unbilled_expense: accumulator.unbilled_expense + Number(response.summary.unbilled_expense || 0),
          total_records: accumulator.total_records + Number(response.summary.total_records || 0),
        }), {
          total_expense: 0,
          billed_expense: 0,
          projected_expense: 0,
          unbilled_expense: 0,
          total_records: 0,
        }),
      );
    } catch (err) {
      toast.error('Failed to load expenses');
    } finally {
      setLoading(false);
    }
  }, [filters, sourceType]);

  useEffect(() => {
    loadMasters().catch(() => toast.error('Failed to load expense masters'));
  }, [loadMasters]);

  useEffect(() => {
    loadExpenses();
  }, [loadExpenses]);

  const openDialog = async (row = null) => {
    setSelected(row);
    if (!row) {
      setFormData({
        ...DEFAULT_FORM,
        financial_year: filters.financialYear[0] || '',
        date: new Date().toISOString().slice(0, 10),
      });
      setError('');
      setDialogOpen(true);
      return;
    }

    if (row.source_type === 'team-recurring' && String(row.id).startsWith('team-')) {
      const sp = servicePeriods.find((p) => p.id === row.service_period_id);
      setFormData({
        source_type: 'team-recurring',
        recurring_expense_client_id: '',
        team_client_allocation_id: row.team_client_allocation_id || '',
        client_id: row.client_id || '',
        team_id: row.team_id || '',
        expense_head_id: row.expense_head_id || '',
        financial_year: sp?.financial_year || filters.financialYear[0] || '',
        service_period_id: row.service_period_id || '',
        date: row.is_projected ? '' : (row.date ? String(row.date).slice(0, 10) : new Date().toISOString().slice(0, 10)),
        billing_status: getBillingStatus(row),
        description: row.description || '',
        projected_amount: row.projected_amount || row.amount || '',
        amount: row.amount || '',
        igst: row.igst || '',
        cgst: row.cgst || '',
        sgst: row.sgst || '',
        other_charges: row.other_charges || '',
        round_off: row.round_off || '',
        total_amount: row.total_amount || '',
        bill_from: row.bill_from || '',
        is_unbilled: row.is_projected ? true : Boolean(row.is_unbilled),
        is_entered_in_books: row.is_projected ? false : Boolean(row.is_entered_in_books),
        is_admin: Boolean(row.is_admin),
      });
      setError('');
      setDialogOpen(true);
      return;
    }

    if (row.source_type === 'recurring' && String(row.id).startsWith('recurring-')) {
      const sp = servicePeriods.find((p) => p.id === row.service_period_id);
      setFormData({
        source_type: 'recurring',
        recurring_expense_client_id: row.recurring_expense_client_id || '',
        client_id: row.client_id || '',
        team_id: row.team_id || '',
        expense_head_id: row.expense_head_id || '',
        financial_year: sp?.financial_year || filters.financialYear[0] || '',
        service_period_id: row.service_period_id || '',
        // Projected: no date yet
        date: row.is_projected ? '' : (row.date ? String(row.date).slice(0, 10) : new Date().toISOString().slice(0, 10)),
        billing_status: getBillingStatus(row),
        description: row.description || '',
        projected_amount: row.projected_amount || row.amount || '',
        amount: row.amount || '',
        igst: row.igst || '',
        cgst: row.cgst || '',
        sgst: row.sgst || '',
        other_charges: row.other_charges || '',
        round_off: row.round_off || '',
        total_amount: row.total_amount || '',
        bill_from: row.bill_from || '',
        is_unbilled: row.is_projected ? true : Boolean(row.is_unbilled),
        is_entered_in_books: row.is_projected ? false : Boolean(row.is_entered_in_books),
        is_admin: Boolean(row.is_admin),
      });
      setError('');
      setDialogOpen(true);
      return;
    }

    try {
      const res = await expenseAPI.getById(row.id);
      const data = res.data.data;
      const sp = servicePeriods.find((p) => p.id === data.service_period_id);
      setFormData({
        source_type: data.source_type || 'non-recurring',
        recurring_expense_client_id: data.recurring_expense_client_id || '',
        team_client_allocation_id: data.team_client_allocation_id || '',
        client_id: data.client_id || '',
        team_id: data.team_id || '',
        expense_head_id: data.expense_head_id || '',
        financial_year: sp?.financial_year || '',
        service_period_id: data.service_period_id || '',
        date: data.date ? String(data.date).slice(0, 10) : '',
        billing_status: getBillingStatus(row || data),
        description: data.description || '',
        projected_amount: row?.projected_amount || data.amount || '',
        amount: data.amount || '',
        igst: data.igst || '',
        cgst: data.cgst || '',
        sgst: data.sgst || '',
        other_charges: data.other_charges || '',
        round_off: data.round_off || '',
        total_amount: data.total_amount || '',
        bill_from: data.bill_from || '',
        is_unbilled: Boolean(data.is_unbilled),
        is_entered_in_books: Boolean(data.is_entered_in_books),
        is_admin: Boolean(data.is_admin),
      });
      setError('');
      setDialogOpen(true);
    } catch (err) {
      toast.error('Failed to load expense details');
    }
  };

  const closeDialog = () => {
    setDialogOpen(false);
    setSelected(null);
    setFormData(DEFAULT_FORM);
    setError('');
  };

  const handleFormChange = (field, value) => {
    const next = { ...formData, [field]: value };
    if (field === 'cgst') next.sgst = value;
    if (field === 'sgst') next.cgst = value;
    next.total_amount = computeTotal(next);
    setFormData(next);
  };

  const handleSubmit = async () => {
    const isRecurringLike = formData.source_type === 'recurring' || formData.source_type === 'team-recurring';
    const isSyntheticId = String(selected?.id || '').startsWith('recurring-') || String(selected?.id || '').startsWith('team-');
    const persistedExpenseId = selected?.expense_record_id || (!isSyntheticId ? selected?.id : '');
    const billingStatus = formData.billing_status || (formData.is_unbilled ? 'unbilled' : 'billed');

    if (isRecurringLike && billingStatus === 'projected') {
      if (persistedExpenseId) {
        setError('Recurring expense entries cannot be deleted or reverted to projected. Update the amount to 0 instead.');
        return;
      }
      closeDialog();
      loadExpenses();
      return;
    }

    if (!formData.expense_head_id || !formData.service_period_id || !formData.date) {
      setError('Expense head, service period, and book date are required');
      return;
    }
    if (formData.amount === '' || Number(formData.amount) < 0) {
      setError('Expense amount is required');
      return;
    }

    const payload = {
      source_type: formData.source_type,
      recurring_expense_client_id: formData.recurring_expense_client_id || undefined,
      team_client_allocation_id: formData.team_client_allocation_id || undefined,
      client_id: formData.client_id || undefined,
      team_id: formData.team_id || undefined,
      expense_head_id: formData.expense_head_id,
      service_period_id: formData.service_period_id,
      date: formData.date,
      description: formData.description.trim() || undefined,
      amount: Number(formData.amount || 0),
      igst: Number(formData.igst || 0),
      cgst: Number(formData.cgst || 0),
      sgst: Number(formData.sgst || 0),
      other_charges: Number(formData.other_charges || 0),
      round_off: Number(formData.round_off || 0),
      total_amount: Number(formData.total_amount || computeTotal(formData)),
      bill_from: formData.bill_from || undefined,
      is_unbilled: billingStatus === 'unbilled',
      is_entered_in_books: isRecurringLike ? billingStatus === 'billed' : Boolean(formData.is_entered_in_books),
      is_admin: Boolean(formData.is_admin),
    };

    try {
      if (persistedExpenseId) {
        await expenseAPI.update(persistedExpenseId, payload);
        toast.success('Expense updated successfully');
      } else {
        await expenseAPI.create(payload);
        const msg = payload.source_type === 'recurring'
          ? 'Recurring expense override saved'
          : payload.source_type === 'team-recurring'
            ? 'Team expense saved'
            : 'Expense created successfully';
        toast.success(msg);
      }

      closeDialog();
      loadExpenses();
    } catch (err) {
      setError(err.response?.data?.message || err.response?.data?.errors?.[0]?.msg || 'Failed to save expense');
    }
  };

  const handleDelete = async () => {
    const expenseId = selected?.expense_record_id || selected?.id;
    if (!expenseId) return;
    if (selected?.source_type === 'recurring' || selected?.source_type === 'team-recurring') {
      toast.error('Recurring expense entries cannot be deleted. Update the amount to 0 instead.');
      setDeleteDialogOpen(false);
      return;
    }
    try {
      await expenseAPI.delete(expenseId);
      toast.success('Expense deleted successfully');
      setDeleteDialogOpen(false);
      setSelected(null);
      loadExpenses();
    } catch (err) {
      toast.error(err.response?.data?.message || 'Failed to delete expense');
    }
  };

  const filteredPeriods = servicePeriods.filter((period) =>
    !filters.financialYear.length || filters.financialYear.includes(period.financial_year)
  );

  const visibleRows = rows.filter((row) => {
    const search = filters.search.trim().toLowerCase();
    if (!search) return true;
    return [
      row.expense_head_name,
      row.vendor_name,
      row.team_name,
      row.client_name,
      row.description,
      row.bill_from,
      row.service_period_name,
    ].some((value) => String(value || '').toLowerCase().includes(search));
  });
  const isRecurringDialog = formData.source_type === 'recurring' || formData.source_type === 'team-recurring';
  const isProjectedSelection = formData.billing_status === 'projected';
  const hasSavedRecurringExpenseOverride = isRecurringDialog && Boolean(
    selected?.expense_record_id ||
    (
      selected &&
      !String(selected.id || '').startsWith('recurring-') &&
      !String(selected.id || '').startsWith('team-')
    )
  );
  const canKeepProjectedExpense = isRecurringDialog && !hasSavedRecurringExpenseOverride;
  const projectedComparisonAmount = Number(formData.projected_amount || 0);
  const currentActualAmount = Number(formData.amount || 0);
  const recurringVarianceAmount = currentActualAmount - projectedComparisonAmount;
  const actualBillingLabel = formData.billing_status === 'unbilled' ? 'Unbilled' : 'Billed';
  const showProjectedReference = isRecurringDialog
    && !isProjectedSelection
    && Math.abs(currentActualAmount - projectedComparisonAmount) > 0.0001;

  const columns = [
    {
      field: 'source_type',
      headerName: 'Type',
      width: 120,
      renderCell: (params) => (
        <Chip
          size="small"
          label={params.value === 'recurring' ? 'Recurring' : params.value === 'team-recurring' ? 'Team' : 'Non-Recurring'}
          color={params.value === 'recurring' ? 'secondary' : params.value === 'team-recurring' ? 'info' : 'primary'}
        />
      ),
    },
    { field: 'service_period_name', headerName: 'Period', width: 100, renderCell: (params) => params.value || '-' },
    { field: 'expense_head_name', headerName: 'Expense Head', flex: 1, minWidth: 180 },
    { field: 'vendor_name', headerName: 'Vendor', flex: 0.8, minWidth: 130, renderCell: (params) => params.value || '-' },
    { field: 'team_name', headerName: 'Team', flex: 1, minWidth: 150, renderCell: (params) => params.value || '-' },
    { field: 'client_name', headerName: 'Group', flex: 1, minWidth: 150, renderCell: (params) => params.value || '-' },
    { field: 'description', headerName: 'Description', flex: 1.2, minWidth: 220, renderCell: (params) => params.value || '-' },
    { field: 'date', headerName: 'Book Date', width: 115, renderCell: (params) => params.value ? String(params.value).slice(0, 10) : '-' },
    { field: 'amount', headerName: 'Amount', width: 140, renderCell: (params) => formatCurrency(params.value) },
    { field: 'total_amount', headerName: 'Total', width: 140, renderCell: (params) => formatCurrency(params.value) },
    { field: 'bill_from', headerName: 'Bill From', width: 140, renderCell: (params) => params.value || '-' },
    {
      field: 'is_unbilled',
      headerName: 'Status',
      width: 120,
      renderCell: (params) => {
        if (params.row.is_projected) {
          return <Chip size="small" label="Projected" color="info" />;
        }
        return <Chip size="small" label={params.row.is_unbilled ? 'Unbilled' : 'Billed'} color={params.row.is_unbilled ? 'warning' : 'success'} />;
      },
    },
    {
      field: 'is_admin',
      headerName: 'Admin',
      width: 80,
      renderCell: (params) => params.value ? <Chip size="small" label="Yes" color="warning" /> : null,
    },
    {
      field: 'actions',
      headerName: 'Actions',
      width: 120,
      sortable: false,
      renderCell: (params) => (
        <Box>
          <IconButton size="small" color="primary" onClick={() => openDialog(params.row)}>
            <Edit fontSize="small" />
          </IconButton>
          {user?.can_delete && params.row.source_type === 'non-recurring' && (
            <IconButton
              size="small"
              color="error"
              onClick={() => {
                setSelected(params.row);
                setDeleteDialogOpen(true);
              }}
            >
              <Delete fontSize="small" />
            </IconButton>
          )}
        </Box>
      ),
    },
  ];

  const financialYearOptions = financialYears.map((fy) => ({ value: fy, label: fy }));
  const expenseHeadOptions = expenseHeads.filter((head) => head.is_active !== false).map((head) => ({ value: head.id, label: head.name }));
  const teamOptions = teams.filter((team) => team.is_active !== false).map((team) => ({ value: team.id, label: team.name }));
  const clientOptions = activeClients.map((client) => ({ value: client.id, label: client.name }));

  const setArrayFilter = (field) => (value) => {
    setFilters((prev) => {
      const next = { ...prev, [field]: value };
      if (field === 'financialYear') next.servicePeriodId = '';
      return next;
    });
  };

  return (
    <Box>
      <PageHeader
        eyebrow="Expense Workspace"
        title="Expense command surface"
        actions={[
          <HoverActionButton key="add" icon={<Add fontSize="small" />} label="Add expense" onClick={() => openDialog()} tone="mint" />,
          <HoverActionButton key="refresh" icon={<Refresh fontSize="small" />} label="Refresh" onClick={loadExpenses} tone="peach" />,
          <HoverActionButton
            key="mode"
            icon={<Tune fontSize="small" />}
            label={filterMode === 'dropdown' ? 'Dropdown filters' : 'Full filters'}
            onClick={() => setFilterMode((prev) => (prev === 'dropdown' ? 'full' : 'dropdown'))}
            tone="sand"
          />,
        ]}
        chips={[
          { label: `${filters.financialYear.length || 0} FY active`, background: alpha(theme.palette.primary.light, 0.34) },
          { label: `${visibleRows.length} visible rows`, background: alpha(theme.palette.secondary.light, 0.42) },
        ]}
      />

      <Grid container spacing={2} sx={{ mb: 2.5 }}>
        <Grid item xs={12} md={3}>
          <MetricCard eyebrow="Total" title="Visible expense" value={formatCurrency(summary.total_expense || 0)} icon={<MoneyOff fontSize="small" />} tone="mint" />
        </Grid>
        <Grid item xs={12} md={3}>
          <MetricCard eyebrow="Booked" title="Billed expense" value={formatCurrency(summary.billed_expense || 0)} icon={<Refresh fontSize="small" />} tone="peach" />
        </Grid>
        <Grid item xs={12} md={3}>
          <MetricCard eyebrow="Unbilled" title="Past unbilled" value={formatCurrency(summary.unbilled_expense || 0)} icon={<ViewSidebar fontSize="small" />} tone="sand" />
        </Grid>
        <Grid item xs={12} md={3}>
          <MetricCard eyebrow="Projected" title="Future expense" value={formatCurrency(summary.projected_expense || 0)} icon={<Tune fontSize="small" />} tone="rose" />
        </Grid>
      </Grid>

      <FilterPanel
        mode={filterMode}
        onModeChange={setFilterMode}
        title="Expense filters"
        onClear={
          <Button
            variant="outlined"
            onClick={() => setFilters((prev) => ({
              ...prev,
              financialYear: prev.financialYear[0] ? [prev.financialYear[0]] : [resolveDefaultFinancialYear(financialYears)],
              clientId: [],
              teamId: [],
              expenseHeadId: [],
              servicePeriodId: '',
              search: '',
            }))}
          >
            Clear filters
          </Button>
        }
      >
        <Tabs value={sourceType} onChange={(_, value) => setSourceType(value)} sx={{ mb: 2 }}>
          <Tab value="all" label="All" />
          <Tab value="recurring" label="Recurring" />
          <Tab value="non-recurring" label="Non-Recurring" />
          <Tab value="team-recurring" label="Team" />
        </Tabs>

        <Grid container spacing={1.5}>
          <Grid item xs={12} md={2.4}>
            <SelectionField
              label="Fiscal Year"
              value={filters.financialYear}
              options={financialYearOptions}
              onChange={setArrayFilter('financialYear')}
              mode={filterMode}
              tone="mint"
              multi
              emptyLabel="Select FY"
            />
          </Grid>
          <Grid item xs={12} md={2.4}>
            <SelectionField
              label="Expense Head"
              value={filters.expenseHeadId}
              options={expenseHeadOptions}
              onChange={setArrayFilter('expenseHeadId')}
              mode={filterMode}
              tone="peach"
              multi
            />
          </Grid>
          <Grid item xs={12} md={2.4}>
            <SelectionField
              label="Team"
              value={filters.teamId}
              options={teamOptions}
              onChange={setArrayFilter('teamId')}
              mode={filterMode}
              tone="sand"
              multi
            />
          </Grid>
          <Grid item xs={12} md={2.4}>
            <SelectionField
              label="Group"
              value={filters.clientId}
              options={clientOptions}
              onChange={setArrayFilter('clientId')}
              mode={filterMode}
              tone="sage"
              multi
            />
          </Grid>
          <Grid item xs={12} md={2.4}>
            <TextField
              fullWidth
              placeholder="Search expense, team, client, or period"
              value={filters.search}
              onChange={(e) => setFilters((prev) => ({ ...prev, search: e.target.value }))}
              InputProps={{ startAdornment: <Search sx={{ mr: 1, color: 'text.secondary' }} /> }}
            />
          </Grid>
          <Grid item xs={12} md={4}>
            <FormControl fullWidth>
              <InputLabel>Period</InputLabel>
              <Select
                value={filters.servicePeriodId}
                label="Period"
                onChange={(e) => setFilters((prev) => ({ ...prev, servicePeriodId: e.target.value }))}
              >
                <MenuItem value="">All</MenuItem>
                {filteredPeriods.map((period) => (
                  <MenuItem key={period.id} value={period.id}>{period.display_name}</MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>
        </Grid>
      </FilterPanel>

      <SectionCard
        title="Expense ledger"
        tone="sage"
        contentSx={{ p: 0 }}
      >
        <DataGrid
          rows={visibleRows}
          columns={columns}
          autoHeight
          pageSize={10}
          rowsPerPageOptions={[10, 25, 50]}
          loading={loading}
          disableSelectionOnClick
          sx={{ border: 'none', p: 1.25 }}
        />
      </SectionCard>

      <Dialog open={dialogOpen} onClose={closeDialog} maxWidth="md" fullWidth PaperProps={{ sx: { borderRadius: '16px' } }}>
        <DialogTitle>{selected ? 'Edit Expense' : 'Add Expense'}</DialogTitle>
        <DialogContent>
          {error && <Alert severity="error" sx={{ mb: 2, mt: 1 }}>{error}</Alert>}
          {isRecurringDialog && (
            <Alert severity="info" sx={{ mb: 2, mt: 1 }}>
              <Typography variant="subtitle2" sx={{ mb: 0.5 }}>
                {isProjectedSelection ? 'Projected amount reference' : 'Projected vs actual amount'}
              </Typography>
              <Typography variant="body2">
                Projected amount: {formatCurrency(projectedComparisonAmount)}
              </Typography>
              {isProjectedSelection ? (
                <Typography variant="body2" color="text.secondary">
                  Selecting projected restores the amount field to the original projected value for this period.
                </Typography>
              ) : (
                <>
                  <Typography variant="body2">
                    {actualBillingLabel} amount: {formatCurrency(currentActualAmount)}
                  </Typography>
                  {showProjectedReference && (
                    <Typography variant="body2" sx={{ color: recurringVarianceAmount >= 0 ? 'success.main' : 'error.main', fontWeight: 700 }}>
                      Variance from projected: {recurringVarianceAmount >= 0 ? '+' : ''}{formatCurrency(recurringVarianceAmount)}
                    </Typography>
                  )}
                </>
              )}
              <Typography variant="body2" sx={{ mt: 0.75 }}>
                {formData.source_type === 'team-recurring'
                  ? 'This row is generated from team allocation. Saving as billed or unbilled creates or updates the live expense entry for this period.'
                  : 'This row comes from recurring expense master. Saving as billed or unbilled creates or updates the period-specific live entry.'}
              </Typography>
            </Alert>
          )}

          <Grid container spacing={2} sx={{ mt: 0.5 }}>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth required>
                <InputLabel>Expense Head</InputLabel>
                <Select
                  value={formData.expense_head_id}
                  label="Expense Head"
                  onChange={(e) => handleFormChange('expense_head_id', e.target.value)}
                >
                  {activeExpenseHeads.map((item) => (
                    <MenuItem key={item.id} value={item.id}>{item.name}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth>
                <InputLabel>Team</InputLabel>
                <Select
                  value={formData.team_id}
                  label="Team"
                  onChange={(e) => handleFormChange('team_id', e.target.value)}
                >
                  <MenuItem value="">None</MenuItem>
                  {activeTeams.map((item) => (
                    <MenuItem key={item.id} value={item.id}>{item.name}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth>
                <InputLabel>Group</InputLabel>
                <Select
                  value={formData.client_id}
                  label="Group"
                  onChange={(e) => handleFormChange('client_id', e.target.value)}
                >
                  <MenuItem value="">None</MenuItem>
                  {activeClients.map((item) => (
                    <MenuItem key={item.id} value={item.id}>{item.name}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={3}>
              <FormControl fullWidth>
                <InputLabel>Financial Year</InputLabel>
                <Select
                  value={formData.financial_year || ''}
                  label="Financial Year"
                  onChange={(e) => handleFormChange('financial_year', e.target.value)}
                >
                  <MenuItem value="">Any</MenuItem>
                  {financialYears.map((fy) => (
                    <MenuItem key={fy} value={fy}>{fy}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={3}>
              <FormControl fullWidth required>
                <InputLabel>Service Period</InputLabel>
                <Select
                  value={formData.service_period_id}
                  label="Service Period"
                  onChange={(e) => handleFormChange('service_period_id', e.target.value)}
                >
                  {(formData.financial_year
                    ? servicePeriods.filter((p) => p.financial_year === formData.financial_year)
                    : servicePeriods
                  ).map((period) => (
                    <MenuItem key={period.id} value={period.id}>{period.display_name}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                required
                label="Book Date"
                type="date"
                value={formData.date}
                onChange={(e) => handleFormChange('date', e.target.value)}
                InputLabelProps={{ shrink: true }}
                disabled={isRecurringDialog && isProjectedSelection}
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth>
                <InputLabel>Bill From</InputLabel>
                <Select
                  value={formData.bill_from}
                  label="Bill From"
                  onChange={(e) => handleFormChange('bill_from', e.target.value)}
                  disabled={isRecurringDialog && isProjectedSelection}
                >
                  <MenuItem value="">None</MenuItem>
                  {activeBillFroms.map((item) => (
                    <MenuItem key={item.id} value={item.name}>{item.name}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12}>
              <TextField
                fullWidth
                multiline
                minRows={2}
                label="Description"
                value={formData.description}
                onChange={(e) => handleFormChange('description', e.target.value)}
                disabled={isRecurringDialog && isProjectedSelection}
              />
            </Grid>
            <Grid item xs={12} md={4}>
              <TextField
                fullWidth
                required
                label="Expense Amount"
                type="number"
                value={formData.amount}
                onChange={(e) => handleFormChange('amount', e.target.value)}
                helperText={showProjectedReference ? `Projected reference: ${formatCurrency(projectedComparisonAmount)}` : ''}
                disabled={isRecurringDialog && isProjectedSelection}
              />
            </Grid>
            <Grid item xs={12} md={4}>
              <TextField
                fullWidth
                label="CGST"
                type="number"
                value={formData.cgst}
                onChange={(e) => handleFormChange('cgst', e.target.value)}
                disabled={isRecurringDialog && isProjectedSelection}
              />
            </Grid>
            <Grid item xs={12} md={4}>
              <TextField
                fullWidth
                label="SGST"
                type="number"
                value={formData.sgst}
                onChange={(e) => handleFormChange('sgst', e.target.value)}
                disabled={isRecurringDialog && isProjectedSelection}
              />
            </Grid>
            <Grid item xs={12} md={4}>
              <TextField
                fullWidth
                label="IGST"
                type="number"
                value={formData.igst}
                onChange={(e) => handleFormChange('igst', e.target.value)}
                disabled={isRecurringDialog && isProjectedSelection}
              />
            </Grid>
            <Grid item xs={12} md={4}>
              <TextField
                fullWidth
                label="Others"
                type="number"
                value={formData.other_charges}
                onChange={(e) => handleFormChange('other_charges', e.target.value)}
                disabled={isRecurringDialog && isProjectedSelection}
              />
            </Grid>
            <Grid item xs={12} md={4}>
              <TextField
                fullWidth
                label="Round Off"
                type="number"
                value={formData.round_off}
                onChange={(e) => handleFormChange('round_off', e.target.value)}
                disabled={isRecurringDialog && isProjectedSelection}
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Total"
                type="number"
                value={formData.total_amount}
                onChange={(e) => handleFormChange('total_amount', e.target.value)}
                disabled={isRecurringDialog && isProjectedSelection}
              />
            </Grid>
            <Grid item xs={12} md={3}>
              <FormControl fullWidth>
                <InputLabel>Books Status</InputLabel>
                <Select
                  value={formData.is_entered_in_books ? 'yes' : 'no'}
                  label="Books Status"
                  onChange={(e) => handleFormChange('is_entered_in_books', e.target.value === 'yes')}
                  disabled={isRecurringDialog}
                >
                  <MenuItem value="yes">Entered</MenuItem>
                  <MenuItem value="no">Pending</MenuItem>
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={3}>
              <FormControl fullWidth>
                <InputLabel>Billing Status</InputLabel>
                <Select
                  value={formData.billing_status}
                  label="Billing Status"
                  onChange={(e) => {
                    const nextStatus = e.target.value;
                    setFormData((prev) => {
                      const next = {
                        ...prev,
                        billing_status: nextStatus,
                        is_unbilled: nextStatus === 'unbilled',
                        is_entered_in_books: nextStatus === 'billed',
                      };
                      if (isRecurringDialog && nextStatus === 'projected') {
                        next.amount = prev.projected_amount ?? '';
                        next.is_unbilled = true;
                        next.is_entered_in_books = false;
                      }
                      next.total_amount = computeTotal(next);
                      return next;
                    });
                  }}
                >
                  {canKeepProjectedExpense && <MenuItem value="projected">Projected</MenuItem>}
                  <MenuItem value="billed">Billed</MenuItem>
                  <MenuItem value="unbilled">Unbilled</MenuItem>
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={3}>
              <FormControlLabel
                control={
                  <Checkbox
                    checked={formData.is_admin}
                    onChange={(e) => handleFormChange('is_admin', e.target.checked)}
                  />
                }
                label="Admin"
                sx={{ mt: 1 }}
              />
            </Grid>
          </Grid>
        </DialogContent>
        <DialogActions>
          <Button onClick={closeDialog} sx={{ textTransform: 'none' }}>Cancel</Button>
          <Button variant="contained" onClick={handleSubmit} sx={{ textTransform: 'none' }}>
            {isRecurringDialog && isProjectedSelection && canKeepProjectedExpense
              ? 'Keep Projected'
              : (selected ? 'Save Changes' : 'Create Expense')}
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={deleteDialogOpen} onClose={() => setDeleteDialogOpen(false)} PaperProps={{ sx: { borderRadius: '16px' } }}>
        <DialogTitle>Delete Expense</DialogTitle>
        <DialogContent>
          <Typography>Delete this expense entry? This will mark it inactive.</Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteDialogOpen(false)} sx={{ textTransform: 'none' }}>Cancel</Button>
          <Button variant="contained" color="error" onClick={handleDelete} sx={{ textTransform: 'none' }}>Delete</Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default Expenses;
