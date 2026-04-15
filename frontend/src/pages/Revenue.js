import React, { useState, useEffect, useCallback } from 'react';
import {
  Box, Button, TextField, Dialog, DialogTitle,
  DialogContent, DialogActions, IconButton, Typography, Chip, Alert, Grid,
  FormControl, InputLabel, Select, MenuItem, Tabs, Tab,
  Table, TableBody, TableCell, TableHead, TableRow
} from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import { DataGrid } from '@mui/x-data-grid';
import { Add, Edit, Delete, Search, Refresh, MonetizationOn, ReceiptLong, Tune, ViewInAr } from '@mui/icons-material';
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
  revenueAPI,
  clientAPI,
  billingNameAPI,
  serviceTypeAPI,
  servicePeriodAPI,
  billFromAPI,
  teamAPI,
} from '../services/api';
import { resolveDefaultFinancialYear } from '../utils/periods';

const DEFAULT_FORM = {
  client_id: '',
  client_billing_row_id: '',
  billing_name_id: '',
  billing_name_label: '',
  service_type_id: '',
  financial_year: '',
  service_period_id: '',
  date: '',
  invoice_no: '',
  billing_status: 'billed',
  is_unbilled: false,
  projected_amount: '',
  revenue_amount: '',
  currency: 'INR',
  hsn_code: '',
  gst_rate: '',
  bill_from: '',
  notes: '',
};

const formatAmount = (value, currency = 'INR') => {
  const numericValue = Number(value || 0);
  return `${currency} ${numericValue.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
};

const getBillingStatus = (row) => {
  if (row?.is_projected) return 'projected';
  return row?.is_unbilled ? 'unbilled' : 'billed';
};

const Revenue = () => {
  const theme = useTheme();
  const { user } = useAuth();
  const [revenues, setRevenues] = useState([]);
  const [clients, setClients] = useState([]);
  const [billingNames, setBillingNames] = useState([]);
  const [serviceTypes, setServiceTypes] = useState([]);
  const [servicePeriods, setServicePeriods] = useState([]);
  const [financialYears, setFinancialYears] = useState([]);
  const [billFroms, setBillFroms] = useState([]);
  const [reviewers, setReviewers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [selected, setSelected] = useState(null);
  const [revenueType, setRevenueType] = useState('all');
  const [filterMode, setFilterMode] = useState('dropdown');
  const [filters, setFilters] = useState({
    financial_years: [],
    client_id: [],
    billing_name_id: [],
    service_type_id: [],
    reviewer_id: [],
    service_period_id: '',
    search: '',
  });
  const [formData, setFormData] = useState(DEFAULT_FORM);
  const [error, setError] = useState('');
  const [summary, setSummary] = useState({
    billed_revenue: 0,
    unbilled_revenue: 0,
    projected_revenue: 0,
    total_revenue: 0,
  });

  const loadMasterData = useCallback(async () => {
    try {
      const [
        clientRes,
        billingRes,
        serviceTypeRes,
        servicePeriodRes,
        financialYearRes,
        billFromRes,
        teamRes,
      ] = await Promise.all([
        clientAPI.getAll({ includeInactive: true }),
        billingNameAPI.getAll(),
        serviceTypeAPI.getAll(),
        servicePeriodAPI.getAll(),
        servicePeriodAPI.getFinancialYears(),
        billFromAPI.getAll(),
        teamAPI.getAll(),
      ]);

      const years = financialYearRes.data.data || [];
      const defaultFY = resolveDefaultFinancialYear(years);

      setClients(clientRes.data.data || []);
      setBillingNames(billingRes.data.data || []);
      setServiceTypes(serviceTypeRes.data.data || []);
      setServicePeriods(servicePeriodRes.data.data || []);
      setFinancialYears(years);
      setBillFroms(billFromRes.data.data || []);
      setReviewers((teamRes.data.data || []).filter((teamMember) => teamMember.is_reviewer));
      setFilters((prev) => ({
        ...prev,
        financial_years: prev.financial_years.length ? prev.financial_years : (defaultFY ? [defaultFY] : []),
      }));
      setFormData((prev) => ({
        ...prev,
        financial_year: prev.financial_year || defaultFY,
      }));
    } catch (err) {
      toast.error('Failed to load revenue masters');
    }
  }, []);

  const loadRevenues = useCallback(async () => {
    setLoading(true);
    try {
      const yearsToLoad = filters.financial_years.length ? filters.financial_years : [];
      if (!yearsToLoad.length) {
        setRevenues([]);
        setSummary({
          billed_revenue: 0,
          unbilled_revenue: 0,
          projected_revenue: 0,
          total_revenue: 0,
        });
        return;
      }

      const responses = await Promise.all(
        yearsToLoad.map(async (financialYear) => {
          const params = {
            sourceType: revenueType === 'deviation' ? 'recurring' : revenueType,
            financialYear,
          };
          if (filters.client_id.length) params.clientId = filters.client_id.join(',');
          if (filters.billing_name_id.length) params.billingNameId = filters.billing_name_id.join(',');
          if (filters.service_type_id.length) params.serviceTypeId = filters.service_type_id.join(',');
          if (filters.reviewer_id.length) params.reviewerId = filters.reviewer_id.join(',');
          if (filters.service_period_id) params.servicePeriodId = filters.service_period_id;

          const [listResponse, summaryResponse] = await Promise.all([
            revenueAPI.getAll(params),
            revenueAPI.getSummary(params),
          ]);

          return {
            rows: listResponse.data.data || [],
            summary: summaryResponse.data.data || {
              billed_revenue: 0,
              unbilled_revenue: 0,
              projected_revenue: 0,
              total_revenue: 0,
            },
          };
        }),
      );

      setRevenues(
        responses
          .flatMap((response) => response.rows)
          .sort((left, right) => (left.client_name || '').localeCompare(right.client_name || '') || new Date(left.period_start_date || 0) - new Date(right.period_start_date || 0)),
      );
      setSummary(
        responses.reduce((accumulator, response) => ({
          billed_revenue: accumulator.billed_revenue + Number(response.summary.billed_revenue || 0),
          unbilled_revenue: accumulator.unbilled_revenue + Number(response.summary.unbilled_revenue || 0),
          projected_revenue: accumulator.projected_revenue + Number(response.summary.projected_revenue || 0),
          total_revenue: accumulator.total_revenue + Number(response.summary.total_revenue || 0),
        }), {
          billed_revenue: 0,
          unbilled_revenue: 0,
          projected_revenue: 0,
          total_revenue: 0,
        }),
      );
    } catch (err) {
      toast.error('Failed to load revenue');
    } finally {
      setLoading(false);
    }
  }, [filters, revenueType]);

  useEffect(() => {
    loadMasterData();
  }, [loadMasterData]);

  useEffect(() => {
    loadRevenues();
  }, [loadRevenues]);

  const filteredBillingNames = billingNames.filter((billing) => (
    filters.client_id.length === 0 || filters.client_id.includes(billing.client_id)
  ));

  const formBillingNames = billingNames.filter((billing) => (
    !formData.client_id || billing.client_id === formData.client_id
  ));

  const filteredPeriods = servicePeriods.filter((period) => (
    !filters.financial_years.length || filters.financial_years.includes(period.financial_year)
  ));

  const formPeriods = servicePeriods.filter((period) => (
    !formData.financial_year || period.financial_year === formData.financial_year
  ));

  const handleOpenDialog = async (item = null) => {
    if (item) {
      if (item.source_type === 'recurring' && String(item.id).startsWith('recurring-')) {
        setSelected(item);
        setFormData({
          client_id: item.client_id || '',
          client_billing_row_id: item.client_billing_row_id || '',
          billing_name_id: item.billing_name_id || '',
          billing_name_label: item.billing_name || '',
          service_type_id: item.service_type_id || '',
          financial_year: item.financial_year || filters.financial_years[0] || '',
          service_period_id: item.service_period_id || '',
          // Projected entries: no date or invoice yet
          date: item.is_projected ? '' : (item.date ? String(item.date).slice(0, 10) : ''),
          invoice_no: item.is_projected ? '' : (item.invoice_no || ''),
          billing_status: getBillingStatus(item),
          is_unbilled: item.is_projected ? true : (item.is_unbilled !== undefined ? Boolean(item.is_unbilled) : true),
          projected_amount: item.projected_amount || item.revenue_amount || '',
          revenue_amount: item.revenue_amount || '',
          currency: item.currency || 'INR',
          hsn_code: item.hsn_code || '',
          gst_rate: item.gst_rate ?? '',
          bill_from: item.bill_from || '',
          notes: item.notes || '',
        });
        setError('');
        setDialogOpen(true);
        return;
      }
      try {
        const res = await revenueAPI.getById(item.id);
        const data = res.data.data;
        setSelected(item);
        setFormData({
          client_id: data.client_id || '',
          client_billing_row_id: data.client_billing_row_id || '',
          billing_name_id: data.billing_name_id || '',
          billing_name_label: data.billing_name || item.billing_name || '',
          service_type_id: data.service_type_id || '',
          financial_year: data.financial_year || filters.financial_years[0] || '',
          service_period_id: data.service_period_id || '',
          date: data.date ? String(data.date).slice(0, 10) : '',
          invoice_no: data.invoice_no || '',
          billing_status: getBillingStatus(item),
          is_unbilled: Boolean(data.is_unbilled),
          projected_amount: item.projected_amount || data.revenue_amount || '',
          revenue_amount: data.revenue_amount || '',
          currency: data.currency || 'INR',
          hsn_code: data.hsn_code || '',
          gst_rate: data.gst_rate ?? '',
          bill_from: data.bill_from || '',
          notes: data.notes || '',
        });
      } catch (err) {
        toast.error('Failed to load revenue details');
        return;
      }
    } else {
      setSelected(null);
      setFormData({
        ...DEFAULT_FORM,
        financial_year: filters.financial_years[0] || resolveDefaultFinancialYear(financialYears),
        date: new Date().toISOString().slice(0, 10),
      });
    }

    setError('');
    setDialogOpen(true);
  };

  const handleCloseDialog = () => {
    setDialogOpen(false);
    setSelected(null);
    setFormData({
      ...DEFAULT_FORM,
      financial_year: filters.financial_years[0] || resolveDefaultFinancialYear(financialYears),
    });
    setError('');
  };

  const handleServiceTypeChange = (serviceTypeId) => {
    const selectedServiceType = serviceTypes.find((serviceType) => serviceType.id === serviceTypeId);
    setFormData((prev) => ({
      ...prev,
      service_type_id: serviceTypeId,
      hsn_code: selectedServiceType?.hsn_code || '',
      gst_rate: selectedServiceType?.gst_rate ?? '',
    }));
  };

  const handleFinancialYearChange = (financialYear) => {
    setFormData((prev) => ({
      ...prev,
      financial_year: financialYear,
      service_period_id: '',
    }));
  };

  const handleServicePeriodChange = (servicePeriodId) => {
    const selectedPeriod = servicePeriods.find((period) => period.id === servicePeriodId);
    setFormData((prev) => ({
      ...prev,
      service_period_id: servicePeriodId,
      financial_year: selectedPeriod?.financial_year || prev.financial_year,
      date: selectedPeriod?.start_date ? String(selectedPeriod.start_date).slice(0, 10) : prev.date,
    }));
  };

  const handleSubmit = async () => {
    const isRecurringEdit = selected?.source_type === 'recurring';
    const isSyntheticRecurringRow = String(selected?.id || '').startsWith('recurring-');
    const persistedRevenueId = selected?.rr_id || (!isSyntheticRecurringRow ? selected?.id : '');
    const billingStatus = formData.billing_status || (formData.is_unbilled ? 'unbilled' : 'billed');

    if (isRecurringEdit && billingStatus === 'projected') {
      if (persistedRevenueId) {
        setError('Recurring revenue entries cannot be deleted or reverted to projected. Update the amount to 0 instead.');
        return;
      }
      handleCloseDialog();
      loadRevenues();
      return;
    }

    if (!formData.client_id || !formData.service_period_id || !formData.date) {
      setError('Client, service period, and date are required');
      return;
    }
    if (!isRecurringEdit && !formData.service_type_id) {
      setError('Service type is required');
      return;
    }
    if (formData.revenue_amount === '' || Number(formData.revenue_amount) < 0) {
      setError('Revenue amount must be zero or greater');
      return;
    }

    try {
      const payload = {
        source_type: isRecurringEdit ? 'recurring' : 'non-recurring',
        client_id: formData.client_id,
        client_billing_row_id: formData.client_billing_row_id || undefined,
        billing_name_id: formData.billing_name_id || undefined,
        service_type_id: formData.service_type_id || undefined,
        service_period_id: formData.service_period_id,
        date: formData.date,
        invoice_no: formData.invoice_no.trim() || undefined,
        is_unbilled: billingStatus === 'unbilled',
        revenue_amount: Number(formData.revenue_amount),
        currency: formData.currency || 'INR',
        hsn_code: formData.hsn_code.trim() || undefined,
        gst_rate: formData.gst_rate === '' ? undefined : Number(formData.gst_rate),
        bill_from: formData.bill_from || undefined,
        notes: formData.notes.trim() || undefined,
      };

      if (persistedRevenueId) {
        await revenueAPI.update(persistedRevenueId, payload);
        toast.success('Revenue updated successfully');
      } else {
        await revenueAPI.create(payload);
        toast.success(isRecurringEdit ? 'Recurring revenue saved successfully' : 'Revenue created successfully');
      }

      handleCloseDialog();
      loadRevenues();
    } catch (err) {
      setError(
        err.response?.data?.message ||
        err.response?.data?.errors?.[0]?.msg ||
        'Operation failed'
      );
    }
  };

  const handleDelete = async () => {
    if (selected?.source_type === 'recurring') {
      toast.error('Recurring revenue entries cannot be deleted. Update the amount to 0 instead.');
      setDeleteDialogOpen(false);
      return;
    }
    try {
      await revenueAPI.delete(selected.id);
      toast.success('Revenue deleted successfully');
      setDeleteDialogOpen(false);
      setSelected(null);
      loadRevenues();
    } catch (err) {
      toast.error(err.response?.data?.message || 'Failed to delete revenue');
    }
  };

  const visibleRows = revenues.filter((row) => {
    const search = filters.search.trim().toLowerCase();
    if (!search) return true;
    return [
      row.client_name,
      row.billing_name,
      row.service_type_name,
      row.service_period_name,
      row.unique_key,
      row.invoice_no,
    ].some((value) => String(value || '').toLowerCase().includes(search));
  });

  const activeClients = clients.filter((client) => client.is_active !== false);
  const isRecurringDialog = selected?.source_type === 'recurring';
  const isProjectedSelection = formData.billing_status === 'projected';
  const hasSavedRecurringRevenueOverride = isRecurringDialog && Boolean(
    selected?.rr_id || (selected && !String(selected.id || '').startsWith('recurring-'))
  );
  const canKeepProjectedRevenue = isRecurringDialog && !hasSavedRecurringRevenueOverride;
  const projectedComparisonAmount = Number(formData.projected_amount || 0);
  const currentRecurringAmount = Number(formData.revenue_amount || 0);
  const recurringVarianceAmount = currentRecurringAmount - projectedComparisonAmount;

  const columns = [
    {
      field: 'source_type',
      headerName: 'Type',
      width: 130,
      renderCell: (params) => (
        <Chip
          label={params.value === 'recurring' ? 'Recurring' : 'Non-Recurring'}
          color={params.value === 'recurring' ? 'secondary' : 'primary'}
          size="small"
        />
      ),
    },
    { field: 'client_name', headerName: 'Group', flex: 1, minWidth: 150 },
    { field: 'billing_name', headerName: 'Client', flex: 1, minWidth: 160, renderCell: (params) => params.value || '-' },
    { field: 'service_type_name', headerName: 'Service Type', flex: 1, minWidth: 150 },
    { field: 'financial_year', headerName: 'FY', width: 110, renderCell: (params) => params.value || '-' },
    { field: 'service_period_name', headerName: 'Period', width: 100, renderCell: (params) => params.value || '-' },
    {
      field: 'revenue_amount',
      headerName: 'Amount',
      width: 140,
      renderCell: (params) => formatAmount(params.row.revenue_amount, params.row.currency),
    },
    {
      field: 'total_amount',
      headerName: 'Total',
      width: 140,
      renderCell: (params) => formatAmount(params.row.total_amount, params.row.currency),
    },
    {
      field: 'is_unbilled',
      headerName: 'Status',
      width: 120,
      renderCell: (params) => {
        if (params.row.is_projected) {
          return <Chip label="Projected" color="info" size="small" />;
        }
        return (
          <Chip
            label={params.row.is_unbilled ? 'Unbilled' : 'Billed'}
            color={params.row.is_unbilled ? 'warning' : 'success'}
            size="small"
          />
        );
      },
    },
    { field: 'invoice_no', headerName: 'Invoice No', width: 130, renderCell: (params) => params.value || '-' },
    { field: 'bill_from', headerName: 'Bill From', width: 140, renderCell: (params) => params.value || '-' },
    {
      field: 'actions',
      headerName: 'Actions',
      width: 120,
      sortable: false,
      renderCell: (params) => (
        <Box>
          <IconButton size="small" onClick={() => handleOpenDialog(params.row)} color="primary">
            <Edit fontSize="small" />
          </IconButton>
          {user?.can_delete && params.row.source_type === 'non-recurring' && (
            <IconButton
              size="small"
              onClick={() => {
                setSelected(params.row);
                setDeleteDialogOpen(true);
              }}
              color="error"
            >
              <Delete fontSize="small" />
            </IconButton>
          )}
        </Box>
      ),
    },
  ];

  const financialYearOptions = financialYears.map((fy) => ({ value: fy, label: fy }));
  const clientOptions = activeClients.map((client) => ({ value: client.id, label: client.name }));
  const billingOptions = filteredBillingNames.map((billing) => ({ value: billing.id, label: billing.name }));
  const serviceTypeOptions = serviceTypes.filter((serviceType) => serviceType.is_active !== false).map((serviceType) => ({ value: serviceType.id, label: serviceType.name }));
  const reviewerOptions = reviewers.map((reviewer) => ({ value: reviewer.id, label: reviewer.name }));

  const setArrayFilter = (field) => (value) => {
    setFilters((prev) => {
      const next = { ...prev, [field]: value };
      if (field === 'client_id') next.billing_name_id = [];
      if (field === 'financial_years') next.service_period_id = '';
      return next;
    });
  };

  return (
    <Box>
      <PageHeader
        eyebrow="Revenue Workspace"
        title="Revenue command surface"
        actions={[
          revenueType === 'non-recurring'
            ? <HoverActionButton key="add" icon={<Add fontSize="small" />} label="Add revenue" onClick={() => handleOpenDialog()} tone="mint" />
            : null,
          <HoverActionButton key="refresh" icon={<Refresh fontSize="small" />} label="Refresh" onClick={loadRevenues} tone="peach" />,
          <HoverActionButton
            key="mode"
            icon={<Tune fontSize="small" />}
            label={filterMode === 'dropdown' ? 'Dropdown filters' : 'Full filters'}
            onClick={() => setFilterMode((prev) => (prev === 'dropdown' ? 'full' : 'dropdown'))}
            tone="sand"
          />,
        ].filter(Boolean)}
        chips={[
          { label: `${filters.financial_years.length || 0} FY active`, background: alpha(theme.palette.primary.light, 0.36) },
          { label: `${visibleRows.length} visible rows`, background: alpha(theme.palette.secondary.light, 0.42) },
        ]}
      />

      <Grid container spacing={2} sx={{ mb: 2.5 }}>
        <Grid item xs={12} md={3}>
          <MetricCard eyebrow="Billed" title="Booked revenue" value={formatAmount(summary.billed_revenue || 0)} icon={<ReceiptLong fontSize="small" />} tone="mint" />
        </Grid>
        <Grid item xs={12} md={3}>
          <MetricCard eyebrow="Unbilled" title="Past unbilled" value={formatAmount(summary.unbilled_revenue || 0)} icon={<ViewInAr fontSize="small" />} tone="sand" />
        </Grid>
        <Grid item xs={12} md={3}>
          <MetricCard eyebrow="Projected" title="Future projection" value={formatAmount(summary.projected_revenue || 0)} icon={<MonetizationOn fontSize="small" />} tone="peach" />
        </Grid>
        <Grid item xs={12} md={3}>
          <MetricCard eyebrow="Total" title="Visible revenue" value={formatAmount(summary.total_revenue || 0)} icon={<Refresh fontSize="small" />} tone="rose" />
        </Grid>
      </Grid>

      <FilterPanel
        mode={filterMode}
        onModeChange={setFilterMode}
        title="Revenue filters"
        onClear={
          <Button
            variant="outlined"
            onClick={() => setFilters((prev) => ({
              ...prev,
              financial_years: prev.financial_years[0] ? [prev.financial_years[0]] : [resolveDefaultFinancialYear(financialYears)],
              client_id: [],
              billing_name_id: [],
              service_type_id: [],
              reviewer_id: [],
              service_period_id: '',
              search: '',
            }))}
          >
            Clear filters
          </Button>
        }
      >
        <Tabs value={revenueType} onChange={(_, value) => setRevenueType(value)} sx={{ mb: 2 }}>
          <Tab value="all" label="All" />
          <Tab value="recurring" label="Recurring" />
          <Tab value="non-recurring" label="Non-Recurring" />
          <Tab value="deviation" label="Variance Report" />
        </Tabs>

        <Grid container spacing={1.5} alignItems="stretch">
          <Grid item xs={12} md={2.4}>
            <SelectionField
              label="Fiscal Year"
              value={filters.financial_years}
              options={financialYearOptions}
              onChange={setArrayFilter('financial_years')}
              mode={filterMode}
              tone="mint"
              multi
              emptyLabel="Select FY"
            />
          </Grid>
          <Grid item xs={12} md={2.4}>
            <SelectionField
              label="Group"
              value={filters.client_id}
              options={clientOptions}
              onChange={setArrayFilter('client_id')}
              mode={filterMode}
              tone="peach"
              multi
            />
          </Grid>
          <Grid item xs={12} md={2.4}>
            <SelectionField
              label="Client"
              value={filters.billing_name_id}
              options={billingOptions}
              onChange={setArrayFilter('billing_name_id')}
              mode={filterMode}
              tone="sand"
              multi
            />
          </Grid>
          <Grid item xs={12} md={2.4}>
            <SelectionField
              label="Service Type"
              value={filters.service_type_id}
              options={serviceTypeOptions}
              onChange={setArrayFilter('service_type_id')}
              mode={filterMode}
              tone="sage"
              multi
            />
          </Grid>
          <Grid item xs={12} md={2.4}>
            <SelectionField
              label="Reviewer"
              value={filters.reviewer_id}
              options={reviewerOptions}
              onChange={setArrayFilter('reviewer_id')}
              mode={filterMode}
              tone="rose"
              multi
            />
          </Grid>
          <Grid item xs={12} md={4}>
            <TextField
              fullWidth
              size="small"
              placeholder="Search revenue, invoice, client, or period"
              value={filters.search}
              onChange={(e) => setFilters((prev) => ({ ...prev, search: e.target.value }))}
              InputProps={{ startAdornment: <Search sx={{ mr: 1, color: 'text.secondary' }} /> }}
            />
          </Grid>
          <Grid item xs={12} md={4}>
            <FormControl fullWidth size="small">
              <InputLabel>Period</InputLabel>
              <Select
                value={filters.service_period_id}
                label="Period"
                onChange={(e) => setFilters((prev) => ({ ...prev, service_period_id: e.target.value }))}
              >
                <MenuItem value="">All</MenuItem>
                {filteredPeriods.map((period) => (
                  <MenuItem key={period.id} value={period.id}>
                    {period.display_name || period.name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>
        </Grid>
      </FilterPanel>

      <SectionCard
        title={revenueType === 'deviation' ? 'Variance report' : 'Revenue ledger'}
        tone={revenueType === 'deviation' ? 'sand' : 'mint'}
        contentSx={{ p: revenueType === 'deviation' ? 2 : 0 }}
      >
        {revenueType === 'deviation' ? (
          (() => {
            const recurringRows = revenues.filter((r) => r.source_type === 'recurring' && !r.is_projected);
            const grouped = {};
            recurringRows.forEach((r) => {
              const key = `${r.client_name}||${r.billing_name || '-'}||${r.service_type_name || '-'}||${r.service_period_name}`;
              if (!grouped[key]) {
                grouped[key] = {
                  client_name: r.client_name,
                  billing_name: r.billing_name || '-',
                  service_type_name: r.service_type_name || '-',
                  service_period_name: r.service_period_name,
                  period_start_date: r.period_start_date,
                  projected: parseFloat(r.projected_amount) || parseFloat(r.revenue_amount) || 0,
                  status: r.is_unbilled ? 'Unbilled' : 'Billed',
                  actual: parseFloat(r.revenue_amount) || 0,
                };
              }
            });
            const deviationRows = Object.values(grouped).sort((a, b) =>
              new Date(a.period_start_date || 0) - new Date(b.period_start_date || 0)
            );
            return (
              <Table size="small">
                <TableHead>
                  <TableRow sx={{ background: alpha(theme.palette.primary.light, 0.36) }}>
                    <TableCell>Group</TableCell>
                    <TableCell>Client</TableCell>
                    <TableCell>Service Type</TableCell>
                    <TableCell>Period</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell align="right">Projected</TableCell>
                    <TableCell align="right">Actual amount</TableCell>
                    <TableCell align="right">Variance</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {deviationRows.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={8} align="center">
                        <Typography color="text.secondary" sx={{ py: 2 }}>No modified projected revenue found for the variance report</Typography>
                      </TableCell>
                    </TableRow>
                  ) : deviationRows.map((row, idx) => {
                    const deviation = row.actual - row.projected;
                    return (
                      <TableRow key={idx} hover>
                        <TableCell>{row.client_name}</TableCell>
                        <TableCell>{row.billing_name}</TableCell>
                        <TableCell>{row.service_type_name}</TableCell>
                        <TableCell>{row.service_period_name}</TableCell>
                        <TableCell>{row.status}</TableCell>
                        <TableCell align="right">{formatAmount(row.projected)}</TableCell>
                        <TableCell align="right">{formatAmount(row.actual)}</TableCell>
                        <TableCell align="right">
                          <Typography variant="body2" color={deviation >= 0 ? 'success.main' : 'error.main'} fontWeight="bold">
                            {deviation >= 0 ? '+' : ''}{formatAmount(deviation)}
                          </Typography>
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            );
          })()
        ) : (
          <DataGrid
            rows={visibleRows}
            columns={columns}
            pageSize={10}
            rowsPerPageOptions={[10, 25, 50]}
            autoHeight
            loading={loading}
            disableSelectionOnClick
            sx={{ border: 'none', p: 1.25 }}
          />
        )}
      </SectionCard>

      <Dialog open={dialogOpen} onClose={handleCloseDialog} maxWidth="md" fullWidth PaperProps={{ sx: { borderRadius: '16px' } }}>
        <DialogTitle>{selected ? 'Edit Revenue' : 'Add Revenue'}</DialogTitle>
        <DialogContent>
          {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
          {isRecurringDialog && (
            <Alert severity={isProjectedSelection ? 'warning' : 'info'} sx={{ mb: 2 }}>
              <Typography variant="subtitle2" sx={{ mb: 0.5 }}>
                Projected vs {isProjectedSelection ? 'projected' : formData.billing_status} amount before GST
              </Typography>
              <Typography variant="body2">
                Projected amount before GST: {formatAmount(projectedComparisonAmount, formData.currency)}
              </Typography>
              {isProjectedSelection ? (
                <Typography variant="body2" color="text.secondary">
                  Keeping this as projected will not create a billed or unbilled revenue entry for the selected period.
                </Typography>
              ) : (
                <>
                  <Typography variant="body2">
                    {formData.billing_status === 'unbilled' ? 'Unbilled' : 'Billed'} amount before GST: {formatAmount(currentRecurringAmount, formData.currency)}
                  </Typography>
                  <Typography variant="body2" sx={{ color: recurringVarianceAmount >= 0 ? 'success.main' : 'error.main', fontWeight: 700 }}>
                    Variance to update amount: {recurringVarianceAmount >= 0 ? '+' : ''}{formatAmount(recurringVarianceAmount, formData.currency)}
                  </Typography>
                </>
              )}
            </Alert>
          )}
          <Grid container spacing={2} sx={{ mt: 1 }}>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth required>
                <InputLabel>Group</InputLabel>
                <Select
                  value={formData.client_id}
                  label="Group"
                  onChange={(e) => setFormData((prev) => ({
                    ...prev,
                    client_id: e.target.value,
                    billing_name_id: '',
                    billing_name_label: '',
                  }))}
                >
                  {activeClients.map((client) => (
                    <MenuItem key={client.id} value={client.id}>{client.name}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth>
                <InputLabel>Client</InputLabel>
                <Select
                  value={formData.billing_name_id}
                  label="Client"
                  displayEmpty
                  renderValue={(value) => {
                    if (value) {
                      return formBillingNames.find((billing) => billing.id === value)?.name || formData.billing_name_label || 'None';
                    }
                    return formData.billing_name_label || 'None';
                  }}
                  onChange={(e) => setFormData((prev) => ({
                    ...prev,
                    billing_name_id: e.target.value,
                    billing_name_label: '',
                  }))}
                >
                  <MenuItem value="">None</MenuItem>
                  {formBillingNames.map((billing) => (
                    <MenuItem key={billing.id} value={billing.id}>{billing.name}</MenuItem>
                  ))}
                </Select>
              </FormControl>
              {!formData.billing_name_id && formData.billing_name_label && (
                <Typography variant="caption" color="text.secondary" sx={{ mt: 0.75, display: 'block' }}>
                  Client from Group Master: {formData.billing_name_label}
                </Typography>
              )}
            </Grid>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth required>
                <InputLabel>Service Type</InputLabel>
                <Select
                  value={formData.service_type_id}
                  label="Service Type"
                  onChange={(e) => handleServiceTypeChange(e.target.value)}
                >
                  {serviceTypes.map((serviceType) => (
                    <MenuItem key={serviceType.id} value={serviceType.id}>{serviceType.name}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={3}>
              <FormControl fullWidth required>
                <InputLabel>Financial Year</InputLabel>
                <Select
                  value={formData.financial_year}
                  label="Financial Year"
                  onChange={(e) => handleFinancialYearChange(e.target.value)}
                >
                  {financialYears.map((fy) => (
                    <MenuItem key={fy} value={fy}>{fy}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={3}>
              <FormControl fullWidth required>
                <InputLabel>Period</InputLabel>
                <Select
                  value={formData.service_period_id}
                  label="Period"
                  onChange={(e) => handleServicePeriodChange(e.target.value)}
                >
                  {formPeriods.map((period) => (
                    <MenuItem key={period.id} value={period.id}>
                      {period.display_name || period.name}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={3}>
              <TextField
                fullWidth
                label="Date"
                type="date"
                value={formData.date}
                onChange={(e) => setFormData((prev) => ({ ...prev, date: e.target.value }))}
                InputLabelProps={{ shrink: true }}
                disabled={isRecurringDialog && isProjectedSelection}
                required
              />
            </Grid>
            <Grid item xs={12} md={3}>
              <TextField
                fullWidth
                label="Invoice No"
                value={formData.invoice_no}
                onChange={(e) => setFormData((prev) => ({ ...prev, invoice_no: e.target.value }))}
                disabled={isRecurringDialog && isProjectedSelection}
              />
            </Grid>
            <Grid item xs={12} md={3}>
              <TextField
                fullWidth
                label="Amount"
                type="number"
                value={formData.revenue_amount}
                onChange={(e) => setFormData((prev) => ({ ...prev, revenue_amount: e.target.value }))}
                inputProps={{ min: 0, step: 0.01 }}
                disabled={isRecurringDialog && isProjectedSelection}
                required
              />
            </Grid>
            <Grid item xs={12} md={3}>
              <FormControl fullWidth>
                <InputLabel>Currency</InputLabel>
                <Select
                  value={formData.currency}
                  label="Currency"
                  onChange={(e) => setFormData((prev) => ({ ...prev, currency: e.target.value }))}
                >
                  {['INR', 'USD', 'EUR', 'GBP', 'AED', 'SGD', 'AUD', 'CAD', 'JPY', 'CHF'].map((currency) => (
                    <MenuItem key={currency} value={currency}>{currency}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={3}>
              <TextField
                fullWidth
                label="HSN Code"
                value={formData.hsn_code}
                onChange={(e) => setFormData((prev) => ({ ...prev, hsn_code: e.target.value }))}
              />
            </Grid>
            <Grid item xs={12} md={3}>
              <TextField
                fullWidth
                label="GST Rate"
                type="number"
                value={formData.gst_rate}
                onChange={(e) => setFormData((prev) => ({ ...prev, gst_rate: e.target.value }))}
                inputProps={{ min: 0, max: 28, step: 0.5 }}
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth>
                <InputLabel>Billing Status</InputLabel>
                <Select
                  value={formData.billing_status}
                  label="Billing Status"
                  onChange={(e) => {
                    const nextStatus = e.target.value;
                    setFormData((prev) => ({
                      ...prev,
                      billing_status: nextStatus,
                      is_unbilled: nextStatus !== 'billed',
                      ...(isRecurringDialog && nextStatus === 'projected' ? {
                        date: '',
                        invoice_no: '',
                      } : {}),
                    }));
                  }}
                >
                  {canKeepProjectedRevenue && <MenuItem value="projected">Projected</MenuItem>}
                  <MenuItem value="billed">Billed</MenuItem>
                  <MenuItem value="unbilled">Unbilled</MenuItem>
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth>
                <InputLabel>Bill From</InputLabel>
                <Select
                  value={formData.bill_from}
                  label="Bill From"
                  onChange={(e) => setFormData((prev) => ({ ...prev, bill_from: e.target.value }))}
                >
                  <MenuItem value="">None</MenuItem>
                  {billFroms.filter((billFrom) => billFrom.is_active !== false).map((billFrom) => (
                    <MenuItem key={billFrom.id} value={billFrom.name}>{billFrom.name}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12}>
              <TextField
                fullWidth
                multiline
                minRows={3}
                label="Notes"
                value={formData.notes}
                onChange={(e) => setFormData((prev) => ({ ...prev, notes: e.target.value }))}
              />
            </Grid>
          </Grid>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCloseDialog} sx={{ textTransform: 'none' }}>Cancel</Button>
          <Button variant="contained" onClick={handleSubmit} sx={{ textTransform: 'none' }}>
            {isRecurringDialog && isProjectedSelection && canKeepProjectedRevenue ? 'Keep Projected' : (selected ? 'Update' : 'Create')}
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={deleteDialogOpen} onClose={() => setDeleteDialogOpen(false)} PaperProps={{ sx: { borderRadius: '16px' } }}>
        <DialogTitle>Confirm Delete</DialogTitle>
        <DialogContent>
          <Typography>Are you sure you want to delete this revenue entry?</Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteDialogOpen(false)} sx={{ textTransform: 'none' }}>Cancel</Button>
          <Button variant="contained" color="error" onClick={handleDelete} sx={{ textTransform: 'none' }}>Delete</Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default Revenue;
