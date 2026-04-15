import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Box, Button, Checkbox, Chip, Dialog, DialogActions, DialogContent,
  DialogTitle, Divider, FormControl, FormControlLabel, Grid, IconButton, InputLabel, Link, MenuItem, Paper,
  Select, Table, TableBody, TableCell, TableHead, TableRow, TextField, ToggleButton,
  ToggleButtonGroup, Typography, Alert
} from '@mui/material';
import {
  Add, Cancel, CheckCircle, Delete, Edit, EventBusy, History,
  OpenInNew, Refresh, RemoveCircle, Search, WarningAmber
} from '@mui/icons-material';
import { toast } from 'react-toastify';
import {
  HoverActionButton,
  MetricCard,
  PageHeader,
  SectionCard,
} from '../../components/FuturisticUI';
import { expenseHeadAPI, recurringExpenseAPI, teamAPI, clientAPI, vendorAPI, servicePeriodAPI } from '../../services/api';
import { summarizeDependencies } from '../../utils/deleteDependencies';

const MIN_PERIOD = 'Apr-26';

const periodToDate = (period) => {
  if (!period) return null;
  const [mon, yy] = period.split('-');
  const monthMap = {
    Jan: 0, Feb: 1, Mar: 2, Apr: 3, May: 4, Jun: 5,
    Jul: 6, Aug: 7, Sep: 8, Oct: 9, Nov: 10, Dec: 11
  };
  return new Date(2000 + parseInt(yy, 10), monthMap[mon], 1);
};

const comparePeriods = (a, b) => {
  const dateA = periodToDate(a);
  const dateB = periodToDate(b);
  if (!dateA && !dateB) return 0;
  if (!dateA) return -1;
  if (!dateB) return 1;
  return dateA - dateB;
};

const formatPeriodRange = (startPeriod, endPeriod) =>
  startPeriod ? `${startPeriod}${endPeriod ? ` to ${endPeriod}` : ' to Open'}` : 'Period not set';

const nextMonthLabel = (period) => {
  if (!period) return '';
  const date = periodToDate(period);
  if (!date) return '';
  date.setMonth(date.getMonth() + 1);
  return `${date.toLocaleString('en-US', { month: 'short' })}-${String(date.getFullYear()).slice(-2)}`;
};

const getLatestEndedPeriod = (expense = {}) => {
  const periods = [expense?.end_period, ...(expense?.allocations || []).map((allocation) => allocation?.end_period)]
    .filter(Boolean);
  if (!periods.length) return '';
  return periods.reduce((latest, current) => {
    if (!latest) return current;
    return comparePeriods(current, latest) > 0 ? current : latest;
  }, '');
};

const getDraftDuplicateState = (details = []) => {
  const seen = new Set();
  for (const detail of details) {
    if (!detail?.team_id || !detail?.client_id) continue;
    const key = `${detail.team_id}--${detail.client_id}`;
    if (seen.has(key)) {
      return true;
    }
    seen.add(key);
  }
  return false;
};

// Flat allocation row for the edit dialog
const AllocationRow = ({ allocation, index, allTeams, allClients, allReviewers, onUpdate, onRemove, siblingAllocations }) => {
  const isDuplicate = siblingAllocations.some((a, i) =>
    i !== index && a.team_id && a.client_id &&
    a.team_id === allocation.team_id && a.client_id === allocation.client_id
  );

  return (
    <Box sx={{ display: 'flex', gap: 1, mb: 1.5, alignItems: 'center', p: 1, borderRadius: 1, bgcolor: isDuplicate ? 'warning.50' : 'grey.50', border: isDuplicate ? '1px solid' : 'none', borderColor: 'warning.main' }}>
      {isDuplicate && <WarningAmber color="warning" fontSize="small" />}
      <FormControl size="small" sx={{ minWidth: 150, flex: 1 }}>
        <InputLabel>Team *</InputLabel>
        <Select value={allocation.team_id || ''} label="Team *" onChange={(e) => onUpdate(index, { ...allocation, team_id: e.target.value })}>
          {allTeams.filter(t => t.is_active !== false).map(t => <MenuItem key={t.id} value={t.id}>{t.name}</MenuItem>)}
        </Select>
      </FormControl>
      <FormControl size="small" sx={{ minWidth: 150, flex: 1 }}>
        <InputLabel>Group *</InputLabel>
        <Select value={allocation.client_id || ''} label="Group *" onChange={(e) => onUpdate(index, { ...allocation, client_id: e.target.value })}>
          {allClients.filter(c => c.is_active !== false).map(c => <MenuItem key={c.id} value={c.id}>{c.name}</MenuItem>)}
        </Select>
      </FormControl>
      <FormControl size="small" sx={{ minWidth: 140 }}>
        <InputLabel>Reviewer</InputLabel>
        <Select value={allocation.reviewer_id || ''} label="Reviewer" onChange={(e) => onUpdate(index, { ...allocation, reviewer_id: e.target.value })}>
          <MenuItem value=""><em>None</em></MenuItem>
          {allReviewers.map(r => <MenuItem key={r.id} value={r.id}>{r.name}</MenuItem>)}
        </Select>
      </FormControl>
      <TextField size="small" label="Amount" type="number" value={allocation.amount || ''} onChange={(e) => onUpdate(index, { ...allocation, amount: parseFloat(e.target.value) || 0 })} sx={{ width: 130 }} inputProps={{ min: 0, step: 0.01 }} />
      <IconButton size="small" color="error" onClick={() => onRemove(index)}><RemoveCircle fontSize="small" /></IconButton>
    </Box>
  );
};



// End Period dialog — close current allocation period and create next live period
const EndPeriodDialog = ({ open, allocation, teamMembers, periodOptions, onClose, onSubmit }) => {
  const [endPeriod, setEndPeriod] = useState('');
  const [newAmount, setNewAmount] = useState('');
  const [newReviewerId, setNewReviewerId] = useState('');

  useEffect(() => {
    if (open && allocation) {
      setEndPeriod('');
      setNewAmount(allocation.amount != null ? String(allocation.amount) : '');
      setNewReviewerId(allocation.reviewer_id || '');
    }
  }, [open, allocation]);

  const reviewers = (teamMembers || []).filter(t => t.is_reviewer && t.is_active !== false);
  const filteredEndPeriodOptions = (periodOptions || []).filter(p =>
    !allocation?.start_period || comparePeriods(p, allocation.start_period) >= 0
  );
  const valid = endPeriod && filteredEndPeriodOptions.includes(endPeriod);

  return (
    <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
      <DialogTitle sx={{ bgcolor: '#fff7ed', color: '#92400e' }}>End Allocation Period</DialogTitle>
      <DialogContent sx={{ pt: 3 }}>
        {allocation && (
          <Alert severity="info" sx={{ mb: 2 }}>
            Ending period for: <strong>{allocation.team_name}</strong> / <strong>{allocation.client_name}</strong>
            <br />Current start: <strong>{allocation.start_period || '—'}</strong> | Amount: <strong>₹{Number(allocation.amount || 0).toLocaleString('en-IN')}</strong>
          </Alert>
        )}
        <Grid container spacing={2}>
          <Grid item xs={12}>
            <FormControl fullWidth required>
              <InputLabel>End Period *</InputLabel>
              <Select value={endPeriod} label="End Period *" onChange={(e) => setEndPeriod(e.target.value)}>
                {filteredEndPeriodOptions.map(p => <MenuItem key={p} value={p}>{p}</MenuItem>)}
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={12} md={6}>
            <TextField
              fullWidth label="New Amount (next period)" type="number"
              value={newAmount}
              onChange={(e) => setNewAmount(e.target.value)}
              helperText="Leave unchanged to keep same amount"
              inputProps={{ min: 0, step: 0.01 }}
            />
          </Grid>
          <Grid item xs={12} md={6}>
            <FormControl fullWidth>
              <InputLabel>New Reviewer (next period)</InputLabel>
              <Select value={newReviewerId} label="New Reviewer (next period)" onChange={(e) => setNewReviewerId(e.target.value)}>
                <MenuItem value=""><em>No change</em></MenuItem>
                {reviewers.map(r => <MenuItem key={r.id} value={r.id}>{r.name}</MenuItem>)}
              </Select>
            </FormControl>
          </Grid>
        </Grid>
        {endPeriod && (
          <Alert severity="success" sx={{ mt: 2 }}>
            A new live allocation will start from <strong>{nextMonthLabel(endPeriod)}</strong>
          </Alert>
        )}
      </DialogContent>
      <DialogActions sx={{ p: 2 }}>
        <Button onClick={onClose}>Cancel</Button>
        <Button
          variant="contained"
          color="warning"
          disabled={!valid}
          onClick={() => onSubmit({
            end_period: endPeriod,
            ...(newAmount !== '' && newAmount !== String(allocation?.amount) ? { new_amount: parseFloat(newAmount) || 0 } : {}),
            ...(newReviewerId !== (allocation?.reviewer_id || '') ? { new_reviewer_id: newReviewerId || null } : {}),
          })}
        >
          End Period & Create New
        </Button>
      </DialogActions>
    </Dialog>
  );
};

const RecurringExpenses = () => {
  const navigate = useNavigate();
  const [rows, setRows] = useState([]);
  const [expenseHeads, setExpenseHeads] = useState([]);
  const [teamMembers, setTeamMembers] = useState([]);
  const [clients, setClients] = useState([]);
  const [vendors, setVendors] = useState([]);
  const [servicePeriods, setServicePeriods] = useState([]);
  const [loading, setLoading] = useState(true);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [endPeriodDialogOpen, setEndPeriodDialogOpen] = useState(false);
  const [endPeriodAllocation, setEndPeriodAllocation] = useState(null);
  const [selected, setSelected] = useState(null);
  const [error, setError] = useState('');
  const [statusFilter, setStatusFilter] = useState('active');
  const [searchTerm, setSearchTerm] = useState('');
  const [deleteDeps, setDeleteDeps] = useState([]);
  const [deleteMessage, setDeleteMessage] = useState('');
  const [linkedExpenses, setLinkedExpenses] = useState([]);
  const [depsLoading, setDepsLoading] = useState(false);
  const [toggleConfirmOpen, setToggleConfirmOpen] = useState(false);

  // History dialog
  const [historyDialogOpen, setHistoryDialogOpen] = useState(false);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyLabel, setHistoryLabel] = useState('');
  const [historyParentId, setHistoryParentId] = useState(null);
  const [historyDrafts, setHistoryDrafts] = useState([]);
  const [historySavingKey, setHistorySavingKey] = useState('');
  const [historyError, setHistoryError] = useState('');

  const [formData, setFormData] = useState({
    expense_head_id: '',
    vendor_id: '',
    amount: '',
    start_period: '',
    end_period: '',
    is_active: true,
    is_admin: false,
    allocations: []
  });

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const [recurringRes, headsRes, teamsRes, clientsRes, vendorsRes, periodsRes] = await Promise.all([
        recurringExpenseAPI.getAll({ includeInactive: true }),
        expenseHeadAPI.getAll(),
        teamAPI.getAll(),
        clientAPI.getAll(),
        vendorAPI.getAll(),
        servicePeriodAPI.getAll({ all: 'true', includeInactive: 'true' }),
      ]);

      setRows(recurringRes.data.data || []);
      setExpenseHeads(headsRes.data.data || []);
      setTeamMembers(teamsRes.data.data || []);
      setClients(clientsRes.data.data || []);
      setVendors(vendorsRes.data.data || []);
      setServicePeriods((periodsRes.data.data || []).map(p => p.display_name));
    } catch (err) {
      toast.error('Failed to load recurring expenses');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadData();
  }, [loadData]);

  const applyRecurringExpenseToForm = (data = {}) => {
    const latestEndedPeriod = getLatestEndedPeriod(data);
    const continuationStartPeriod = nextMonthLabel(latestEndedPeriod);
    const isClosedRecord = data.is_active === false || Boolean(data.end_period);

    setSelected(data);
    setFormData({
      expense_head_id: data.expense_head_id || '',
      vendor_id: data.vendor_id || '',
      amount: data.amount || '',
      start_period: isClosedRecord ? (continuationStartPeriod || data.start_period || '') : (data.start_period || ''),
      end_period: isClosedRecord ? '' : (data.end_period || ''),
      is_active: isClosedRecord ? true : data.is_active !== false,
      is_admin: data.is_admin || false,
      allocations: isClosedRecord ? [] : (data.allocations || []).filter(a => a.is_live).map(a => ({
        id: a.id,
        team_id: a.team_id || '',
        client_id: a.client_id || '',
        reviewer_id: a.reviewer_id || '',
        amount: a.amount || 0,
        start_period: a.start_period || '',
      }))
    });
  };

  const refreshSelectedRecurringExpense = async (recurringExpenseId) => {
    if (!recurringExpenseId) return null;
    const res = await recurringExpenseAPI.get(recurringExpenseId);
    applyRecurringExpenseToForm(res.data.data);
    return res.data.data;
  };

  const handleOpenDialog = async (row = null) => {
    if (row) {
      try {
        await refreshSelectedRecurringExpense(row.id);
      } catch {
        toast.error('Failed to load expense details');
        return;
      }
    } else {
      setSelected(null);
      setFormData({
        expense_head_id: '',
        vendor_id: '',
        amount: '',
        start_period: '',
        end_period: '',
        is_active: true,
        is_admin: false,
        allocations: []
      });
    }
    setError('');
    setDialogOpen(true);
  };

  const addAllocation = () => {
    setFormData(prev => ({
      ...prev,
      allocations: [...prev.allocations, { team_id: '', client_id: '', reviewer_id: '', amount: 0, start_period: prev.start_period || '' }]
    }));
  };

  const updateAllocation = (index, data) => {
    const next = [...formData.allocations];
    next[index] = data;
    setFormData(prev => ({ ...prev, allocations: next }));
  };

  const removeAllocation = (index) => {
    setFormData(prev => ({ ...prev, allocations: prev.allocations.filter((_, i) => i !== index) }));
  };

  const calculateTotalAllocated = () =>
    formData.allocations.reduce((sum, a) => sum + (parseFloat(a.amount) || 0), 0);

  const handleStatusChange = (nextStatus) => {
    setError('');
    setFormData((prev) => {
      if (nextStatus === 'active') {
        const continuationStartPeriod = nextMonthLabel(getLatestEndedPeriod(selected || {}));
        return {
          ...prev,
          is_active: true,
          start_period: (selected?.end_period || selected?.is_active === false)
            ? (continuationStartPeriod || prev.start_period)
            : prev.start_period,
          end_period: '',
          allocations: (selected?.end_period || selected?.is_active === false) ? [] : prev.allocations
        };
      }

      return {
        ...prev,
        is_active: false,
        start_period: selected?.start_period || prev.start_period,
        end_period: selected?.end_period || prev.end_period || prev.start_period || '',
        allocations: selected?.end_period || selected?.is_active === false ? [] : prev.allocations
      };
    });
  };

  const handleSubmit = async () => {
    if (!formData.expense_head_id || !formData.amount || !formData.start_period) {
      setError('Expense head, amount, and start period are required');
      return;
    }

    // Duplicate check (skip for admin)
    const validAllocations = formData.is_admin ? [] : formData.allocations.filter(a => a.team_id && a.client_id);
    if (!formData.is_admin) {
      const keys = validAllocations.map(a => `${a.team_id}--${a.client_id}`);
      if (keys.length !== new Set(keys).size) {
        setError('Duplicate team+group combination found in allocations');
        return;
      }

      const totalAllocated = calculateTotalAllocated();
      const mainAmount = parseFloat(formData.amount) || 0;
      if (validAllocations.length > 0 && Math.abs(totalAllocated - mainAmount) > 0.01) {
        setError(`Allocated amounts (₹${totalAllocated.toLocaleString('en-IN')}) do not match total (₹${mainAmount.toLocaleString('en-IN')})`);
        return;
      }
    }

    if (formData.end_period && comparePeriods(formData.end_period, formData.start_period) < 0) {
      setError('End period cannot be before start period');
      return;
    }

    if (!formData.is_active && !formData.end_period) {
      setError('End period is required when status is inactive');
      return;
    }

    if (formData.is_active && selected?.id && selected?.end_period && !formData.end_period) {
      const latestEndedPeriod = getLatestEndedPeriod(selected);
      const reactivationStartFloor = nextMonthLabel(latestEndedPeriod);
      if (reactivationStartFloor && comparePeriods(formData.start_period, reactivationStartFloor) < 0) {
        setError(`Start period can only be selected from ${reactivationStartFloor} onward when reopening this recurring expense`);
        return;
      }
    }

    try {
      const payload = {
        expense_head_id: formData.expense_head_id,
        vendor_id: formData.vendor_id || null,
        amount: parseFloat(formData.amount),
        start_period: formData.start_period,
        end_period: formData.end_period || null,
        is_active: formData.is_active,
        is_admin: formData.is_admin || false,
        allocations: formData.is_admin ? [] : validAllocations.map(a => ({
          ...(a.id ? { id: a.id } : {}),
          team_id: a.team_id,
          client_id: a.client_id,
          reviewer_id: a.reviewer_id || null,
          amount: parseFloat(a.amount) || 0,
          start_period: a.start_period || formData.start_period,
        }))
      };

      if (selected) {
        await recurringExpenseAPI.update(selected.id, payload);
        toast.success('Recurring expense updated successfully');
      } else {
        await recurringExpenseAPI.create(payload);
        toast.success('Recurring expense created successfully');
      }

      setDialogOpen(false);
      loadData();
    } catch (err) {
      setError(err.response?.data?.message || err.response?.data?.errors?.[0]?.msg || 'Operation failed');
    }
  };

  const handleOpenDeleteDialog = async (item) => {
    setSelected(item);
    setDeleteDeps([]);
    setDeleteMessage('');
    setLinkedExpenses([]);
    setDepsLoading(true);
    setDeleteDialogOpen(true);
    try {
      const res = await recurringExpenseAPI.checkDependencies(item.id);
      const data = res.data?.data || {};
      setDeleteDeps(data.dependencies || []);
      setDeleteMessage(data.message || '');
      setLinkedExpenses(data.expenses || []);
    } catch {
      setDeleteDeps([]);
      setDeleteMessage('');
      setLinkedExpenses([]);
    }
    finally { setDepsLoading(false); }
  };

  const handleDelete = async () => {
    try {
      const res = await recurringExpenseAPI.delete(selected.id);
      toast.success(res.data?.message || 'Recurring expense deleted successfully');
      setDeleteDialogOpen(false);
      setDeleteDeps([]);
      setDeleteMessage('');
      setLinkedExpenses([]);
      setSelected(null);
      loadData();
    } catch (err) {
      if (err.response?.status === 409) {
        const data = err.response?.data?.data || {};
        setDeleteDeps(data.dependencies || []);
        setDeleteMessage(err.response?.data?.message || data.message || 'Delete is blocked because this recurring expense is still linked elsewhere.');
        setLinkedExpenses(data.expenses || []);
      }
      toast.error(err.response?.data?.message || 'Failed to delete');
    }
  };

  const handleToggleActive = async () => {
    if (!selected) return;
    try {
      await recurringExpenseAPI.update(selected.id, { is_active: !selected.is_active });
      toast.success(`Recurring expense ${selected.is_active !== false ? 'deactivated' : 'activated'} successfully`);
      setToggleConfirmOpen(false);
      loadData();
    } catch (err) {
      toast.error(err.response?.data?.message || 'Failed to update status');
      setToggleConfirmOpen(false);
    }
  };

  const handleEndPeriodOpen = (recurringExpenseId, allocation) => {
    setEndPeriodAllocation({ ...allocation, recurringExpenseId });
    setEndPeriodDialogOpen(true);
  };

  const handleEndPeriodSubmit = async ({ end_period, new_amount, new_reviewer_id }) => {
    try {
      const { recurringExpenseId, id: allocationId } = endPeriodAllocation;
      await recurringExpenseAPI.endAllocationPeriod(recurringExpenseId, allocationId, {
        end_period,
        ...(new_amount !== undefined ? { new_amount: parseFloat(new_amount) } : {}),
        ...(new_reviewer_id !== undefined ? { new_reviewer_id: new_reviewer_id || null } : {}),
      });
      toast.success(`Period ended at ${end_period}. New allocation created.`);
      setEndPeriodDialogOpen(false);
      await loadData();
      if (dialogOpen && selected?.id === recurringExpenseId) {
        await refreshSelectedRecurringExpense(recurringExpenseId);
      }
    } catch (err) {
      toast.error(err.response?.data?.message || 'Failed to end period');
    }
  };


  const buildHistoryDrafts = (allocs) => {
    const periodMap = {};
    allocs.forEach((a) => {
      const key = `${a.start_period || ''}||${a.end_period || ''}`;
      if (!periodMap[key]) {
        periodMap[key] = {
          start_period: a.start_period,
          end_period: a.end_period,
          is_live: a.is_live,
          details: [],
          amount: 0,
          total: 0,
        };
      }
      periodMap[key].details.push({
        ...a,
        amount: parseFloat(a.amount) || 0,
      });
      periodMap[key].total += parseFloat(a.amount) || 0;
      periodMap[key].amount += parseFloat(a.amount) || 0;
    });
    return Object.values(periodMap).sort((a, b) =>
      comparePeriods(a.start_period, b.start_period) || comparePeriods(a.end_period, b.end_period)
    );
  };

  const syncDraftTotals = (draft) => {
    const total = (draft.details || []).reduce((sum, d) => sum + (parseFloat(d.amount) || 0), 0);
    return { ...draft, total, amount: draft.amount === '' ? '' : draft.amount };
  };

  const getHistoryDraftKey = (draft) => `${draft?.start_period || ''}||${draft?.end_period || ''}`;

  const handleViewHistory = async (parent) => {
    setHistoryLabel(`${parent.expense_head_name || ''}${parent.vendor_name ? ` (${parent.vendor_name})` : ''}`);
    setHistoryParentId(parent.id);
    setHistoryLoading(true);
    setHistoryDrafts([]);
    setHistoryError('');
    setHistoryDialogOpen(true);
    try {
      const res = await recurringExpenseAPI.get(parent.id);
      const data = res.data.data;
      const allocs = data.allocations || [];
      if (allocs.length > 0) {
        setHistoryDrafts(buildHistoryDrafts(allocs));
      } else {
        // Show parent-level entry when no allocations exist
        setHistoryDrafts([{
          start_period: data.start_period,
          end_period: data.end_period,
          is_live: data.is_active !== false && !data.end_period,
          details: [],
          amount: parseFloat(data.amount) || 0,
          total: 0,
        }]);
      }
    } catch {
      toast.error('Failed to load history');
      setHistoryDrafts([]);
    } finally {
      setHistoryLoading(false);
    }
  };

  const refreshHistory = async (parentId) => {
    if (!parentId) return;
    try {
      const res = await recurringExpenseAPI.get(parentId);
      setHistoryDrafts(buildHistoryDrafts(res.data.data.allocations || []));
    } catch {
      // keep existing
    }
  };

  const handleCloseHistory = () => {
    setHistoryDialogOpen(false);
    setHistoryLabel('');
    setHistoryParentId(null);
    setHistoryDrafts([]);
    setHistorySavingKey('');
    setHistoryError('');
  };

  const handleOpenSelectedHistory = async () => {
    if (!selected?.id) return;
    await handleViewHistory(selected);
  };

  const handleHistoryDetailChange = (draftIndex, detailIndex, field, value) => {
    setHistoryError('');
    setHistoryDrafts((prev) => prev.map((draft, di) => {
      if (di !== draftIndex) return draft;
      const nextDraft = {
        ...draft,
        details: draft.details.map((detail, ddi) =>
          ddi === detailIndex
            ? { ...detail, [field]: field === 'amount' ? (value === '' ? '' : parseFloat(value) || 0) : value }
            : detail
        ),
      };
      return syncDraftTotals(nextDraft);
    }));
  };

  const handleHistoryDraftChange = (draftIndex, field, value) => {
    setHistoryError('');
    setHistoryDrafts((prev) => prev.map((draft, index) => (
      index === draftIndex
        ? { ...draft, [field]: field === 'amount' ? value : value }
        : draft
    )));
  };

  const handleSaveHistoryPeriod = async (draftIndex) => {
    if (!historyParentId) return;
    const draft = historyDrafts[draftIndex];
    const periodLabel = formatPeriodRange(draft?.start_period, draft?.end_period);

    // Validate amounts
    const invalidDetail = (draft.details || []).find((d) =>
      d.amount === '' || isNaN(Number(d.amount)) || Number(d.amount) < 0
    );
    if (invalidDetail) {
      setHistoryError(`Enter a valid amount for every row in ${periodLabel}`);
      return;
    }

    const amount = parseFloat(draft.amount);
    if (draft.amount === '' || Number.isNaN(amount) || amount < 0) {
      setHistoryError(`Enter a valid total amount for ${periodLabel}`);
      return;
    }

    if (!draft.start_period) {
      setHistoryError(`Start period is required for ${periodLabel}`);
      return;
    }

    if (draft.end_period && comparePeriods(draft.end_period, draft.start_period) < 0) {
      setHistoryError(`End period cannot be before start period for ${periodLabel}`);
      return;
    }

    if (getDraftDuplicateState(draft.details || [])) {
      setHistoryError(`Duplicate team and group combinations found in ${periodLabel}`);
      return;
    }

    if (Math.abs((draft.total || 0) - amount) > 0.01) {
      setHistoryError(`Allocation total for ${periodLabel} must match total amount ₹${amount.toLocaleString('en-IN')}`);
      return;
    }

    const savingKey = getHistoryDraftKey(draft);
    setHistorySavingKey(savingKey);
    setHistoryError('');

    try {
      // Save each allocation individually
      for (const detail of (draft.details || [])) {
        if (!detail.id) continue;
        await recurringExpenseAPI.updateAllocation(historyParentId, detail.id, {
          team_id: detail.team_id || undefined,
          client_id: detail.client_id || undefined,
          reviewer_id: detail.reviewer_id || null,
          amount: parseFloat(detail.amount) || 0,
          start_period: draft.start_period,
          end_period: draft.end_period || null,
        });
      }
      toast.success(`Updated ${periodLabel}`);
      await loadData();
      await refreshHistory(historyParentId);
      if (dialogOpen && selected?.id === historyParentId) {
        await refreshSelectedRecurringExpense(historyParentId);
      }
    } catch (err) {
      setHistoryError(err.response?.data?.message || `Failed to update ${periodLabel}`);
    } finally {
      setHistorySavingKey('');
    }
  };

  const historicalAllocationCount = useMemo(
    () => ((selected?.allocations || []).filter((allocation) => !allocation.is_live).length),
    [selected]
  );
  const latestEndedPeriod = useMemo(() => getLatestEndedPeriod(selected), [selected]);
  const reactivationStartFloor = useMemo(() => nextMonthLabel(latestEndedPeriod), [latestEndedPeriod]);
  const startPeriodOptions = useMemo(
    () => servicePeriods.filter((period) => {
      if (period === formData.start_period) return true;
      if (comparePeriods(period, MIN_PERIOD) < 0) return false;
      if (formData.is_active && selected?.end_period && reactivationStartFloor) {
        return comparePeriods(period, reactivationStartFloor) >= 0;
      }
      return true;
    }),
    [formData.is_active, formData.start_period, reactivationStartFloor, selected?.end_period, servicePeriods]
  );

  // Flatten parent rows + allocations into individual display rows (like Revenue's flat list)
  const flatRows = useMemo(() => {
    const result = [];
    rows.forEach((parent) => {
      if (parent.is_admin) {
        result.push({
          _rowKey: `${parent.id}-admin`,
          type: 'admin',
          parent,
          expense_head_name: parent.expense_head_name,
          vendor_name: parent.vendor_name,
          team_name: null,
          client_name: null,
          reviewer_name: null,
          amount: parent.amount,
          start_period: parent.start_period,
          end_period: parent.end_period,
          alloc: null,
          is_live: !parent.end_period && parent.is_active !== false,
        });
      } else if ((parent.allocations || []).length === 0) {
        result.push({
          _rowKey: `${parent.id}-noalloc`,
          type: 'none',
          parent,
          expense_head_name: parent.expense_head_name,
          vendor_name: parent.vendor_name,
          team_name: null,
          client_name: null,
          reviewer_name: null,
          amount: parent.amount,
          start_period: parent.start_period,
          end_period: parent.end_period,
          alloc: null,
          is_live: !parent.end_period && parent.is_active !== false,
        });
      } else {
        // Show only live allocations in the main table; historical ones accessible via History
        const liveAllocs = (parent.allocations || []).filter(a => a.is_live);
        // For inactive parents (no live allocs), show the most recent allocation (last in array)
        const displayAllocs = liveAllocs.length > 0
          ? liveAllocs
          : (parent.allocations || []).length > 0
            ? [(parent.allocations || [])[(parent.allocations || []).length - 1]]
            : [];
        if (displayAllocs.length === 0) {
          // No allocations at all — show parent-level row
          result.push({
            _rowKey: `${parent.id}-noalloc`,
            type: 'none',
            parent,
            expense_head_name: parent.expense_head_name,
            vendor_name: parent.vendor_name,
            team_name: null,
            client_name: null,
            reviewer_name: null,
            amount: parent.amount,
            start_period: parent.start_period,
            end_period: parent.end_period,
            alloc: null,
            is_live: !parent.end_period && parent.is_active !== false,
          });
        } else {
          displayAllocs.forEach((alloc) => {
            result.push({
              _rowKey: `${parent.id}-${alloc.id}`,
              type: 'allocation',
              parent,
              alloc,
              expense_head_name: parent.expense_head_name,
              vendor_name: parent.vendor_name,
              team_name: alloc.team_name,
              client_name: alloc.client_name,
              reviewer_name: alloc.reviewer_name,
              amount: alloc.amount,
              start_period: alloc.start_period,
              end_period: alloc.end_period || parent.end_period,
              is_live: alloc.is_live && parent.is_active !== false && !parent.end_period,
            });
          });
        }
      }
    });
    return result;
  }, [rows]);

  const filteredFlatRows = useMemo(() => flatRows.filter((r) => {
    const parentActive = r.parent?.is_active !== false;
    if (statusFilter === 'active' && !parentActive) return false;
    if (statusFilter === 'inactive' && parentActive) return false;
    if (searchTerm) {
      const term = searchTerm.toLowerCase();
      return (
        (r.expense_head_name || '').toLowerCase().includes(term) ||
        (r.vendor_name || '').toLowerCase().includes(term) ||
        (r.team_name || '').toLowerCase().includes(term) ||
        (r.client_name || '').toLowerCase().includes(term) ||
        (r.reviewer_name || '').toLowerCase().includes(term)
      );
    }
    return true;
  }), [flatRows, statusFilter, searchTerm]);

  // Calculate totals
  const totalAmount = rows.reduce((sum, r) => sum + (parseFloat(r.amount) || 0), 0);
  const activeCount = rows.filter(r => r.is_active !== false && !r.end_period).length;

  return (
    <Box>
      <PageHeader
        eyebrow="Recurring Expense Master"
        title="Recurring expense control"
        actions={[
          <HoverActionButton key="add" icon={<Add fontSize="small" />} label="Add Recurring" onClick={() => handleOpenDialog()} tone="mint" />,
          <HoverActionButton key="refresh" icon={<Refresh fontSize="small" />} label="Refresh" onClick={loadData} tone="peach" />,
        ]}
      />

      <Grid container spacing={2} sx={{ mb: 2.5 }}>
        <Grid item xs={12} md={4}>
          <MetricCard eyebrow="Total" title="Recurring expenses" value={`₹${totalAmount.toLocaleString('en-IN')}`} tone="mint" />
        </Grid>
        <Grid item xs={12} md={4}>
          <MetricCard eyebrow="Active" title="Active items" value={String(activeCount)} tone="peach" />
        </Grid>
        <Grid item xs={12} md={4}>
          <MetricCard eyebrow="Total" title="All items" value={String(rows.length)} tone="sand" />
        </Grid>
      </Grid>

      <SectionCard title="Recurring expense ledger" tone="sage" contentSx={{ p: 2 }}>
        <Box sx={{ display: 'flex', gap: 2, mb: 2, alignItems: 'center' }}>
          <TextField size="small" placeholder="Search..." value={searchTerm} onChange={(e) => setSearchTerm(e.target.value)} InputProps={{ startAdornment: <Search sx={{ mr: 1, color: 'text.secondary' }} /> }} sx={{ width: 300 }} />
          <ToggleButtonGroup value={statusFilter} exclusive size="small" onChange={(_, v) => v && setStatusFilter(v)}>
            <ToggleButton value="active">Active</ToggleButton>
            <ToggleButton value="inactive">Inactive</ToggleButton>
            <ToggleButton value="all">All</ToggleButton>
          </ToggleButtonGroup>
        </Box>
          <Table size="small">
            <TableHead>
              <TableRow sx={{ bgcolor: 'grey.100' }}>
                <TableCell sx={{ fontWeight: 'bold' }}>Expense Head</TableCell>
                <TableCell sx={{ fontWeight: 'bold' }}>Vendor</TableCell>
                <TableCell sx={{ fontWeight: 'bold' }}>Team</TableCell>
                <TableCell sx={{ fontWeight: 'bold' }}>Group</TableCell>
                <TableCell sx={{ fontWeight: 'bold' }}>Reviewer</TableCell>
                <TableCell sx={{ fontWeight: 'bold' }} align="right">Amount</TableCell>
                <TableCell sx={{ fontWeight: 'bold' }}>Period</TableCell>
                <TableCell sx={{ fontWeight: 'bold' }}>Status</TableCell>
                <TableCell sx={{ fontWeight: 'bold' }}>Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {loading ? (
                <TableRow>
                  <TableCell colSpan={9} align="center" sx={{ py: 4 }}>
                    <Typography color="text.secondary">Loading...</Typography>
                  </TableCell>
                </TableRow>
              ) : filteredFlatRows.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={9} align="center" sx={{ py: 4 }}>
                    <Typography color="text.secondary">No recurring expenses found</Typography>
                  </TableCell>
                </TableRow>
              ) : filteredFlatRows.map((fr) => (
                <TableRow key={fr._rowKey} hover sx={{ opacity: fr.parent?.is_active !== false ? 1 : 0.55 }}>
                  <TableCell>{fr.expense_head_name}</TableCell>
                  <TableCell>{fr.vendor_name || <Typography variant="body2" color="text.secondary">—</Typography>}</TableCell>
                  <TableCell>
                    {fr.type === 'admin'
                      ? <Chip label="Admin" size="small" color="info" />
                      : fr.team_name || <Typography variant="body2" color="text.secondary">—</Typography>}
                  </TableCell>
                  <TableCell>{fr.client_name || <Typography variant="body2" color="text.secondary">—</Typography>}</TableCell>
                  <TableCell>{fr.reviewer_name || <Typography variant="body2" color="text.secondary">—</Typography>}</TableCell>
                  <TableCell align="right">
                    <Typography fontWeight={fr.is_live ? 'bold' : 'normal'} variant="body2">
                      ₹{Number(fr.amount || 0).toLocaleString('en-IN')}
                    </Typography>
                  </TableCell>
                  <TableCell sx={{ whiteSpace: 'nowrap' }}>
                    {formatPeriodRange(fr.start_period, fr.end_period)}
                  </TableCell>
                  <TableCell>
                    {fr.parent?.is_active === false
                      ? <Chip label="Inactive" size="small" color="default" />
                      : <Chip label="Active" size="small" color="success" />}
                  </TableCell>
                  <TableCell>
                    <IconButton size="small" color="primary" title="Edit" onClick={() => handleOpenDialog(fr.parent || fr)}>
                      <Edit fontSize="small" />
                    </IconButton>
                    <IconButton size="small" color="secondary" title="History" onClick={() => handleViewHistory(fr.parent || fr)}>
                      <History fontSize="small" />
                    </IconButton>
                    <IconButton
                      size="small"
                      title={fr.parent?.is_active !== false ? 'Deactivate' : 'Activate'}
                      color={fr.parent?.is_active !== false ? 'error' : 'success'}
                      onClick={() => { setSelected(fr.parent || fr); setToggleConfirmOpen(true); }}
                    >
                      {fr.parent?.is_active !== false ? <Cancel fontSize="small" /> : <CheckCircle fontSize="small" />}
                    </IconButton>
                    {fr.type === 'allocation' && fr.is_live && fr.alloc && (
                      <IconButton size="small" color="warning" title="End Period" onClick={() => handleEndPeriodOpen((fr.parent || fr).id, fr.alloc)}>
                        <EventBusy fontSize="small" />
                      </IconButton>
                    )}
                    <IconButton size="small" color="error" title="Delete" onClick={() => handleOpenDeleteDialog(fr.parent || fr)}>
                      <Delete fontSize="small" />
                    </IconButton>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
      </SectionCard>

      {/* Add/Edit Dialog */}
      <Dialog open={dialogOpen} onClose={() => setDialogOpen(false)} maxWidth="md" fullWidth>
        <DialogTitle sx={{ bgcolor: '#f0fdfa', color: '#0f766e' }}>
          {selected ? 'Edit Recurring Expense' : 'Add Recurring Expense'}
        </DialogTitle>
        <DialogContent sx={{ pt: 3 }}>
          {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}

          <Grid container spacing={2} sx={{ mt: 0.5 }}>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth>
                <InputLabel>Expense Head *</InputLabel>
                <Select
                  value={formData.expense_head_id}
                  label="Expense Head *"
                  onChange={(e) => setFormData(prev => ({ ...prev, expense_head_id: e.target.value }))}
                >
                  {expenseHeads.filter(eh => eh.is_active !== false).map((head) => (
                    <MenuItem key={head.id} value={head.id}>{head.name}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth>
                <InputLabel>Vendor</InputLabel>
                <Select
                  value={formData.vendor_id}
                  label="Vendor"
                  onChange={(e) => setFormData(prev => ({ ...prev, vendor_id: e.target.value }))}
                >
                  <MenuItem value=""><em>None</em></MenuItem>
                  {vendors.filter(v => v.is_active !== false).map((v) => (
                    <MenuItem key={v.id} value={v.id}>{v.name}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={4}>
              <TextField
                fullWidth
                label="Total Amount *"
                type="number"
                value={formData.amount}
                onChange={(e) => setFormData(prev => ({ ...prev, amount: e.target.value }))}
              />
            </Grid>
            <Grid item xs={12} md={4}>
              <FormControl fullWidth>
                <InputLabel>Start Period *</InputLabel>
                <Select
                  value={formData.start_period}
                  label="Start Period *"
                  onChange={(e) => setFormData(prev => ({ ...prev, start_period: e.target.value }))}
                >
                  {startPeriodOptions.map((p) => <MenuItem key={p} value={p}>{p}</MenuItem>)}
                </Select>
              </FormControl>
              {formData.is_active && selected?.end_period && reactivationStartFloor && (
                <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, display: 'block' }}>
                  Reopen start period can be selected from {reactivationStartFloor} onward.
                </Typography>
              )}
            </Grid>
            <Grid item xs={12} md={4}>
              <FormControl fullWidth>
                <InputLabel>End Period</InputLabel>
                <Select
                  value={formData.end_period}
                  label="End Period"
                  onChange={(e) => {
                    const val = e.target.value;
                    setFormData(prev => ({
                      ...prev,
                      end_period: val,
                      ...(val ? { is_active: false } : {}),
                    }));
                  }}
                >
                  <MenuItem value="">Open Ended</MenuItem>
                  {servicePeriods
                    .filter(p => !formData.start_period || comparePeriods(p, formData.start_period) >= 0)
                    .map((p) => <MenuItem key={p} value={p}>{p}</MenuItem>)}
                </Select>
              </FormControl>
            </Grid>
          </Grid>

          <Box sx={{ mt: 2 }}>
            <Box sx={{ mb: 2.5 }}>
              <Typography variant="subtitle2" sx={{ fontWeight: 600, mb: 1 }}>Status</Typography>
              <Box sx={{ display: 'flex', gap: 1, bgcolor: 'grey.100', borderRadius: '10px', p: 0.5, width: 'fit-content' }}>
                {[{ key: 'active', label: 'Active' }, { key: 'inactive', label: 'Inactive' }].map((option) => {
                  const selectedStatus = (formData.is_active ? 'active' : 'inactive') === option.key;
                  return (
                    <Button
                      key={option.key}
                      size="small"
                      variant={selectedStatus ? 'contained' : 'text'}
                      onClick={() => handleStatusChange(option.key)}
                      sx={{ borderRadius: '8px', textTransform: 'none', fontWeight: selectedStatus ? 600 : 500, px: 2.5 }}
                    >
                      {option.label}
                    </Button>
                  );
                })}
              </Box>
            </Box>

            <FormControlLabel
              control={
                <Checkbox
                  checked={formData.is_admin || false}
                  onChange={(e) => setFormData(prev => ({ ...prev, is_admin: e.target.checked, allocations: e.target.checked ? [] : prev.allocations }))}
                />
              }
              label="Admin Cost (no team/group allocation needed)"
            />
          </Box>

          {!formData.is_admin && (
          <>
          <Divider sx={{ my: 3 }} />

          <Box sx={{ mb: 2, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Box>
              <Typography variant="h6">Live Team & Group Allocations</Typography>
              <Typography variant="body2" color="text.secondary">
                Current live period: {formatPeriodRange(formData.start_period, formData.end_period)}
              </Typography>
            </Box>
            <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
              <Link
                component="button"
                type="button"
                underline="hover"
                onClick={handleOpenSelectedHistory}
                disabled={!selected?.id || historicalAllocationCount === 0}
              >
                History
              </Link>
              {formData.is_active && (
                <Typography color="text.secondary">
                  Allocated: ₹{calculateTotalAllocated().toLocaleString('en-IN')}
                </Typography>
              )}
              {formData.is_active && formData.amount && formData.allocations.length > 0 && Math.abs(calculateTotalAllocated() - parseFloat(formData.amount)) > 0.01 && (
                <Chip
                  label={`Diff: ₹${Math.abs(calculateTotalAllocated() - parseFloat(formData.amount)).toLocaleString('en-IN')}`}
                  color="warning"
                  size="small"
                />
              )}
              {formData.is_active && <Button size="small" variant="outlined" startIcon={<Add />} onClick={addAllocation}>Add Row</Button>}
            </Box>
          </Box>

          {formData.is_active && formData.allocations.map((alloc, index) => (
            <AllocationRow
              key={index}
              allocation={alloc}
              index={index}
              allTeams={teamMembers}
              allClients={clients}
              allReviewers={teamMembers.filter(t => t.is_reviewer && t.is_active !== false)}
              onUpdate={updateAllocation}
              onRemove={removeAllocation}
              siblingAllocations={formData.allocations}
            />
          ))}

          {formData.is_active && formData.allocations.length === 0 && (
            <Paper sx={{ p: 3, textAlign: 'center', bgcolor: 'grey.50' }}>
              <Typography color="text.secondary">No allocations added. Click "Add Row" to add a team+group allocation.</Typography>
            </Paper>
          )}
          {!formData.is_active && (
            <Paper sx={{ p: 3, textAlign: 'center', bgcolor: 'grey.50' }}>
              <Typography color="text.secondary">This recurring expense is inactive. Switch status to Active to continue with the next open period and add new team/group allocations.</Typography>
            </Paper>
          )}
          </>
          )}
        </DialogContent>
        <DialogActions sx={{ p: 2 }}>
          <Button onClick={() => setDialogOpen(false)}>Cancel</Button>
          <Button variant="contained" onClick={handleSubmit}>{selected ? 'Update' : 'Create'}</Button>
        </DialogActions>
      </Dialog>

      {/* History Dialog — Editable like Teams/Clients module */}
      <Dialog open={historyDialogOpen} onClose={handleCloseHistory} maxWidth="lg" fullWidth PaperProps={{ sx: { borderRadius: '16px' } }}>
        <DialogTitle>
          Allocation History
          {historyLabel && (
            <Chip label={historyLabel} size="small" sx={{ ml: 1.5, bgcolor: '#f0fdfa', color: '#0f766e', fontWeight: 500 }} />
          )}
        </DialogTitle>
        <DialogContent sx={{ pt: 3 }}>
          <Alert severity="info" sx={{ mb: 2 }}>
            Closed periods can be corrected here. Update total amount, start period, end period, and the team/group breakup for the full period, then click "Save Period".
          </Alert>

          {historyError && <Alert severity="error" sx={{ mb: 2 }}>{historyError}</Alert>}

          {historyLoading ? (
            <Box sx={{ p: 4, textAlign: 'center' }}>
              <Typography color="text.secondary">Loading history...</Typography>
            </Box>
          ) : historyDrafts.length === 0 ? (
            <Typography>No closed allocation periods available yet.</Typography>
          ) : (
            <Box>
              {historyDrafts.map((draft, draftIndex) => {
                const saving = historySavingKey === getHistoryDraftKey(draft);
                const isEditable = !!draft.end_period;
                const periodLabel = formatPeriodRange(draft.start_period, draft.end_period);
                return (
                  <SectionCard
                    key={getHistoryDraftKey(draft)}
                    title={periodLabel}
                    tone="sage"
                    contentSx={{ p: 2 }}
                    sx={{ mb: 2 }}
                  >
                    <Grid container spacing={2} alignItems="center" sx={{ mb: 2 }}>
                      <Grid item xs={12} md={3}>
                        {isEditable ? (
                          <FormControl fullWidth size="small">
                            <InputLabel>Start Period</InputLabel>
                            <Select
                              value={draft.start_period || ''}
                              label="Start Period"
                              onChange={(e) => handleHistoryDraftChange(draftIndex, 'start_period', e.target.value)}
                            >
                              {servicePeriods.map((period) => (
                                <MenuItem key={period} value={period}>{period}</MenuItem>
                              ))}
                            </Select>
                          </FormControl>
                        ) : (
                          <TextField
                            fullWidth size="small" label="Start Period"
                            value={draft.start_period} InputProps={{ readOnly: true }}
                          />
                        )}
                      </Grid>
                      <Grid item xs={12} md={3}>
                        {isEditable ? (
                          <FormControl fullWidth size="small">
                            <InputLabel>End Period</InputLabel>
                            <Select
                              value={draft.end_period || ''}
                              label="End Period"
                              onChange={(e) => handleHistoryDraftChange(draftIndex, 'end_period', e.target.value)}
                            >
                              <MenuItem value="">Open</MenuItem>
                              {servicePeriods
                                .filter((period) => !draft.start_period || comparePeriods(period, draft.start_period) >= 0)
                                .map((period) => (
                                  <MenuItem key={period} value={period}>{period}</MenuItem>
                                ))}
                            </Select>
                          </FormControl>
                        ) : (
                          <TextField
                            fullWidth size="small" label="End Period"
                            value={draft.end_period || 'Open'} InputProps={{ readOnly: true }}
                          />
                        )}
                      </Grid>
                      <Grid item xs={12} md={3}>
                        {isEditable ? (
                          <TextField
                            fullWidth
                            size="small"
                            label="Total Amount"
                            type="number"
                            value={draft.amount}
                            onChange={(e) => handleHistoryDraftChange(draftIndex, 'amount', e.target.value)}
                            inputProps={{ min: 0, step: 0.01 }}
                          />
                        ) : (
                          <Chip
                            label={draft.is_live ? 'Live (current)' : 'Historical (closed)'}
                            size="small"
                            color={draft.is_live ? 'success' : 'default'}
                          />
                        )}
                      </Grid>
                      <Grid item xs={12} md={3}>
                        <Box sx={{ display: 'flex', justifyContent: { xs: 'flex-start', md: 'flex-end' }, gap: 1, alignItems: 'center', height: '100%' }}>
                          <Chip
                            label={`Alloc. Total: ₹${Number(draft.total || 0).toLocaleString('en-IN')}`}
                            color={Math.abs((draft.total || 0) - (parseFloat(draft.amount) || 0)) < 0.01 ? 'success' : 'error'}
                            size="small"
                          />
                          {isEditable && (
                            <Button
                              variant="contained"
                              size="small"
                              onClick={() => handleSaveHistoryPeriod(draftIndex)}
                              disabled={saving}
                              sx={{ textTransform: 'none' }}
                            >
                              {saving ? 'Saving...' : 'Save Period'}
                            </Button>
                          )}
                        </Box>
                      </Grid>
                    </Grid>

                    {draft.details.length > 0 ? (
                      <Table size="small">
                        <TableHead>
                          <TableRow>
                            <TableCell width={72}>Line</TableCell>
                            <TableCell>Team</TableCell>
                            <TableCell>Group</TableCell>
                            <TableCell>Reviewer</TableCell>
                            <TableCell width={160}>Amount</TableCell>
                            <TableCell width={100}>Share</TableCell>
                          </TableRow>
                        </TableHead>
                        <TableBody>
                          {draft.details.map((alloc, detailIndex) => {
                            const sharePercent = draft.total > 0 ? ((parseFloat(alloc.amount) || 0) / draft.total * 100) : 0;
                            return (
                              <TableRow key={alloc.id || `${draftIndex}-${detailIndex}`}>
                                <TableCell>
                                  <Chip label={detailIndex + 1} size="small" variant="outlined" />
                                </TableCell>
                                <TableCell sx={{ minWidth: 160 }}>
                                  {isEditable ? (
                                    <FormControl size="small" fullWidth>
                                      <Select
                                        value={alloc.team_id || ''}
                                        displayEmpty
                                        onChange={(e) => handleHistoryDetailChange(draftIndex, detailIndex, 'team_id', e.target.value)}
                                      >
                                        <MenuItem value="" disabled><em>Select Team</em></MenuItem>
                                        {teamMembers.filter(t => t.is_active !== false).map(t => (
                                          <MenuItem key={t.id} value={t.id}>{t.name}</MenuItem>
                                        ))}
                                      </Select>
                                    </FormControl>
                                  ) : (
                                    <Typography variant="body2">{alloc.team_name || '—'}</Typography>
                                  )}
                                </TableCell>
                                <TableCell sx={{ minWidth: 160 }}>
                                  {isEditable ? (
                                    <FormControl size="small" fullWidth>
                                      <Select
                                        value={alloc.client_id || ''}
                                        displayEmpty
                                        onChange={(e) => handleHistoryDetailChange(draftIndex, detailIndex, 'client_id', e.target.value)}
                                      >
                                        <MenuItem value="" disabled><em>Select Group</em></MenuItem>
                                        {clients.filter(c => c.is_active !== false).map(c => (
                                          <MenuItem key={c.id} value={c.id}>{c.name}</MenuItem>
                                        ))}
                                      </Select>
                                    </FormControl>
                                  ) : (
                                    <Typography variant="body2">{alloc.client_name || '—'}</Typography>
                                  )}
                                </TableCell>
                                <TableCell sx={{ minWidth: 160 }}>
                                  {isEditable ? (
                                    <FormControl size="small" fullWidth>
                                      <Select
                                        value={alloc.reviewer_id || ''}
                                        displayEmpty
                                        onChange={(e) => handleHistoryDetailChange(draftIndex, detailIndex, 'reviewer_id', e.target.value)}
                                      >
                                        <MenuItem value=""><em>None</em></MenuItem>
                                        {teamMembers.filter(t => t.is_reviewer && t.is_active !== false).map(r => (
                                          <MenuItem key={r.id} value={r.id}>{r.name}</MenuItem>
                                        ))}
                                      </Select>
                                    </FormControl>
                                  ) : (
                                    <Typography variant="body2">{alloc.reviewer_name || '—'}</Typography>
                                  )}
                                </TableCell>
                                <TableCell>
                                  {isEditable ? (
                                    <TextField
                                      size="small" type="number" value={alloc.amount}
                                      onChange={(e) => handleHistoryDetailChange(draftIndex, detailIndex, 'amount', e.target.value)}
                                      inputProps={{ min: 0, step: 0.01 }}
                                    />
                                  ) : (
                                    <Typography variant="body2" sx={{ fontWeight: 600 }}>
                                      ₹{Number(alloc.amount || 0).toLocaleString('en-IN')}
                                    </Typography>
                                  )}
                                </TableCell>
                                <TableCell>
                                  <Chip
                                    label={`${sharePercent.toFixed(1)}%`}
                                    size="small"
                                    color={sharePercent > 0 ? 'primary' : 'default'}
                                    variant="outlined"
                                  />
                                </TableCell>
                              </TableRow>
                            );
                          })}
                          <TableRow>
                            <TableCell colSpan={4}><strong>Total</strong></TableCell>
                            <TableCell>
                              <Chip
                                label={`₹${Number(draft.total || 0).toLocaleString('en-IN')}`}
                                color="success"
                                size="small"
                              />
                            </TableCell>
                            <TableCell></TableCell>
                          </TableRow>
                        </TableBody>
                      </Table>
                    ) : (
                      <Typography variant="body2" color="text.secondary">
                        No allocations defined for this period.
                      </Typography>
                    )}
                  </SectionCard>
                );
              })}
            </Box>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCloseHistory} sx={{ textTransform: 'none' }}>Close</Button>
        </DialogActions>
      </Dialog>

      {/* End Period Dialog */}
      <EndPeriodDialog
        open={endPeriodDialogOpen}
        allocation={endPeriodAllocation}
        teamMembers={teamMembers}
        periodOptions={servicePeriods}
        onClose={() => setEndPeriodDialogOpen(false)}
        onSubmit={handleEndPeriodSubmit}
      />

      {/* Toggle Active/Inactive Dialog */}
      <Dialog open={toggleConfirmOpen} onClose={() => setToggleConfirmOpen(false)} PaperProps={{ sx: { borderRadius: '16px' } }} maxWidth="xs" fullWidth>
        <DialogTitle sx={{
          bgcolor: selected?.is_active !== false ? 'rgba(220,38,38,0.06)' : 'rgba(22,163,74,0.06)',
          borderBottom: '1px solid',
          borderColor: selected?.is_active !== false ? 'rgba(220,38,38,0.12)' : 'rgba(22,163,74,0.12)',
          fontWeight: 700,
          color: selected?.is_active !== false ? '#991B1B' : '#14532d'
        }}>
          {selected?.is_active !== false ? 'Deactivate Recurring Expense' : 'Activate Recurring Expense'}
        </DialogTitle>
        <DialogContent sx={{ pt: 3 }}>
          <Typography variant="body2">
            {selected?.is_active !== false
              ? `Deactivate "${selected?.expense_head_name || ''}"? It will no longer generate expense entries for future periods.`
              : `Activate "${selected?.expense_head_name || ''}"? It will resume generating expense entries.`}
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setToggleConfirmOpen(false)} sx={{ textTransform: 'none' }}>Cancel</Button>
          <Button variant="contained" color={selected?.is_active !== false ? 'error' : 'success'} onClick={handleToggleActive} sx={{ textTransform: 'none', boxShadow: 'none' }}>
            {selected?.is_active !== false ? 'Deactivate' : 'Activate'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Delete Dialog */}
      <Dialog open={deleteDialogOpen} onClose={() => { setDeleteDialogOpen(false); setDeleteDeps([]); setDeleteMessage(''); setLinkedExpenses([]); }} PaperProps={{ sx: { borderRadius: '16px' } }} maxWidth="md" fullWidth>
        <DialogTitle sx={{ bgcolor: 'rgba(220,38,38,0.06)', borderBottom: '1px solid rgba(220,38,38,0.12)', fontWeight: 700, color: '#991B1B' }}>
          Delete Recurring Expense
        </DialogTitle>
        <DialogContent sx={{ pt: 3 }}>
          {depsLoading ? (
            <Typography color="text.secondary">Checking dependencies...</Typography>
          ) : linkedExpenses.length > 0 ? (
            <>
              <Alert severity="warning" sx={{ mb: 2, borderRadius: '8px' }}>
                {deleteMessage || 'This recurring expense is still linked to generated expense rows. Update or restore those lines first, then delete this recurring master.'}
              </Alert>
              <Typography variant="body2" sx={{ mb: 1.5 }}>
                {summarizeDependencies(deleteDeps)}
              </Typography>
              <Table size="small" sx={{ mb: 1 }}>
                <TableHead>
                  <TableRow>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Period</TableCell>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Team</TableCell>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Client</TableCell>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Head</TableCell>
                    <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Amount</TableCell>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Status</TableCell>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Action</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {linkedExpenses.map((exp) => (
                    <TableRow key={exp.id} sx={{ opacity: exp.is_modified ? 1 : 0.6 }}>
                      <TableCell sx={{ fontSize: '0.75rem' }}>{exp.service_period_name || '-'}</TableCell>
                      <TableCell sx={{ fontSize: '0.75rem' }}>{exp.team_name || '-'}</TableCell>
                      <TableCell sx={{ fontSize: '0.75rem' }}>{exp.client_name || '-'}</TableCell>
                      <TableCell sx={{ fontSize: '0.75rem' }}>{exp.expense_head_name || '-'}</TableCell>
                      <TableCell align="right" sx={{ fontSize: '0.75rem' }}>₹{Number(exp.total_amount || exp.amount || 0).toLocaleString('en-IN')}</TableCell>
                      <TableCell>
                        <Chip
                          label={exp.is_modified ? 'Modified' : 'Auto-created'}
                          size="small"
                          color={exp.is_modified ? 'warning' : 'default'}
                          sx={{ fontSize: '0.65rem', height: 20 }}
                        />
                      </TableCell>
                      <TableCell>
                        {exp.is_modified ? (
                          <IconButton size="small" color="primary" title="Edit in Expenses" onClick={() => { setDeleteDialogOpen(false); navigate('/expenses'); }}>
                            <OpenInNew sx={{ fontSize: 16 }} />
                          </IconButton>
                        ) : (
                          <Typography variant="caption" color="text.secondary">Still linked</Typography>
                        )}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </>
          ) : (
            <Typography>
              {selected?.is_active !== false
                ? 'Delete this recurring expense entry? This will mark it as inactive.'
                : 'Delete this inactive recurring expense entry permanently?'}
            </Typography>
          )}
        </DialogContent>
        <DialogActions sx={{ p: 2.5, borderTop: '1px solid rgba(0,0,0,0.08)' }}>
          <Button onClick={() => { setDeleteDialogOpen(false); setDeleteDeps([]); setDeleteMessage(''); setLinkedExpenses([]); }} sx={{ textTransform: 'none' }}>Cancel</Button>
          <Button variant="contained" color="error" onClick={handleDelete} disabled={depsLoading || linkedExpenses.length > 0} sx={{ textTransform: 'none', boxShadow: 'none' }}>
            {selected?.is_active !== false ? 'Delete' : 'Delete Permanently'}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default RecurringExpenses;
