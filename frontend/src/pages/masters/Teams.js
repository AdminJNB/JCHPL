import React, { useState, useEffect, useCallback } from 'react';
import {
  Box, Button, TextField, Dialog, DialogTitle,
  DialogContent, DialogActions, IconButton, Typography, Chip, Alert,
  FormControlLabel, Checkbox, FormControl, FormHelperText, InputLabel, Select, MenuItem,
  Table, TableBody, TableCell, TableHead, TableRow, Grid, ToggleButton, ToggleButtonGroup, Link
} from '@mui/material';
import { DataGrid } from '@mui/x-data-grid';
import { Add, Edit, Delete, Search, Refresh, AddCircle, RemoveCircle, CheckCircle, Cancel } from '@mui/icons-material';
import { toast } from 'react-toastify';
import {
  HoverActionButton,
  MetricCard,
  PageHeader,
  SectionCard,
} from '../../components/FuturisticUI';
import { teamAPI, clientAPI, servicePeriodAPI, expenseHeadAPI } from '../../services/api';
import { flattenDependencyItems, summarizeDependencies } from '../../utils/deleteDependencies';

const periodToDate = (period) => {
  if (!period) return null;
  const [mon, yy] = period.split('-');
  const monthMap = {
    Jan: 0, Feb: 1, Mar: 2, Apr: 3, May: 4, Jun: 5,
    Jul: 6, Aug: 7, Sep: 8, Oct: 9, Nov: 10, Dec: 11
  };
  return new Date(2000 + parseInt(yy, 10), monthMap[mon], 1);
};

const formatPeriod = (date) => {
  const mon = date.toLocaleString('en-US', { month: 'short' });
  return `${mon}-${date.getFullYear().toString().slice(-2)}`;
};

const nextPeriod = (period) => {
  const date = periodToDate(period);
  if (!date || Number.isNaN(date.getTime())) return null;
  date.setMonth(date.getMonth() + 1);
  return formatPeriod(date);
};

const comparePeriods = (a, b) => {
  const dateA = periodToDate(a);
  const dateB = periodToDate(b);
  if (!dateA && !dateB) return 0;
  if (!dateA) return -1;
  if (!dateB) return 1;
  return dateA - dateB;
};

const formatLineList = (lines) => {
  if (!lines.length) return '';
  if (lines.length === 1) return `line ${lines[0]}`;
  if (lines.length === 2) return `lines ${lines[0]} and ${lines[1]}`;
  return `lines ${lines.slice(0, -1).join(', ')}, and ${lines[lines.length - 1]}`;
};

const formatPercentage = (value) =>
  `${(parseFloat(value) || 0).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%`;

const roundAllocationAmount = (value) => Math.round((parseFloat(value) || 0) * 100) / 100;

const calculateAllocationPercentage = (allocationAmount, periodAmount) => {
  const total = parseFloat(periodAmount) || 0;
  if (total <= 0) return 0;
  return Math.round((((parseFloat(allocationAmount) || 0) / total) * 100) * 10000) / 10000;
};

const calculateAllocationAmount = (allocationPercentage, periodAmount) => {
  const total = parseFloat(periodAmount) || 0;
  if (total <= 0) return 0;
  return roundAllocationAmount(((parseFloat(allocationPercentage) || 0) / 100) * total);
};

const getLatestCompensationAmount = (rows = []) => {
  const validRows = (rows || []).filter((row) =>
    row?.start_period && row?.amount !== '' && row?.amount !== null && row?.amount !== undefined
  );
  if (!validRows.length) return 0;
  return parseFloat(validRows[validRows.length - 1].amount) || 0;
};

const getStoredAllocationAmount = (allocation = {}, periodAmount = 0) => {
  if (allocation?.allocation_amount !== undefined && allocation?.allocation_amount !== null && allocation?.allocation_amount !== '') {
    return roundAllocationAmount(allocation.allocation_amount);
  }
  return calculateAllocationAmount(allocation?.allocation_percentage, periodAmount);
};

const inferAllocationMode = (allocations = []) =>
  (allocations || []).some((allocation) => allocation?.allocation_method === 'manual')
    ? 'manual'
    : 'percentage';

const syncLiveAllocationForMode = (allocation = {}, mode = 'percentage', periodAmount = 0) => {
  if (mode === 'manual') {
    const allocationAmount = getStoredAllocationAmount(allocation, periodAmount);
    return {
      ...allocation,
      allocation_method: 'manual',
      allocation_amount: allocationAmount,
      allocation_percentage: calculateAllocationPercentage(allocationAmount, periodAmount)
    };
  }

  const allocationPercentage = parseFloat(allocation?.allocation_percentage) || 0;
  return {
    ...allocation,
    allocation_method: 'percentage',
    allocation_percentage: allocationPercentage,
    allocation_amount: calculateAllocationAmount(allocationPercentage, periodAmount)
  };
};

const syncLiveAllocationsForMode = (allocations = [], mode = 'percentage', periodAmount = 0) =>
  (allocations || []).map((allocation) => syncLiveAllocationForMode(allocation, mode, periodAmount));

const syncHistoryDraftPercentages = (draft = {}) => {
  const periodAmount = parseFloat(draft.amount) || 0;
  return {
    ...draft,
    allocations: (draft.allocations || []).map((allocation) => ({
      ...allocation,
      allocation_percentage: calculateAllocationPercentage(allocation.allocation_amount, periodAmount)
    }))
  };
};

const getAllocationDuplicateState = (allocations = []) => {
  const groups = new Map();

  allocations.forEach((allocation, index) => {
    if (!allocation?.client_id) return;
    const key = `${allocation.client_id}||${allocation.expense_head_id || ''}`;
    const existing = groups.get(key) || [];
    existing.push(index);
    groups.set(key, existing);
  });

  const rowErrors = {};
  const duplicateGroups = [];

  groups.forEach((indexes) => {
    if (indexes.length < 2) return;
    const lines = indexes.map((index) => index + 1);
    duplicateGroups.push(lines);

    indexes.forEach((index) => {
      const otherLines = indexes
        .filter((otherIndex) => otherIndex !== index)
        .map((otherIndex) => otherIndex + 1);

      rowErrors[index] = `Same group + expense head is already selected on ${formatLineList(otherLines)}.`;
    });
  });

  return {
    rowErrors,
    hasDuplicates: duplicateGroups.length > 0,
    summary: duplicateGroups.length
      ? `Duplicate group + expense head combinations found on ${duplicateGroups.map((lines) => formatLineList(lines)).join('; ')}.`
      : ''
  };
};

const Teams = () => {
  const [teams, setTeams] = useState([]);
  const [clients, setClients] = useState([]);
  const [servicePeriods, setServicePeriods] = useState([]);
  const [expenseHeads, setExpenseHeads] = useState([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [statusFilter, setStatusFilter] = useState('active');
  const [dialogOpen, setDialogOpen] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [toggleConfirmOpen, setToggleConfirmOpen] = useState(false);
  const [historyDialogOpen, setHistoryDialogOpen] = useState(false);
  const [deleteDeps, setDeleteDeps] = useState([]);
  const [deleteMessage, setDeleteMessage] = useState('');
  const [depsLoading, setDepsLoading] = useState(false);
  const [selected, setSelected] = useState(null);
  const [formData, setFormData] = useState({ 
    name: '', mobile: '', email: '', is_reviewer: false, is_admin: false,
    client_allocations: [], compensation_rows: [{ start_period: '', end_period: '', amount: '' }]
  });
  const [historyDrafts, setHistoryDrafts] = useState([]);
  const [historySavingKey, setHistorySavingKey] = useState('');
  const [allocationMode, setAllocationMode] = useState('percentage');
  const [liveStartMinPeriod, setLiveStartMinPeriod] = useState(null);
  const [error, setError] = useState('');

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const [teamRes, clientRes, periodRes, expenseHeadRes] = await Promise.all([
        teamAPI.getAll({ includeInactive: true }),
        clientAPI.getAll(),
        servicePeriodAPI.getAll(),
        expenseHeadAPI.getAll()
      ]);
      setTeams(teamRes.data.data);
      setClients(clientRes.data.data);
      setServicePeriods(periodRes.data.data);
      setExpenseHeads(expenseHeadRes.data.data || []);
    } catch (err) {
      toast.error('Failed to load data');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadData(); }, [loadData]);

  const getLiveStartMinPeriod = (rows) => {
    if (!rows || rows.length <= 1) return null;
    const priorRow = rows[rows.length - 2];
    return nextPeriod(priorRow.end_period || priorRow.start_period);
  };

  const buildTeamFormState = (data = {}) => {
    const compensationRows = data.compensation_rows?.length
      ? data.compensation_rows
      : [{ start_period: '', end_period: '', amount: '' }];
    const liveAmount = getLatestCompensationAmount(compensationRows);
    const nextAllocationMode = inferAllocationMode(data.client_allocations || []);

    return {
      name: data.name || '',
      mobile: data.mobile || '',
      email: data.email || '',
      is_reviewer: data.is_reviewer || false,
      is_admin: data.is_admin || false,
      client_allocations: syncLiveAllocationsForMode(data.client_allocations || [], nextAllocationMode, liveAmount),
      compensation_rows: compensationRows
    };
  };

  const buildHistoryDrafts = (compensationRows = []) =>
    (compensationRows || [])
      .slice(0, -1)
      .map((row) => ({
        start_period: row.start_period,
        end_period: row.end_period || '',
        amount: row.amount ?? '',
        allocations: (row.client_allocations || []).map((allocation) => ({
          client_id: allocation.client_id || '',
          client_name: allocation.client_name || '',
          expense_head_id: allocation.expense_head_id || '',
          expense_head_name: allocation.expense_head_name || '',
          allocation_amount: Number(allocation.amount || allocation.allocation_amount || 0),
          allocation_percentage: Number(allocation.allocation_percentage || 0)
        }))
      }));

  const applyTeamDataToForm = (data = {}) => {
    const nextFormData = buildTeamFormState(data);
    setSelected(data);
    setFormData(nextFormData);
    setHistoryDrafts(buildHistoryDrafts(nextFormData.compensation_rows));
    setLiveStartMinPeriod(getLiveStartMinPeriod(nextFormData.compensation_rows));
    setAllocationMode(inferAllocationMode(data.client_allocations || []));
  };

  const handleOpenDialog = async (item = null) => {
    if (item) {
      setSelected(item);
      // Load full team data with allocations
      try {
        const res = await teamAPI.getById(item.id);
        applyTeamDataToForm(res.data.data);
      } catch (err) {
        applyTeamDataToForm(item);
      }
    } else {
      setSelected(null);
      setFormData({
        name: '', mobile: '', email: '', is_reviewer: false, is_admin: false,
        client_allocations: [], compensation_rows: [{ start_period: '', end_period: '', amount: '' }]
      });
      setHistoryDrafts([]);
      setLiveStartMinPeriod(null);
      setAllocationMode('percentage');
    }
    setError('');
    setDialogOpen(true);
  };

  const handleAllocationChange = (index, field, value) => {
    setError('');
    setFormData((prev) => {
      const periodAmount = getLatestCompensationAmount(prev.compensation_rows);
      const nextAllocations = [...prev.client_allocations];
      const nextAllocation = { ...nextAllocations[index], [field]: value, allocation_method: allocationMode };
      nextAllocations[index] = (field === 'allocation_amount' || field === 'allocation_percentage')
        ? syncLiveAllocationForMode(nextAllocation, allocationMode, periodAmount)
        : nextAllocation;
      return { ...prev, client_allocations: nextAllocations };
    });
  };

  const addAllocation = () => {
    setError('');
    setFormData(p => ({ ...p, client_allocations: [...p.client_allocations, {
      client_id: '',
      allocation_percentage: 0,
      allocation_amount: 0,
      allocation_method: allocationMode,
      expense_head_id: ''
    }] }));
  };

  const removeAllocation = (index) => {
    setError('');
    setFormData(p => ({ ...p, client_allocations: p.client_allocations.filter((_, i) => i !== index) }));
  };

  const handleOpenHistory = () => {
    setHistoryDrafts(buildHistoryDrafts(formData.compensation_rows));
    setError('');
    setHistoryDialogOpen(true);
  };

  const getHistoryDraftKey = (draft) => `${draft?.start_period || ''}||${draft?.end_period || ''}`;

  const handleHistoryDraftChange = (draftIndex, field, value) => {
    setError('');
    setHistoryDrafts((prev) => prev.map((draft, index) => (
      index === draftIndex
        ? (field === 'amount'
          ? syncHistoryDraftPercentages({ ...draft, [field]: value })
          : { ...draft, [field]: value })
        : draft
    )));
  };

  const handleHistoryAllocationChange = (draftIndex, allocationIndex, field, value) => {
    setError('');
    setHistoryDrafts((prev) => prev.map((draft, index) => {
      if (index !== draftIndex) return draft;
      const nextDraft = {
        ...draft,
        allocations: draft.allocations.map((allocation, currentAllocationIndex) => (
          currentAllocationIndex === allocationIndex ? { ...allocation, [field]: value } : allocation
        ))
      };
      return field === 'allocation_amount' ? syncHistoryDraftPercentages(nextDraft) : nextDraft;
    }));
  };

  const addHistoryAllocation = (draftIndex) => {
    setError('');
    setHistoryDrafts((prev) => prev.map((draft, index) => (
      index === draftIndex
        ? {
            ...draft,
            allocations: [...draft.allocations, {
              client_id: '',
              client_name: '',
              expense_head_id: '',
              expense_head_name: '',
              allocation_amount: 0,
              allocation_percentage: 0
            }]
          }
        : draft
    )));
  };

  const removeHistoryAllocation = (draftIndex, allocationIndex) => {
    setError('');
    setHistoryDrafts((prev) => prev.map((draft, index) => (
      index === draftIndex
        ? syncHistoryDraftPercentages({
            ...draft,
            allocations: draft.allocations.filter((_, currentAllocationIndex) => currentAllocationIndex !== allocationIndex)
          })
        : draft
    )));
  };

  const getHistoryAllocationTotal = (allocations = []) =>
    allocations.reduce((sum, allocation) => sum + (parseFloat(allocation.allocation_amount) || 0), 0);

  const getHistoryAllocationPercentageTotal = (allocations = []) =>
    allocations.reduce((sum, allocation) => sum + (parseFloat(allocation.allocation_percentage) || 0), 0);

  const handleAllocationModeChange = (_, value) => {
    if (!value || value === allocationMode) return;
    setError('');
    setFormData((prev) => ({
      ...prev,
      client_allocations: syncLiveAllocationsForMode(prev.client_allocations, value, getLatestCompensationAmount(prev.compensation_rows))
    }));
    setAllocationMode(value);
  };

  const refreshSelectedTeam = async (teamId) => {
    const res = await teamAPI.getById(teamId);
    applyTeamDataToForm(res.data.data);
    return res.data.data;
  };

  const handleSaveHistoryPeriod = async (draftIndex) => {
    if (!selected?.id) return;

    const draft = historyDrafts[draftIndex];
    const amount = parseFloat(draft?.amount);
    const periodLabel = `${draft?.start_period || '-'} to ${draft?.end_period || 'Open'}`;

    if (draft?.amount === '' || Number.isNaN(amount) || amount < 0) {
      setError(`Enter a valid amount for ${periodLabel}`);
      return;
    }

    const cleanedAllocations = (draft.allocations || []).filter((allocation) => allocation?.client_id);
    const duplicateState = getAllocationDuplicateState(cleanedAllocations);
    if (duplicateState.hasDuplicates) {
      setError(`${periodLabel}: ${duplicateState.summary}`);
      return;
    }

    const totalAllocation = getHistoryAllocationTotal(cleanedAllocations);
    if (cleanedAllocations.length > 0) {
      if (amount <= 0) {
        setError(`Enter a positive amount for ${periodLabel} before saving allocations`);
        return;
      }
      if (Math.abs(totalAllocation - amount) > 0.01) {
        setError(`Allocation total for ${periodLabel} must match INR ${amount.toFixed(2)} (current: INR ${totalAllocation.toFixed(2)})`);
        return;
      }
    }

    const payloadAllocations = cleanedAllocations.map((allocation) => ({
      client_id: allocation.client_id,
      expense_head_id: allocation.expense_head_id || undefined,
      allocation_amount: parseFloat(allocation.allocation_amount) || 0,
      allocation_percentage: parseFloat(allocation.allocation_percentage) || calculateAllocationPercentage(allocation.allocation_amount, amount),
      allocation_method: 'manual'
    }));

    const savingKey = getHistoryDraftKey(draft);
    setHistorySavingKey(savingKey);
    setError('');

    try {
      await teamAPI.updateCompensationPeriod(selected.id, {
        original_start_period: draft.start_period,
        original_end_period: draft.end_period || undefined,
        amount,
        client_allocations: payloadAllocations
      });
      await refreshSelectedTeam(selected.id);
      await loadData();
      toast.success(`Updated ${periodLabel}`);
    } catch (err) {
      setError(
        err.response?.data?.message ||
        err.response?.data?.errors?.[0]?.msg ||
        `Failed to update ${periodLabel}`
      );
    } finally {
      setHistorySavingKey('');
    }
  };

  const getTotalAllocation = () => {
    if (allocationMode === 'manual') {
      return formData.client_allocations.reduce((sum, a) => sum + (parseFloat(a.allocation_amount) || 0), 0);
    }
    return formData.client_allocations.reduce((sum, a) => sum + (parseFloat(a.allocation_percentage) || 0), 0);
  };

  const currentCompensationAmount = getLatestCompensationAmount(formData.compensation_rows);
  const liveCompensationRow = formData.compensation_rows[formData.compensation_rows.length - 1] || { start_period: '', end_period: '', amount: '' };
  const historicCompensationRows = formData.compensation_rows.slice(0, -1);
  const liveStartOptions = servicePeriods.filter((sp) => {
    const period = sp.display_name || sp.name;
    return !liveStartMinPeriod || comparePeriods(period, liveStartMinPeriod) >= 0;
  });
  const liveEndOptions = servicePeriods.filter((sp) => {
    const period = sp.display_name || sp.name;
    return !liveCompensationRow.start_period || comparePeriods(period, liveCompensationRow.start_period) >= 0;
  });
  const allocationDuplicateState = getAllocationDuplicateState(formData.client_allocations);

  const handleLiveCompensationChange = (field, value) => {
    setError('');
    setFormData((prev) => {
      const updatedRows = [...prev.compensation_rows];
      updatedRows[updatedRows.length - 1] = { ...updatedRows[updatedRows.length - 1], [field]: value };
      const nextAmount = getLatestCompensationAmount(updatedRows);
      return {
        ...prev,
        compensation_rows: updatedRows,
        client_allocations: field === 'amount'
          ? syncLiveAllocationsForMode(prev.client_allocations, allocationMode, nextAmount)
          : prev.client_allocations
      };
    });
  };

  const addCompensationRow = () => {
    const rows = [...formData.compensation_rows];
    const lastRow = rows[rows.length - 1];
    if (!lastRow?.start_period || lastRow?.amount === '' || lastRow?.amount === null || lastRow?.amount === undefined) {
      setError('Complete the current compensation row before adding the next one');
      return;
    }
    if (!lastRow?.end_period) {
      setError('Set the end period for the current row before adding the next period');
      return;
    }
    setFormData((prev) => ({
      ...prev,
      compensation_rows: [...prev.compensation_rows, { start_period: '', end_period: '', amount: '' }],
      client_allocations: []
    }));
    setLiveStartMinPeriod(nextPeriod(lastRow.end_period));
  };

  const removeCompensationRow = (index) => {
    const updated = formData.compensation_rows.filter((_, i) => i !== index);
    if (updated.length === 0) {
      updated.push({ start_period: '', end_period: '', amount: '' });
    }
    if (index > 0 && updated[index - 1]) {
      updated[index - 1] = { ...updated[index - 1], end_period: '' };
    }
    setFormData((prev) => ({ ...prev, compensation_rows: updated }));
    setLiveStartMinPeriod(getLiveStartMinPeriod(updated));
  };

  const handleSubmit = async () => {
    if (!formData.name.trim()) { setError('Name is required'); return; }
    if (formData.mobile && !/^[0-9]{10}$/.test(formData.mobile)) { setError('Invalid mobile number'); return; }
    if (formData.email && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(formData.email)) { setError('Invalid email format'); return; }

    const populatedCompensationRows = formData.compensation_rows.filter((row) => {
      const hasAmount = row?.amount !== '' && row?.amount !== null && row?.amount !== undefined;
      return row?.start_period || row?.end_period || hasAmount;
    });

    if (populatedCompensationRows.length === 0) {
      setError('At least one compensation row is required');
      return;
    }

    for (let index = 0; index < populatedCompensationRows.length; index += 1) {
      const row = populatedCompensationRows[index];
      const rowLabel = index === populatedCompensationRows.length - 1
        ? 'live compensation row'
        : `compensation row #${index + 1}`;

      if (!row.start_period) {
        setError(`Start period is required for ${rowLabel}`);
        return;
      }
      if (row.amount === '' || row.amount === null || row.amount === undefined) {
        setError(`Amount is required for ${rowLabel}`);
        return;
      }
      if (row.end_period && comparePeriods(row.end_period, row.start_period) < 0) {
        setError(`End period cannot be before start period for ${rowLabel}`);
        return;
      }
      if (index < populatedCompensationRows.length - 1 && !row.end_period) {
        setError(`Set the end period for ${rowLabel} before adding a later period`);
        return;
      }
    }

    const cleanedAllocations = formData.client_allocations
      .filter((alloc) => alloc?.client_id)
      .map((alloc) => ({
        client_id: alloc.client_id,
        expense_head_id: alloc.expense_head_id || undefined,
        allocation_percentage: allocationMode === 'manual'
          ? calculateAllocationPercentage(alloc.allocation_amount, currentCompensationAmount)
          : (parseFloat(alloc.allocation_percentage) || 0),
        allocation_amount: allocationMode === 'manual'
          ? roundAllocationAmount(alloc.allocation_amount)
          : calculateAllocationAmount(alloc.allocation_percentage, currentCompensationAmount),
        allocation_method: allocationMode
      }));

    if (cleanedAllocations.length > 0) {
      if (allocationDuplicateState.hasDuplicates) {
        setError(allocationDuplicateState.summary);
        return;
      }
      if (allocationMode === 'manual') {
        if (currentCompensationAmount <= 0) { setError('Enter a valid current compensation amount before manual group allocation'); return; }
        const total = formData.client_allocations.reduce((sum, alloc) => sum + (parseFloat(alloc.allocation_amount) || 0), 0);
        if (Math.abs(total - currentCompensationAmount) > 0.01) { setError(`Group allocations must total INR ${currentCompensationAmount.toFixed(2)} (current: INR ${total.toFixed(2)})`); return; }
      } else {
        const total = cleanedAllocations.reduce((sum, alloc) => sum + alloc.allocation_percentage, 0);
        if (Math.abs(total - 100) > 0.01) { setError(`Group allocations must total 100% (current: ${total}%)`); return; }
      }
    }

    const cleanedRows = populatedCompensationRows
      .map((row) => ({
        start_period: row.start_period,
        end_period: row.end_period || undefined,
        amount: parseFloat(row.amount) || 0
      }));

    try {
      const payload = {
        name: formData.name.trim(),
        mobile: formData.mobile.trim() || undefined,
        email: formData.email.trim() || undefined,
        is_reviewer: formData.is_reviewer,
        is_admin: formData.is_admin,
        client_allocations: cleanedAllocations,
        compensation_rows: cleanedRows
      };

      if (selected) {
        await teamAPI.update(selected.id, payload);
        toast.success('Team updated successfully');
      } else {
        await teamAPI.create(payload);
        toast.success('Team created successfully');
      }
      setDialogOpen(false);
      loadData();
    } catch (err) {
      setError(
        err.response?.data?.message ||
        err.response?.data?.errors?.[0]?.msg ||
        'Operation failed'
      );
    }
  };

  const handleOpenDeleteDialog = async (team) => {
    setSelected(team);
    setDeleteDeps([]);
    setDeleteMessage('');
    setDepsLoading(true);
    setDeleteDialogOpen(true);
    try {
      const res = await teamAPI.checkDependencies(team.id);
      setDeleteDeps(res.data?.data?.dependencies || []);
      setDeleteMessage(res.data?.data?.message || '');
    } catch {
      setDeleteDeps([]);
      setDeleteMessage('');
    }
    finally { setDepsLoading(false); }
  };

  const handleDelete = async () => {
    try {
      await teamAPI.delete(selected.id);
      toast.success('Team deleted successfully');
      setDeleteDialogOpen(false);
      setDeleteDeps([]);
      setDeleteMessage('');
      loadData();
    } catch (err) {
      if (err.response?.status === 409) {
        setDeleteDeps(err.response?.data?.data?.dependencies || []);
        setDeleteMessage(err.response?.data?.message || err.response?.data?.data?.message || 'Delete is blocked because this team is still linked elsewhere.');
      }
      toast.error(err.response?.data?.message || 'Failed to delete');
    }
  };

  const deleteLinkedItems = flattenDependencyItems(deleteDeps);

  const handleToggleActive = async () => {
    if (!selected) return;
    try {
      await teamAPI.update(selected.id, { is_active: !selected.is_active });
      toast.success(`${selected.name} ${selected.is_active ? 'deactivated' : 'activated'} successfully`);
      setToggleConfirmOpen(false);
      loadData();
    } catch (err) {
      toast.error(err.response?.data?.message || 'Failed to update status');
      setToggleConfirmOpen(false);
    }
  };

  const filteredData = teams.filter(t => {
    if (!t.name.toLowerCase().includes(searchTerm.toLowerCase())) return false;
    if (statusFilter === 'active') return t.is_active;
    if (statusFilter === 'inactive') return !t.is_active;
    return true;
  });
  const totalCompensation = teams.reduce((sum, team) => {
    const latest = team.compensation_history?.[team.compensation_history.length - 1];
    return sum + (Number(latest?.amount || 0));
  }, 0);
  const reviewerCount = teams.filter((team) => team.is_reviewer).length;
  const columns = [
    { field: 'name', headerName: 'Name', flex: 1, minWidth: 150 },
    { field: 'client_allocations', headerName: 'Groups', width: 200, renderCell: (p) => (p.value || []).map(a => a.client_name).filter(Boolean).join(', ') || '-' },
    { field: 'amount', headerName: 'Current Amount', width: 140, renderCell: (p) => p.row.compensation_history?.length ? `INR ${Number(p.row.compensation_history[p.row.compensation_history.length - 1].amount).toLocaleString('en-IN', { minimumFractionDigits: 2 })}` : '-' },
    { field: 'start_period', headerName: 'From', width: 90, renderCell: (p) => p.row.compensation_history?.length ? p.row.compensation_history[0].from_period : (p.value || '-') },
    { field: 'end_period', headerName: 'To', width: 90, renderCell: (p) => p.row.compensation_history?.length ? (p.row.compensation_history[p.row.compensation_history.length - 1].to_period || 'Open') : (p.value || '-') },
    { field: 'mobile', headerName: 'Mobile', width: 120, renderCell: (p) => p.value || '-' },
    { field: 'email', headerName: 'Email', flex: 1, minWidth: 180, renderCell: (p) => p.value || '-' },
    {
      field: 'is_active',
      headerName: 'Status',
      width: 110,
      renderCell: (p) => <Chip label={p.value ? 'Active' : 'Inactive'} color={p.value ? 'success' : 'default'} size="small" />
    },
    { field: 'is_reviewer', headerName: 'Reviewer', width: 100, renderCell: (p) => <Chip label={p.value ? 'Yes' : 'No'} color={p.value ? 'primary' : 'default'} size="small" /> },
    { field: 'is_admin', headerName: 'Admin', width: 90, renderCell: (p) => p.value ? <Chip label="Yes" color="warning" size="small" /> : null },
    { field: 'actions', headerName: 'Actions', width: 160, sortable: false, renderCell: (params) => (
      <Box>
        <IconButton size="small" onClick={() => handleOpenDialog(params.row)} color="primary"><Edit fontSize="small" /></IconButton>
        <IconButton size="small" onClick={() => { setSelected(params.row); setToggleConfirmOpen(true); }} color={params.row.is_active ? 'error' : 'success'}>
          {params.row.is_active ? <Cancel fontSize="small" /> : <CheckCircle fontSize="small" />}
        </IconButton>
        <IconButton size="small" onClick={() => handleOpenDeleteDialog(params.row)} color="error"><Delete fontSize="small" /></IconButton>
      </Box>
    )},
  ];

  return (
    <Box>
      <PageHeader
        eyebrow="Team Master"
        title="Team allocation control"
        actions={[
          <HoverActionButton key="add" icon={<Add fontSize="small" />} label="Add team" onClick={() => handleOpenDialog()} tone="mint" />,
          <HoverActionButton key="refresh" icon={<Refresh fontSize="small" />} label="Refresh" onClick={loadData} tone="peach" />,
        ]}
      />

      <Grid container spacing={2} sx={{ mb: 2.5 }}>
        <Grid item xs={12} md={4}>
          <MetricCard eyebrow="Total" title="Team members" value={String(teams.length)} tone="mint" />
        </Grid>
        <Grid item xs={12} md={4}>
          <MetricCard eyebrow="Reviewers" title="Marked reviewers" value={String(reviewerCount)} tone="peach" />
        </Grid>
        <Grid item xs={12} md={4}>
          <MetricCard eyebrow="Compensation" title="Current live amount" value={`INR ${totalCompensation.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`} tone="sand" />
        </Grid>
      </Grid>

      <SectionCard title="Team ledger" tone="sage" contentSx={{ p: 2 }}>
          <Box sx={{ display: 'flex', gap: 2, mb: 2, alignItems: 'center' }}>
            <TextField size="small" placeholder="Search..." value={searchTerm} onChange={(e) => setSearchTerm(e.target.value)} InputProps={{ startAdornment: <Search sx={{ mr: 1, color: 'text.secondary' }} /> }} sx={{ width: 300 }} />
            <ToggleButtonGroup value={statusFilter} exclusive size="small" onChange={(_, v) => v && setStatusFilter(v)}>
              <ToggleButton value="active">Active</ToggleButton>
              <ToggleButton value="inactive">Inactive</ToggleButton>
              <ToggleButton value="all">All</ToggleButton>
            </ToggleButtonGroup>
          </Box>
          <DataGrid
            rows={filteredData}
            columns={columns}
            pageSize={10}
            rowsPerPageOptions={[10, 25, 50]}
            autoHeight
            loading={loading}
            disableSelectionOnClick
            getRowClassName={(params) => !params.row.is_active ? 'row-inactive' : ''}
            sx={{
              border: 'none',
              '& .row-inactive': {
                bgcolor: 'grey.100',
                color: 'text.disabled',
                fontStyle: 'italic',
                opacity: 0.7,
              },
            }}
          />
      </SectionCard>

      <Dialog open={dialogOpen} onClose={() => setDialogOpen(false)} maxWidth="md" fullWidth PaperProps={{ sx: { borderRadius: '16px' } }}>
        <DialogTitle>{selected ? 'Edit Team' : 'Add Team'}</DialogTitle>
        <DialogContent>
          {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
          <Grid container spacing={2}>
            <Grid item xs={12} md={6}>
              <TextField fullWidth label="Name" value={formData.name} onChange={(e) => setFormData(p => ({ ...p, name: e.target.value }))} margin="normal" required />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField fullWidth label="Mobile" value={formData.mobile} onChange={(e) => setFormData(p => ({ ...p, mobile: e.target.value }))} margin="normal" inputProps={{ maxLength: 10 }} />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField fullWidth label="Email" type="email" value={formData.email} onChange={(e) => setFormData(p => ({ ...p, email: e.target.value }))} margin="normal" />
            </Grid>
            <Grid item xs={12} md={3}>
              <FormControlLabel control={<Checkbox checked={formData.is_reviewer} onChange={(e) => setFormData(p => ({ ...p, is_reviewer: e.target.checked }))} />} label="Is Reviewer" sx={{ mt: 2 }} />
            </Grid>
            <Grid item xs={12} md={3}>
              <FormControlLabel control={<Checkbox checked={formData.is_admin} onChange={(e) => setFormData(p => ({ ...p, is_admin: e.target.checked }))} />} label="Admin" sx={{ mt: 2 }} />
            </Grid>
          </Grid>

          <Box sx={{ mt: 3 }}>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
              <Typography variant="subtitle1">Live Compensation</Typography>
              <Box sx={{ display: 'flex', gap: 2, alignItems: 'center' }}>
                <Link
                  component="button"
                  type="button"
                  underline="hover"
                  onClick={handleOpenHistory}
                  disabled={historicCompensationRows.length === 0}
                >
                  History
                </Link>
                <Button
                  size="small"
                  startIcon={<AddCircle />}
                  onClick={addCompensationRow}
                  disabled={!liveCompensationRow.start_period || !liveCompensationRow.end_period || liveCompensationRow.amount === '' || liveCompensationRow.amount === null || liveCompensationRow.amount === undefined}
                >
                  Add Next Period
                </Button>
              </Box>
            </Box>
            <Grid container spacing={2}>
              <Grid item xs={12} md={4}>
                <FormControl fullWidth size="small">
                  <InputLabel>Start Period</InputLabel>
                  <Select value={liveCompensationRow.start_period || ''} label="Start Period" onChange={(e) => handleLiveCompensationChange('start_period', e.target.value)}>
                    <MenuItem value="">Select</MenuItem>
                    {liveStartOptions.map((sp) => <MenuItem key={sp.id} value={sp.display_name || sp.name}>{sp.display_name || sp.name}</MenuItem>)}
                  </Select>
                </FormControl>
              </Grid>
              <Grid item xs={12} md={4}>
                <FormControl fullWidth size="small">
                  <InputLabel>End Period</InputLabel>
                  <Select value={liveCompensationRow.end_period || ''} label="End Period" onChange={(e) => handleLiveCompensationChange('end_period', e.target.value)}>
                    <MenuItem value="">Live / Open</MenuItem>
                    {liveEndOptions.map((sp) => <MenuItem key={sp.id} value={sp.display_name || sp.name}>{sp.display_name || sp.name}</MenuItem>)}
                  </Select>
                </FormControl>
              </Grid>
              <Grid item xs={12} md={3}>
                <TextField
                  fullWidth
                  size="small"
                  label="Amount"
                  type="number"
                  value={liveCompensationRow.amount}
                  onChange={(e) => handleLiveCompensationChange('amount', e.target.value)}
                  inputProps={{ min: 0, step: 0.01 }}
                />
              </Grid>
              <Grid item xs={12} md={1} sx={{ display: 'flex', alignItems: 'center' }}>
                <IconButton size="small" onClick={() => removeCompensationRow(formData.compensation_rows.length - 1)} color="error">
                  <RemoveCircle fontSize="small" />
                </IconButton>
              </Grid>
            </Grid>
          </Box>

          <Box sx={{ mt: 3 }}>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
              <Typography variant="subtitle1">Group Allocations</Typography>
              <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
                <ToggleButtonGroup
                  value={allocationMode}
                  exclusive
                  size="small"
                  onChange={handleAllocationModeChange}
                >
                  <ToggleButton value="percentage">Percentage</ToggleButton>
                  <ToggleButton value="manual">Manual</ToggleButton>
                </ToggleButtonGroup>
                <Button size="small" startIcon={<AddCircle />} onClick={addAllocation}>Add More Group</Button>
              </Box>
            </Box>
            {allocationDuplicateState.hasDuplicates && (
              <Alert severity="error" sx={{ mb: 1.5 }}>
                {allocationDuplicateState.summary}
              </Alert>
            )}
            {formData.client_allocations.length > 0 ? (
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell width={72}>Line</TableCell>
                    <TableCell>Group</TableCell>
                    <TableCell width={170}>Expense Head</TableCell>
                    <TableCell width={140}>{allocationMode === 'manual' ? 'Alloc. Amount' : 'Alloc. %'}</TableCell>
                    <TableCell width={50}></TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {formData.client_allocations.map((alloc, idx) => {
                    const availableClients = clients.filter((c) => c.is_active !== false);
                    const rowError = allocationDuplicateState.rowErrors[idx] || '';
                    return (
                    <TableRow
                      key={idx}
                      sx={rowError ? { bgcolor: 'rgba(211, 47, 47, 0.04)' } : undefined}
                    >
                      <TableCell>
                        <Chip
                          label={idx + 1}
                          size="small"
                          color={rowError ? 'error' : 'default'}
                          variant={rowError ? 'filled' : 'outlined'}
                        />
                      </TableCell>
                      <TableCell>
                        <FormControl fullWidth size="small" error={Boolean(rowError)}>
                          <Select value={alloc.client_id} onChange={(e) => handleAllocationChange(idx, 'client_id', e.target.value)}>
                            {alloc.client_id && !availableClients.find(c => c.id === alloc.client_id) && (
                              <MenuItem value={alloc.client_id}>{clients.find(c => c.id === alloc.client_id)?.name || alloc.client_id}</MenuItem>
                            )}
                            {availableClients.map(c => <MenuItem key={c.id} value={c.id}>{c.name}</MenuItem>)}
                          </Select>
                        </FormControl>
                      </TableCell>
                      <TableCell>
                        <FormControl fullWidth size="small" error={Boolean(rowError)}>
                          <Select value={alloc.expense_head_id || ''} onChange={(e) => handleAllocationChange(idx, 'expense_head_id', e.target.value)}>
                            <MenuItem value="">None</MenuItem>
                            {expenseHeads.filter(eh => eh.is_active !== false).map((eh) => (
                              <MenuItem key={eh.id} value={eh.id}>{eh.name}</MenuItem>
                            ))}
                          </Select>
                          {rowError && <FormHelperText>{rowError}</FormHelperText>}
                        </FormControl>
                      </TableCell>
                      <TableCell>
                        <TextField
                          size="small"
                          type="number"
                          value={allocationMode === 'manual' ? (alloc.allocation_amount ?? 0) : alloc.allocation_percentage}
                          onChange={(e) => handleAllocationChange(
                            idx,
                            allocationMode === 'manual' ? 'allocation_amount' : 'allocation_percentage',
                            parseFloat(e.target.value) || 0
                          )}
                          inputProps={allocationMode === 'manual' ? { min: 0, step: 0.01 } : { min: 0, max: 100, step: 0.5 }}
                        />
                      </TableCell>
                      <TableCell>
                        <IconButton size="small" onClick={() => removeAllocation(idx)} color="error"><RemoveCircle fontSize="small" /></IconButton>
                      </TableCell>
                    </TableRow>
                    );
                  })}
                  <TableRow>
                    <TableCell colSpan={3}><strong>Total</strong></TableCell>
                    <TableCell>
                      <Chip
                        label={allocationMode === 'manual' ? `INR ${getTotalAllocation().toFixed(2)}` : `${getTotalAllocation()}%`}
                        color={
                          allocationMode === 'manual'
                            ? (Math.abs(getTotalAllocation() - currentCompensationAmount) < 0.01 ? 'success' : 'error')
                            : (Math.abs(getTotalAllocation() - 100) < 0.01 ? 'success' : 'error')
                        }
                        size="small"
                      />
                    </TableCell>
                    <TableCell></TableCell>
                  </TableRow>
                </TableBody>
              </Table>
            ) : (
              <Typography variant="body2" color="text.secondary">No group allocations defined. Click "Add More Group" to add allocations.</Typography>
            )}
            {allocationMode === 'manual' && (
              <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                Manual allocation uses the latest compensation amount as base: INR {currentCompensationAmount.toFixed(2)}.
              </Typography>
            )}
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDialogOpen(false)} sx={{ textTransform: 'none' }}>Cancel</Button>
          <Button variant="contained" onClick={handleSubmit} sx={{ textTransform: 'none' }}>{selected ? 'Update' : 'Create'}</Button>
        </DialogActions>
      </Dialog>

      <Dialog open={deleteDialogOpen} onClose={() => { setDeleteDialogOpen(false); setDeleteDeps([]); setDeleteMessage(''); }} PaperProps={{ sx: { borderRadius: '16px' } }} maxWidth="sm" fullWidth>
        <DialogTitle sx={{ bgcolor: 'rgba(220,38,38,0.06)', borderBottom: '1px solid rgba(220,38,38,0.12)', fontWeight: 700, color: '#991B1B' }}>
          Delete Team
        </DialogTitle>
        <DialogContent sx={{ pt: 3 }}>
          {depsLoading ? (
            <Typography color="text.secondary">Checking dependencies...</Typography>
          ) : deleteDeps.length > 0 ? (
            <>
              <Alert severity="warning" sx={{ mb: 2, borderRadius: '8px' }}>
                {deleteMessage || `"${selected?.name}" is linked to records in other modules. Update those lines first, then delete this team.`}
              </Alert>
              <Typography variant="body2" sx={{ mb: 1.5 }}>
                {summarizeDependencies(deleteDeps)}
              </Typography>
              {deleteLinkedItems.map((item, idx) => (
                <Typography key={idx} variant="body2" sx={{ ml: 2, mb: 0.5 }}>{/*
                  • {dep.count} {dep.type} record(s)
                */}• {item.label} — {item.line}</Typography>
              ))}
            </>
          ) : (
            <Typography>Are you sure you want to delete "{selected?.name}"?</Typography>
          )}
        </DialogContent>
        <DialogActions sx={{ p: 2.5, borderTop: '1px solid rgba(0,0,0,0.08)' }}>
          <Button onClick={() => { setDeleteDialogOpen(false); setDeleteDeps([]); setDeleteMessage(''); }} sx={{ textTransform: 'none' }}>Cancel</Button>
          <Button variant="contained" color="error" onClick={handleDelete} disabled={depsLoading || deleteDeps.length > 0} sx={{ textTransform: 'none', boxShadow: 'none' }}>
            Delete
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={toggleConfirmOpen} onClose={() => setToggleConfirmOpen(false)} PaperProps={{ sx: { borderRadius: '16px' } }} maxWidth="xs" fullWidth>
        <DialogTitle sx={{
          bgcolor: selected?.is_active ? 'rgba(220,38,38,0.06)' : 'rgba(22,163,74,0.06)',
          borderBottom: '1px solid',
          borderColor: selected?.is_active ? 'rgba(220,38,38,0.12)' : 'rgba(22,163,74,0.12)',
          fontWeight: 700,
          color: selected?.is_active ? '#991B1B' : '#14532d'
        }}>
          {selected?.is_active ? 'Deactivate Team' : 'Activate Team'}
        </DialogTitle>
        <DialogContent sx={{ pt: 3 }}>
          <Typography variant="body2">
            {selected?.is_active
              ? `Deactivate "${selected?.name}"? It will no longer appear in selection lists.`
              : `Activate "${selected?.name}"? It will become available for selection.`}
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setToggleConfirmOpen(false)} sx={{ textTransform: 'none' }}>Cancel</Button>
          <Button variant="contained" color={selected?.is_active ? 'error' : 'success'} onClick={handleToggleActive} sx={{ textTransform: 'none', boxShadow: 'none' }}>
            {selected?.is_active ? 'Deactivate' : 'Activate'}
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={historyDialogOpen} onClose={() => setHistoryDialogOpen(false)} maxWidth="md" fullWidth PaperProps={{ sx: { borderRadius: '16px' } }}>
        <DialogTitle>Compensation by Period</DialogTitle>
        <DialogContent>
          <Alert severity="info" sx={{ mb: 2 }}>
            Closed-period amounts and allocations can be corrected here. Client, expense head, and allocation rows can be changed for the selected closed period, while future recurring line items stay independent.
          </Alert>
          {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
          {historyDrafts.length > 0 ? (
            <Box>
              {historyDrafts.map((draft, idx) => {
                const periodLabel = `${draft.start_period} to ${draft.end_period || 'Open'}`;
                const allocationTotal = getHistoryAllocationTotal(draft.allocations);
                const allocationPercentageTotal = getHistoryAllocationPercentageTotal(draft.allocations);
                const amount = parseFloat(draft.amount || 0) || 0;
                const allocationDuplicateState = getAllocationDuplicateState(draft.allocations);
                const availableClients = clients.filter((client) => client.is_active !== false);
                const activeExpenseHeads = expenseHeads.filter((expenseHead) => expenseHead.is_active !== false);
                const isSaving = historySavingKey === getHistoryDraftKey(draft);

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
                        <TextField
                          fullWidth
                          size="small"
                          label="Start Period"
                          value={draft.start_period}
                          InputProps={{ readOnly: true }}
                        />
                      </Grid>
                      <Grid item xs={12} md={3}>
                        <TextField
                          fullWidth
                          size="small"
                          label="End Period"
                          value={draft.end_period || 'Open'}
                          InputProps={{ readOnly: true }}
                        />
                      </Grid>
                      <Grid item xs={12} md={3}>
                        <TextField
                          fullWidth
                          size="small"
                          label="Amount"
                          type="number"
                          value={draft.amount ?? ''}
                          onChange={(e) => handleHistoryDraftChange(idx, 'amount', e.target.value)}
                          inputProps={{ min: 0, step: 0.01 }}
                        />
                      </Grid>
                      <Grid item xs={12} md={3}>
                        <Box sx={{ display: 'flex', justifyContent: { xs: 'flex-start', md: 'flex-end' }, gap: 1, alignItems: 'center', height: '100%' }}>
                          <Chip
                            label={`Alloc. Total: INR ${allocationTotal.toFixed(2)}`}
                            color={draft.allocations.length === 0 || Math.abs(allocationTotal - amount) < 0.01 ? 'success' : 'error'}
                            size="small"
                          />
                          <Chip
                            label={`Alloc. %: ${formatPercentage(allocationPercentageTotal)}`}
                            color={draft.allocations.length === 0 || Math.abs(allocationPercentageTotal - 100) < 0.01 ? 'success' : 'error'}
                            size="small"
                          />
                          <Button
                            variant="contained"
                            size="small"
                            onClick={() => handleSaveHistoryPeriod(idx)}
                            disabled={isSaving}
                            sx={{ textTransform: 'none' }}
                          >
                            {isSaving ? 'Saving...' : 'Save Period'}
                          </Button>
                        </Box>
                      </Grid>
                    </Grid>

                    {allocationDuplicateState.hasDuplicates && (
                      <Alert severity="error" sx={{ mb: 1.5 }}>
                        {allocationDuplicateState.summary}
                      </Alert>
                    )}

                    {draft.allocations.length > 0 ? (
                      <Table size="small">
                        <TableHead>
                          <TableRow>
                            <TableCell width={72}>Line</TableCell>
                            <TableCell>Group</TableCell>
                            <TableCell width={180}>Expense Head</TableCell>
                            <TableCell width={160}>Allocation Amount</TableCell>
                            <TableCell width={140}>Allocation %</TableCell>
                            <TableCell width={50}></TableCell>
                          </TableRow>
                        </TableHead>
                        <TableBody>
                          {draft.allocations.map((allocation, allocationIndex) => {
                            const rowError = allocationDuplicateState.rowErrors[allocationIndex] || '';
                            const selectedClient = clients.find((client) => client.id === allocation.client_id);
                            const selectedExpenseHead = expenseHeads.find((expenseHead) => expenseHead.id === allocation.expense_head_id);

                            return (
                              <TableRow
                                key={`${getHistoryDraftKey(draft)}-${allocationIndex}`}
                                sx={rowError ? { bgcolor: 'rgba(211, 47, 47, 0.04)' } : undefined}
                              >
                                <TableCell>
                                  <Chip
                                    label={allocationIndex + 1}
                                    size="small"
                                    color={rowError ? 'error' : 'default'}
                                    variant={rowError ? 'filled' : 'outlined'}
                                  />
                                </TableCell>
                                <TableCell>
                                  <FormControl fullWidth size="small" error={Boolean(rowError)}>
                                    <Select
                                      value={allocation.client_id}
                                      onChange={(e) => handleHistoryAllocationChange(idx, allocationIndex, 'client_id', e.target.value)}
                                    >
                                      {allocation.client_id && !availableClients.find((client) => client.id === allocation.client_id) && (
                                        <MenuItem value={allocation.client_id}>
                                          {selectedClient?.name || allocation.client_name || allocation.client_id}
                                        </MenuItem>
                                      )}
                                      {availableClients.map((client) => (
                                        <MenuItem key={client.id} value={client.id}>{client.name}</MenuItem>
                                      ))}
                                    </Select>
                                    {rowError && <FormHelperText>{rowError}</FormHelperText>}
                                  </FormControl>
                                </TableCell>
                                <TableCell>
                                  <FormControl fullWidth size="small" error={Boolean(rowError)}>
                                    <Select
                                      value={allocation.expense_head_id || ''}
                                      onChange={(e) => handleHistoryAllocationChange(idx, allocationIndex, 'expense_head_id', e.target.value)}
                                    >
                                      <MenuItem value="">None</MenuItem>
                                      {allocation.expense_head_id && !activeExpenseHeads.find((expenseHead) => expenseHead.id === allocation.expense_head_id) && (
                                        <MenuItem value={allocation.expense_head_id}>
                                          {selectedExpenseHead?.name || allocation.expense_head_name || allocation.expense_head_id}
                                        </MenuItem>
                                      )}
                                      {activeExpenseHeads.map((expenseHead) => (
                                        <MenuItem key={expenseHead.id} value={expenseHead.id}>{expenseHead.name}</MenuItem>
                                      ))}
                                    </Select>
                                  </FormControl>
                                </TableCell>
                                <TableCell>
                                  <TextField
                                    size="small"
                                    type="number"
                                    value={allocation.allocation_amount ?? 0}
                                    onChange={(e) => handleHistoryAllocationChange(
                                      idx,
                                      allocationIndex,
                                      'allocation_amount',
                                      parseFloat(e.target.value) || 0
                                    )}
                                    inputProps={{ min: 0, step: 0.01 }}
                                  />
                                </TableCell>
                                <TableCell>
                                  <Chip
                                    label={formatPercentage(allocation.allocation_percentage)}
                                    size="small"
                                    color={Math.abs((parseFloat(allocation.allocation_percentage) || 0)) > 0 ? 'primary' : 'default'}
                                    variant="outlined"
                                  />
                                </TableCell>
                                <TableCell>
                                  <IconButton size="small" onClick={() => removeHistoryAllocation(idx, allocationIndex)} color="error">
                                    <RemoveCircle fontSize="small" />
                                  </IconButton>
                                </TableCell>
                              </TableRow>
                            );
                          })}
                          <TableRow>
                            <TableCell colSpan={3}><strong>Total</strong></TableCell>
                            <TableCell>
                              <Chip
                                label={`INR ${allocationTotal.toFixed(2)}`}
                                color={Math.abs(allocationTotal - amount) < 0.01 ? 'success' : 'error'}
                                size="small"
                              />
                            </TableCell>
                            <TableCell>
                              <Chip
                                label={formatPercentage(allocationPercentageTotal)}
                                color={Math.abs(allocationPercentageTotal - 100) < 0.01 ? 'success' : 'error'}
                                size="small"
                              />
                            </TableCell>
                            <TableCell></TableCell>
                          </TableRow>
                        </TableBody>
                      </Table>
                    ) : (
                      <Typography variant="body2" color="text.secondary">
                        No allocations defined for this closed period. Add allocation rows if this amount should be split across groups.
                      </Typography>
                    )}

                    <Box sx={{ mt: 1.5 }}>
                      <Button
                        size="small"
                        startIcon={<AddCircle />}
                        onClick={() => addHistoryAllocation(idx)}
                        sx={{ textTransform: 'none' }}
                      >
                        Add Allocation
                      </Button>
                    </Box>
                  </SectionCard>
                );
              })}
            </Box>
          ) : (
            <Typography>No closed compensation periods available yet.</Typography>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => { setHistoryDialogOpen(false); setHistoryDrafts([]); setError(''); }} sx={{ textTransform: 'none' }}>Close</Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default Teams;
