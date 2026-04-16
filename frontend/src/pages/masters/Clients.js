import React, { useState, useEffect, useCallback, useMemo } from 'react';
import {
  Box, Button, TextField, Dialog, DialogTitle,
  DialogContent, DialogActions, IconButton, Typography, Chip, Alert,
  Table, TableBody, TableCell,
  TableHead, TableRow, Grid, Paper, Tooltip,
  TableContainer, alpha, InputAdornment,
  ToggleButton, ToggleButtonGroup
} from '@mui/material';
import {
  Add, Edit, Search, Refresh, AddCircle, Delete,
  KeyboardArrowDown, History,
  Business, TrendingUp, Person, RemoveCircle
} from '@mui/icons-material';
import { toast } from 'react-toastify';
import {
  HoverActionButton,
  MetricCard,
  PageHeader,
  SectionCard,
} from '../../components/FuturisticUI';
import { clientAPI, servicePeriodAPI, serviceTypeAPI, teamAPI, billFromAPI, billingNameAPI } from '../../services/api';
import { flattenDependencyItems, summarizeDependencies } from '../../utils/deleteDependencies';

// ─── Pastel Color Palette ──────────────────────────────────────────
const COLORS = {
  primary: '#0d9488',
  secondary: '#5eead4',
  accent: '#e11d48',
  background: '#fafaf8',
  cardBg: 'rgba(255,255,255,0.72)',
  headerBg: 'linear-gradient(135deg, #f0fdfa 0%, #ccfbf1 50%, #99f6e4 100%)',
  success: '#16a34a',
  warning: '#d97706',
  error: '#dc2626',
  tableHeader: '#f0fdfa',
  tableBorder: '#ccfbf1',
  textPrimary: '#1c1917',
  textSecondary: '#78716c',
};

const PERIOD_REGEX = /^[A-Z][a-z]{2}-\d{2}$/;

const MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const MONTH_MAP = { Jan: 0, Feb: 1, Mar: 2, Apr: 3, May: 4, Jun: 5, Jul: 6, Aug: 7, Sep: 8, Oct: 9, Nov: 10, Dec: 11 };

const derivePanFromGstin = (gstin) => (gstin && gstin.length >= 12 ? gstin.slice(2, 12) : '');

const comparePeriods = (a, b) => {
  const monthMap = { Jan: 0, Feb: 1, Mar: 2, Apr: 3, May: 4, Jun: 5, Jul: 6, Aug: 7, Sep: 8, Oct: 9, Nov: 10, Dec: 11 };
  const parse = (p) => {
    if (!p || !PERIOD_REGEX.test(p)) return null;
    const [mon, yy] = p.split('-');
    return new Date(2000 + parseInt(yy, 10), monthMap[mon], 1);
  };
  const dA = parse(a), dB = parse(b);
  if (!dA && !dB) return 0;
  if (!dA) return -1;
  if (!dB) return 1;
  return dA - dB;
};

// Returns the period immediately following the given period
const nextPeriodFrom = (period) => {
  if (!period || !PERIOD_REGEX.test(period)) return '';
  const [mon, yy] = period.split('-');
  const d = new Date(2000 + parseInt(yy, 10), MONTH_MAP[mon], 1);
  d.setMonth(d.getMonth() + 1);
  return `${MONTH_NAMES[d.getMonth()]}-${d.getFullYear().toString().slice(-2)}`;
};

const formatAmount = (value) => {
  const n = Number(value || 0);
  return `₹ ${n.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
};

const formatPercentage = (value) =>
  `${Number(value || 0).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%`;

const calculateAllocationPercentage = (amount, total) => {
  const denominator = Number(total || 0);
  if (!denominator) return 0;
  return (Number(amount || 0) / denominator) * 100;
};

const createEmptyReviewer = () => ({
  reviewer_id: '',
  amount: ''
});

const createEmptyBillFrom = (overrides = {}) => ({
  bill_from_id: '',
  reviewers: [createEmptyReviewer()],
  ...overrides
});

const createEmptyServiceType = (overrides = {}) => ({
  service_type_id: '',
  start_period: '',
  end_period: '',
  bill_froms: [createEmptyBillFrom()],
  min_start_period: '',
  lock_periods: false,
  ...overrides
});

const parseAmount = (value) => Number.parseFloat(value || 0) || 0;

const sumReviewerAmounts = (reviewers = []) =>
  reviewers.reduce((sum, reviewer) => sum + parseAmount(reviewer.amount), 0);

const sumBillFromAmounts = (billFroms = []) =>
  billFroms.reduce((sum, billFrom) => sum + sumReviewerAmounts(billFrom.reviewers), 0);

const formatPeriodRange = (startPeriod, endPeriod) => {
  if (!startPeriod) return 'Period not set';
  return endPeriod ? `${startPeriod} to ${endPeriod}` : `${startPeriod} to Open`;
};

const isStoredBillingRow = (row) => row.is_active !== false;
const isDisplayActiveRow = (row) => row.is_active !== false && !row.end_period;

const isServiceTypeActive = (serviceType) => {
  if (!serviceType) return false;
  return (serviceType.bill_froms || []).some((bf) => bf.reviewers.length > 0);
};

const getReactivationFloor = (row, history = []) => {
  if (!row || isDisplayActiveRow(row)) return '';
  if (row?.end_period) return row.end_period;
  const latestEndedRow = history.find((entry) => entry.end_period);
  return latestEndedRow?.end_period || '';
};

const getServiceTypeReactivationStartFloor = (serviceType) => {
  const history = serviceType?.history_periods || [];
  const lastEndedPeriod = history.length ? history[history.length - 1]?.end_period : '';
  return lastEndedPeriod ? nextPeriodFrom(lastEndedPeriod) : '';
};

const PickerDialog = ({
  open,
  title,
  loading,
  options,
  search,
  onSearchChange,
  onSelect,
  onClose
}) => {
  const filtered = useMemo(() => {
    const q = (search || '').trim().toLowerCase();
    if (!q) return options;
    return (options || []).filter((o) =>
      (o.label || '').toLowerCase().includes(q) ||
      (o.secondary || '').toLowerCase().includes(q)
    );
  }, [options, search]);

  return (
    <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth PaperProps={{ sx: { borderRadius: '16px' } }}>
      <DialogTitle sx={{ bgcolor: COLORS.tableHeader, borderBottom: `1px solid ${COLORS.tableBorder}`, fontWeight: 600, color: COLORS.textPrimary }}>
        {title}
      </DialogTitle>
      <DialogContent sx={{ pt: 2.5 }}>
        <TextField
          fullWidth
          placeholder="Search..."
          value={search}
          onChange={(e) => onSearchChange(e.target.value)}
          size="small"
          sx={{ mb: 2, '& .MuiOutlinedInput-root': { borderRadius: '10px' } }}
        />

        {loading ? (
          <Box sx={{ p: 4, textAlign: 'center' }}>
            <Typography sx={{ color: COLORS.textSecondary }}>Loading...</Typography>
          </Box>
        ) : filtered.length === 0 ? (
          <Box sx={{ p: 3, textAlign: 'center' }}>
            <Typography sx={{ color: COLORS.textSecondary }}>No results</Typography>
          </Box>
        ) : (
          <TableContainer component={Paper} elevation={0} sx={{ border: `1px solid ${COLORS.tableBorder}`, borderRadius: '12px', overflow: 'hidden' }}>
            <Table size="small">
              <TableBody>
                {filtered.map((o) => (
                  <TableRow
                    key={String(o.value)}
                    hover
                    sx={{ cursor: 'pointer' }}
                    onClick={() => onSelect(o.value)}
                  >
                    <TableCell sx={{ color: COLORS.textPrimary, fontWeight: 600 }}>{o.label}</TableCell>
                    <TableCell sx={{ color: COLORS.textSecondary, width: '40%' }}>{o.secondary || ''}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        )}
      </DialogContent>
      <DialogActions sx={{ p: 2.5, borderTop: `1px solid ${COLORS.tableBorder}` }}>
        <Button onClick={onClose} sx={{ borderRadius: '8px', textTransform: 'none', color: COLORS.textSecondary }}>
          Close
        </Button>
      </DialogActions>
    </Dialog>
  );
};

// ─── Group billing rows into hierarchy ─────────────────────────────
const groupBillingRows = (rows) => {
  const groups = {};
  (rows || []).forEach((row) => {
    const key = `${row.billing_name || ''}||${row.gstin || ''}||${row.pan || ''}`;
    if (!groups[key]) {
      groups[key] = {
        billing_name: row.billing_name,
        gstin: row.gstin,
        pan: row.pan,
        service_types: {},
        total: 0
      };
    }

    const serviceTypeKey = `${row.service_type_id || '_none_'}`;
    if (!groups[key].service_types[serviceTypeKey]) {
      groups[key].service_types[serviceTypeKey] = {
        service_type_id: row.service_type_id,
        service_type_name: row.service_type_name || 'Not Assigned',
        start_period: '',
        end_period: '',
        bill_froms: {},
        history_periods: {},
        total: 0
      };
    }

    const st = groups[key].service_types[serviceTypeKey];
    const isLive = !row.end_period;

    if (isLive) {
      // Live rows: visible / editable bill_froms
      if (!st.start_period || comparePeriods(row.start_period, st.start_period) < 0) {
        st.start_period = row.start_period || '';
      }
      const billFromKey = `${row.bill_from_id || '_none_'}`;
      if (!st.bill_froms[billFromKey]) {
        st.bill_froms[billFromKey] = {
          bill_from_id: row.bill_from_id,
          bill_from_name: row.bill_from_name || 'Not Assigned',
          reviewers: [],
          total: 0
        };
      }
      st.bill_froms[billFromKey].reviewers.push(row);
      if (isStoredBillingRow(row)) {
        const amount = parseAmount(row.amount);
        st.bill_froms[billFromKey].total += amount;
        st.total += amount;
        groups[key].total += amount;
      }
    } else {
      // Historic rows: grouped by period into history_periods
      const periodKey = `${row.start_period || ''}||${row.end_period || ''}`;
      if (!st.history_periods[periodKey]) {
        st.history_periods[periodKey] = {
          start_period: row.start_period || '',
          end_period: row.end_period || '',
          bill_froms: {},
          total: 0
        };
      }
      const hp = st.history_periods[periodKey];
      const billFromKey = `${row.bill_from_id || '_none_'}`;
      if (!hp.bill_froms[billFromKey]) {
        hp.bill_froms[billFromKey] = {
          bill_from_id: row.bill_from_id,
          bill_from_name: row.bill_from_name || 'Not Assigned',
          reviewers: [],
          total: 0
        };
      }
      hp.bill_froms[billFromKey].reviewers.push(row);
      if (isStoredBillingRow(row)) {
        const amount = parseAmount(row.amount);
        hp.bill_froms[billFromKey].total += amount;
        hp.total += amount;
      }
    }
  });

  return Object.values(groups).map((group) => ({
    ...group,
    service_types: Object.values(group.service_types)
      .map((st) => ({
        ...st,
        bill_froms: Object.values(st.bill_froms).sort((a, b) => a.bill_from_name.localeCompare(b.bill_from_name)),
        history_periods: Object.values(st.history_periods)
          .map((hp) => ({ ...hp, bill_froms: Object.values(hp.bill_froms).sort((a, b) => a.bill_from_name.localeCompare(b.bill_from_name)) }))
          .sort((a, b) => comparePeriods(a.start_period, b.start_period))
      }))
      .sort((a, b) => a.service_type_name.localeCompare(b.service_type_name))
  }));
};

// ─── Expandable Client Row ─────────────────────────────────────────
const ClientDetailRow = ({
  client,
  onEdit,
  onDelete,
  onOpenBillingPopup,
  canDelete
}) => {
  const billingRows = client.billing_rows || [];
  const hierarchy = useMemo(() => groupBillingRows(client.billing_rows || []), [client.billing_rows]);
  const activeRows = billingRows.filter(isDisplayActiveRow);
  const billingSourceCount = hierarchy.reduce(
    (sum, group) => sum + group.service_types.reduce((serviceTypeSum, serviceType) => serviceTypeSum + serviceType.bill_froms.length, 0),
    0
  );

  return (
    <>
      <TableRow
        hover
        sx={{
          '& > *': { borderBottom: 'unset' },
          cursor: 'default',
          bgcolor: 'inherit',
          transition: 'all 0.2s ease',
          '&:hover': { bgcolor: alpha(COLORS.primary, 0.06) }
        }}
      >
        <TableCell padding="checkbox" sx={{ borderBottom: `1px solid ${COLORS.tableBorder}` }}>
          <IconButton
            size="small"
            sx={{ color: COLORS.primary }}
            onClick={(e) => { e.stopPropagation(); onOpenBillingPopup(client); }}
          >
            <KeyboardArrowDown />
          </IconButton>
        </TableCell>
        <TableCell sx={{ borderBottom: `1px solid ${COLORS.tableBorder}` }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
            <Box sx={{
              width: 40, height: 40, borderRadius: '10px',
              bgcolor: alpha(COLORS.primary, 0.1),
              display: 'flex', alignItems: 'center', justifyContent: 'center'
            }}>
              <Business sx={{ color: COLORS.primary, fontSize: 20 }} />
            </Box>
            <Box>
              <Typography variant="body1" sx={{ fontWeight: 600, color: COLORS.textPrimary }}>
                {client.name}
              </Typography>
              <Typography variant="caption" sx={{ color: COLORS.textSecondary }}>
                {hierarchy.length} {hierarchy.length === 1 ? 'client' : 'clients'} � {activeRows.length} active {activeRows.length === 1 ? 'row' : 'rows'}
              </Typography>
            </Box>
          </Box>
        </TableCell>
        <TableCell align="center" sx={{ borderBottom: `1px solid ${COLORS.tableBorder}` }}>
          <Typography variant="body2" sx={{ color: COLORS.textSecondary, fontWeight: 500 }}>
            {billingSourceCount}
          </Typography>
        </TableCell>
        <TableCell align="right" sx={{ borderBottom: `1px solid ${COLORS.tableBorder}` }}>
          <Typography variant="body1" sx={{ fontWeight: 700, color: COLORS.primary }}>
            {formatAmount(client.total_amount)}
          </Typography>
        </TableCell>
        <TableCell align="center" sx={{ borderBottom: `1px solid ${COLORS.tableBorder}` }}>
          <Box sx={{ display: 'flex', justifyContent: 'center', gap: 0.5 }}>
            <Tooltip title="Edit Group">
              <IconButton
                size="small"
                onClick={(e) => { e.stopPropagation(); onEdit(client); }}
                sx={{ color: COLORS.primary, '&:hover': { bgcolor: alpha(COLORS.primary, 0.1) } }}
              >
                <Edit fontSize="small" />
              </IconButton>
            </Tooltip>

            {canDelete && (
            <Tooltip title="Delete Group">
              <IconButton
                size="small"
                onClick={(e) => { e.stopPropagation(); onDelete(client); }}
                sx={{ color: '#DC2626', '&:hover': { bgcolor: alpha('#DC2626', 0.08) } }}
              >
                <Delete fontSize="small" />
              </IconButton>
            </Tooltip>
            )}
          </Box>
        </TableCell>
      </TableRow>
    </>
  );
};

// ═══════════════════════════════════════════════════════════════════
// Main Clients Component
// ═══════════════════════════════════════════════════════════════════
const Clients = () => {
  const [clients, setClients] = useState([]);
  const [serviceTypes, setServiceTypes] = useState([]);
  const [reviewers, setReviewers] = useState([]);
  const [billFroms, setBillFroms] = useState([]);
  const [billingNameMasters, setBillingNameMasters] = useState([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [statusFilter, setStatusFilter] = useState('active');
  const [monthlyTotal, setMonthlyTotal] = useState({ total_amount: 0, client_count: 0 });

  // Client dialog
  const [dialogOpen, setDialogOpen] = useState(false);
  const [selectedClient, setSelectedClient] = useState(null);
  const [formData, setFormData] = useState({ name: '' });
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [clientToDelete, setClientToDelete] = useState(null);
  const [deleteDeps, setDeleteDeps] = useState([]);
  const [deleteMessage, setDeleteMessage] = useState('');
  const [depsLoading, setDepsLoading] = useState(false);

  // Billing group dialog (nested add form)
  const [billingGroupDialogOpen, setBillingGroupDialogOpen] = useState(false);
  const [billingGroupClient, setBillingGroupClient] = useState(null);
  const [billingGroupForm, setBillingGroupForm] = useState({
    billing_name: '', gstin: '', pan: '', billing_name_id: '',
    service_types: [createEmptyServiceType()]
  });
  const [billingGroupLockedGroup, setBillingGroupLockedGroup] = useState(false);
  const [billingGroupLockedServiceType, setBillingGroupLockedServiceType] = useState(false);
  const [billingGroupLockedBillFrom, setBillingGroupLockedBillFrom] = useState(false);

  // Single billing row edit dialog
  const [editRowDialogOpen, setEditRowDialogOpen] = useState(false);
  const [editRowClient, setEditRowClient] = useState(null);
  const [editRowData, setEditRowData] = useState(null);
  const [editRowForm, setEditRowForm] = useState({
    billing_name: '', pan: '', gstin: '', service_type_id: '', bill_from_id: '', reviewer_id: '',
    start_period: '', end_period: '', amount: '', is_active: true
  });

  // History dialog
  const [historyDialogOpen, setHistoryDialogOpen] = useState(false);
  const [historyClient, setHistoryClient] = useState(null);
  const [billingHistory, setBillingHistory] = useState([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyTitle, setHistoryTitle] = useState('Billing Row Periods');
  const [historyChipLabel, setHistoryChipLabel] = useState('');
  const [historyEditable, setHistoryEditable] = useState(false);
  const [historyEditScope, setHistoryEditScope] = useState('');
  const [historyGroupKey, setHistoryGroupKey] = useState('');
  const [historyServiceType, setHistoryServiceType] = useState(null);
  const [historyDrafts, setHistoryDrafts] = useState([]);
  const [historySavingKey, setHistorySavingKey] = useState('');

  // Edit history detail
  const [editHistoryDialogOpen, setEditHistoryDialogOpen] = useState(false);
  const [editHistoryDetail, setEditHistoryDetail] = useState(null);
  const [editHistoryForm, setEditHistoryForm] = useState({ bill_from_id: '', reviewer_id: '', amount: '' });

  // Billing group popup (billing-level edit/view)
  const [billingPopupOpen, setBillingPopupOpen] = useState(false);
  const [billingPopupClient, setBillingPopupClient] = useState(null);
  const [billingPopupRows, setBillingPopupRows] = useState([]);
  const [billingPopupHierarchy, setBillingPopupHierarchy] = useState([]);
  const [billingPopupLoading, setBillingPopupLoading] = useState(false);

  const [editGroupDialogOpen, setEditGroupDialogOpen] = useState(false);
  const [editGroupClient, setEditGroupClient] = useState(null);
  const [editGroupGroup, setEditGroupGroup] = useState(null);
  const [editGroupForm, setEditGroupForm] = useState({ billing_name: '', billing_name_id: '', gstin: '', pan: '' });

  const [editServiceTypeDialogOpen, setEditServiceTypeDialogOpen] = useState(false);
  const [editServiceTypeClient, setEditServiceTypeClient] = useState(null);
  const [editServiceTypeGroup, setEditServiceTypeGroup] = useState(null);
  const [editServiceTypeData, setEditServiceTypeData] = useState(null);
  const [editServiceTypeForm, setEditServiceTypeForm] = useState({ service_type_id: '', start_period: '', end_period: '', is_active: true });

  // Continuation dialog: shown after setting end_period on a service type
  const [continuationDialogOpen, setContinuationDialogOpen] = useState(false);
  const [continuationContext, setContinuationContext] = useState({ client: null, group: null, serviceTypeId: '', endPeriod: '' });

  // Reactivation extra fields (shown in Edit Service Type dialog when reactivating)
  const [reactivationForm, setReactivationForm] = useState({ bill_from_id: '', reviewer_id: '', amount: '' });

  const [editBillFromDialogOpen, setEditBillFromDialogOpen] = useState(false);
  const [editBillFromClient, setEditBillFromClient] = useState(null);
  const [editBillFromGroup, setEditBillFromGroup] = useState(null);
  const [editBillFromServiceType, setEditBillFromServiceType] = useState(null);
  const [editBillFromData, setEditBillFromData] = useState(null);
  const [editBillFromForm, setEditBillFromForm] = useState({ bill_from_id: '' });

  // Picker popup (replaces dropdown selects)
  const [pickerOpen, setPickerOpen] = useState(false);
  const [pickerTitle, setPickerTitle] = useState('');
  const [pickerOptions, setPickerOptions] = useState([]);
  const [pickerLoading, setPickerLoading] = useState(false);
  const [pickerSearch, setPickerSearch] = useState('');
  const [pickerOnSelect, setPickerOnSelect] = useState(null);

  // Client-in-multiple-groups conflict warning state (cleared when billing group dialog closes)
  const [conflictingGroups, setConflictingGroups] = useState([]);

  const [error, setError] = useState('');

  const normalizeGroupKey = (group) => {
    const billingName = (group?.billing_name || '').trim().toLowerCase();
    const pan = (group?.pan || '').trim().toLowerCase();
    const gstin = (group?.gstin || '').trim().toLowerCase();
    return `${billingName}||${pan}||${gstin}`;
  };

  const openPicker = async ({ title, loadOptions, onSelect }) => {
    setPickerTitle(title);
    setPickerOptions([]);
    setPickerSearch('');
    setPickerLoading(true);
    setPickerOnSelect(() => onSelect);
    setPickerOpen(true);
    try {
      const opts = await loadOptions();
      setPickerOptions(opts || []);
    } catch (e) {
      toast.error('Failed to load list');
      setPickerOptions([]);
    } finally {
      setPickerLoading(false);
    }
  };

  const closePicker = () => {
    setPickerOpen(false);
    setPickerTitle('');
    setPickerOptions([]);
    setPickerSearch('');
    setPickerLoading(false);
    setPickerOnSelect(null);
  };

  const getServiceTypeLabel = (id) => (activeServiceTypes.find((st) => st.id === id)?.name || '');
  const getBillFromLabel = (id) => (activeBillFroms.find((bf) => bf.id === id)?.name || '');
  const getReviewerLabel = (id) => (reviewers.find((r) => r.id === id)?.name || '');

  // ─── Load Data ─────────────────────────────────────────────────
  const loadClients = useCallback(async () => {
    setLoading(true);
    try {
      const [clientRes, serviceTypeRes, reviewerRes, billFromRes, billingNameRes] = await Promise.all([
        clientAPI.getAll({ statusFilter: 'all' }),
        serviceTypeAPI.getAll(),
        teamAPI.getReviewers(),
        billFromAPI.getAll(),
        billingNameAPI.getAll({ includeInactive: false })
      ]);

      const data = clientRes.data.data || [];
      setClients(data);
      setServiceTypes(serviceTypeRes.data.data || []);
      setReviewers(reviewerRes.data.data || []);
      setBillFroms(billFromRes.data.data || []);
      setBillingNameMasters(billingNameRes.data.data || []);

      const liveClients = data.filter((client) => Number(client.active_billing_count || 0) > 0);
      setMonthlyTotal({
        total_amount: data.reduce((sum, client) => sum + parseFloat(client.total_amount || 0), 0),
        client_count: liveClients.length
      });
    } catch (err) {
      toast.error('Failed to load groups');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadClients(); }, [loadClients]);

  const activeServiceTypes = useMemo(() => serviceTypes.filter(st => st.is_active !== false), [serviceTypes]);
  const activeBillFroms = useMemo(() => billFroms.filter(bf => bf.is_active !== false), [billFroms]);
  const activeBillingNameMasters = useMemo(() => billingNameMasters.filter(b => b.is_active !== false), [billingNameMasters]);
  const billingGroupOverallTotal = useMemo(
    () => billingGroupForm.service_types.reduce((sum, serviceType) => sum + sumBillFromAmounts(serviceType.bill_froms), 0),
    [billingGroupForm]
  );
  const billingGroupServiceTypeCount = useMemo(
    () => billingGroupForm.service_types.length,
    [billingGroupForm]
  );
  const billingGroupBillFromCount = useMemo(
    () => billingGroupForm.service_types.reduce((sum, serviceType) => sum + serviceType.bill_froms.length, 0),
    [billingGroupForm]
  );
  const billingGroupReviewerCount = useMemo(
    () => billingGroupForm.service_types.reduce(
      (sum, serviceType) => sum + serviceType.bill_froms.reduce((billFromSum, billFrom) => billFromSum + billFrom.reviewers.length, 0),
      0
    ),
    [billingGroupForm]
  );
  // ─── Client Dialog Handlers ────────────────────────────────────
  const handleOpenDialog = async (client = null) => {
    setSelectedClient(client);
    setFormData({
      name: client ? client.name : ''
    });
    setError('');
    setDialogOpen(true);
  };

  const handleCloseDialog = () => {
    setDialogOpen(false);
    setSelectedClient(null);
    setFormData({ name: '' });
    setError('');
  };

  const handleSubmitClient = async () => {
    if (!formData.name.trim()) { setError('Group name is required'); return; }
    try {
      if (selectedClient) {
        await clientAPI.update(selectedClient.id, {
          name: formData.name.trim()
        });
        toast.success('Group updated');
      } else {
        await clientAPI.create({
          name: formData.name.trim()
        });
        toast.success('Group created');
      }
      handleCloseDialog();
      loadClients();
    } catch (err) {
      setError(err.response?.data?.message || 'Operation failed');
    }
  };

  // ─── Billing Group Dialog (Nested Add) ─────────────────────────
  const handleOpenDeleteDialog = async (client) => {
    setClientToDelete(client);
    setError('');
    setDeleteDeps([]);
    setDeleteMessage('');
    setDepsLoading(true);
    setDeleteDialogOpen(true);
    try {
      const res = await clientAPI.checkDependencies(client.id);
      setDeleteDeps(res.data?.data?.dependencies || []);
      setDeleteMessage(res.data?.data?.message || '');
    } catch {
      setDeleteDeps([]);
      setDeleteMessage('');
    } finally {
      setDepsLoading(false);
    }
  };

  const handleCloseDeleteDialog = () => {
    setDeleteDialogOpen(false);
    setClientToDelete(null);
    setDeleteDeps([]);
    setDeleteMessage('');
    setError('');
  };

  const handleDeleteClient = async () => {
    if (!clientToDelete) return;
    try {
      await clientAPI.delete(clientToDelete.id);
      toast.success(`Group "${clientToDelete.name}" deleted.`);
      handleCloseDeleteDialog();
      loadClients();
    } catch (err) {
      if (err.response?.status === 409) {
        setDeleteDeps(err.response?.data?.data?.dependencies || []);
        setDeleteMessage(err.response?.data?.message || err.response?.data?.data?.message || 'Delete is blocked because this group is still linked elsewhere.');
      }
      setError(err.response?.data?.message || 'Failed to delete group');
    }
  };

  const deleteLinkedItems = flattenDependencyItems(deleteDeps);

  const getPopupGroupRows = useCallback((group) => {
    const key = normalizeGroupKey(group);
    return (billingPopupRows || []).filter((r) => normalizeGroupKey({
      billing_name: r.billing_name,
      pan: r.pan,
      gstin: r.gstin
    }) === key);
  }, [billingPopupRows]);

  const getPopupServiceTypeRows = useCallback((group, serviceType) => {
    const groupRows = getPopupGroupRows(group);
    return groupRows.filter((r) => (r.service_type_id || '') === (serviceType.service_type_id || ''));
  }, [getPopupGroupRows]);

  const getPopupServiceTypeLiveRows = useCallback((group, serviceType) => {
    return getPopupServiceTypeRows(group, serviceType).filter((r) => !r.end_period);
  }, [getPopupServiceTypeRows]);

  const getPopupBillFromRows = useCallback((group, serviceType, billFrom) => {
    const serviceRows = getPopupServiceTypeLiveRows(group, serviceType);
    return serviceRows.filter((r) => (r.bill_from_id || '') === (billFrom.bill_from_id || ''));
  }, [getPopupServiceTypeLiveRows]);

  const handleOpenEditGroup = (client, group) => {
    setEditGroupClient(client);
    setEditGroupGroup(group);
    const matchedMaster = billingNameMasters.find((b) => b.name.toLowerCase().trim() === (group?.billing_name || '').toLowerCase().trim());
    setEditGroupForm({
      billing_name: group?.billing_name || '',
      billing_name_id: matchedMaster?.id || '',
      gstin: group?.gstin || '',
      pan: group?.pan || ''
    });
    setError('');
    setEditGroupDialogOpen(true);
  };

  const handleCloseEditGroup = () => {
    setEditGroupDialogOpen(false);
    setEditGroupClient(null);
    setEditGroupGroup(null);
    setEditGroupForm({ billing_name: '', billing_name_id: '', gstin: '', pan: '' });
    setError('');
  };

  const handleSubmitEditGroup = async () => {
    if (!editGroupClient || !editGroupGroup) return;
    if (!editGroupForm.billing_name.trim()) { setError('Client is required'); return; }
    try {
      const rows = getPopupGroupRows(editGroupGroup);
      await Promise.all(rows.map((row) => clientAPI.updateBillingRow(editGroupClient.id, row.id, {
        billing_name: editGroupForm.billing_name.trim(),
        gstin: editGroupForm.gstin?.toUpperCase() || null,
        pan: editGroupForm.pan || null
      })));
      toast.success('Client updated');
      handleCloseEditGroup();
      await loadClients();
      await refreshBillingPopupForClient(editGroupClient.id);
    } catch (err) {
      setError(err.response?.data?.message || 'Failed to update client');
    }
  };

  const handleOpenEditServiceType = (client, group, serviceType) => {
    setEditServiceTypeClient(client);
    setEditServiceTypeGroup(group);
    setEditServiceTypeData(serviceType);
    const hasLiveRows = isServiceTypeActive(serviceType);
    const lastHistory = !hasLiveRows && (serviceType.history_periods || []).length
      ? serviceType.history_periods[serviceType.history_periods.length - 1]
      : null;
    setEditServiceTypeForm({
      service_type_id: serviceType?.service_type_id || '',
      start_period: hasLiveRows ? (serviceType?.start_period || '') : (lastHistory ? nextPeriodFrom(lastHistory.end_period) : ''),
      end_period: '',
      is_active: hasLiveRows
    });
    setError('');
    setEditServiceTypeDialogOpen(true);
  };

  const handleCloseEditServiceType = () => {
    setEditServiceTypeDialogOpen(false);
    setEditServiceTypeClient(null);
    setEditServiceTypeGroup(null);
    setEditServiceTypeData(null);
    setEditServiceTypeForm({ service_type_id: '', start_period: '', end_period: '', is_active: true });
    setReactivationForm({ bill_from_id: '', reviewer_id: '', amount: '' });
    setError('');
  };

  const handleSubmitEditServiceType = async () => {
    if (!editServiceTypeClient || !editServiceTypeGroup || !editServiceTypeData) return;
    if (!editServiceTypeForm.service_type_id) { setError('Service type is required'); return; }
    if (!editServiceTypeForm.start_period) { setError('Start period is required'); return; }
    if (!editServiceTypeForm.is_active && !editServiceTypeForm.end_period) { setError('End period is required when closing the current period'); return; }

    const targetEnd = editServiceTypeForm.is_active ? null : (editServiceTypeForm.end_period || null);
    if (targetEnd && comparePeriods(targetEnd, editServiceTypeForm.start_period) < 0) {
      setError('End period cannot be before start period');
      return;
    }

    // Check if this is a reactivation (service was inactive, now going active)
    const wasInactive = !isServiceTypeActive(editServiceTypeData);
    const isReactivating = editServiceTypeForm.is_active && wasInactive;

    if (isReactivating) {
      // Reactivation path: create NEW billing rows — don't modify old inactive rows
      if (!editServiceTypeForm.start_period) { setError('A new start period is required to continue this service type'); return; }
      if (!reactivationForm.amount || parseFloat(reactivationForm.amount) < 0) {
        setError('Amount is required to continue this service type');
        return;
      }

      const rows = getPopupServiceTypeRows(editServiceTypeGroup, editServiceTypeData);
      // Validate start period is after the existing end periods
      const reactivationFloors = rows
        .map((r) => getReactivationFloor(r, r.history || []))
        .filter(Boolean);
      const maxFloor = reactivationFloors.reduce((max, cur) => {
        if (!max) return cur;
        return comparePeriods(cur, max) > 0 ? cur : max;
      }, '');

      const reactivationStartFloor = maxFloor ? nextPeriodFrom(maxFloor) : '';
      if (reactivationStartFloor && comparePeriods(editServiceTypeForm.start_period, reactivationStartFloor) < 0) {
        setError(`Start period can only be selected from ${reactivationStartFloor} onward when continuing this service type`);
        return;
      }

      try {
        // Build a single new billing row with reactivation details
        const templateRow = rows[0];
        await clientAPI.addBillingRowsBatch(editServiceTypeClient.id, { rows: [{
          billing_name: templateRow.billing_name,
          pan: templateRow.pan || null,
          gstin: templateRow.gstin || null,
          service_type_id: editServiceTypeForm.service_type_id,
          bill_from_id: reactivationForm.bill_from_id || null,
          reviewer_id: reactivationForm.reviewer_id || null,
          start_period: editServiceTypeForm.start_period,
          end_period: null,
          amount: parseFloat(reactivationForm.amount) || 0
        }] });
        toast.success('Service type continued with a new billing entry');
        handleCloseEditServiceType();
        await loadClients();
        await refreshBillingPopupForClient(editServiceTypeClient.id);
      } catch (err) {
        setError(err.response?.data?.message || 'Failed to continue service type');
      }
      return;
    }

    const rows = getPopupServiceTypeLiveRows(editServiceTypeGroup, editServiceTypeData);

    try {
      await Promise.all(rows.map((row) => clientAPI.updateBillingRow(editServiceTypeClient.id, row.id, {
        service_type_id: editServiceTypeForm.service_type_id,
        start_period: editServiceTypeForm.start_period,
        end_period: targetEnd,
        is_active: editServiceTypeForm.is_active
      })));

      handleCloseEditServiceType();
      await loadClients();
      await refreshBillingPopupForClient(editServiceTypeClient.id);

      // If end period was just set (service going inactive), offer to create continuation
      if (!editServiceTypeForm.is_active && targetEnd) {
        setContinuationContext({
          client: editServiceTypeClient,
          group: editServiceTypeGroup,
          serviceTypeId: editServiceTypeForm.service_type_id,
          endPeriod: targetEnd
        });
        setContinuationDialogOpen(true);
      } else {
        toast.success('Service type updated');
      }
    } catch (err) {
      setError(err.response?.data?.message || 'Failed to update service type');
    }
  };

  const handleOpenEditBillFrom = (client, group, serviceType, billFrom) => {
    setEditBillFromClient(client);
    setEditBillFromGroup(group);
    setEditBillFromServiceType(serviceType);
    setEditBillFromData(billFrom);
    setEditBillFromForm({ bill_from_id: billFrom?.bill_from_id || '' });
    setError('');
    setEditBillFromDialogOpen(true);
  };

  const handleCloseEditBillFrom = () => {
    setEditBillFromDialogOpen(false);
    setEditBillFromClient(null);
    setEditBillFromGroup(null);
    setEditBillFromServiceType(null);
    setEditBillFromData(null);
    setEditBillFromForm({ bill_from_id: '' });
    setError('');
  };

  const handleSubmitEditBillFrom = async () => {
    if (!editBillFromClient || !editBillFromGroup || !editBillFromServiceType || !editBillFromData) return;
    try {
      const rows = getPopupBillFromRows(editBillFromGroup, editBillFromServiceType, editBillFromData);
      await Promise.all(rows.map((row) => clientAPI.updateBillingRow(editBillFromClient.id, row.id, {
        bill_from_id: editBillFromForm.bill_from_id || null
      })));
      toast.success('Bill from updated');
      handleCloseEditBillFrom();
      await loadClients();
      await refreshBillingPopupForClient(editBillFromClient.id);
    } catch (err) {
      setError(err.response?.data?.message || 'Failed to update bill from');
    }
  };

  const emptyBillingGroupForm = () => ({
    billing_name: '', gstin: '', pan: '', billing_name_id: '',
    service_types: [createEmptyServiceType()]
  });

  const handleOpenBillingGroupDialog = (client, prefillGroup = null, prefillServiceType = null, prefillBillFrom = null) => {
    setBillingGroupClient(client);
    setBillingGroupLockedGroup(Boolean(prefillGroup));
    setBillingGroupLockedServiceType(Boolean(prefillServiceType));
    setBillingGroupLockedBillFrom(Boolean(prefillBillFrom));
    if (prefillGroup) {
      const matchedMaster = billingNameMasters.find((b) => b.name.toLowerCase().trim() === (prefillGroup.billing_name || '').toLowerCase().trim());
      setBillingGroupForm({
        billing_name: prefillGroup.billing_name || '',
        billing_name_id: matchedMaster?.id || '',
        gstin: prefillGroup.gstin || '',
        pan: prefillGroup.pan || '',
        service_types: [
          createEmptyServiceType(prefillServiceType ? {
            service_type_id: prefillServiceType.service_type_id || '',
            start_period: prefillServiceType.start_period || '',
            end_period: prefillServiceType.end_period || '',
            min_start_period: prefillServiceType.min_start_period || '',
            lock_periods: prefillServiceType.lock_periods !== undefined ? prefillServiceType.lock_periods : true,
            bill_froms: [createEmptyBillFrom(prefillBillFrom ? { bill_from_id: prefillBillFrom.bill_from_id || '' } : {})]
          } : {})
        ]
      });
    } else {
      setBillingGroupForm(emptyBillingGroupForm());
    }
    setError('');
    setBillingGroupDialogOpen(true);
  };

  const handleOpenBillingPopup = async (client) => {
    setBillingPopupClient(client);
    setBillingPopupRows([]);
    setBillingPopupHierarchy([]);
    setBillingPopupLoading(true);
    setBillingPopupOpen(true);
    try {
      const res = await clientAPI.getById(client.id);
      const rows = res.data.data?.billing_rows || [];
      setBillingPopupRows(rows);
      setBillingPopupHierarchy(groupBillingRows(rows));
    } catch (e) {
      toast.error('Failed to load billing details');
      setBillingPopupRows([]);
      setBillingPopupHierarchy([]);
    } finally {
      setBillingPopupLoading(false);
    }
  };

  const refreshBillingPopupForClient = async (clientId) => {
    if (!billingPopupOpen || !clientId) return;
    setBillingPopupLoading(true);
    try {
      const res = await clientAPI.getById(clientId);
      const rows = res.data.data?.billing_rows || [];
      setBillingPopupRows(rows);
      setBillingPopupHierarchy(groupBillingRows(rows));
    } catch (e) {
      toast.error('Failed to refresh billing details');
    } finally {
      setBillingPopupLoading(false);
    }
  };

  const handleCloseBillingPopup = () => {
    setBillingPopupOpen(false);
    setBillingPopupClient(null);
    setBillingPopupRows([]);
    setBillingPopupHierarchy([]);
    setBillingPopupLoading(false);
  };

  const handleCloseBillingGroupDialog = () => {
    setBillingGroupDialogOpen(false);
    setBillingGroupClient(null);
    setBillingGroupForm(emptyBillingGroupForm());
    setBillingGroupLockedGroup(false);
    setBillingGroupLockedServiceType(false);
    setBillingGroupLockedBillFrom(false);
    setConflictingGroups([]);
    setError('');
  };

  // Called when a billing name master is selected from the picker
  const handleSelectBillingNameMaster = (masterId) => {
    const master = activeBillingNameMasters.find((b) => b.id === masterId);
    if (master) {
      setBillingGroupForm((prev) => ({
        ...prev,
        billing_name_id: master.id,
        billing_name: master.name,
        gstin: master.gstin || '',
        pan: master.pan || '',
      }));
      // Warn if this client is already in another group
      const nameLower = master.name.trim().toLowerCase();
      const conflicts = clients
        .filter((c) => c.id !== billingGroupClient?.id &&
          (c.billing_rows || []).some((row) => (row.billing_name || '').toLowerCase().trim() === nameLower))
        .map((c) => c.name);
      setConflictingGroups(conflicts);
    }
    closePicker();
  };

  const handleServiceTypeChange = (serviceTypeIndex, field, value) => {
    setBillingGroupForm(prev => {
      const newServiceTypes = [...prev.service_types];
      newServiceTypes[serviceTypeIndex] = { ...newServiceTypes[serviceTypeIndex], [field]: value };
      return { ...prev, service_types: newServiceTypes };
    });
  };

  const handleBillFromChange = (serviceTypeIndex, bfIndex, field, value) => {
    setBillingGroupForm(prev => {
      const newServiceTypes = [...prev.service_types];
      const newBillFroms = [...newServiceTypes[serviceTypeIndex].bill_froms];
      newBillFroms[bfIndex] = { ...newBillFroms[bfIndex], [field]: value };
      newServiceTypes[serviceTypeIndex] = { ...newServiceTypes[serviceTypeIndex], bill_froms: newBillFroms };
      return { ...prev, service_types: newServiceTypes };
    });
  };

  const handleReviewerChange = (serviceTypeIndex, bfIndex, revIndex, field, value) => {
    setBillingGroupForm(prev => ({
      ...prev,
      service_types: prev.service_types.map((serviceType, currentServiceTypeIndex) => {
        if (currentServiceTypeIndex !== serviceTypeIndex) return serviceType;
        return {
          ...serviceType,
          bill_froms: serviceType.bill_froms.map((billFrom, currentBillFromIndex) => {
            if (currentBillFromIndex !== bfIndex) return billFrom;
            return {
              ...billFrom,
              reviewers: billFrom.reviewers.map((reviewer, currentReviewerIndex) => (
                currentReviewerIndex === revIndex ? { ...reviewer, [field]: value } : reviewer
              ))
            };
          })
        };
      })
    }));
  };

  const addServiceType = () => {
    setBillingGroupForm(prev => ({
      ...prev,
      service_types: [...prev.service_types, createEmptyServiceType()]
    }));
  };

  const removeServiceType = (serviceTypeIndex) => {
    setBillingGroupForm(prev => ({
      ...prev,
      service_types: prev.service_types.filter((_, index) => index !== serviceTypeIndex)
    }));
  };

  const addBillFrom = (serviceTypeIndex) => {
    setBillingGroupForm(prev => {
      const newServiceTypes = [...prev.service_types];
      newServiceTypes[serviceTypeIndex] = {
        ...newServiceTypes[serviceTypeIndex],
        bill_froms: [...newServiceTypes[serviceTypeIndex].bill_froms, createEmptyBillFrom()]
      };
      return { ...prev, service_types: newServiceTypes };
    });
  };

  const removeBillFrom = (serviceTypeIndex, bfIndex) => {
    setBillingGroupForm(prev => {
      const newServiceTypes = [...prev.service_types];
      newServiceTypes[serviceTypeIndex] = {
        ...newServiceTypes[serviceTypeIndex],
        bill_froms: newServiceTypes[serviceTypeIndex].bill_froms.filter((_, index) => index !== bfIndex)
      };
      return { ...prev, service_types: newServiceTypes };
    });
  };

  const addReviewer = (serviceTypeIndex, bfIndex) => {
    setBillingGroupForm(prev => {
      const newServiceTypes = [...prev.service_types];
      const newBillFroms = [...newServiceTypes[serviceTypeIndex].bill_froms];
      newBillFroms[bfIndex] = {
        ...newBillFroms[bfIndex],
        reviewers: [...newBillFroms[bfIndex].reviewers, createEmptyReviewer()]
      };
      newServiceTypes[serviceTypeIndex] = { ...newServiceTypes[serviceTypeIndex], bill_froms: newBillFroms };
      return { ...prev, service_types: newServiceTypes };
    });
  };

  const removeReviewer = (serviceTypeIndex, bfIndex, revIndex) => {
    setBillingGroupForm(prev => {
      const newServiceTypes = [...prev.service_types];
      const newBillFroms = [...newServiceTypes[serviceTypeIndex].bill_froms];
      newBillFroms[bfIndex] = {
        ...newBillFroms[bfIndex],
        reviewers: newBillFroms[bfIndex].reviewers.filter((_, index) => index !== revIndex)
      };
      newServiceTypes[serviceTypeIndex] = { ...newServiceTypes[serviceTypeIndex], bill_froms: newBillFroms };
      return { ...prev, service_types: newServiceTypes };
    });
  };

  const handleSubmitBillingGroup = async () => {
    const { billing_name, gstin, pan, service_types } = billingGroupForm;
    if (!billing_name.trim()) { setError('Client is required'); return; }

    const rows = [];
    for (let serviceTypeIndex = 0; serviceTypeIndex < service_types.length; serviceTypeIndex += 1) {
      const serviceType = service_types[serviceTypeIndex];
      if (!serviceType.service_type_id) {
        setError(`Service type is required for row #${serviceTypeIndex + 1}`);
        return;
      }
      if (!serviceType.start_period) {
        setError(`Start period is required for service type #${serviceTypeIndex + 1}`);
        return;
      }
      if (serviceType.min_start_period && comparePeriods(serviceType.start_period, serviceType.min_start_period) < 0) {
        setError(`Start period can only be selected from ${serviceType.min_start_period} onward for service type #${serviceTypeIndex + 1}`);
        return;
      }
      if (serviceType.end_period && comparePeriods(serviceType.end_period, serviceType.start_period) < 0) {
        setError(`End period cannot be before start period for service type #${serviceTypeIndex + 1}`);
        return;
      }
      if (!serviceType.bill_froms.length) {
        setError(`Add at least one bill from under service type #${serviceTypeIndex + 1}`);
        return;
      }

      for (let billFromIndex = 0; billFromIndex < serviceType.bill_froms.length; billFromIndex += 1) {
        const billFrom = serviceType.bill_froms[billFromIndex];
        if (!billFrom.reviewers.length) {
          setError(`Add at least one reviewer under bill from #${billFromIndex + 1} for service type #${serviceTypeIndex + 1}`);
          return;
        }

        for (let reviewerIndex = 0; reviewerIndex < billFrom.reviewers.length; reviewerIndex += 1) {
          const rev = billFrom.reviewers[reviewerIndex];
          if (rev.amount === '' || parseAmount(rev.amount) < 0) {
            setError(`Valid amount is required for reviewer #${reviewerIndex + 1} under bill from #${billFromIndex + 1}`);
            return;
          }
          rows.push({
            billing_name: billing_name.trim(),
            gstin: gstin?.toUpperCase() || null,
            pan: pan || null,
            service_type_id: serviceType.service_type_id,
            bill_from_id: billFrom.bill_from_id || null,
            reviewer_id: rev.reviewer_id || null,
            start_period: serviceType.start_period,
            end_period: serviceType.end_period || null,
            amount: parseAmount(rev.amount)
          });
        }
      }
    }

    if (rows.length === 0) { setError('At least one reviewer entry is required'); return; }

    await doSaveBillingGroup(rows);
  };

  const doSaveBillingGroup = async (rows) => {
    const clientId = billingGroupClient.id;
    try {
      await clientAPI.addBillingRowsBatch(clientId, { rows });
      toast.success(`${rows.length} billing row(s) added`);
      handleCloseBillingGroupDialog();
      loadClients();
      refreshBillingPopupForClient(clientId);
    } catch (err) {
      setError(err.response?.data?.message || 'Failed to add billing rows');
    }
  };

  // ─── Edit Single Billing Row ───────────────────────────────────
  const handleOpenEditRow = async (client, row) => {
    setEditRowClient(client);
    setEditRowData(row);
    setEditRowForm({
      billing_name: row.billing_name || '',
      pan: row.pan || '',
      gstin: row.gstin || '',
      service_type_id: row.service_type_id || '',
      bill_from_id: row.bill_from_id || '',
      reviewer_id: row.reviewer_id || '',
      start_period: row.start_period || '',
      end_period: row.end_period || '',
      amount: row.amount || '',
      is_active: isDisplayActiveRow(row)
    });
    setError('');
    setEditRowDialogOpen(true);
  };

  const handleCloseEditRow = () => {
    setEditRowDialogOpen(false);
    setEditRowClient(null);
    setEditRowData(null);
    setEditRowForm({
      billing_name: '', pan: '', gstin: '', service_type_id: '', bill_from_id: '', reviewer_id: '',
      start_period: '', end_period: '', amount: '', is_active: true
    });
    setError('');
  };

  const handleEditRowFieldChange = (field, value) => {
    setEditRowForm(prev => {
      const updated = { ...prev, [field]: value };
      if (field === 'gstin' && value) {
        updated.pan = derivePanFromGstin(value.toUpperCase());
      }
      if (field === 'end_period') {
        updated.is_active = !value;
      }
      return updated;
    });
  };

  const handleSubmitEditRow = async () => {
    if (!editRowForm.amount || parseFloat(editRowForm.amount) < 0) { setError('Valid amount is required'); return; }

    try {
      const editClientId = editRowClient?.id;
      await clientAPI.updateBillingRow(editRowClient.id, editRowData.id, {
        reviewer_id: editRowForm.reviewer_id || null,
        amount: parseFloat(editRowForm.amount) || 0
      });
      toast.success('Billing row updated');
      handleCloseEditRow();
      await loadClients();
      if (billingPopupClient?.id === editClientId) {
        await refreshBillingPopupForClient(editClientId);
      }
    } catch (err) {
      setError(err.response?.data?.message || 'Failed to update billing row');
    }
  };

  // ─── History Dialog ────────────────────────────────────────────
  const handleViewHistory = async (client, billingRow) => {
    setHistoryClient(client);
    setHistoryTitle('Billing Row Periods');
    setHistoryChipLabel(billingRow?.billing_name || '');
    setHistoryEditable(false);
    setHistoryEditScope('');
    setHistoryGroupKey('');
    setHistoryServiceType(null);
    setHistoryLoading(true);
    setHistoryDialogOpen(true);
    try {
      const res = await clientAPI.getById(client.id);
      const row = res.data.data.billing_rows?.find(r => r.id === billingRow.id);
      const records = (row?.history || []).map((h) => {
        const amount = Number(h.amount || 0);
        return {
          start_period: h.start_period,
          end_period: h.end_period,
          amount,
          is_current: h.is_current === true,
          details: [{
            service_type_name: h.service_type_name || '-',
            bill_from_name: h.bill_from_name || '-',
            reviewer_name: h.reviewer_name || '-',
            amount,
            percentage: calculateAllocationPercentage(amount, amount),
            period_total: amount,
          }],
        };
      });
      records.sort((a, b) => comparePeriods(a.start_period, b.start_period) || comparePeriods(a.end_period, b.end_period));
      setBillingHistory(records);
    } catch (err) {
      toast.error('Failed to load history');
      setBillingHistory([]);
    } finally {
      setHistoryLoading(false);
    }
  };

  const handleViewBillingGroupHistory = (client, group) => {
    const groupKey = normalizeGroupKey(group);
    setHistoryClient(client);
    setHistoryTitle('Client Periods');
    setHistoryChipLabel(group?.billing_name || '');
    setHistoryEditable(true);
    setHistoryEditScope('billingGroup');
    setHistoryGroupKey(groupKey);
    setHistoryServiceType(null);
    setHistoryLoading(false);
    setBillingHistory([]);
    setHistoryDrafts(buildBillingGroupHistoryDrafts(group));
    setHistoryDialogOpen(true);
  };

  const handleCloseHistory = () => {
    setHistoryDialogOpen(false);
    setHistoryClient(null);
    setHistoryTitle('Billing Row Periods');
    setHistoryChipLabel('');
    setHistoryEditable(false);
    setHistoryEditScope('');
    setHistoryGroupKey('');
    setHistoryServiceType(null);
    setBillingHistory([]);
    setHistoryDrafts([]);
    setHistorySavingKey('');
    setEditHistoryDialogOpen(false);
    setEditHistoryDetail(null);
    setEditHistoryForm({ bill_from_id: '', reviewer_id: '', amount: '' });
    setError('');
  };

  const buildServiceTypeHistoryDrafts = (serviceType) =>
    (serviceType?.history_periods || []).map((period) => {
      const details = (period.bill_froms || []).flatMap((bf) =>
        (bf.reviewers || []).map((row) => ({
          row_id: row.id,
          service_type_id: row.service_type_id || serviceType.service_type_id,
          service_type_name: serviceType.service_type_name || '-',
          bill_from_id: bf.bill_from_id || '',
          bill_from_name: bf.bill_from_name || '-',
          reviewer_id: row.reviewer_id || '',
          reviewer_name: row.reviewer_name || '-',
          amount: Number(row.amount || 0),
          billing_name: row.billing_name,
          start_period: row.start_period || period.start_period,
          end_period: row.end_period || period.end_period,
        }))
      );

      const total = details.reduce((sum, detail) => sum + Number(detail.amount || 0), 0);
      return {
        start_period: period.start_period,
        end_period: period.end_period,
        amount: total,
        details: details.map((detail) => ({
          ...detail,
          percentage: calculateAllocationPercentage(detail.amount, total),
          period_total: total,
        })),
      };
    });

  const buildBillingGroupHistoryDrafts = (group) => {
    const draftsByPeriod = {};

    (group?.service_types || []).forEach((serviceType) => {
      (serviceType.history_periods || []).forEach((period) => {
        const periodKey = `${period.start_period || ''}||${period.end_period || ''}`;
        if (!draftsByPeriod[periodKey]) {
          draftsByPeriod[periodKey] = {
            start_period: period.start_period,
            end_period: period.end_period,
            amount: 0,
            details: [],
          };
        }

        const details = (period.bill_froms || []).flatMap((billFrom) =>
          (billFrom.reviewers || []).map((row) => ({
            row_id: row.id,
            service_type_id: row.service_type_id || serviceType.service_type_id,
            service_type_name: serviceType.service_type_name || row.service_type_name || '-',
            bill_from_id: billFrom.bill_from_id || '',
            bill_from_name: billFrom.bill_from_name || '-',
            reviewer_id: row.reviewer_id || '',
            reviewer_name: row.reviewer_name || '-',
            amount: Number(row.amount || 0),
            billing_name: row.billing_name,
            start_period: row.start_period || period.start_period,
            end_period: row.end_period || period.end_period,
          }))
        );

        draftsByPeriod[periodKey].details.push(...details);
      });
    });

    return Object.values(draftsByPeriod)
      .map(syncClientHistoryDraft)
      .sort((a, b) => comparePeriods(a.start_period, b.start_period) || comparePeriods(a.end_period, b.end_period));
  };

  const getHistoryDraftKey = (draft) => `${draft?.start_period || ''}||${draft?.end_period || ''}`;

  const syncClientHistoryDraft = (draft) => {
    const total = (draft.details || []).reduce((sum, detail) => sum + Number(detail.amount || 0), 0);
    return {
      ...draft,
      amount: total,
      details: (draft.details || []).map((detail) => ({
        ...detail,
        amount: Number(detail.amount || 0),
        percentage: calculateAllocationPercentage(detail.amount, total),
        period_total: total,
      })),
    };
  };

  const refreshEditableHistory = async (clientId, options = {}) => {
    if (!clientId) return;

    const clientRes = await clientAPI.getById(clientId);
    const updatedClient = clientRes.data.data;
    if (!updatedClient) return;

    const updatedHierarchy = groupBillingRows(updatedClient.billing_rows || []);
    setHistoryClient(updatedClient);

    const targetGroupKey = options.groupKey || '';
    if (options.editScope === 'billingGroup') {
      const updatedGroup = updatedHierarchy.find((group) => normalizeGroupKey(group) === targetGroupKey);
      setHistoryGroupKey(targetGroupKey);
      setHistoryServiceType(null);
      setHistoryChipLabel(updatedGroup?.billing_name || historyChipLabel);
      setHistoryDrafts(updatedGroup ? buildBillingGroupHistoryDrafts(updatedGroup) : []);
      return;
    }

    const updatedGroup = updatedHierarchy.find((group) => normalizeGroupKey(group) === targetGroupKey)
      || updatedHierarchy.find((group) =>
        group.service_types.some((serviceType) => serviceType.service_type_id === options.serviceTypeId)
      );

    if (updatedGroup) {
      const updatedServiceType = updatedGroup.service_types.find(
        (serviceType) => serviceType.service_type_id === options.serviceTypeId
      );
      if (updatedServiceType) {
        setHistoryGroupKey(normalizeGroupKey(updatedGroup));
        setHistoryServiceType(updatedServiceType);
        setHistoryChipLabel(updatedServiceType.service_type_name || historyChipLabel);
        setHistoryDrafts(buildServiceTypeHistoryDrafts(updatedServiceType));
        return;
      }
    }

    setHistoryDrafts([]);
  };

  const handleViewServiceTypeHistory = (client, group, serviceType) => {
    setHistoryClient(client);
    setHistoryTitle('Service Type Periods');
    setHistoryChipLabel(serviceType?.service_type_name || '');
    setHistoryLoading(false);
    setHistoryEditable(true);
    setHistoryEditScope('serviceType');
    setHistoryGroupKey(normalizeGroupKey(group));
    setHistoryServiceType(serviceType);
    setBillingHistory([]);
    setHistoryDrafts(buildServiceTypeHistoryDrafts(serviceType));
    setHistoryDialogOpen(true);
  };

  const handleHistoryDraftDetailChange = (draftIndex, detailIndex, field, value) => {
    setError('');
    setHistoryDrafts((prev) => prev.map((draft, currentDraftIndex) => {
      if (currentDraftIndex !== draftIndex) return draft;
      const nextDraft = {
        ...draft,
        details: draft.details.map((detail, currentDetailIndex) => (
          currentDetailIndex === detailIndex
            ? { ...detail, [field]: field === 'amount' ? value : value || '' }
            : detail
        ))
      };
      return syncClientHistoryDraft(nextDraft);
    }));
  };

  const handleSaveHistoryPeriod = async (draftIndex) => {
    if (!historyClient?.id) return;

    const draft = historyDrafts[draftIndex];
    const periodLabel = `${draft?.start_period || '-'} to ${draft?.end_period || 'Open'}`;
    if (!draft?.end_period) {
      setError(`Only closed periods can be updated from history (${periodLabel})`);
      return;
    }

    const invalidDetail = (draft.details || []).find((detail) =>
      detail.amount === '' || Number.isNaN(Number(detail.amount)) || Number(detail.amount) < 0
    );
    if (invalidDetail) {
      setError(`Enter a valid amount for every line in ${periodLabel}`);
      return;
    }

    const duplicateLines = new Set();
    for (const detail of (draft.details || [])) {
      const key = `${detail.service_type_id || ''}||${detail.bill_from_id || ''}||${detail.reviewer_id || ''}`;
      if (duplicateLines.has(key)) {
        setError(`Duplicate bill from and reviewer combinations are not allowed in ${periodLabel}`);
        return;
      }
      duplicateLines.add(key);
    }

    const payload = {
      original_start_period: draft.start_period,
      original_end_period: draft.end_period,
      rows: (draft.details || []).map((detail) => ({
        row_id: detail.row_id,
        bill_from_id: detail.bill_from_id || undefined,
        reviewer_id: detail.reviewer_id || undefined,
        amount: Number(detail.amount || 0),
      })),
    };

    const savingKey = getHistoryDraftKey(draft);
    setHistorySavingKey(savingKey);
    setError('');

    try {
      await clientAPI.updateBillingPeriod(historyClient.id, payload);
      toast.success(`Updated ${periodLabel}`);
      await loadClients();
      if (billingPopupClient?.id === historyClient.id) {
        await refreshBillingPopupForClient(historyClient.id);
      }
      await refreshEditableHistory(historyClient.id, {
        editScope: historyEditScope,
        groupKey: historyGroupKey,
        serviceTypeId: historyServiceType?.service_type_id || draft.details?.[0]?.service_type_id,
      });
    } catch (err) {
      setError(err.response?.data?.message || `Failed to update ${periodLabel}`);
    } finally {
      setHistorySavingKey('');
    }
  };

  const handleOpenEditHistoryDetail = (detail) => {
    setEditHistoryDetail(detail);
    setEditHistoryForm({
      bill_from_id: detail.bill_from_id || '',
      reviewer_id: detail.reviewer_id || '',
      amount: detail.amount || ''
    });
    setError('');
    setEditHistoryDialogOpen(true);
  };

  const handleCloseEditHistoryDetail = () => {
    setEditHistoryDialogOpen(false);
    setEditHistoryDetail(null);
    setEditHistoryForm({ bill_from_id: '', reviewer_id: '', amount: '' });
    setError('');
  };

  const handleSubmitEditHistoryDetail = async () => {
    if (!editHistoryDetail?.row_id || !historyClient?.id) return;
    if (editHistoryForm.amount === '' || parseFloat(editHistoryForm.amount) < 0) {
      setError('Valid amount is required');
      return;
    }

    try {
      await clientAPI.updateBillingRow(historyClient.id, editHistoryDetail.row_id, {
        bill_from_id: editHistoryForm.bill_from_id || null,
        reviewer_id: editHistoryForm.reviewer_id || null,
        amount: parseFloat(editHistoryForm.amount) || 0,
      });
      toast.success('History record updated');
      handleCloseEditHistoryDetail();
      await loadClients();
      if (billingPopupClient?.id === historyClient.id) {
        await refreshBillingPopupForClient(historyClient.id);
      }
      await refreshEditableHistory(historyClient.id, {
        editScope: historyEditScope || 'serviceType',
        groupKey: historyGroupKey,
        serviceTypeId: historyServiceType?.service_type_id || editHistoryDetail.service_type_id,
      });
    } catch (err) {
      setError(err.response?.data?.message || 'Failed to update history record');
    }
  };

  // ─── Filtered  Clients ─────────────────────────────────────────
  const filteredClients = useMemo(() => {
    return clients.filter(c => {
      if (!c.name.toLowerCase().includes(searchTerm.toLowerCase())) return false;
      if (statusFilter === 'active') return c.is_active !== false;
      if (statusFilter === 'inactive') return c.is_active === false;
      return true;
    });
  }, [clients, searchTerm, statusFilter]);

  // ═══════════════════════════════════════════════════════════════
  // RENDER
  // ═══════════════════════════════════════════════════════════════
  return (
    <Box sx={{ p: 3, bgcolor: COLORS.background, minHeight: '100vh' }}>

      {/* ─── Header Card ─── */}
      <PageHeader
        eyebrow="Group Master"
        title="Group management"
        actions={[
          <HoverActionButton key="add" icon={<Add fontSize="small" />} label="Add Group" onClick={() => handleOpenDialog()} tone="mint" />,
          <HoverActionButton key="refresh" icon={<Refresh fontSize="small" />} label="Refresh" onClick={loadClients} tone="peach" />,
        ]}
        chips={[
          { label: `${monthlyTotal.client_count} active groups` },
          { label: formatAmount(monthlyTotal.total_amount) },
        ]}
      />

      <Grid container spacing={2} sx={{ mb: 2.5 }}>
        <Grid item xs={12} md={6}>
          <MetricCard eyebrow="Monthly Total" title="Active billing" value={formatAmount(monthlyTotal.total_amount)} icon={<TrendingUp fontSize="small" />} tone="mint" />
        </Grid>
        <Grid item xs={12} md={6}>
          <MetricCard eyebrow="Groups" title="Active groups" value={String(monthlyTotal.client_count)} icon={<Person fontSize="small" />} tone="peach" />
        </Grid>
      </Grid>

      <SectionCard title="Group ledger" tone="sage" contentSx={{ p: 2 }}>
        <Box sx={{ display: 'flex', gap: 2, mb: 2, alignItems: 'center' }}>
          <TextField
            placeholder="Search groups..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            InputProps={{ startAdornment: <Search sx={{ color: COLORS.textSecondary, mr: 1 }} /> }}
            size="small"
            sx={{
              width: 320,
              '& .MuiOutlinedInput-root': {
                borderRadius: '10px', bgcolor: COLORS.tableHeader,
                '& fieldset': { border: 'none' },
                '&:hover': { bgcolor: alpha(COLORS.primary, 0.05) },
                '&.Mui-focused': { bgcolor: 'white', boxShadow: `0 0 0 2px ${alpha(COLORS.primary, 0.2)}` }
              }
            }}
          />
          <ToggleButtonGroup value={statusFilter} exclusive size="small" onChange={(_, v) => v && setStatusFilter(v)}>
            <ToggleButton value="active">Active</ToggleButton>
            <ToggleButton value="inactive">Inactive</ToggleButton>
            <ToggleButton value="all">All</ToggleButton>
          </ToggleButtonGroup>
        </Box>
        {loading ? (
            <Box sx={{ p: 6, textAlign: 'center' }}>
              <Typography sx={{ color: COLORS.textSecondary }}>Loading groups...</Typography>
            </Box>
          ) : filteredClients.length === 0 ? (
            <Box sx={{ p: 6, textAlign: 'center' }}>
              <Business sx={{ fontSize: 48, color: alpha(COLORS.textSecondary, 0.3), mb: 2 }} />
              <Typography sx={{ color: COLORS.textSecondary }}>No groups found</Typography>
            </Box>
          ) : (
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow sx={{ bgcolor: COLORS.tableHeader }}>
                    <TableCell padding="checkbox" sx={{ borderBottom: `2px solid ${COLORS.tableBorder}` }} />
                    <TableCell sx={{ fontWeight: 600, color: COLORS.textPrimary, borderBottom: `2px solid ${COLORS.tableBorder}` }}>Group Name</TableCell>
                    <TableCell align="center" sx={{ fontWeight: 600, color: COLORS.textPrimary, borderBottom: `2px solid ${COLORS.tableBorder}` }}>Billing Sources</TableCell>
                    <TableCell align="right" sx={{ fontWeight: 600, color: COLORS.textPrimary, borderBottom: `2px solid ${COLORS.tableBorder}` }}>Monthly Total</TableCell>
                    <TableCell align="center" sx={{ fontWeight: 600, color: COLORS.textPrimary, borderBottom: `2px solid ${COLORS.tableBorder}` }}>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {filteredClients.map((client) => (
                    <ClientDetailRow
                      key={client.id}
                      client={client}
                      onEdit={handleOpenDialog}
                      onDelete={handleOpenDeleteDialog}
                      onOpenBillingPopup={handleOpenBillingPopup}
                      canDelete
                    />
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          )}
      </SectionCard>


      {/* ═══════════════════════════════════════════════════════════
          Client Dialog (Add/Edit client name)
          ═══════════════════════════════════════════════════════════ */}
      <Dialog
        open={dialogOpen}
        onClose={handleCloseDialog}
        maxWidth="sm"
        fullWidth
        PaperProps={{ sx: { borderRadius: '16px' } }}
      >
        <DialogTitle sx={{
          bgcolor: COLORS.tableHeader,
          borderBottom: `1px solid ${COLORS.tableBorder}`,
          fontWeight: 600, color: COLORS.textPrimary
        }}>
              {selectedClient ? 'Edit Group' : 'Add New Group'}
        </DialogTitle>
        <DialogContent sx={{ pt: 3 }}>
          {error && <Alert severity="error" sx={{ mb: 2, borderRadius: '8px' }}>{error}</Alert>}
          <TextField
            fullWidth
            label="Group Name"
            value={formData.name}
            onChange={(e) => setFormData(prev => ({ ...prev, name: e.target.value }))}
            margin="normal"
            required
            autoFocus
            sx={{ '& .MuiOutlinedInput-root': { borderRadius: '10px' } }}
          />
          {selectedClient && (
            <Alert severity="info" sx={{ mt: 2, borderRadius: '8px' }}>
              Group billing is period-based. Use the arrow on the group row to manage the live setup and closed history.
            </Alert>
          )}
        </DialogContent>
        <DialogActions sx={{ p: 2.5, borderTop: `1px solid ${COLORS.tableBorder}` }}>
          <Button onClick={handleCloseDialog} sx={{ borderRadius: '8px', textTransform: 'none', color: COLORS.textSecondary }}>
            Cancel
          </Button>
          <Button
            onClick={handleSubmitClient}
            variant="contained"
            sx={{ borderRadius: '8px', textTransform: 'none', bgcolor: COLORS.primary, '&:hover': { bgcolor: '#0f766e' } }}
          >
            {selectedClient ? 'Update' : 'Create'}
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog
        open={deleteDialogOpen}
        onClose={handleCloseDeleteDialog}
        maxWidth="md"
        fullWidth
        PaperProps={{ sx: { borderRadius: '16px' } }}
      >
        <DialogTitle sx={{
          bgcolor: alpha('#DC2626', 0.06),
          borderBottom: `1px solid ${alpha('#DC2626', 0.12)}`,
          fontWeight: 700,
          color: '#991B1B'
        }}>
          Delete Group
        </DialogTitle>
        <DialogContent sx={{ pt: 3 }}>
          {error && <Alert severity="error" sx={{ mb: 2, borderRadius: '8px' }}>{error}</Alert>}
          {depsLoading ? (
            <Typography color="text.secondary">Checking dependencies...</Typography>
          ) : deleteDeps.length > 0 ? (
            <>
              <Alert severity="warning" sx={{ borderRadius: '10px', mb: 2 }}>
                {deleteMessage || `"${clientToDelete?.name}" is linked to records in other modules. Update those lines first, then delete this group.`}
              </Alert>
              <Typography variant="body2" sx={{ color: COLORS.textPrimary, mb: 1.5 }}>
                {summarizeDependencies(deleteDeps)}
              </Typography>
              {deleteLinkedItems.map((item, idx) => (
                <Typography key={idx} variant="body2" sx={{ ml: 1, mb: 0.75, color: COLORS.textPrimary }}>
                  • {item.label} — {item.line}
                </Typography>
              ))}
            </>
          ) : (
            <Typography variant="body2" sx={{ color: COLORS.textPrimary, lineHeight: 1.7 }}>
              {clientToDelete
                ? `Delete "${clientToDelete.name}"? Its own billing setup rows will be removed with the group.`
                : 'Delete this group?'}
            </Typography>
          )}
        </DialogContent>
        <DialogActions sx={{ p: 2.5, borderTop: `1px solid ${COLORS.tableBorder}` }}>
          <Button
            onClick={handleCloseDeleteDialog}
            sx={{ borderRadius: '8px', textTransform: 'none', color: COLORS.textSecondary }}
          >
            Cancel
          </Button>
          <Button
            onClick={handleDeleteClient}
            variant="contained"
            color="error"
            disabled={depsLoading || deleteDeps.length > 0}
            sx={{ borderRadius: '8px', textTransform: 'none', boxShadow: 'none' }}
          >
            Delete Group
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog
        open={billingGroupDialogOpen}
        onClose={handleCloseBillingGroupDialog}
        maxWidth="lg"
        fullWidth
        PaperProps={{ sx: { borderRadius: '16px' } }}
      >
        <DialogTitle sx={{
          bgcolor: COLORS.tableHeader,
          borderBottom: `1px solid ${COLORS.tableBorder}`,
          fontWeight: 600, color: COLORS.textPrimary
        }}>
          Add Client
          {billingGroupClient && (
            <Typography variant="body2" sx={{ color: COLORS.textSecondary, fontWeight: 400, mt: 0.5 }}>
              Group: {billingGroupClient.name}
            </Typography>
          )}
        </DialogTitle>
        <DialogContent sx={{ pt: 3 }}>
          {error && <Alert severity="error" sx={{ mb: 2, borderRadius: '8px' }}>{error}</Alert>}
          {conflictingGroups.length > 0 && (
            <Alert severity="warning" sx={{ mb: 2, borderRadius: '8px' }}>
              This client is already added to: <strong>{conflictingGroups.join(', ')}</strong>. You can still save.
            </Alert>
          )}

          {/* Group Header Fields */}
          <Paper elevation={0} sx={{ p: 2.5, mb: 3, bgcolor: alpha(COLORS.primary, 0.04), borderRadius: '12px', border: `1px solid ${alpha(COLORS.primary, 0.15)}` }}>
            <Typography variant="subtitle2" sx={{ fontWeight: 600, color: COLORS.textPrimary, mb: 2 }}>
              Client Details
            </Typography>
            <Grid container spacing={2}>
              <Grid item xs={12} md={4}>
                <TextField
                  fullWidth label="Client" required
                  value={billingGroupForm.billing_name}
                  size="small"
                  InputProps={{
                    readOnly: true,
                    endAdornment: (
                      <InputAdornment position="end">
                        <Button
                          size="small"
                          variant="text"
                          disabled={billingGroupLockedGroup}
                          onClick={() => openPicker({
                            title: 'Select Client',
                            loadOptions: async () => {
                              const list = activeBillingNameMasters;
                              return list.map((b) => ({
                                value: b.id,
                                label: b.name,
                                secondary: [b.gstin, b.pan].filter(Boolean).join(' | ')
                              }));
                            },
                            onSelect: handleSelectBillingNameMaster
                          })}
                          sx={{ textTransform: 'none', fontWeight: 600 }}
                        >
                          Select
                        </Button>
                      </InputAdornment>
                    )
                  }}
                  sx={{ '& .MuiOutlinedInput-root': { borderRadius: '8px' } }}
                />
              </Grid>
              <Grid item xs={12} md={4}>
                <TextField
                  fullWidth label="GSTIN"
                  value={billingGroupForm.gstin}
                  size="small"
                  InputProps={{ readOnly: true }}
                  helperText="Auto-filled from selected Client"
                  sx={{ '& .MuiOutlinedInput-root': { borderRadius: '8px' } }}
                />
              </Grid>
              <Grid item xs={12} md={4}>
                <TextField
                  fullWidth label="PAN"
                  value={billingGroupForm.pan}
                  size="small"
                  InputProps={{ readOnly: true }}
                  helperText="Auto-filled from selected Client"
                  sx={{ '& .MuiOutlinedInput-root': { borderRadius: '8px' } }}
                />
              </Grid>
            </Grid>
          </Paper>

          {billingGroupForm.service_types.map((serviceType, serviceTypeIndex) => (
            <Paper
              key={serviceTypeIndex}
              elevation={0}
              sx={{
                mb: 2, p: 2.5,
                border: `1px solid ${COLORS.tableBorder}`,
                borderRadius: '12px',
                bgcolor: alpha(COLORS.accent, 0.03)
              }}
            >
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                <Typography variant="subtitle2" sx={{ fontWeight: 600, color: COLORS.textPrimary }}>
                  Service Type #{serviceTypeIndex + 1}
                </Typography>
                {billingGroupForm.service_types.length > 1 && !billingGroupLockedServiceType && (
                  <IconButton
                    size="small"
                    onClick={() => removeServiceType(serviceTypeIndex)}
                    sx={{ color: '#DC2626', '&:hover': { bgcolor: alpha('#DC2626', 0.08) } }}
                  >
                    <RemoveCircle fontSize="small" />
                  </IconButton>
                )}
              </Box>

              <Grid container spacing={2} sx={{ mb: 2 }}>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    size="small"
                    label="Service Type"
                    required
                    value={getServiceTypeLabel(serviceType.service_type_id)}
                    InputProps={{
                      readOnly: true,
                      endAdornment: (
                        <InputAdornment position="end">
                          <Button
                            size="small"
                            variant="text"
                            disabled={billingGroupLockedServiceType}
                            onClick={() => openPicker({
                              title: 'Select Service Type',
                              loadOptions: async () => {
                                const res = await serviceTypeAPI.getAll();
                                const list = (res.data.data || []).filter((st) => st.is_active !== false);
                                return list.map((st) => ({ value: st.id, label: st.name }));
                              },
                              onSelect: (value) => {
                                handleServiceTypeChange(serviceTypeIndex, 'service_type_id', value);
                                closePicker();
                              }
                            })}
                            sx={{ textTransform: 'none', fontWeight: 600 }}
                          >
                            Select
                          </Button>
                        </InputAdornment>
                      )
                    }}
                    sx={{ '& .MuiOutlinedInput-root': { borderRadius: '8px' } }}
                  />
                </Grid>
                <Grid item xs={12} md={2}>
                  <TextField
                    fullWidth
                    size="small"
                    label="Start Period"
                    required
                    value={serviceType.start_period || ''}
                    InputProps={{
                      readOnly: true,
                      endAdornment: (
                        <InputAdornment position="end">
                          <Button
                            size="small"
                            variant="text"
                            disabled={serviceType.lock_periods}
                            onClick={() => openPicker({
                              title: 'Select Start Period',
                              loadOptions: async () => {
                                const res = await servicePeriodAPI.getAll();
                                const list = (res.data.data || []).filter((sp) => sp.is_active);
                                const filtered = serviceType.min_start_period
                                  ? list.filter((sp) => comparePeriods(sp.display_name, serviceType.min_start_period) >= 0)
                                  : list;
                                return filtered.map((sp) => ({ value: sp.display_name, label: sp.display_name }));
                              },
                              onSelect: (value) => {
                                handleServiceTypeChange(serviceTypeIndex, 'start_period', value);
                                if (serviceType.end_period && comparePeriods(serviceType.end_period, value) < 0) {
                                  handleServiceTypeChange(serviceTypeIndex, 'end_period', '');
                                }
                                closePicker();
                              }
                            })}
                            sx={{ textTransform: 'none', fontWeight: 600 }}
                          >
                            Select
                          </Button>
                        </InputAdornment>
                      )
                    }}
                    sx={{ '& .MuiOutlinedInput-root': { borderRadius: '8px' } }}
                  />
                </Grid>
                <Grid item xs={12} md={2}>
                  <TextField
                    fullWidth
                    size="small"
                    label="End Period"
                    value={serviceType.end_period || 'Open'}
                    InputProps={{
                      readOnly: true,
                      endAdornment: (
                        <InputAdornment position="end">
                          <Button
                            size="small"
                            variant="text"
                            disabled={serviceType.lock_periods}
                            onClick={() => openPicker({
                              title: 'Select End Period',
                              loadOptions: async () => {
                                const res = await servicePeriodAPI.getAll();
                                const list = (res.data.data || []).filter((sp) => sp.is_active);
                                const start = serviceType.start_period;
                                const filtered = start ? list.filter((sp) => comparePeriods(sp.display_name, start) >= 0) : list;
                                return [{ value: '', label: 'Open' }, ...filtered.map((sp) => ({ value: sp.display_name, label: sp.display_name }))];
                              },
                              onSelect: (value) => {
                                handleServiceTypeChange(serviceTypeIndex, 'end_period', value);
                                closePicker();
                              }
                            })}
                            sx={{ textTransform: 'none', fontWeight: 600 }}
                          >
                            Select
                          </Button>
                        </InputAdornment>
                      )
                    }}
                    sx={{ '& .MuiOutlinedInput-root': { borderRadius: '8px' } }}
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <Paper
                    elevation={0}
                    sx={{
                      height: '100%',
                      minHeight: 56,
                      px: 1.5,
                      display: 'flex',
                      flexDirection: 'column',
                      justifyContent: 'center',
                      bgcolor: 'white',
                      borderRadius: '8px',
                      border: `1px solid ${alpha(COLORS.accent, 0.25)}`
                    }}
                  >
                    <Typography variant="caption" sx={{ color: COLORS.textSecondary, fontWeight: 500 }}>
                      Service Type Total/Month
                    </Typography>
                    <Typography variant="subtitle2" sx={{ color: COLORS.accent, fontWeight: 700 }}>
                      {formatAmount(sumBillFromAmounts(serviceType.bill_froms))}
                    </Typography>
                  </Paper>
                </Grid>
              </Grid>

              {serviceType.bill_froms.map((bf, bfIndex) => (
                <Paper
                  key={bfIndex}
                  elevation={0}
                  sx={{
                    mb: 2,
                    p: 2,
                    bgcolor: 'white',
                    borderRadius: '10px',
                    border: `1px solid ${alpha(COLORS.tableBorder, 0.7)}`
                  }}
                >
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                    <Typography variant="subtitle2" sx={{ fontWeight: 600, color: COLORS.textPrimary }}>
                      Bill From #{bfIndex + 1}
                    </Typography>
                    {serviceType.bill_froms.length > 1 && (
                      <IconButton
                        size="small"
                        onClick={() => removeBillFrom(serviceTypeIndex, bfIndex)}
                        disabled={billingGroupLockedBillFrom}
                        sx={{ color: '#DC2626', '&:hover': { bgcolor: alpha('#DC2626', 0.08) } }}
                      >
                        <RemoveCircle fontSize="small" />
                      </IconButton>
                    )}
                  </Box>

                  <Grid container spacing={2} sx={{ mb: 2 }}>
                    <Grid item xs={12} md={6}>
                      <TextField
                        fullWidth
                        size="small"
                        label="Bill From"
                        value={bf.bill_from_id ? getBillFromLabel(bf.bill_from_id) : 'None'}
                        InputProps={{
                          readOnly: true,
                          endAdornment: (
                            <InputAdornment position="end">
                              <Button
                                size="small"
                                variant="text"
                                disabled={billingGroupLockedBillFrom}
                                onClick={() => {
                                  // De-duplicate: exclude already-selected bill froms in this service type
                                  const usedBillFromIds = new Set(
                                    billingGroupForm.service_types[serviceTypeIndex].bill_froms
                                      .filter((_, i) => i !== bfIndex)
                                      .map((b) => b.bill_from_id)
                                      .filter(Boolean)
                                  );
                                  openPicker({
                                    title: 'Select Bill From',
                                    loadOptions: async () => {
                                      const res = await billFromAPI.getAll();
                                      const list = (res.data.data || []).filter((b) => b.is_active !== false && !usedBillFromIds.has(b.id));
                                      return [{ value: '', label: 'None' }, ...list.map((b) => ({ value: b.id, label: b.name }))];
                                    },
                                    onSelect: (value) => {
                                      handleBillFromChange(serviceTypeIndex, bfIndex, 'bill_from_id', value);
                                      closePicker();
                                    }
                                  });
                                }}
                                sx={{ textTransform: 'none', fontWeight: 600 }}
                              >
                                Select
                              </Button>
                            </InputAdornment>
                          )
                        }}
                        sx={{ '& .MuiOutlinedInput-root': { borderRadius: '8px' } }}
                      />
                    </Grid>
                    <Grid item xs={12} md={6}>
                      <Paper
                        elevation={0}
                        sx={{
                          height: '100%',
                          minHeight: 56,
                          px: 1.5,
                          display: 'flex',
                          flexDirection: 'column',
                          justifyContent: 'center',
                          bgcolor: alpha(COLORS.secondary, 0.08),
                          borderRadius: '8px',
                          border: `1px solid ${alpha(COLORS.secondary, 0.2)}`
                        }}
                      >
                        <Typography variant="caption" sx={{ color: COLORS.textSecondary, fontWeight: 500 }}>
                          Bill From Total
                        </Typography>
                        <Typography variant="subtitle2" sx={{ color: '#047857', fontWeight: 700 }}>
                          {formatAmount(sumReviewerAmounts(bf.reviewers))}
                        </Typography>
                      </Paper>
                    </Grid>
                  </Grid>

                  {bf.reviewers.map((rev, revIndex) => (
                    <Box
                      key={revIndex}
                      sx={{
                        display: 'flex', alignItems: 'center', gap: 1.5, mb: 1.5,
                        p: 1.5, bgcolor: alpha(COLORS.primary, 0.02), borderRadius: '8px',
                        border: `1px solid ${alpha(COLORS.tableBorder, 0.6)}`
                      }}
                    >
                      <TextField
                        label="Reviewer"
                        size="small"
                        value={rev.reviewer_id ? getReviewerLabel(rev.reviewer_id) : 'None'}
                        sx={{ minWidth: 220, '& .MuiOutlinedInput-root': { borderRadius: '8px' } }}
                        InputProps={{
                          readOnly: true,
                          endAdornment: (
                            <InputAdornment position="end">
                              <Button
                                size="small"
                                variant="text"
                                onClick={() => {
                                  // De-duplicate: exclude reviewers already selected in this bill from
                                  const usedReviewerIds = new Set(
                                    billingGroupForm.service_types[serviceTypeIndex].bill_froms[bfIndex].reviewers
                                      .filter((_, i) => i !== revIndex)
                                      .map((r) => r.reviewer_id)
                                      .filter(Boolean)
                                  );
                                  openPicker({
                                    title: 'Select Reviewer',
                                    loadOptions: async () => {
                                      const res = await teamAPI.getReviewers();
                                      const list = (res.data.data || []).filter((r) => !usedReviewerIds.has(r.id));
                                      return [{ value: '', label: 'None' }, ...list.map((r) => ({ value: r.id, label: r.name }))];
                                    },
                                    onSelect: (value) => {
                                      handleReviewerChange(serviceTypeIndex, bfIndex, revIndex, 'reviewer_id', value);
                                      closePicker();
                                    }
                                  });
                                }}
                                sx={{ textTransform: 'none', fontWeight: 600 }}
                              >
                                Select
                              </Button>
                            </InputAdornment>
                          )
                        }}
                      />

                      <TextField
                        label="Amount/Month"
                        type="number"
                        required
                        size="small"
                        value={rev.amount}
                        onChange={(e) => handleReviewerChange(serviceTypeIndex, bfIndex, revIndex, 'amount', e.target.value)}
                        InputProps={{ startAdornment: <Typography sx={{ mr: 0.5, color: COLORS.textSecondary, fontSize: '0.85rem' }}>â‚¹</Typography> }}
                        sx={{ minWidth: 150, '& .MuiOutlinedInput-root': { borderRadius: '8px' } }}
                      />

                      {bf.reviewers.length > 1 && (
                        <IconButton
                          size="small"
                          onClick={() => removeReviewer(serviceTypeIndex, bfIndex, revIndex)}
                          sx={{ color: '#DC2626', ml: 'auto', '&:hover': { bgcolor: alpha('#DC2626', 0.08) } }}
                        >
                          <Delete fontSize="small" />
                        </IconButton>
                      )}
                    </Box>
                  ))}

                  <Button
                    size="small"
                    startIcon={<Add />}
                    onClick={() => addReviewer(serviceTypeIndex, bfIndex)}
                    sx={{
                      mt: 0.5, color: COLORS.primary, textTransform: 'none', fontWeight: 500,
                      '&:hover': { bgcolor: alpha(COLORS.primary, 0.08) }
                    }}
                  >
                    Add Reviewer
                  </Button>
                </Paper>
              ))}

              <Button
                startIcon={<AddCircle />}
                onClick={() => addBillFrom(serviceTypeIndex)}
                disabled={billingGroupLockedBillFrom}
                sx={{
                  color: COLORS.secondary, textTransform: 'none', fontWeight: 600,
                  borderRadius: '8px', border: `1px dashed ${COLORS.secondary}`,
                  width: '100%', py: 1,
                  '&:hover': { bgcolor: alpha(COLORS.secondary, 0.08) }
                }}
              >
                Add Another Bill From
              </Button>
            </Paper>
          ))}

          <Grid container spacing={2} sx={{ mb: 2 }}>
            <Grid item xs={12} md={3}>
              <Paper elevation={0} sx={{ p: 2, bgcolor: alpha(COLORS.primary, 0.05), borderRadius: '12px', border: `1px solid ${alpha(COLORS.primary, 0.15)}` }}>
                <Typography variant="caption" sx={{ color: COLORS.textSecondary, fontWeight: 500 }}>
                  Overall Monthly Total
                </Typography>
                <Typography variant="h6" sx={{ color: COLORS.primary, fontWeight: 700 }}>
                  {formatAmount(billingGroupOverallTotal)}
                </Typography>
              </Paper>
            </Grid>
            <Grid item xs={12} md={3}>
              <Paper elevation={0} sx={{ p: 2, bgcolor: alpha(COLORS.warning, 0.12), borderRadius: '12px', border: `1px solid ${alpha(COLORS.warning, 0.3)}` }}>
                <Typography variant="caption" sx={{ color: COLORS.textSecondary, fontWeight: 500 }}>
                  Service Types
                </Typography>
                <Typography variant="h6" sx={{ color: '#92400E', fontWeight: 700 }}>
                  {billingGroupServiceTypeCount}
                </Typography>
              </Paper>
            </Grid>
            <Grid item xs={12} md={3}>
              <Paper elevation={0} sx={{ p: 2, bgcolor: alpha(COLORS.secondary, 0.08), borderRadius: '12px', border: `1px solid ${alpha(COLORS.secondary, 0.2)}` }}>
                <Typography variant="caption" sx={{ color: COLORS.textSecondary, fontWeight: 500 }}>
                  Billing Sources
                </Typography>
                <Typography variant="h6" sx={{ color: '#047857', fontWeight: 700 }}>
                  {billingGroupBillFromCount}
                </Typography>
              </Paper>
            </Grid>
            <Grid item xs={12} md={3}>
              <Paper elevation={0} sx={{ p: 2, bgcolor: alpha(COLORS.accent, 0.08), borderRadius: '12px', border: `1px solid ${alpha(COLORS.accent, 0.25)}` }}>
                <Typography variant="caption" sx={{ color: COLORS.textSecondary, fontWeight: 500 }}>
                  Reviewers Added
                </Typography>
                <Typography variant="h6" sx={{ color: COLORS.accent, fontWeight: 700 }}>
                  {billingGroupReviewerCount}
                </Typography>
              </Paper>
            </Grid>
          </Grid>

          {!billingGroupLockedServiceType && (
            <Button
              startIcon={<AddCircle />}
              onClick={addServiceType}
              sx={{
                color: COLORS.accent, textTransform: 'none', fontWeight: 600,
                borderRadius: '8px', border: `1px dashed ${COLORS.accent}`,
                width: '100%', py: 1.2,
                '&:hover': { bgcolor: alpha(COLORS.accent, 0.08) }
              }}
            >
              Add Another Service Type
            </Button>
          )}


        </DialogContent>
        <DialogActions sx={{ p: 2.5, borderTop: `1px solid ${COLORS.tableBorder}` }}>
          <Button onClick={handleCloseBillingGroupDialog} sx={{ borderRadius: '8px', textTransform: 'none', color: COLORS.textSecondary }}>
            Cancel
          </Button>
          <Button
            onClick={handleSubmitBillingGroup}
            variant="contained"
            sx={{ borderRadius: '8px', textTransform: 'none', bgcolor: COLORS.primary, '&:hover': { bgcolor: '#0f766e' } }}
          >
            Save
          </Button>
        </DialogActions>
      </Dialog>

      {/* ═══════════════════════════════════════════════════════════
          Edit Single Billing Row Dialog
          ═══════════════════════════════════════════════════════════ */}
      <Dialog
        open={editRowDialogOpen}
        onClose={handleCloseEditRow}
        maxWidth="md"
        fullWidth
        PaperProps={{ sx: { borderRadius: '16px' } }}
      >
        <DialogTitle sx={{
          bgcolor: COLORS.tableHeader,
          borderBottom: `1px solid ${COLORS.tableBorder}`,
          fontWeight: 600, color: COLORS.textPrimary
        }}>
          Edit Reviewer Entry
          {editRowClient && (
            <Typography variant="body2" sx={{ color: COLORS.textSecondary, fontWeight: 400, mt: 0.5 }}>
              Group: {editRowClient.name}
            </Typography>
          )}
        </DialogTitle>
        <DialogContent sx={{ pt: 3 }}>
          {error && <Alert severity="error" sx={{ mb: 2, borderRadius: '8px' }}>{error}</Alert>}
          <Paper elevation={0} sx={{ p: 2, mb: 2.5, bgcolor: alpha(COLORS.primary, 0.04), borderRadius: '12px', border: `1px solid ${alpha(COLORS.primary, 0.15)}` }}>
            <Typography variant="body2" sx={{ color: COLORS.textSecondary }}>
              Client: <strong style={{ color: COLORS.textPrimary }}>{editRowForm.billing_name || '-'}</strong>
            </Typography>
            <Typography variant="body2" sx={{ color: COLORS.textSecondary }}>
              Service Type: <strong style={{ color: COLORS.textPrimary }}>{getServiceTypeLabel(editRowForm.service_type_id) || '-'}</strong>
            </Typography>
            <Typography variant="body2" sx={{ color: COLORS.textSecondary }}>
              Period: <strong style={{ color: COLORS.textPrimary }}>{formatPeriodRange(editRowForm.start_period, editRowForm.end_period)}</strong>
            </Typography>
            <Typography variant="body2" sx={{ color: COLORS.textSecondary }}>
              Bill From: <strong style={{ color: COLORS.textPrimary }}>{editRowForm.bill_from_id ? getBillFromLabel(editRowForm.bill_from_id) : 'None'}</strong>
            </Typography>
          </Paper>

          <Grid container spacing={2.5}>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Reviewer"
                value={editRowForm.reviewer_id ? getReviewerLabel(editRowForm.reviewer_id) : 'None'}
                InputProps={{
                  readOnly: true,
                  endAdornment: (
                    <InputAdornment position="end">
                      <Button
                        size="small"
                        variant="text"
                        onClick={() => openPicker({
                          title: 'Select Reviewer',
                          loadOptions: async () => {
                            const res = await teamAPI.getReviewers();
                            const list = res.data.data || [];
                            return [{ value: '', label: 'None' }, ...list.map((r) => ({ value: r.id, label: r.name }))];
                          },
                          onSelect: (value) => {
                            handleEditRowFieldChange('reviewer_id', value);
                            closePicker();
                          }
                        })}
                        sx={{ textTransform: 'none', fontWeight: 600 }}
                      >
                        Select
                      </Button>
                    </InputAdornment>
                  )
                }}
                sx={{ '& .MuiOutlinedInput-root': { borderRadius: '10px' } }}
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Amount per Month"
                type="number"
                required
                value={editRowForm.amount}
                onChange={(e) => handleEditRowFieldChange('amount', e.target.value)}
                InputProps={{ startAdornment: <Typography sx={{ mr: 1, color: COLORS.textSecondary }}>₹</Typography> }}
                sx={{ '& .MuiOutlinedInput-root': { borderRadius: '10px' } }}
              />
            </Grid>
          </Grid>
        </DialogContent>
        <DialogActions sx={{ p: 2.5, borderTop: `1px solid ${COLORS.tableBorder}` }}>
          <Button onClick={handleCloseEditRow} sx={{ borderRadius: '8px', textTransform: 'none', color: COLORS.textSecondary }}>
            Cancel
          </Button>
          <Button
            onClick={handleSubmitEditRow}
            variant="contained"
            sx={{ borderRadius: '8px', textTransform: 'none', bgcolor: COLORS.primary, '&:hover': { bgcolor: '#0f766e' } }}
          >
            Update
          </Button>
        </DialogActions>
      </Dialog>

      {/* ═══════════════════════════════════════════════════════════
          History Dialog
          ═══════════════════════════════════════════════════════════ */}
      <Dialog
        open={historyDialogOpen}
        onClose={handleCloseHistory}
        maxWidth="lg"
        fullWidth
        PaperProps={{ sx: { borderRadius: '16px' } }}
      >
        <DialogTitle sx={{
          bgcolor: alpha(COLORS.accent, 0.1),
          borderBottom: `1px solid ${COLORS.tableBorder}`,
          fontWeight: 600, color: COLORS.textPrimary,
          display: 'flex', alignItems: 'center', gap: 1.5
        }}>
          <History sx={{ color: COLORS.accent }} />
          {historyTitle}
          {historyChipLabel && (
            <Chip
              label={historyChipLabel}
              size="small"
              sx={{ ml: 1, bgcolor: alpha(COLORS.primary, 0.1), color: COLORS.primary, fontWeight: 500 }}
            />
          )}
        </DialogTitle>
        <DialogContent sx={{ pt: 3 }}>
          {historyClient && (
            <Box sx={{ mb: 3 }}>
              <Typography variant="body2" sx={{ color: COLORS.textSecondary }}>
                Group: <strong style={{ color: COLORS.textPrimary }}>{historyClient.name}</strong>
              </Typography>
            </Box>
          )}

          {historyEditable && (
            <Alert severity="info" sx={{ mb: 2, borderRadius: '8px' }}>
              {historyEditScope === 'billingGroup'
                ? 'Closed billing-group periods can be corrected here, similar to Teams history. Update the full closed period in one save, and projected recurring revenue stays linked unless that period was manually overridden in Revenues.'
                : 'Closed periods can be corrected here, similar to Teams history. Projected recurring revenue remains linked unless that period has already been manually overridden in Revenues.'}
            </Alert>
          )}

          {historyLoading ? (
            <Box sx={{ p: 4, textAlign: 'center' }}>
              <Typography sx={{ color: COLORS.textSecondary }}>Loading history...</Typography>
            </Box>
          ) : historyEditable ? (
            historyDrafts.length === 0 ? (
              <Paper
                elevation={0}
                sx={{
                  p: 4, textAlign: 'center',
                  bgcolor: alpha(COLORS.primary, 0.03),
                  borderRadius: '12px',
                  border: `1px dashed ${COLORS.tableBorder}`
                }}
              >
                <History sx={{ fontSize: 48, color: alpha(COLORS.textSecondary, 0.3), mb: 2 }} />
                <Typography sx={{ color: COLORS.textSecondary }}>
                  No closed period records found for this service type yet.
                </Typography>
              </Paper>
            ) : (
              <Box>
                {historyDrafts.map((draft, draftIndex) => {
                  const saving = historySavingKey === getHistoryDraftKey(draft);
                  return (
                    <Paper
                      key={getHistoryDraftKey(draft)}
                      elevation={0}
                      sx={{
                        mb: 2,
                        borderRadius: '12px',
                        border: `1px solid ${COLORS.tableBorder}`,
                        overflow: 'hidden'
                      }}
                    >
                      <Box
                        sx={{
                          px: 2.5,
                          py: 1.75,
                          bgcolor: alpha(COLORS.accent, 0.06),
                          borderBottom: `1px solid ${COLORS.tableBorder}`,
                          display: 'flex',
                          justifyContent: 'space-between',
                          alignItems: 'center',
                          flexWrap: 'wrap',
                          gap: 1.5
                        }}
                      >
                        <Box>
                          <Typography variant="subtitle2" sx={{ fontWeight: 700, color: COLORS.textPrimary }}>
                            {formatPeriodRange(draft.start_period, draft.end_period)}
                          </Typography>
                          <Typography variant="caption" sx={{ color: COLORS.textSecondary }}>
                            Update bill from, reviewer, and amount for the full closed period in one save.
                          </Typography>
                        </Box>
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                          <Chip
                            label={`Period Total ${formatAmount(draft.amount)}`}
                            size="small"
                            sx={{ bgcolor: alpha(COLORS.primary, 0.1), color: COLORS.primary, fontWeight: 600 }}
                          />
                          <Button
                            variant="contained"
                            size="small"
                            disabled={saving}
                            onClick={() => handleSaveHistoryPeriod(draftIndex)}
                            sx={{ borderRadius: '8px', textTransform: 'none', bgcolor: COLORS.primary, '&:hover': { bgcolor: '#0f766e' } }}
                          >
                            {saving ? 'Saving...' : 'Save Period'}
                          </Button>
                        </Box>
                      </Box>

                      <Table size="small">
                        <TableHead>
                          <TableRow sx={{ bgcolor: COLORS.tableHeader }}>
                            {historyEditScope === 'billingGroup' && (
                              <TableCell sx={{ fontWeight: 600, color: COLORS.textPrimary }}>Service Type</TableCell>
                            )}
                            <TableCell sx={{ fontWeight: 600, color: COLORS.textPrimary }}>Bill From</TableCell>
                            <TableCell sx={{ fontWeight: 600, color: COLORS.textPrimary }}>Reviewer</TableCell>
                            <TableCell align="right" sx={{ fontWeight: 600, color: COLORS.textPrimary }}>Amount / Month</TableCell>
                            <TableCell align="right" sx={{ fontWeight: 600, color: COLORS.textPrimary }}>Share</TableCell>
                          </TableRow>
                        </TableHead>
                        <TableBody>
                          {draft.details.map((detail, detailIndex) => (
                            <TableRow key={detail.row_id || `${getHistoryDraftKey(draft)}-${detailIndex}`}>
                              {historyEditScope === 'billingGroup' && (
                                <TableCell sx={{ minWidth: 180, color: COLORS.textSecondary }}>
                                  {detail.service_type_name || '-'}
                                </TableCell>
                              )}
                              <TableCell sx={{ minWidth: 220 }}>
                                <TextField
                                  fullWidth
                                  size="small"
                                  label="Bill From"
                                  value={detail.bill_from_id
                                    ? (getBillFromLabel(detail.bill_from_id) || detail.bill_from_name || detail.bill_from_id)
                                    : 'None'}
                                  InputProps={{
                                    readOnly: true,
                                    endAdornment: (
                                      <InputAdornment position="end">
                                        <Button
                                          size="small"
                                          variant="text"
                                          onClick={() => openPicker({
                                            title: 'Select Bill From',
                                            loadOptions: async () => {
                                              const res = await billFromAPI.getAll();
                                              const list = (res.data.data || []).filter((billFrom) => billFrom.is_active !== false);
                                              const currentOption = detail.bill_from_id && !list.find((billFrom) => billFrom.id === detail.bill_from_id)
                                                ? [{ value: detail.bill_from_id, label: detail.bill_from_name || detail.bill_from_id }]
                                                : [];
                                              return [{ value: '', label: 'None' }, ...currentOption, ...list.map((billFrom) => ({ value: billFrom.id, label: billFrom.name }))];
                                            },
                                            onSelect: (value) => {
                                              handleHistoryDraftDetailChange(draftIndex, detailIndex, 'bill_from_id', value);
                                              closePicker();
                                            }
                                          })}
                                          sx={{ textTransform: 'none', fontWeight: 600 }}
                                        >
                                          Select
                                        </Button>
                                      </InputAdornment>
                                    )
                                  }}
                                  sx={{ '& .MuiOutlinedInput-root': { borderRadius: '10px' } }}
                                />
                              </TableCell>
                              <TableCell sx={{ minWidth: 220 }}>
                                <TextField
                                  fullWidth
                                  size="small"
                                  label="Reviewer"
                                  value={detail.reviewer_id
                                    ? (getReviewerLabel(detail.reviewer_id) || detail.reviewer_name || detail.reviewer_id)
                                    : 'None'}
                                  InputProps={{
                                    readOnly: true,
                                    endAdornment: (
                                      <InputAdornment position="end">
                                        <Button
                                          size="small"
                                          variant="text"
                                          onClick={() => openPicker({
                                            title: 'Select Reviewer',
                                            loadOptions: async () => {
                                              const res = await teamAPI.getReviewers();
                                              const list = res.data.data || [];
                                              const currentOption = detail.reviewer_id && !list.find((reviewer) => reviewer.id === detail.reviewer_id)
                                                ? [{ value: detail.reviewer_id, label: detail.reviewer_name || detail.reviewer_id }]
                                                : [];
                                              return [{ value: '', label: 'None' }, ...currentOption, ...list.map((reviewer) => ({ value: reviewer.id, label: reviewer.name }))];
                                            },
                                            onSelect: (value) => {
                                              handleHistoryDraftDetailChange(draftIndex, detailIndex, 'reviewer_id', value);
                                              closePicker();
                                            }
                                          })}
                                          sx={{ textTransform: 'none', fontWeight: 600 }}
                                        >
                                          Select
                                        </Button>
                                      </InputAdornment>
                                    )
                                  }}
                                  sx={{ '& .MuiOutlinedInput-root': { borderRadius: '10px' } }}
                                />
                              </TableCell>
                              <TableCell align="right" sx={{ minWidth: 180 }}>
                                <TextField
                                  fullWidth
                                  size="small"
                                  type="number"
                                  label="Amount / Month"
                                  value={detail.amount}
                                  onChange={(e) => handleHistoryDraftDetailChange(draftIndex, detailIndex, 'amount', e.target.value)}
                                  InputProps={{ startAdornment: <Typography sx={{ mr: 1, color: COLORS.textSecondary }}>Rs</Typography> }}
                                  sx={{ '& .MuiOutlinedInput-root': { borderRadius: '10px' } }}
                                />
                              </TableCell>
                              <TableCell align="right">
                                <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end' }}>
                                  <Typography variant="body2" sx={{ fontWeight: 600, color: COLORS.primary }}>
                                    {formatAmount(detail.amount)}
                                  </Typography>
                                  <Typography variant="caption" sx={{ color: COLORS.textSecondary }}>
                                    {formatPercentage(detail.percentage)}
                                  </Typography>
                                </Box>
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </Paper>
                  );
                })}
              </Box>
            )
          ) : billingHistory.length === 0 ? (
            <Paper
              elevation={0}
              sx={{
                p: 4, textAlign: 'center',
                bgcolor: alpha(COLORS.primary, 0.03),
                borderRadius: '12px',
                border: `1px dashed ${COLORS.tableBorder}`
              }}
            >
              <History sx={{ fontSize: 48, color: alpha(COLORS.textSecondary, 0.3), mb: 2 }} />
              <Typography sx={{ color: COLORS.textSecondary }}>
                No period records found for this billing setup.
              </Typography>
            </Paper>
          ) : (
            <TableContainer
              component={Paper}
              elevation={0}
              sx={{
                border: `1px solid ${COLORS.tableBorder}`,
                borderRadius: '12px',
                overflow: 'hidden'
              }}
            >
                <Table size="small">
                <TableHead>
                    <TableRow sx={{ bgcolor: COLORS.tableHeader }}>
                      <TableCell sx={{ fontWeight: 600, color: COLORS.textPrimary }}>Start Period</TableCell>
                      <TableCell sx={{ fontWeight: 600, color: COLORS.textPrimary }}>End Period</TableCell>
                      <TableCell align="right" sx={{ fontWeight: 600, color: COLORS.textPrimary }}>Amount</TableCell>
                      <TableCell sx={{ fontWeight: 600, color: COLORS.textPrimary }}>Service Type</TableCell>
                      <TableCell sx={{ fontWeight: 600, color: COLORS.textPrimary }}>Bill From</TableCell>
                      <TableCell sx={{ fontWeight: 600, color: COLORS.textPrimary }}>Reviewer</TableCell>
                      <TableCell align="right" sx={{ fontWeight: 600, color: COLORS.textPrimary }}>Allocation</TableCell>
                      {historyEditable && (
                        <TableCell align="center" sx={{ fontWeight: 600, color: COLORS.textPrimary }}>Actions</TableCell>
                      )}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {billingHistory.map((record, index) => {
                    const details = record.details || [];
                    const rowSpan = details.length > 0 ? details.length : 1;
                    return details.length > 0 ? (
                      details.map((detail, detailIndex) => (
                        <TableRow
                          key={`history-${index}-${detailIndex}`}
                          sx={{
                            '&:last-child td': { borderBottom: 0 },
                            bgcolor: record.is_current ? alpha(COLORS.accent, 0.05) : 'inherit'
                          }}
                        >
                          {detailIndex === 0 && (
                            <>
                              <TableCell rowSpan={rowSpan} sx={{ color: COLORS.textSecondary }}>{record.start_period}</TableCell>
                              <TableCell rowSpan={rowSpan} sx={{ color: COLORS.textSecondary }}>{record.end_period || 'Open'}</TableCell>
                              <TableCell rowSpan={rowSpan} align="right" sx={{ fontWeight: 600, color: COLORS.primary }}>{formatAmount(record.amount)}</TableCell>
                            </>
                          )}
                          <TableCell sx={{ color: COLORS.textSecondary }}>{detail.service_type_name}</TableCell>
                          <TableCell sx={{ color: COLORS.textSecondary }}>{detail.bill_from_name}</TableCell>
                          <TableCell sx={{ color: COLORS.textSecondary }}>{detail.reviewer_name}</TableCell>
                          <TableCell align="right" sx={{ fontWeight: 600, color: COLORS.primary }}>
                            <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end' }}>
                              <Typography variant="body2" sx={{ fontWeight: 600, color: COLORS.primary }}>
                                {formatAmount(detail.amount)}
                              </Typography>
                              <Typography variant="caption" sx={{ color: COLORS.textSecondary }}>
                                {formatPercentage(detail.percentage)}
                              </Typography>
                            </Box>
                          </TableCell>
                          {historyEditable && (
                            <TableCell align="center">
                              {detail.row_id ? (
                                <Tooltip title="Edit History Record">
                                  <IconButton
                                    size="small"
                                    onClick={() => handleOpenEditHistoryDetail(detail)}
                                    sx={{ color: COLORS.primary, '&:hover': { bgcolor: alpha(COLORS.primary, 0.1) } }}
                                  >
                                    <Edit fontSize="small" />
                                  </IconButton>
                                </Tooltip>
                              ) : '-'}
                            </TableCell>
                          )}
                        </TableRow>
                      ))
                    ) : (
                      <TableRow
                        key={`history-${index}`}
                        sx={{
                          '&:last-child td': { borderBottom: 0 },
                          bgcolor: record.is_current ? alpha(COLORS.accent, 0.05) : 'inherit'
                        }}
                      >
                        <TableCell sx={{ color: COLORS.textSecondary }}>{record.start_period}</TableCell>
                        <TableCell sx={{ color: COLORS.textSecondary }}>{record.end_period || 'Open'}</TableCell>
                        <TableCell align="right" sx={{ fontWeight: 600, color: COLORS.primary }}>{formatAmount(record.amount)}</TableCell>
                        <TableCell sx={{ color: COLORS.textSecondary }}>-</TableCell>
                        <TableCell sx={{ color: COLORS.textSecondary }}>-</TableCell>
                        <TableCell sx={{ color: COLORS.textSecondary }}>-</TableCell>
                        <TableCell align="right">-</TableCell>
                        {historyEditable && <TableCell align="center">-</TableCell>}
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </TableContainer>
          )}
        </DialogContent>
        <DialogActions sx={{ p: 2.5, borderTop: `1px solid ${COLORS.tableBorder}` }}>
          <Button
            onClick={handleCloseHistory}
            variant="contained"
            sx={{ borderRadius: '8px', textTransform: 'none', bgcolor: COLORS.primary, '&:hover': { bgcolor: '#0f766e' } }}
          >
            Close
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog
        open={editHistoryDialogOpen}
        onClose={handleCloseEditHistoryDetail}
        maxWidth="sm"
        fullWidth
        PaperProps={{ sx: { borderRadius: '16px' } }}
      >
        <DialogTitle sx={{ bgcolor: COLORS.tableHeader, borderBottom: `1px solid ${COLORS.tableBorder}`, fontWeight: 700, color: COLORS.textPrimary }}>
          Edit History Record
        </DialogTitle>
        <DialogContent sx={{ pt: 3 }}>
          {error && <Alert severity="error" sx={{ mb: 2, borderRadius: '8px' }}>{error}</Alert>}

          {editHistoryDetail && (
            <Paper elevation={0} sx={{ p: 2, mb: 2.5, bgcolor: alpha(COLORS.primary, 0.04), borderRadius: '10px', border: `1px solid ${alpha(COLORS.primary, 0.12)}` }}>
              <Typography variant="caption" sx={{ color: COLORS.textSecondary }}>Closed Period</Typography>
              <Typography variant="subtitle2" sx={{ fontWeight: 700, color: COLORS.textPrimary }}>
                {formatPeriodRange(editHistoryDetail.start_period, editHistoryDetail.end_period)}
              </Typography>
              <Typography variant="caption" sx={{ color: COLORS.textSecondary }}>
                Allocation after update: {formatPercentage(calculateAllocationPercentage(editHistoryForm.amount, editHistoryDetail.period_total))}
              </Typography>
            </Paper>
          )}

          <Grid container spacing={2.5}>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Bill From"
                value={editHistoryForm.bill_from_id
                  ? (getBillFromLabel(editHistoryForm.bill_from_id) || editHistoryDetail?.bill_from_name || editHistoryForm.bill_from_id)
                  : 'None'}
                InputProps={{
                  readOnly: true,
                  endAdornment: (
                    <InputAdornment position="end">
                      <Button
                        size="small"
                        variant="text"
                        onClick={() => openPicker({
                          title: 'Select Bill From',
                          loadOptions: async () => {
                            const res = await billFromAPI.getAll();
                            const list = (res.data.data || []).filter((b) => b.is_active !== false);
                            const currentOption = editHistoryDetail?.bill_from_id && !list.find((b) => b.id === editHistoryDetail.bill_from_id)
                              ? [{ value: editHistoryDetail.bill_from_id, label: editHistoryDetail.bill_from_name || editHistoryDetail.bill_from_id }]
                              : [];
                            return [{ value: '', label: 'None' }, ...currentOption, ...list.map((b) => ({ value: b.id, label: b.name }))];
                          },
                          onSelect: (value) => {
                            setEditHistoryForm((p) => ({ ...p, bill_from_id: value }));
                            closePicker();
                          }
                        })}
                        sx={{ textTransform: 'none', fontWeight: 600 }}
                      >
                        Select
                      </Button>
                    </InputAdornment>
                  )
                }}
                sx={{ '& .MuiOutlinedInput-root': { borderRadius: '10px' } }}
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Reviewer"
                value={editHistoryForm.reviewer_id
                  ? (getReviewerLabel(editHistoryForm.reviewer_id) || editHistoryDetail?.reviewer_name || editHistoryForm.reviewer_id)
                  : 'None'}
                InputProps={{
                  readOnly: true,
                  endAdornment: (
                    <InputAdornment position="end">
                      <Button
                        size="small"
                        variant="text"
                        onClick={() => openPicker({
                          title: 'Select Reviewer',
                          loadOptions: async () => {
                            const res = await teamAPI.getReviewers();
                            const list = res.data.data || [];
                            const currentOption = editHistoryDetail?.reviewer_id && !list.find((r) => r.id === editHistoryDetail.reviewer_id)
                              ? [{ value: editHistoryDetail.reviewer_id, label: editHistoryDetail.reviewer_name || editHistoryDetail.reviewer_id }]
                              : [];
                            return [{ value: '', label: 'None' }, ...currentOption, ...list.map((r) => ({ value: r.id, label: r.name }))];
                          },
                          onSelect: (value) => {
                            setEditHistoryForm((p) => ({ ...p, reviewer_id: value }));
                            closePicker();
                          }
                        })}
                        sx={{ textTransform: 'none', fontWeight: 600 }}
                      >
                        Select
                      </Button>
                    </InputAdornment>
                  )
                }}
                sx={{ '& .MuiOutlinedInput-root': { borderRadius: '10px' } }}
              />
            </Grid>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="Amount / Month"
                type="number"
                required
                value={editHistoryForm.amount}
                onChange={(e) => setEditHistoryForm((p) => ({ ...p, amount: e.target.value }))}
                helperText={editHistoryDetail?.period_total ? `Period total: ${formatAmount(editHistoryDetail.period_total)}` : ''}
                InputProps={{ startAdornment: <Typography sx={{ mr: 1, color: COLORS.textSecondary }}>₹</Typography> }}
                sx={{ '& .MuiOutlinedInput-root': { borderRadius: '10px' } }}
              />
            </Grid>
          </Grid>
        </DialogContent>
        <DialogActions sx={{ p: 2.5, borderTop: `1px solid ${COLORS.tableBorder}` }}>
          <Button onClick={handleCloseEditHistoryDetail} sx={{ borderRadius: '8px', textTransform: 'none', color: COLORS.textSecondary }}>
            Cancel
          </Button>
          <Button onClick={handleSubmitEditHistoryDetail} variant="contained" sx={{ borderRadius: '8px', textTransform: 'none', bgcolor: COLORS.primary, '&:hover': { bgcolor: '#0f766e' } }}>
            Update
          </Button>
        </DialogActions>
      </Dialog>

      {/* ═══════════════════════════════════════════════════════════
          Billing Group Popup (billing-level edit/view)
          ═══════════════════════════════════════════════════════════ */}
      <Dialog
        open={billingPopupOpen}
        onClose={handleCloseBillingPopup}
        maxWidth="lg"
        fullWidth
        PaperProps={{ sx: { borderRadius: '16px' } }}
      >
        <DialogTitle sx={{ bgcolor: COLORS.tableHeader, borderBottom: `1px solid ${COLORS.tableBorder}`, fontWeight: 700, color: COLORS.textPrimary }}>
          Billing Details
        </DialogTitle>
        <DialogContent sx={{ pt: 3 }}>
          {billingPopupClient && (
            <Typography variant="body2" sx={{ color: COLORS.textSecondary, mb: 2 }}>
              Group: <strong style={{ color: COLORS.textPrimary }}>{billingPopupClient.name}</strong>
            </Typography>
          )}

          {billingPopupLoading ? (
            <Box sx={{ p: 4, textAlign: 'center' }}>
              <Typography sx={{ color: COLORS.textSecondary }}>Loading latest billing information...</Typography>
            </Box>
          ) : billingPopupHierarchy.length === 0 ? (
            <Paper elevation={0} sx={{ p: 4, textAlign: 'center', bgcolor: alpha(COLORS.primary, 0.03), borderRadius: '12px', border: `1px dashed ${COLORS.tableBorder}` }}>
              <Typography sx={{ color: COLORS.textSecondary }}>No billing rows found for this group.</Typography>
            </Paper>
          ) : (
            billingPopupHierarchy.map((group, idx) => (
              <Paper key={idx} elevation={0} sx={{ mb: 2, border: `1px solid ${COLORS.tableBorder}`, borderRadius: '12px', overflow: 'hidden' }}>
                <Box sx={{
                  px: 2.5,
                  py: 1.5,
                  bgcolor: alpha(COLORS.primary, 0.06),
                  borderBottom: `1px solid ${COLORS.tableBorder}`,
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  flexWrap: 'wrap',
                  gap: 1
                }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, flexWrap: 'wrap' }}>
                    <Typography variant="subtitle2" sx={{ fontWeight: 700, color: COLORS.textPrimary }}>
                      {group.billing_name}
                    </Typography>
                    {group.pan && (
                      <Typography variant="caption" sx={{ color: COLORS.textSecondary }}>
                        PAN: <strong>{group.pan}</strong>
                      </Typography>
                    )}
                    {group.gstin && (
                      <Typography variant="caption" sx={{ color: COLORS.textSecondary }}>
                        GSTIN: <strong>{group.gstin}</strong>
                      </Typography>
                    )}
                  </Box>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
                    <Tooltip title="Edit Client">
                      <IconButton
                        size="small"
                        onClick={() => billingPopupClient && handleOpenEditGroup(billingPopupClient, group)}
                        sx={{ color: COLORS.primary, '&:hover': { bgcolor: alpha(COLORS.primary, 0.1) } }}
                      >
                        <Edit fontSize="small" />
                      </IconButton>
                    </Tooltip>
                    <Button
                      size="small"
                      startIcon={<AddCircle />}
                      onClick={() => billingPopupClient && handleOpenBillingGroupDialog(billingPopupClient, group)}
                      sx={{ borderRadius: '8px', textTransform: 'none', color: COLORS.primary }}
                    >
                      Add Service Type
                    </Button>
                    <Tooltip title="View History (Client)">
                      <IconButton
                        size="small"
                        onClick={() => billingPopupClient && handleViewBillingGroupHistory(billingPopupClient, group)}
                        sx={{ color: COLORS.accent, '&:hover': { bgcolor: alpha(COLORS.accent, 0.1) } }}
                      >
                        <History fontSize="small" />
                      </IconButton>
                    </Tooltip>
                    <Box sx={{ bgcolor: alpha(COLORS.primary, 0.1), borderRadius: '8px', px: 1.5, py: 0.5 }}>
                      <Typography variant="caption" sx={{ color: COLORS.textSecondary, fontWeight: 500 }}>
                        Group Total
                      </Typography>
                      <Typography variant="subtitle2" sx={{ fontWeight: 700, color: COLORS.primary }}>
                        {formatAmount(group.total)}
                      </Typography>
                    </Box>
                  </Box>
                </Box>
                {group.service_types.map((serviceType, serviceTypeIndex) => (
                  <Box key={`${serviceType.service_type_id || 'service'}-${serviceTypeIndex}`}>
                    <Box sx={{
                      px: 2.5,
                      py: 1,
                      bgcolor: alpha(COLORS.accent, 0.06),
                      borderBottom: `1px solid ${alpha(COLORS.tableBorder, 0.6)}`,
                      display: 'flex',
                      justifyContent: 'space-between',
                      alignItems: 'center',
                      flexWrap: 'wrap',
                      gap: 1
                    }}>
                      <Box>
                        <Typography variant="body2" sx={{ fontWeight: 700, color: COLORS.textPrimary }}>
                          Service Type: {serviceType.service_type_name}
                        </Typography>
                        <Typography variant="caption" sx={{ color: COLORS.textSecondary }}>
                          {isServiceTypeActive(serviceType)
                            ? formatPeriodRange(serviceType.start_period, '')
                            : (serviceType.history_periods?.length
                              ? `Ended ${serviceType.history_periods[serviceType.history_periods.length - 1].end_period}`
                              : 'No live period')}
                        </Typography>
                      </Box>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
                        <Tooltip title="Edit Service Type Period">
                          <IconButton
                            size="small"
                            onClick={() => billingPopupClient && handleOpenEditServiceType(billingPopupClient, group, serviceType)}
                            sx={{ color: COLORS.primary, '&:hover': { bgcolor: alpha(COLORS.primary, 0.1) } }}
                          >
                            <Edit fontSize="small" />
                          </IconButton>
                        </Tooltip>
                        {(serviceType.history_periods || []).length > 0 && (
                          <Tooltip title="View Service Type History">
                            <IconButton
                              size="small"
                              onClick={() => billingPopupClient && handleViewServiceTypeHistory(billingPopupClient, group, serviceType)}
                              sx={{ color: COLORS.accent, '&:hover': { bgcolor: alpha(COLORS.accent, 0.1) } }}
                            >
                              <History fontSize="small" />
                            </IconButton>
                          </Tooltip>
                        )}
                        {isServiceTypeActive(serviceType) && (
                          <Button
                            size="small"
                            startIcon={<AddCircle />}
                            onClick={() => billingPopupClient && handleOpenBillingGroupDialog(billingPopupClient, group, serviceType)}
                            sx={{ borderRadius: '8px', textTransform: 'none', color: COLORS.primary }}
                          >
                            Add Bill From
                          </Button>
                        )}
                        <Button
                          size="small"
                          disableElevation
                          variant={isServiceTypeActive(serviceType) ? 'contained' : 'outlined'}
                          sx={{
                            borderRadius: '999px',
                            textTransform: 'none',
                            fontWeight: 600,
                            px: 1.5,
                            bgcolor: isServiceTypeActive(serviceType) ? alpha(COLORS.success, 0.9) : 'transparent',
                            borderColor: isServiceTypeActive(serviceType) ? alpha(COLORS.success, 0.9) : COLORS.tableBorder,
                            color: isServiceTypeActive(serviceType) ? 'white' : COLORS.textSecondary,
                            '&:hover': {
                              bgcolor: isServiceTypeActive(serviceType) ? alpha(COLORS.success, 0.95) : alpha(COLORS.primary, 0.06)
                            }
                          }}
                        >
                          {isServiceTypeActive(serviceType) ? 'Live' : 'Closed'}
                        </Button>
                        <Typography variant="body2" sx={{ fontWeight: 700, color: COLORS.accent }}>
                          Total: {formatAmount(serviceType.total)}
                        </Typography>
                      </Box>
                    </Box>

                    {serviceType.bill_froms.map((bf, bfIdx) => (
                      <Box key={`${bf.bill_from_id || 'bill-from'}-${bfIdx}`}>
                        <Box sx={{ px: 2.5, py: 1, bgcolor: alpha(COLORS.secondary, 0.06), borderBottom: `1px solid ${alpha(COLORS.tableBorder, 0.5)}`, display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 1 }}>
                          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, flexWrap: 'wrap' }}>
                            <Typography variant="body2" sx={{ fontWeight: 600, color: COLORS.textPrimary }}>
                              Bill From: {bf.bill_from_name}
                            </Typography>
                            <Tooltip title="Edit Bill From">
                              <IconButton
                                size="small"
                                onClick={() => billingPopupClient && handleOpenEditBillFrom(billingPopupClient, group, serviceType, bf)}
                                sx={{ color: COLORS.primary, '&:hover': { bgcolor: alpha(COLORS.primary, 0.1) } }}
                              >
                                <Edit fontSize="small" />
                              </IconButton>
                            </Tooltip>
                            <Button
                              size="small"
                              startIcon={<AddCircle />}
                              onClick={() => billingPopupClient && handleOpenBillingGroupDialog(billingPopupClient, group, serviceType, bf)}
                              sx={{ borderRadius: '8px', textTransform: 'none', color: COLORS.primary }}
                            >
                              Add Reviewer
                            </Button>
                          </Box>
                          <Typography variant="body2" sx={{ fontWeight: 700, color: '#047857' }}>
                            Total: {formatAmount(bf.total)}
                          </Typography>
                        </Box>

                        <Table size="small">
                          <TableHead>
                            <TableRow sx={{ bgcolor: COLORS.tableHeader }}>
                              <TableCell sx={{ fontWeight: 600, color: COLORS.textPrimary, fontSize: '0.78rem', pl: 4 }}>Reviewer</TableCell>
                              <TableCell align="right" sx={{ fontWeight: 600, color: COLORS.textPrimary, fontSize: '0.78rem' }}>Amount/Month</TableCell>
                              <TableCell align="center" sx={{ fontWeight: 600, color: COLORS.textPrimary, fontSize: '0.78rem' }}>Actions</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {bf.reviewers.map((row, rIdx) => (
                              <TableRow key={row.id || rIdx} sx={{ '&:last-child td': { borderBottom: 0 } }}>
                                <TableCell sx={{ color: COLORS.textPrimary, fontWeight: 500, pl: 4 }}>
                                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                                    <Person sx={{ fontSize: 16, color: COLORS.textSecondary }} />
                                    {row.reviewer_name || 'Not Assigned'}
                                  </Box>
                                </TableCell>
                                <TableCell align="right" sx={{ fontWeight: 700, color: COLORS.primary }}>{formatAmount(row.amount)}</TableCell>
                                <TableCell align="center">
                                  <Tooltip title="Edit (Billing level)">
                                    <IconButton
                                      size="small"
                                      onClick={() => handleOpenEditRow(billingPopupClient, row)}
                                      sx={{ color: COLORS.primary, '&:hover': { bgcolor: alpha(COLORS.primary, 0.1) } }}
                                    >
                                      <Edit fontSize="small" />
                                    </IconButton>
                                  </Tooltip>
                                  <Tooltip title="View History">
                                    <IconButton
                                      size="small"
                                      onClick={() => handleViewHistory(billingPopupClient, row)}
                                      sx={{ color: COLORS.accent, '&:hover': { bgcolor: alpha(COLORS.accent, 0.1) } }}
                                    >
                                      <History fontSize="small" />
                                    </IconButton>
                                  </Tooltip>
                                </TableCell>
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      </Box>
                    ))}
                  </Box>
                ))}
              </Paper>
            ))
          )}
        </DialogContent>
        <DialogActions sx={{ p: 2.5, borderTop: `1px solid ${COLORS.tableBorder}` }}>
            {billingPopupClient && (
              <Button
                onClick={() => handleOpenBillingGroupDialog(billingPopupClient)}
                startIcon={<AddCircle />}
                sx={{ borderRadius: '8px', textTransform: 'none', color: COLORS.primary, mr: 'auto' }}
              >
                Add Client
              </Button>
            )}
          <Button onClick={handleCloseBillingPopup} variant="contained" sx={{ borderRadius: '8px', textTransform: 'none', bgcolor: COLORS.primary, '&:hover': { bgcolor: '#0f766e' } }}>
            Close
          </Button>
        </DialogActions>
      </Dialog>

        {/* ═══════════════════════════════════════════════════════════
            Edit Billing Group Dialog
            ═══════════════════════════════════════════════════════════ */}
        <Dialog
          open={editGroupDialogOpen}
          onClose={handleCloseEditGroup}
          maxWidth="sm"
          fullWidth
          PaperProps={{ sx: { borderRadius: '16px' } }}
        >
          <DialogTitle sx={{ bgcolor: COLORS.tableHeader, borderBottom: `1px solid ${COLORS.tableBorder}`, fontWeight: 700, color: COLORS.textPrimary }}>
            Edit Client
          </DialogTitle>
          <DialogContent sx={{ pt: 3 }}>
            {error && <Alert severity="error" sx={{ mb: 2, borderRadius: '8px' }}>{error}</Alert>}
            <Grid container spacing={2}>
              <Grid item xs={12}>
                <TextField
                  fullWidth
                  label="Client"
                  required
                  value={editGroupForm.billing_name}
                  InputProps={{
                    readOnly: true,
                    endAdornment: (
                      <InputAdornment position="end">
                        <Button
                          size="small"
                          variant="text"
                          onClick={() => openPicker({
                            title: 'Select Client',
                            loadOptions: async () => {
                              const list = activeBillingNameMasters;
                              return list.map((b) => ({
                                value: b.id,
                                label: b.name,
                                secondary: [b.gstin, b.pan].filter(Boolean).join(' | ')
                              }));
                            },
                            onSelect: (masterId) => {
                              const master = activeBillingNameMasters.find((b) => b.id === masterId);
                              if (master) {
                                setEditGroupForm((p) => ({
                                  ...p,
                                  billing_name_id: master.id,
                                  billing_name: master.name,
                                  gstin: master.gstin || '',
                                  pan: master.pan || '',
                                }));
                              }
                              closePicker();
                            }
                          })}
                          sx={{ textTransform: 'none', fontWeight: 600 }}
                        >
                          Select
                        </Button>
                      </InputAdornment>
                    )
                  }}
                  sx={{ '& .MuiOutlinedInput-root': { borderRadius: '10px' } }}
                />
              </Grid>
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="GSTIN"
                  value={editGroupForm.gstin}
                  onChange={(e) => {
                    const gstin = e.target.value.toUpperCase();
                    setEditGroupForm((p) => ({ ...p, gstin, pan: gstin ? derivePanFromGstin(gstin) : p.pan }));
                  }}
                  sx={{ '& .MuiOutlinedInput-root': { borderRadius: '10px' } }}
                />
              </Grid>
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="PAN"
                  value={editGroupForm.pan}
                  onChange={(e) => setEditGroupForm((p) => ({ ...p, pan: e.target.value.toUpperCase() }))}
                  InputProps={{ readOnly: !!editGroupForm.gstin }}
                  helperText={editGroupForm.gstin ? 'Auto-derived from GSTIN' : ''}
                  sx={{ '& .MuiOutlinedInput-root': { borderRadius: '10px' } }}
                />
              </Grid>
            </Grid>
          </DialogContent>
          <DialogActions sx={{ p: 2.5, borderTop: `1px solid ${COLORS.tableBorder}` }}>
            <Button onClick={handleCloseEditGroup} sx={{ borderRadius: '8px', textTransform: 'none', color: COLORS.textSecondary }}>
              Cancel
            </Button>
            <Button onClick={handleSubmitEditGroup} variant="contained" sx={{ borderRadius: '8px', textTransform: 'none', bgcolor: COLORS.primary, '&:hover': { bgcolor: '#0f766e' } }}>
              Update
            </Button>
          </DialogActions>
        </Dialog>

        <Dialog
          open={editServiceTypeDialogOpen}
          onClose={handleCloseEditServiceType}
          maxWidth="md"
          fullWidth
          PaperProps={{ sx: { borderRadius: '16px' } }}
        >
          <DialogTitle sx={{ bgcolor: COLORS.tableHeader, borderBottom: `1px solid ${COLORS.tableBorder}`, fontWeight: 700, color: COLORS.textPrimary }}>
            Edit Service Type
          </DialogTitle>
          <DialogContent sx={{ pt: 3 }}>
            {error && <Alert severity="error" sx={{ mb: 2, borderRadius: '8px' }}>{error}</Alert>}

            {/* Service type name is fixed — only period is editable */}
            <Paper elevation={0} sx={{ p: 2, mb: 2.5, bgcolor: alpha(COLORS.primary, 0.04), borderRadius: '10px', border: `1px solid ${alpha(COLORS.primary, 0.12)}` }}>
              <Typography variant="caption" sx={{ color: COLORS.textSecondary }}>Service Type (fixed)</Typography>
              <Typography variant="subtitle1" sx={{ fontWeight: 700, color: COLORS.textPrimary }}>
                {getServiceTypeLabel(editServiceTypeForm.service_type_id) || '—'}
              </Typography>
              <Typography variant="caption" sx={{ color: COLORS.textSecondary }}>
                The service type name cannot be changed after creation. You may update the live period or continue it with a new period.
              </Typography>
            </Paper>

            <Box sx={{ mb: 2.5 }}>
              <Typography variant="subtitle2" sx={{ color: COLORS.textPrimary, fontWeight: 600, mb: 1 }}>
                Period State
              </Typography>
              <Box sx={{ display: 'flex', gap: 1, bgcolor: COLORS.tableHeader, borderRadius: '10px', p: 0.5, width: 'fit-content' }}>
                {[{ key: 'active', label: 'Live' }, { key: 'inactive', label: 'Closed' }].map((option) => {
                  const sel = (editServiceTypeForm.is_active ? 'active' : 'inactive') === option.key;
                  return (
                    <Button
                      key={option.key}
                      size="small"
                      variant={sel ? 'contained' : 'text'}
                      onClick={() => setEditServiceTypeForm((p) => ({
                        ...p,
                        is_active: option.key === 'active',
                        end_period: option.key === 'active' ? '' : (p.end_period || p.start_period || '')
                      }))}
                      sx={{
                        borderRadius: '8px',
                        textTransform: 'none',
                        fontWeight: sel ? 600 : 500,
                        bgcolor: sel ? COLORS.primary : 'transparent',
                        color: sel ? 'white' : COLORS.textSecondary,
                        '&:hover': { bgcolor: sel ? '#0f766e' : alpha(COLORS.primary, 0.08) },
                        px: 2.5
                      }}
                    >
                      {option.label}
                    </Button>
                  );
                })}
              </Box>
            </Box>

            <Grid container spacing={2.5}>
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="Start Period"
                  required
                  value={editServiceTypeForm.start_period || ''}
                  InputProps={{
                    readOnly: true,
                    endAdornment: (
                      <InputAdornment position="end">
                        <Button
                          size="small"
                          variant="text"
                          onClick={() => openPicker({
                            title: 'Select Start Period',
                            loadOptions: async () => {
                              const res = await servicePeriodAPI.getAll();
                              const list = (res.data.data || []).filter((sp) => sp.is_active);
                              const reactivationStartFloor = !isServiceTypeActive(editServiceTypeData)
                                ? getServiceTypeReactivationStartFloor(editServiceTypeData)
                                : '';
                              const filtered = reactivationStartFloor
                                ? list.filter((sp) => comparePeriods(sp.display_name, reactivationStartFloor) >= 0)
                                : list;
                              return filtered.map((sp) => ({ value: sp.display_name, label: sp.display_name }));
                            },
                            onSelect: (value) => {
                              setEditServiceTypeForm((p) => {
                                const next = { ...p, start_period: value };
                                if (next.end_period && comparePeriods(next.end_period, value) < 0) next.end_period = '';
                                if (!next.is_active && !next.end_period) next.end_period = value;
                                return next;
                              });
                              closePicker();
                            }
                          })}
                          sx={{ textTransform: 'none', fontWeight: 600 }}
                        >
                          Select
                        </Button>
                      </InputAdornment>
                    )
                  }}
                  sx={{ '& .MuiOutlinedInput-root': { borderRadius: '10px' } }}
                />
              </Grid>
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="End Period"
                  value={editServiceTypeForm.end_period || (editServiceTypeForm.is_active ? 'Open' : '')}
                  InputProps={{
                    readOnly: true,
                    endAdornment: (
                      <InputAdornment position="end">
                        <Button
                          size="small"
                          variant="text"
                          disabled={editServiceTypeForm.is_active}
                          onClick={() => openPicker({
                            title: 'Select End Period',
                            loadOptions: async () => {
                              const res = await servicePeriodAPI.getAll();
                              const list = (res.data.data || []).filter((sp) => sp.is_active);
                              const start = editServiceTypeForm.start_period;
                              const filtered = start ? list.filter((sp) => comparePeriods(sp.display_name, start) >= 0) : list;
                              return filtered.map((sp) => ({ value: sp.display_name, label: sp.display_name }));
                            },
                            onSelect: (value) => {
                              setEditServiceTypeForm((p) => ({ ...p, end_period: value }));
                              closePicker();
                            }
                          })}
                          sx={{ textTransform: 'none', fontWeight: 600 }}
                        >
                          Select
                        </Button>
                      </InputAdornment>
                    )
                  }}
                  sx={{ '& .MuiOutlinedInput-root': { borderRadius: '10px' } }}
                />
              </Grid>
            </Grid>

            {/* Reactivation extra fields: shown when marking an inactive service as active */}
            {editServiceTypeForm.is_active && !isServiceTypeActive(editServiceTypeData) && (
              <Box sx={{ mt: 3 }}>
                <Alert severity="info" sx={{ mb: 2, borderRadius: '8px' }}>
                  Continuing this service will create a new live billing entry. Please provide the new billing details.
                </Alert>
                <Grid container spacing={2.5}>
                  <Grid item xs={12} md={4}>
                    <TextField
                      fullWidth
                      label="New Bill From"
                      value={reactivationForm.bill_from_id ? getBillFromLabel(reactivationForm.bill_from_id) : 'None'}
                      InputProps={{
                        readOnly: true,
                        endAdornment: (
                          <InputAdornment position="end">
                            <Button size="small" variant="text"
                              onClick={() => openPicker({
                                title: 'Select Bill From',
                                loadOptions: async () => {
                                  const res = await billFromAPI.getAll();
                                  const list = (res.data.data || []).filter((b) => b.is_active !== false);
                                  return [{ value: '', label: 'None' }, ...list.map((b) => ({ value: b.id, label: b.name }))];
                                },
                                onSelect: (value) => { setReactivationForm((p) => ({ ...p, bill_from_id: value })); closePicker(); }
                              })}
                              sx={{ textTransform: 'none', fontWeight: 600 }}>Select</Button>
                          </InputAdornment>
                        )
                      }}
                      sx={{ '& .MuiOutlinedInput-root': { borderRadius: '10px' } }}
                    />
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <TextField
                      fullWidth
                      label="New Reviewer"
                      value={reactivationForm.reviewer_id ? getReviewerLabel(reactivationForm.reviewer_id) : 'None'}
                      InputProps={{
                        readOnly: true,
                        endAdornment: (
                          <InputAdornment position="end">
                            <Button size="small" variant="text"
                              onClick={() => openPicker({
                                title: 'Select Reviewer',
                                loadOptions: async () => {
                                  const res = await teamAPI.getReviewers();
                                  const list = res.data.data || [];
                                  return [{ value: '', label: 'None' }, ...list.map((r) => ({ value: r.id, label: r.name }))];
                                },
                                onSelect: (value) => { setReactivationForm((p) => ({ ...p, reviewer_id: value })); closePicker(); }
                              })}
                              sx={{ textTransform: 'none', fontWeight: 600 }}>Select</Button>
                          </InputAdornment>
                        )
                      }}
                      sx={{ '& .MuiOutlinedInput-root': { borderRadius: '10px' } }}
                    />
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <TextField
                      fullWidth
                      label="Amount / Month"
                      type="number"
                      required
                      value={reactivationForm.amount}
                      onChange={(e) => setReactivationForm((p) => ({ ...p, amount: e.target.value }))}
                      InputProps={{ startAdornment: <Typography sx={{ mr: 1, color: COLORS.textSecondary }}>₹</Typography> }}
                      sx={{ '& .MuiOutlinedInput-root': { borderRadius: '10px' } }}
                    />
                  </Grid>
                </Grid>
              </Box>
            )}
          </DialogContent>
          <DialogActions sx={{ p: 2.5, borderTop: `1px solid ${COLORS.tableBorder}` }}>
            <Button onClick={handleCloseEditServiceType} sx={{ borderRadius: '8px', textTransform: 'none', color: COLORS.textSecondary }}>
              Cancel
            </Button>
            <Button onClick={handleSubmitEditServiceType} variant="contained" sx={{ borderRadius: '8px', textTransform: 'none', bgcolor: COLORS.primary, '&:hover': { bgcolor: '#0f766e' } }}>
              {editServiceTypeForm.is_active && !isServiceTypeActive(editServiceTypeData) ? 'Continue Service Type' : 'Update'}
            </Button>
          </DialogActions>
        </Dialog>

        {/* ═══════════════════════════════════════════════════════════
            Edit Bill From Dialog
            ═══════════════════════════════════════════════════════════ */}
        <Dialog
          open={editBillFromDialogOpen}
          onClose={handleCloseEditBillFrom}
          maxWidth="sm"
          fullWidth
          PaperProps={{ sx: { borderRadius: '16px' } }}
        >
          <DialogTitle sx={{ bgcolor: COLORS.tableHeader, borderBottom: `1px solid ${COLORS.tableBorder}`, fontWeight: 700, color: COLORS.textPrimary }}>
            Edit Bill From
          </DialogTitle>
          <DialogContent sx={{ pt: 3 }}>
            {error && <Alert severity="error" sx={{ mb: 2, borderRadius: '8px' }}>{error}</Alert>}
            <TextField
              fullWidth
              label="Bill From"
              value={editBillFromForm.bill_from_id ? getBillFromLabel(editBillFromForm.bill_from_id) : 'None'}
              InputProps={{
                readOnly: true,
                endAdornment: (
                  <InputAdornment position="end">
                    <Button
                      size="small"
                      variant="text"
                      onClick={() => openPicker({
                        title: 'Select Bill From',
                        loadOptions: async () => {
                          const res = await billFromAPI.getAll();
                          const list = (res.data.data || []).filter((b) => b.is_active !== false);
                          return [{ value: '', label: 'None' }, ...list.map((b) => ({ value: b.id, label: b.name }))];
                        },
                        onSelect: (value) => {
                          setEditBillFromForm({ bill_from_id: value });
                          closePicker();
                        }
                      })}
                      sx={{ textTransform: 'none', fontWeight: 600 }}
                    >
                      Select
                    </Button>
                  </InputAdornment>
                )
              }}
              sx={{ '& .MuiOutlinedInput-root': { borderRadius: '10px' } }}
            />
          </DialogContent>
          <DialogActions sx={{ p: 2.5, borderTop: `1px solid ${COLORS.tableBorder}` }}>
            <Button onClick={handleCloseEditBillFrom} sx={{ borderRadius: '8px', textTransform: 'none', color: COLORS.textSecondary }}>
              Cancel
            </Button>
            <Button onClick={handleSubmitEditBillFrom} variant="contained" sx={{ borderRadius: '8px', textTransform: 'none', bgcolor: COLORS.primary, '&:hover': { bgcolor: '#0f766e' } }}>
              Update
            </Button>
          </DialogActions>
        </Dialog>

      <PickerDialog
        open={pickerOpen}
        title={pickerTitle}
        loading={pickerLoading}
        options={pickerOptions}
        search={pickerSearch}
        onSearchChange={setPickerSearch}
        onSelect={(value) => (pickerOnSelect ? pickerOnSelect(value) : closePicker())}
        onClose={closePicker}
      />

      {/* ═══════════════════════════════════════════════════════════
          Continuation Dialog (after setting end period on service type)
          ═══════════════════════════════════════════════════════════ */}
      <Dialog
        open={continuationDialogOpen}
        onClose={() => { setContinuationDialogOpen(false); toast.info('Service type closed for the current period'); }}
        maxWidth="sm"
        fullWidth
        PaperProps={{ sx: { borderRadius: '16px' } }}
      >
        <DialogTitle sx={{ bgcolor: alpha(COLORS.accent, 0.1), borderBottom: `1px solid ${COLORS.tableBorder}`, fontWeight: 700, color: COLORS.textPrimary }}>
          Continue Service?
        </DialogTitle>
        <DialogContent sx={{ pt: 3 }}>
          <Typography variant="body1" sx={{ mb: 2, color: COLORS.textPrimary, lineHeight: 1.7 }}>
            End period has been set to <strong>{continuationContext.endPeriod}</strong>.
          </Typography>
          <Typography variant="body2" sx={{ color: COLORS.textSecondary, lineHeight: 1.7 }}>
            Would you like to create a new billing entry for this service type starting from{' '}
            <strong>{nextPeriodFrom(continuationContext.endPeriod)}</strong>?
          </Typography>
          <Typography variant="caption" sx={{ display: 'block', mt: 1.5, color: COLORS.textSecondary }}>
            If you choose "No", the service type will stay closed after this period. You can continue it later from the billing details popup.
          </Typography>
        </DialogContent>
        <DialogActions sx={{ p: 2.5, gap: 1, borderTop: `1px solid ${COLORS.tableBorder}` }}>
          <Button
            onClick={() => {
              setContinuationDialogOpen(false);
              setContinuationContext({ client: null, group: null, serviceTypeId: '', endPeriod: '' });
              toast.info('Service type closed for the current period');
            }}
            sx={{ borderRadius: '8px', textTransform: 'none', color: COLORS.textSecondary }}
          >
            No, Keep Closed
          </Button>
          <Button
            variant="contained"
            onClick={() => {
              const { client, group, serviceTypeId, endPeriod } = continuationContext;
              setContinuationDialogOpen(false);
              setContinuationContext({ client: null, group: null, serviceTypeId: '', endPeriod: '' });
              if (client && group) {
                handleOpenBillingGroupDialog(client, group, {
                  service_type_id: serviceTypeId || '',
                  start_period: nextPeriodFrom(endPeriod),
                  end_period: '',
                  min_start_period: nextPeriodFrom(endPeriod),
                  lock_periods: false,
                  bill_froms: [createEmptyBillFrom()]
                });
              }
            }}
            sx={{ borderRadius: '8px', textTransform: 'none', bgcolor: COLORS.primary, '&:hover': { bgcolor: '#0f766e' } }}
          >
            Yes, Create New Entry
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default Clients;
