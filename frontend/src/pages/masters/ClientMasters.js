import React, { useState, useEffect, useCallback, useMemo } from 'react';
import {
  Box, Button, TextField, Dialog, DialogTitle,
  DialogContent, DialogActions, IconButton, Typography, Chip, Alert,
  Table, TableBody, TableCell, TableHead, TableRow,
  TableContainer, Tooltip, Grid, alpha,
  ToggleButton, ToggleButtonGroup
} from '@mui/material';
import { Add, Edit, Search, Refresh, CheckCircle, Cancel, Delete, Business } from '@mui/icons-material';
import { HoverActionButton, MetricCard, PageHeader, SectionCard } from '../../components/FuturisticUI';
import { toast } from 'react-toastify';
import { billingNameAPI } from '../../services/api';
import { flattenDependencyItems, summarizeDependencies } from '../../utils/deleteDependencies';

// ─── Pastel Color Palette ────────────────────────────────────────────────────
const COLORS = {
  primary: '#0d9488',
  background: '#fafaf8',
  headerBg: 'linear-gradient(135deg, #f0fdfa 0%, #ccfbf1 50%, #99f6e4 100%)',
  tableHeader: '#f0fdfa',
  tableBorder: '#ccfbf1',
  textPrimary: '#1c1917',
  textSecondary: '#78716c',
};

const PAN_REGEX = /^[A-Z]{5}[0-9]{4}[A-Z]{1}$/;
const GSTIN_REGEX = /^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}Z[0-9A-Z]{1}$/;
const derivePanFromGstin = (gstin) => (gstin && gstin.length >= 12 ? gstin.slice(2, 12) : '');

const ClientMasters = () => {
  const [clients, setClients] = useState([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [statusFilter, setStatusFilter] = useState('active');

  const [dialogOpen, setDialogOpen] = useState(false);
  const [selectedItem, setSelectedItem] = useState(null);
  const [formData, setFormData] = useState({ name: '', gstin: '', pan: '' });
  const [error, setError] = useState('');

  const duplicateMatch = useMemo(() => {
    const pan = (formData.pan || '').toUpperCase().trim();
    const gstin = (formData.gstin || '').toUpperCase().trim();
    if (!pan && !gstin) return null;

    // If GSTIN provided -> GSTIN must be unique (PAN may repeat across different GSTINs)
    if (gstin) {
      const found = clients.find((c) => c.gstin && c.gstin.toUpperCase().trim() === gstin && (!selectedItem || c.id !== selectedItem.id));
      return found ? { type: 'GSTIN', client: found } : null;
    }

    // If GSTIN blank -> PAN must be unique among ALL entries regardless of GSTIN
    if (pan) {
      const found = clients.find((c) => c.pan && c.pan.toUpperCase().trim() === pan && (!selectedItem || c.id !== selectedItem.id));
      return found ? { type: 'PAN', client: found } : null;
    }

    return null;
  }, [clients, formData.pan, formData.gstin, selectedItem]);

  const [toggleConfirmOpen, setToggleConfirmOpen] = useState(false);
  const [itemToToggle, setItemToToggle] = useState(null);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [itemToDelete, setItemToDelete] = useState(null);
  const [deleteDeps, setDeleteDeps] = useState([]);
  const [deleteMessage, setDeleteMessage] = useState('');
  const [depsLoading, setDepsLoading] = useState(false);

  const loadClients = useCallback(async () => {
    setLoading(true);
    try {
      const res = await billingNameAPI.getAll({ includeInactive: true });
      setClients(res.data.data || []);
    } catch (err) {
      toast.error('Failed to load clients');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadClients(); }, [loadClients]);

  const filteredClients = useMemo(() => {
    const q = searchTerm.toLowerCase();
    return clients.filter((c) => {
      if (statusFilter === 'active' && !c.is_active) return false;
      if (statusFilter === 'inactive' && c.is_active) return false;
      return (
        (c.name || '').toLowerCase().includes(q) ||
        (c.gstin || '').toLowerCase().includes(q) ||
        (c.pan || '').toLowerCase().includes(q)
      );
    });
  }, [clients, searchTerm, statusFilter]);

  const activeCount = useMemo(() => clients.filter((c) => c.is_active).length, [clients]);

  const handleOpenDialog = (item = null) => {
    setSelectedItem(item);
    setFormData(item
      ? { name: item.name || '', gstin: item.gstin || '', pan: item.pan || '' }
      : { name: '', gstin: '', pan: '' }
    );
    setError('');
    setDialogOpen(true);
  };

  const handleCloseDialog = () => {
    setDialogOpen(false);
    setSelectedItem(null);
    setFormData({ name: '', gstin: '', pan: '' });
    setError('');
  };

  const handleGstinChange = (value) => {
    const upper = value.toUpperCase();
    setFormData((prev) => ({
      ...prev,
      gstin: upper,
      pan: upper ? derivePanFromGstin(upper) : prev.pan,
    }));
  };

  const handleSubmit = async () => {
    if (!formData.name.trim()) { setError('Client name is required'); return; }
    if (formData.gstin && !GSTIN_REGEX.test(formData.gstin)) { setError('Invalid GSTIN format'); return; }
    if (formData.pan && !PAN_REGEX.test(formData.pan)) { setError('Invalid PAN format'); return; }

    // Duplicate check per rule: GSTIN unique when provided; if GSTIN blank then PAN must be unique
    if (duplicateMatch) {
      const existing = duplicateMatch.client;
      if (duplicateMatch.type === 'GSTIN') {
        setError(`Duplicate entry: another client (${existing.name || existing.id}) already uses this GSTIN`);
      } else {
        setError(`Duplicate entry: another client (${existing.name || existing.id}) already uses this PAN`);
      }
      return;
    }

    try {
      const payload = {
        name: formData.name.trim(),
        gstin: formData.gstin?.toUpperCase() || null,
        pan: formData.pan?.toUpperCase() || null,
      };

      if (selectedItem) {
        await billingNameAPI.update(selectedItem.id, payload);
        toast.success('Client updated successfully');
      } else {
        await billingNameAPI.create(payload);
        toast.success('Client created successfully');
      }
      handleCloseDialog();
      loadClients();
    } catch (err) {
      setError(err.response?.data?.message || err.response?.data?.errors?.[0]?.msg || 'Operation failed');
    }
  };

  const handleToggleConfirm = (item) => {
    setItemToToggle(item);
    setToggleConfirmOpen(true);
  };

  const handleOpenDeleteDialog = async (item) => {
    setItemToDelete(item);
    setError('');
    setDeleteDeps([]);
    setDeleteMessage('');
    setDepsLoading(true);
    setDeleteDialogOpen(true);
    try {
      const res = await billingNameAPI.checkDependencies(item.id);
      setDeleteDeps(res.data?.data?.dependencies || []);
      setDeleteMessage(res.data?.data?.message || '');
    } catch {
      setDeleteDeps([]);
      setDeleteMessage('');
    }
    finally { setDepsLoading(false); }
  };

  const handleCloseDeleteDialog = () => {
    setDeleteDialogOpen(false);
    setItemToDelete(null);
    setDeleteDeps([]);
    setDeleteMessage('');
    setError('');
  };

  const handleDeleteClient = async () => {
    if (!itemToDelete) return;
    try {
      await billingNameAPI.delete(itemToDelete.id);
      toast.success(`Client "${itemToDelete.name}" deleted.`);
      handleCloseDeleteDialog();
      loadClients();
    } catch (err) {
      if (err.response?.status === 409) {
        setDeleteDeps(err.response?.data?.data?.dependencies || []);
        setDeleteMessage(err.response?.data?.message || err.response?.data?.data?.message || 'Delete is blocked because this client is still linked elsewhere.');
      }
      setError(err.response?.data?.message || 'Failed to delete client');
    }
  };

  const linkedItems = flattenDependencyItems(deleteDeps);

  const handleToggleActive = async () => {
    if (!itemToToggle) return;
    try {
      await billingNameAPI.update(itemToToggle.id, { is_active: !itemToToggle.is_active });
      toast.success(`${itemToToggle.name} ${itemToToggle.is_active ? 'deactivated' : 'activated'} successfully`);
      setToggleConfirmOpen(false);
      setItemToToggle(null);
      loadClients();
    } catch (err) {
      toast.error(err.response?.data?.message || 'Failed to update status');
      setToggleConfirmOpen(false);
      setItemToToggle(null);
    }
  };

  return (
    <Box sx={{ p: { xs: 2, md: 3 }, bgcolor: COLORS.background, minHeight: '100vh' }}>

      <PageHeader
        eyebrow="Client Master"
        title="Client management"
        actions={[
          <HoverActionButton key="add" icon={<Add fontSize="small" />} label="Add Client" onClick={() => handleOpenDialog()} tone="mint" />,
          <HoverActionButton key="refresh" icon={<Refresh fontSize="small" />} label="Refresh" onClick={loadClients} tone="peach" />,
        ]}
        chips={[
          { label: `${activeCount} active clients` },
          { label: `${clients.length} total` },
        ]}
      />

      <Grid container spacing={2} sx={{ mb: 2.5 }}>
        <Grid item xs={12} md={6}>
          <MetricCard eyebrow="Active" title="Active clients" value={String(activeCount)} icon={<Business fontSize="small" />} tone="mint" />
        </Grid>
        <Grid item xs={12} md={6}>
          <MetricCard eyebrow="Total" title="All clients" value={String(clients.length)} icon={<Business fontSize="small" />} tone="peach" />
        </Grid>
      </Grid>

      <SectionCard title="Client ledger" tone="sage" contentSx={{ p: 2 }}>
        <Box sx={{ display: 'flex', gap: 2, mb: 2, alignItems: 'center' }}>
          <TextField
            placeholder="Search clients..."
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
              <Typography sx={{ color: COLORS.textSecondary }}>Loading clients...</Typography>
            </Box>
          ) : filteredClients.length === 0 ? (
            <Box sx={{ p: 6, textAlign: 'center' }}>
              <Business sx={{ fontSize: 48, color: alpha(COLORS.textSecondary, 0.3), mb: 2 }} />
              <Typography sx={{ color: COLORS.textSecondary }}>No clients found</Typography>
            </Box>
          ) : (
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow sx={{ bgcolor: COLORS.tableHeader }}>
                    <TableCell sx={{ fontWeight: 600, color: COLORS.textPrimary, borderBottom: `2px solid ${COLORS.tableBorder}` }}>Client Name</TableCell>
                    <TableCell sx={{ fontWeight: 600, color: COLORS.textPrimary, borderBottom: `2px solid ${COLORS.tableBorder}` }}>GSTIN</TableCell>
                    <TableCell sx={{ fontWeight: 600, color: COLORS.textPrimary, borderBottom: `2px solid ${COLORS.tableBorder}` }}>PAN</TableCell>
                    <TableCell align="center" sx={{ fontWeight: 600, color: COLORS.textPrimary, borderBottom: `2px solid ${COLORS.tableBorder}` }}>Status</TableCell>
                    <TableCell align="center" sx={{ fontWeight: 600, color: COLORS.textPrimary, borderBottom: `2px solid ${COLORS.tableBorder}` }}>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {filteredClients.map((item) => (
                    <TableRow
                      key={item.id}
                      sx={{ opacity: item.is_active ? 1 : 0.55, bgcolor: item.is_active ? 'inherit' : alpha(COLORS.textSecondary, 0.04), '&:hover': { bgcolor: alpha(COLORS.primary, 0.04) } }}
                    >
                      <TableCell sx={{ color: COLORS.textPrimary, fontWeight: 600, borderBottom: `1px solid ${COLORS.tableBorder}` }}>
                        {item.name}
                      </TableCell>
                      <TableCell sx={{ color: COLORS.textSecondary, borderBottom: `1px solid ${COLORS.tableBorder}`, fontFamily: 'monospace' }}>
                        {item.gstin || '—'}
                      </TableCell>
                      <TableCell sx={{ color: COLORS.textSecondary, borderBottom: `1px solid ${COLORS.tableBorder}`, fontFamily: 'monospace' }}>
                        {item.pan || '—'}
                      </TableCell>
                      <TableCell align="center" sx={{ borderBottom: `1px solid ${COLORS.tableBorder}` }}>
                        <Chip
                          label={item.is_active ? 'Active' : 'Inactive'}
                          color={item.is_active ? 'success' : 'default'}
                          size="small"
                          sx={{ fontWeight: 500 }}
                        />
                      </TableCell>
                      <TableCell align="center" sx={{ borderBottom: `1px solid ${COLORS.tableBorder}` }}>
                        <Box sx={{ display: 'flex', justifyContent: 'center', gap: 0.5 }}>
                          <Tooltip title="Edit Client">
                            <IconButton
                              size="small"
                              onClick={() => handleOpenDialog(item)}
                              sx={{ color: COLORS.primary, '&:hover': { bgcolor: alpha(COLORS.primary, 0.1) } }}
                            >
                              <Edit fontSize="small" />
                            </IconButton>
                          </Tooltip>
                          <Tooltip title={item.is_active ? 'Deactivate' : 'Activate'}>
                            <IconButton
                              size="small"
                              onClick={() => handleToggleConfirm(item)}
                              sx={{ color: item.is_active ? '#DC2626' : '#16a34a', '&:hover': { bgcolor: alpha(item.is_active ? '#DC2626' : '#16a34a', 0.08) } }}
                            >
                              {item.is_active ? <Cancel fontSize="small" /> : <CheckCircle fontSize="small" />}
                            </IconButton>
                          </Tooltip>
                          <Tooltip title="Delete Client">
                            <IconButton
                              size="small"
                              onClick={() => handleOpenDeleteDialog(item)}
                              sx={{ color: '#DC2626', '&:hover': { bgcolor: alpha('#DC2626', 0.08) } }}
                            >
                              <Delete fontSize="small" />
                            </IconButton>
                          </Tooltip>
                        </Box>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          )}
      </SectionCard>

      {/* Add / Edit Dialog */}
      <Dialog
        open={dialogOpen}
        onClose={handleCloseDialog}
        maxWidth="sm"
        fullWidth
        PaperProps={{ sx: { borderRadius: '16px' } }}
      >
        <DialogTitle sx={{ bgcolor: COLORS.tableHeader, borderBottom: `1px solid ${COLORS.tableBorder}`, fontWeight: 600, color: COLORS.textPrimary }}>
          {selectedItem ? 'Edit Client' : 'Add New Client'}
        </DialogTitle>
        <DialogContent sx={{ pt: 3 }}>
          {error && <Alert severity="error" sx={{ mb: 2, borderRadius: '8px' }}>{error}</Alert>}
          {duplicateMatch && (
            <Alert severity="warning" sx={{ mb: 2, borderRadius: '8px' }}>
              {duplicateMatch.type === 'GSTIN'
                ? `Duplicate GSTIN: another client (${duplicateMatch.client.name || duplicateMatch.client.id}) already uses this GSTIN.`
                : `PAN must be unique when GSTIN is empty: another client (${duplicateMatch.client.name || duplicateMatch.client.id}) already uses this PAN.`}
            </Alert>
          )}
          <Grid container spacing={2}>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="Client Name"
                required
                autoFocus
                value={formData.name}
                onChange={(e) => setFormData((p) => ({ ...p, name: e.target.value }))}
                sx={{ '& .MuiOutlinedInput-root': { borderRadius: '10px' } }}
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="GSTIN"
                placeholder="22AAAAA0000A1Z5"
                value={formData.gstin}
                onChange={(e) => handleGstinChange(e.target.value)}
                inputProps={{ maxLength: 15 }}
                helperText="15-character GSTIN"
                sx={{ '& .MuiOutlinedInput-root': { borderRadius: '10px' } }}
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="PAN"
                placeholder="AAAAA0000A"
                value={formData.pan}
                onChange={(e) => setFormData((p) => ({ ...p, pan: e.target.value.toUpperCase() }))}
                InputProps={{ readOnly: !!formData.gstin }}
                helperText={formData.gstin ? 'Auto-derived from GSTIN' : '10-character PAN'}
                inputProps={{ maxLength: 10 }}
                sx={{ '& .MuiOutlinedInput-root': { borderRadius: '10px' } }}
              />
            </Grid>
          </Grid>
        </DialogContent>
        <DialogActions sx={{ p: 2.5, borderTop: `1px solid ${COLORS.tableBorder}` }}>
          <Button onClick={handleCloseDialog} sx={{ borderRadius: '8px', textTransform: 'none', color: COLORS.textSecondary }}>
            Cancel
          </Button>
          <Button
            onClick={handleSubmit}
            variant="contained"
            sx={{ borderRadius: '8px', textTransform: 'none', bgcolor: COLORS.primary, '&:hover': { bgcolor: '#0f766e' } }}
          >
            {selectedItem ? 'Update' : 'Create'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Delete Confirmation */}
      <Dialog
        open={deleteDialogOpen}
        onClose={handleCloseDeleteDialog}
        maxWidth="sm"
        fullWidth
        PaperProps={{ sx: { borderRadius: '16px' } }}
      >
        <DialogTitle sx={{
          bgcolor: alpha('#DC2626', 0.06),
          borderBottom: `1px solid ${alpha('#DC2626', 0.12)}`,
          fontWeight: 700,
          color: '#991B1B'
        }}>
          Delete Client
        </DialogTitle>
        <DialogContent sx={{ pt: 3 }}>
          {error && <Alert severity="error" sx={{ mb: 2, borderRadius: '8px' }}>{error}</Alert>}
          {depsLoading ? (
            <Typography color="text.secondary">Checking dependencies...</Typography>
          ) : deleteDeps.length > 0 ? (
            <>
              <Alert severity="warning" sx={{ mb: 2, borderRadius: '8px' }}>
                {deleteMessage || `"${itemToDelete?.name}" is linked to records in other modules. Update those lines first, then delete this client master.`}
              </Alert>
              <Typography variant="body2" sx={{ mb: 1.5, color: COLORS.textPrimary }}>
                {summarizeDependencies(deleteDeps)}
              </Typography>
              {linkedItems.map((item, idx) => (
                <Typography key={idx} variant="body2" sx={{ ml: 2, mb: 0.5, color: COLORS.textPrimary }}>{/*
                  • {dep.count} {dep.type} record(s)
                */}• {item.label} — {item.line}</Typography>
              ))}
            </>
          ) : (
            <Typography variant="body2" sx={{ color: COLORS.textPrimary, lineHeight: 1.7 }}>
              {itemToDelete
                ? `Delete "${itemToDelete.name}"?`
                : 'Delete this client?'}
            </Typography>
          )}
        </DialogContent>
        <DialogActions sx={{ p: 2.5, borderTop: `1px solid ${COLORS.tableBorder}` }}>
          <Button onClick={handleCloseDeleteDialog} sx={{ borderRadius: '8px', textTransform: 'none', color: COLORS.textSecondary }}>
            Cancel
          </Button>
          <Button
            onClick={handleDeleteClient}
            variant="contained"
            color="error"
            disabled={depsLoading || deleteDeps.length > 0}
            sx={{ borderRadius: '8px', textTransform: 'none', boxShadow: 'none' }}
          >
            Delete
          </Button>
        </DialogActions>
      </Dialog>

      {/* Toggle Active Confirmation */}
      <Dialog
        open={toggleConfirmOpen}
        onClose={() => { setToggleConfirmOpen(false); setItemToToggle(null); }}
        maxWidth="xs"
        fullWidth
        PaperProps={{ sx: { borderRadius: '16px' } }}
      >
        <DialogTitle sx={{
          bgcolor: alpha(itemToToggle?.is_active ? '#DC2626' : '#16a34a', 0.06),
          borderBottom: `1px solid ${alpha(itemToToggle?.is_active ? '#DC2626' : '#16a34a', 0.12)}`,
          fontWeight: 700,
          color: itemToToggle?.is_active ? '#991B1B' : '#14532d'
        }}>
          {itemToToggle?.is_active ? 'Deactivate Client' : 'Activate Client'}
        </DialogTitle>
        <DialogContent sx={{ pt: 3 }}>
          <Typography variant="body2" sx={{ color: COLORS.textPrimary, lineHeight: 1.7 }}>
            {itemToToggle?.is_active
              ? `Deactivate "${itemToToggle?.name}"? It will no longer appear in selection lists.`
              : `Activate "${itemToToggle?.name}"? It will become available for selection.`}
          </Typography>
        </DialogContent>
        <DialogActions sx={{ p: 2.5, borderTop: `1px solid ${COLORS.tableBorder}` }}>
          <Button onClick={() => { setToggleConfirmOpen(false); setItemToToggle(null); }} sx={{ borderRadius: '8px', textTransform: 'none', color: COLORS.textSecondary }}>
            Cancel
          </Button>
          <Button
            onClick={handleToggleActive}
            variant="contained"
            color={itemToToggle?.is_active ? 'error' : 'success'}
            sx={{ borderRadius: '8px', textTransform: 'none', boxShadow: 'none' }}
          >
            {itemToToggle?.is_active ? 'Deactivate' : 'Activate'}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default ClientMasters;
