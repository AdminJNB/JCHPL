import React, { useState, useEffect, useCallback } from 'react';
import {
  Box, Button, Card, CardContent, TextField, Dialog, DialogTitle,
  DialogContent, DialogActions, IconButton, Typography, Chip, Alert,
  Tabs, Tab, Table, TableBody, TableCell, TableHead, TableRow,
  TableContainer, Grid, Switch, FormControlLabel, ToggleButton, ToggleButtonGroup, Tooltip,
  List, ListItem, ListItemIcon, ListItemText
} from '@mui/material';
import { Add, Edit, Search, Refresh, Check, Close, Delete, Warning, LinkOff } from '@mui/icons-material';
import { toast } from 'react-toastify';
import {
  HoverActionButton,
  MetricCard,
  PageHeader,
  SectionCard,
} from '../../components/FuturisticUI';
import { serviceTypeAPI, expenseHeadAPI, billFromAPI, vendorAPI } from '../../services/api';
import { flattenDependencyItems, summarizeDependencies } from '../../utils/deleteDependencies';

// Tab Panel Component
const TabPanel = ({ children, value, index, ...other }) => (
  <div
    role="tabpanel"
    hidden={value !== index}
    id={`misc-tabpanel-${index}`}
    {...other}
  >
    {value === index && <Box sx={{ py: 3 }}>{children}</Box>}
  </div>
);

// Generic Master Table Component
const MasterTable = ({ 
  title, 
  items, 
  columns, 
  onAdd, 
  onEdit, 
  onToggleActive,
  onDelete,
  loading,
  searchTerm,
  statusFilter,
  canDelete
}) => {
  const filteredItems = items.filter(item => {
    if (!item.name.toLowerCase().includes(searchTerm.toLowerCase())) return false;
    if (statusFilter === 'active') return item.is_active;
    if (statusFilter === 'inactive') return !item.is_active;
    return true;
  });

  return (
    <Card>
      <CardContent sx={{ p: 0 }}>
        {loading ? (
          <Box sx={{ p: 4, textAlign: 'center' }}>
            <Typography>Loading {title.toLowerCase()}...</Typography>
          </Box>
        ) : filteredItems.length === 0 ? (
          <Box sx={{ p: 4, textAlign: 'center' }}>
            <Typography color="text.secondary">No {title.toLowerCase()} found</Typography>
            <Button 
              variant="contained" 
              startIcon={<Add />} 
              sx={{ mt: 2 }}
              onClick={() => onAdd()}
            >
              Add {title}
            </Button>
          </Box>
        ) : (
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow sx={{ bgcolor: 'grey.100' }}>
                  {columns.map((col) => (
                    <TableCell key={col.field} align={col.align || 'left'}>
                      {col.headerName}
                    </TableCell>
                  ))}
                  <TableCell align="center">Status</TableCell>
                  <TableCell align="center">Actions</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {filteredItems.map((item) => (
                  <TableRow 
                    key={item.id}
                    sx={{ 
                      opacity: item.is_active ? 1 : 0.6,
                      bgcolor: item.is_active ? 'inherit' : 'grey.50'
                    }}
                  >
                    {columns.map((col) => (
                      <TableCell key={col.field} align={col.align || 'left'}>
                        {col.render ? col.render(item) : item[col.field] || '-'}
                      </TableCell>
                    ))}
                    <TableCell align="center">
                      <Chip 
                        label={item.is_active ? 'Active' : 'Inactive'} 
                        color={item.is_active ? 'success' : 'default'}
                        size="small"
                      />
                    </TableCell>
                    <TableCell align="center">
                      <Tooltip title="Edit">
                        <IconButton size="small" onClick={() => onEdit(item)}>
                          <Edit fontSize="small" />
                        </IconButton>
                      </Tooltip>
                      <Tooltip title={item.is_active ? 'Deactivate' : 'Activate'}>
                        <IconButton 
                          size="small" 
                          onClick={() => onToggleActive(item)}
                          color={item.is_active ? 'error' : 'success'}
                        >
                          {item.is_active ? <Close fontSize="small" /> : <Check fontSize="small" />}
                        </IconButton>
                      </Tooltip>
                      {canDelete && (
                      <Tooltip title="Delete">
                        <IconButton
                          size="small"
                          onClick={() => onDelete(item)}
                          sx={{ color: '#DC2626', '&:hover': { bgcolor: 'rgba(220,38,38,0.08)' } }}
                        >
                          <Delete fontSize="small" />
                        </IconButton>
                      </Tooltip>
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        )}
      </CardContent>
    </Card>
  );
};

const MiscMasters = () => {
  const [activeTab, setActiveTab] = useState(0);
  const [serviceTypes, setServiceTypes] = useState([]);
  const [expenseHeads, setExpenseHeads] = useState([]);
  const [billFroms, setBillFroms] = useState([]);
  const [vendors, setVendors] = useState([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [statusFilter, setStatusFilter] = useState('active');
  const [dialogOpen, setDialogOpen] = useState(false);
  const [dialogType, setDialogType] = useState(''); // 'serviceType', 'expenseHead', 'billFrom'
  const [selectedItem, setSelectedItem] = useState(null);
  const [formData, setFormData] = useState({});
  const [error, setError] = useState('');
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [itemToDelete, setItemToDelete] = useState(null);
  const [deleteType, setDeleteType] = useState('');
  const [dependencies, setDependencies] = useState([]);
  const [deleteMessage, setDeleteMessage] = useState('');
  const [depsLoading, setDepsLoading] = useState(false);
  const [forceDelete, setForceDelete] = useState(false);

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const [stRes, ehRes, bfRes, vRes] = await Promise.all([
        serviceTypeAPI.getAll({ includeInactive: true }),
        expenseHeadAPI.getAll({ includeInactive: true }),
        billFromAPI.getAll({ includeInactive: true }),
        vendorAPI.getAll({ includeInactive: true })
      ]);
      setServiceTypes(stRes.data.data || []);
      setExpenseHeads(ehRes.data.data || []);
      setBillFroms(bfRes.data.data || []);
      setVendors(vRes.data.data || []);
    } catch (err) {
      toast.error('Failed to load data');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadData();
  }, [loadData]);

  const getDefaultFormData = (type) => {
    switch (type) {
      case 'serviceType':
        return { name: '', hsn_code: '', gst_rate: 18 };
      case 'expenseHead':
        return { name: '', is_recurring: false };
      case 'billFrom':
        return { name: '' };
      case 'vendor':
        return { name: '', gstin: '', pan: '', contact_person: '', email: '', mobile: '' };
      default:
        return {};
    }
  };

  const handleOpenDialog = (type, item = null) => {
    setDialogType(type);
    setSelectedItem(item);
    setFormData(item ? { ...item } : getDefaultFormData(type));
    setError('');
    setDialogOpen(true);
  };

  const handleCloseDialog = () => {
    setDialogOpen(false);
    setDialogType('');
    setSelectedItem(null);
    setFormData({});
    setError('');
  };

  const handleSubmit = async () => {
    if (!formData.name?.trim()) {
      setError('Name is required');
      return;
    }

    try {
      let api, successMessage;
      switch (dialogType) {
        case 'serviceType':
          api = serviceTypeAPI;
          successMessage = 'Service Type';
          break;
        case 'expenseHead':
          api = expenseHeadAPI;
          successMessage = 'Expense Head';
          break;
        case 'billFrom':
          api = billFromAPI;
          successMessage = 'Bill From';
          break;
        case 'vendor':
          api = vendorAPI;
          successMessage = 'Vendor';
          break;
        default:
          return;
      }

      const payload = { ...formData, name: formData.name.trim() };

      if (selectedItem) {
        await api.update(selectedItem.id, payload);
        toast.success(`${successMessage} updated successfully`);
      } else {
        await api.create(payload);
        toast.success(`${successMessage} created successfully`);
      }

      handleCloseDialog();
      loadData();
    } catch (err) {
      setError(err.response?.data?.message || 'Operation failed');
    }
  };

  const handleToggleActive = async (type, item) => {
    try {
      let api;
      switch (type) {
        case 'serviceType':
          api = serviceTypeAPI;
          break;
        case 'expenseHead':
          api = expenseHeadAPI;
          break;
        case 'billFrom':
          api = billFromAPI;
          break;
        case 'vendor':
          api = vendorAPI;
          break;
        default:
          return;
      }

      await api.update(item.id, { is_active: !item.is_active });
      toast.success(`${item.name} ${item.is_active ? 'deactivated' : 'activated'} successfully`);
      loadData();
    } catch (err) {
      toast.error(err.response?.data?.message || 'Failed to update status');
    }
  };

  const getApiForType = (type) => {
    switch (type) {
      case 'serviceType': return serviceTypeAPI;
      case 'expenseHead': return expenseHeadAPI;
      case 'billFrom': return billFromAPI;
      case 'vendor': return vendorAPI;
      default: return null;
    }
  };

  const handleOpenDeleteDialog = async (type, item) => {
    setDeleteType(type);
    setItemToDelete(item);
    setDependencies([]);
    setDeleteMessage('');
    setForceDelete(false);
    setDepsLoading(true);
    setDeleteDialogOpen(true);
    try {
      const api = getApiForType(type);
      if (api?.checkDependencies) {
        const res = await api.checkDependencies(item.id);
        setDependencies(res.data?.data?.dependencies || []);
        setDeleteMessage(res.data?.data?.message || '');
      }
    } catch {
      setDependencies([]);
      setDeleteMessage('');
    } finally {
      setDepsLoading(false);
    }
  };

  const handleDelete = async () => {
    if (!itemToDelete) return;
    try {
      const api = getApiForType(deleteType);
      if (!api) return;
      const params = forceDelete ? { force: 'true' } : undefined;
      await api.delete(itemToDelete.id, params);
      const actionText = itemToDelete.is_active ? 'deactivated successfully' : 'deleted permanently';
      toast.success(`${itemToDelete.name} ${actionText}`);
      setDeleteDialogOpen(false);
      setItemToDelete(null);
      setDependencies([]);
      setDeleteMessage('');
      setForceDelete(false);
      loadData();
    } catch (err) {
      if (err.response?.status === 409) {
        setDependencies(err.response?.data?.data?.dependencies || []);
        setDeleteMessage(err.response?.data?.message || err.response?.data?.data?.message || 'Delete is blocked because this item still has linked records.');
        toast.error(err.response?.data?.message || 'Delete is blocked until linked records are corrected.');
      } else {
        toast.error(err.response?.data?.message || 'Failed to delete');
      }
    }
  };

  const getDeleteLabel = (type) => {
    switch (type) {
      case 'serviceType': return 'Service Type';
      case 'expenseHead': return 'Expense Head';
      case 'billFrom': return 'Bill From';
      case 'vendor': return 'Vendor';
      default: return 'Item';
    }
  };

  const getDeleteDialogTitle = (type, isActive) => {
    if (isActive) return `Deactivate ${getDeleteLabel(type)}`;
    return `Delete ${getDeleteLabel(type)} permanently`;
  };

  const getDeleteButtonLabel = (isActive) => {
    if (isActive) return 'Deactivate';
    return 'Delete permanently';
  };

  const linkedItems = flattenDependencyItems(dependencies);

  // Column definitions for each table
  const serviceTypeColumns = [
    { field: 'name', headerName: 'Service Type Name' },
    { field: 'hsn_code', headerName: 'HSN Code' },
    { 
      field: 'gst_rate', 
      headerName: 'GST Rate',
      align: 'right',
      render: (item) => item.gst_rate ? `${item.gst_rate}%` : '-'
    }
  ];

  const expenseHeadColumns = [
    { field: 'name', headerName: 'Expense Head Name' },
    { 
      field: 'is_recurring', 
      headerName: 'Recurring',
      align: 'center',
      render: (item) => (
        <Chip 
          label={item.is_recurring ? 'Yes' : 'No'} 
          color={item.is_recurring ? 'primary' : 'default'}
          size="small"
          variant="outlined"
        />
      )
    }
  ];

  const billFromColumns = [
    { field: 'name', headerName: 'Bill From Entity Name' }
  ];

  const vendorColumns = [
    { field: 'name', headerName: 'Vendor Name' },
    { field: 'gstin', headerName: 'GSTIN' },
    { field: 'pan', headerName: 'PAN' },
    { field: 'contact_person', headerName: 'Contact Person' },
    { field: 'email', headerName: 'Email' },
    { field: 'mobile', headerName: 'Mobile' },
  ];

  const getDialogTitle = () => {
    const action = selectedItem ? 'Edit' : 'Add';
    switch (dialogType) {
      case 'serviceType': return `${action} Service Type`;
      case 'expenseHead': return `${action} Expense Head`;
      case 'billFrom': return `${action} Bill From`;
      case 'vendor': return `${action} Vendor`;
      default: return '';
    }
  };

  const renderDialogContent = () => {
    switch (dialogType) {
      case 'serviceType':
        return (
          <>
            <TextField
              fullWidth
              label="Service Type Name"
              value={formData.name || ''}
              onChange={(e) => setFormData({ ...formData, name: e.target.value })}
              margin="normal"
              required
              autoFocus
            />
            <TextField
              fullWidth
              label="HSN Code"
              value={formData.hsn_code || ''}
              onChange={(e) => setFormData({ ...formData, hsn_code: e.target.value })}
              margin="normal"
            />
            <TextField
              fullWidth
              label="GST Rate (%)"
              type="number"
              value={formData.gst_rate || ''}
              onChange={(e) => setFormData({ ...formData, gst_rate: parseFloat(e.target.value) || 0 })}
              margin="normal"
              inputProps={{ min: 0, max: 100, step: 0.01 }}
            />
          </>
        );
      case 'expenseHead':
        return (
          <>
            <TextField
              fullWidth
              label="Expense Head Name"
              value={formData.name || ''}
              onChange={(e) => setFormData({ ...formData, name: e.target.value })}
              margin="normal"
              required
              autoFocus
            />
            <FormControlLabel
              control={
                <Switch
                  checked={formData.is_recurring || false}
                  onChange={(e) => setFormData({ ...formData, is_recurring: e.target.checked })}
                  color="primary"
                />
              }
              label="Is Recurring Expense"
              sx={{ mt: 2 }}
            />
          </>
        );
      case 'billFrom':
        return (
          <TextField
            fullWidth
            label="Bill From Entity Name"
            value={formData.name || ''}
            onChange={(e) => setFormData({ ...formData, name: e.target.value })}
            margin="normal"
            required
            autoFocus
          />
        );
      case 'vendor':
        return (
          <>
            <TextField
              fullWidth
              label="Vendor Name"
              value={formData.name || ''}
              onChange={(e) => setFormData({ ...formData, name: e.target.value })}
              margin="normal"
              required
              autoFocus
            />
            <TextField
              fullWidth
              label="GSTIN"
              value={formData.gstin || ''}
              onChange={(e) => {
                const gstin = e.target.value.toUpperCase();
                const updates = { gstin };
                // Auto-extract PAN from GSTIN (characters 3–12, 0-indexed 2–11)
                if (gstin.length === 15) {
                  updates.pan = gstin.substring(2, 12);
                }
                setFormData({ ...formData, ...updates });
              }}
              margin="normal"
              inputProps={{ maxLength: 15, style: { textTransform: 'uppercase' } }}
              helperText="PAN will be auto-filled when 15-digit GSTIN is entered"
            />
            <TextField
              fullWidth
              label="PAN"
              value={formData.pan || ''}
              onChange={(e) => setFormData({ ...formData, pan: e.target.value.toUpperCase() })}
              margin="normal"
              inputProps={{ maxLength: 10, style: { textTransform: 'uppercase' } }}
              helperText={
                formData.gstin && formData.gstin.length === 15
                  ? 'Auto-filled from GSTIN'
                  : 'Must be unique if no GSTIN is provided'
              }
            />
            <TextField
              fullWidth
              label="Contact Person Name"
              value={formData.contact_person || ''}
              onChange={(e) => setFormData({ ...formData, contact_person: e.target.value })}
              margin="normal"
            />
            <TextField
              fullWidth
              label="Email ID"
              value={formData.email || ''}
              onChange={(e) => setFormData({ ...formData, email: e.target.value })}
              margin="normal"
              type="email"
            />
            <TextField
              fullWidth
              label="Mobile No."
              value={formData.mobile || ''}
              onChange={(e) => setFormData({ ...formData, mobile: e.target.value })}
              margin="normal"
              inputProps={{ maxLength: 20 }}
            />
          </>
        );
      default:
        return null;
    }
  };

  return (
    <Box>
      <PageHeader
        eyebrow="Master Setup"
        title="Miscellaneous masters"
        actions={[
          <HoverActionButton key="refresh" icon={<Refresh fontSize="small" />} label="Refresh" onClick={loadData} tone="peach" />,
          <HoverActionButton
            key="add"
            icon={<Add fontSize="small" />}
            label="Add new"
            onClick={() => {
              const type = activeTab === 0 ? 'serviceType' : activeTab === 1 ? 'expenseHead' : activeTab === 2 ? 'billFrom' : 'vendor';
              handleOpenDialog(type);
            }}
            tone="mint"
          />,
        ]}
      />

      <Grid container spacing={2} sx={{ mb: 2.5 }}>
        <Grid item xs={12} md={3}>
          <MetricCard eyebrow="Service Types" title="Configured items" value={String(serviceTypes.length)} tone="mint" />
        </Grid>
        <Grid item xs={12} md={3}>
          <MetricCard eyebrow="Expense Heads" title="Configured items" value={String(expenseHeads.length)} tone="peach" />
        </Grid>
        <Grid item xs={12} md={3}>
          <MetricCard eyebrow="Bill From" title="Configured items" value={String(billFroms.length)} tone="sand" />
        </Grid>
        <Grid item xs={12} md={3}>
          <MetricCard eyebrow="Vendors" title="Configured items" value={String(vendors.length)} tone="sage" />
        </Grid>
      </Grid>

      <SectionCard title="Master controls" tone="sage" contentSx={{ p: 2 }}>
          <Grid container spacing={2} alignItems="center">
            <Grid item xs={12} md={4}>
              <TextField
                fullWidth
                placeholder="Search..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                InputProps={{
                  startAdornment: <Search sx={{ color: 'text.secondary', mr: 1 }} />
                }}
                size="small"
              />
            </Grid>
            <Grid item xs={12} md={4}>
              <ToggleButtonGroup value={statusFilter} exclusive size="small" onChange={(_, v) => v && setStatusFilter(v)}>
                <ToggleButton value="active">Active</ToggleButton>
                <ToggleButton value="inactive">Inactive</ToggleButton>
                <ToggleButton value="all">All</ToggleButton>
              </ToggleButtonGroup>
            </Grid>
          </Grid>
      </SectionCard>

      <SectionCard title="Master records" tone="mint" contentSx={{ p: 0 }}>
        <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
          <Tabs 
            value={activeTab} 
            onChange={(e, newValue) => setActiveTab(newValue)}
            sx={{ px: 2 }}
          >
            <Tab label={`Service Types (${serviceTypes.filter(s => statusFilter === 'all' ? true : statusFilter === 'inactive' ? !s.is_active : s.is_active).length})`} />
            <Tab label={`Expense Heads (${expenseHeads.filter(e => statusFilter === 'all' ? true : statusFilter === 'inactive' ? !e.is_active : e.is_active).length})`} />
            <Tab label={`Bill From (${billFroms.filter(b => statusFilter === 'all' ? true : statusFilter === 'inactive' ? !b.is_active : b.is_active).length})`} />
            <Tab label={`Vendors (${vendors.filter(v => statusFilter === 'all' ? true : statusFilter === 'inactive' ? !v.is_active : v.is_active).length})`} />
          </Tabs>
        </Box>
        
        <TabPanel value={activeTab} index={0}>
          <MasterTable
            title="Service Type"
            items={serviceTypes}
            columns={serviceTypeColumns}
            onAdd={() => handleOpenDialog('serviceType')}
            onEdit={(item) => handleOpenDialog('serviceType', item)}
            onToggleActive={(item) => handleToggleActive('serviceType', item)}
            onDelete={(item) => handleOpenDeleteDialog('serviceType', item)}
            loading={loading}
            searchTerm={searchTerm}
            statusFilter={statusFilter}
            canDelete
          />
        </TabPanel>
        
        <TabPanel value={activeTab} index={1}>
          <MasterTable
            title="Expense Head"
            items={expenseHeads}
            columns={expenseHeadColumns}
            onAdd={() => handleOpenDialog('expenseHead')}
            onEdit={(item) => handleOpenDialog('expenseHead', item)}
            onToggleActive={(item) => handleToggleActive('expenseHead', item)}
            onDelete={(item) => handleOpenDeleteDialog('expenseHead', item)}
            loading={loading}
            searchTerm={searchTerm}
            statusFilter={statusFilter}
            canDelete
          />
        </TabPanel>
        
        <TabPanel value={activeTab} index={2}>
          <MasterTable
            title="Bill From"
            items={billFroms}
            columns={billFromColumns}
            onAdd={() => handleOpenDialog('billFrom')}
            onEdit={(item) => handleOpenDialog('billFrom', item)}
            onToggleActive={(item) => handleToggleActive('billFrom', item)}
            onDelete={(item) => handleOpenDeleteDialog('billFrom', item)}
            loading={loading}
            searchTerm={searchTerm}
            statusFilter={statusFilter}
            canDelete
          />
          <MasterTable
            title="Vendor"
            items={vendors}
            columns={vendorColumns}
            onAdd={() => handleOpenDialog('vendor')}
            onEdit={(item) => handleOpenDialog('vendor', item)}
            onToggleActive={(item) => handleToggleActive('vendor', item)}
            onDelete={(item) => handleOpenDeleteDialog('vendor', item)}
            loading={loading}
            searchTerm={searchTerm}
            statusFilter={statusFilter}
            canDelete
          />
        </TabPanel>
      </SectionCard>

      {/* Add/Edit Dialog */}
      <Dialog open={dialogOpen} onClose={handleCloseDialog} maxWidth="sm" fullWidth>
        <DialogTitle>{getDialogTitle()}</DialogTitle>
        <DialogContent>
          {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
          {renderDialogContent()}
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCloseDialog}>Cancel</Button>
          <Button onClick={handleSubmit} variant="contained">
            {selectedItem ? 'Update' : 'Create'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Delete / Dependency Dialog */}
      <Dialog
        open={deleteDialogOpen}
        onClose={() => { setDeleteDialogOpen(false); setItemToDelete(null); setDependencies([]); setDeleteMessage(''); }}
        maxWidth="sm"
        fullWidth
        PaperProps={{ sx: { borderRadius: '16px' } }}
      >
        <DialogTitle sx={{
          bgcolor: 'rgba(220,38,38,0.06)',
          borderBottom: '1px solid rgba(220,38,38,0.12)',
          fontWeight: 700,
          color: '#991B1B'
        }}>
          {getDeleteDialogTitle(deleteType, itemToDelete?.is_active)}
        </DialogTitle>
        <DialogContent sx={{ pt: 3 }}>
          {depsLoading ? (
            <Typography color="text.secondary">Checking dependencies...</Typography>
          ) : dependencies.length > 0 ? (
            <>
              <Alert severity="warning" icon={<Warning />} sx={{ mb: 2, borderRadius: '8px' }}>
                {deleteMessage || `"${itemToDelete?.name}" is linked to records in other modules. Update those lines first, then delete this master.`}
              </Alert>
              <Typography variant="body2" sx={{ mb: 1.5 }}>
                {summarizeDependencies(dependencies)}
              </Typography>
              <List dense>
                {linkedItems.map((item, idx) => (
                  <ListItem key={idx}>
                    <ListItemIcon sx={{ minWidth: 36 }}>
                      <LinkOff fontSize="small" color="error" />
                    </ListItemIcon>
                    <ListItemText
                      primary={`${item.label}`}
                      secondary={`${item.line} | Module: ${item.dependency_module}`}
                    />
                  </ListItem>
                ))}
              </List>
            </>
          ) : (
            <Typography variant="body2" sx={{ lineHeight: 1.7 }}>
              {itemToDelete?.is_active
                ? `This will mark "${itemToDelete?.name}" as inactive. It will remain visible on the inactive list until deleted permanently.`
                : `This will permanently remove "${itemToDelete?.name}" from the system.`
              }
            </Typography>
          )}
        </DialogContent>
        <DialogActions sx={{ p: 2.5, borderTop: '1px solid rgba(0,0,0,0.08)' }}>
          <Button
            onClick={() => { setDeleteDialogOpen(false); setItemToDelete(null); setDependencies([]); setDeleteMessage(''); setForceDelete(false); }}
            sx={{ borderRadius: '8px', textTransform: 'none' }}
          >
            Cancel
          </Button>
          {dependencies.length > 0 && !itemToDelete?.is_active && (
            <Button
              onClick={() => setForceDelete(!forceDelete)}
              variant={forceDelete ? 'contained' : 'outlined'}
              color={forceDelete ? 'warning' : 'inherit'}
              sx={{ borderRadius: '8px', textTransform: 'none' }}
            >
              {forceDelete ? '✓ Force Delete Enabled' : 'Enable Force Delete'}
            </Button>
          )}
          <Button
            onClick={handleDelete}
            variant="contained"
            color={dependencies.length > 0 && !forceDelete ? 'inherit' : 'error'}
            disabled={depsLoading || (dependencies.length > 0 && !forceDelete)}
            sx={{ borderRadius: '8px', textTransform: 'none', boxShadow: 'none' }}
          >
            {getDeleteButtonLabel(itemToDelete?.is_active)}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default MiscMasters;
