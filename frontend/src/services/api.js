import axios from 'axios';

const api = axios.create({
  baseURL: process.env.REACT_APP_API_URL || '/api',
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor
api.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Response interceptor
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('token');
      localStorage.removeItem('user');
      window.location.href = '/login';
    }
    return Promise.reject(error);
  }
);

// API helper functions
export const authAPI = {
  login: (data) => api.post('/auth/login', data),
  register: (data) => api.post('/auth/register', data),
  logout: () => api.post('/auth/logout'),
  forgotPassword: (data) => api.post('/auth/forgot-password', data),
  resetPassword: (data) => api.post('/auth/reset-password', data),
  me: () => api.get('/auth/me'),
};

export const userAPI = {
  getProfile: () => api.get('/users/profile'),
  updateProfile: (data) => api.put('/users/profile', data),
  changePassword: (data) => api.put('/users/change-password', data),
};

export const clientAPI = {
  getAll: (params) => api.get('/clients', { params }),
  getById: (id) => api.get(`/clients/${id}`),
  create: (data) => api.post('/clients', data),
  update: (id, data) => api.put(`/clients/${id}`, data),
  delete: (id) => api.delete(`/clients/${id}`),
  checkDependencies: (id) => api.get(`/clients/${id}/dependencies`),
  addBillingRow: (clientId, data) => api.post(`/clients/${clientId}/billing-rows`, data),
  addBillingRowsBatch: (clientId, data) => api.post(`/clients/${clientId}/billing-rows/batch`, data),
  updateBillingPeriod: (clientId, data) => api.put(`/clients/${clientId}/billing-period`, data),
  updateBillingRow: (clientId, rowId, data) => api.put(`/clients/${clientId}/billing-rows/${rowId}`, data),
  deleteBillingRow: (clientId, rowId) => api.delete(`/clients/${clientId}/billing-rows/${rowId}`),
};

export const billingNameAPI = {
  getAll: (params) => api.get('/billing-names', { params }),
  getById: (id) => api.get(`/billing-names/${id}`),
  create: (data) => api.post('/billing-names', data),
  update: (id, data) => api.put(`/billing-names/${id}`, data),
  delete: (id) => api.delete(`/billing-names/${id}`),
  checkDependencies: (id) => api.get(`/billing-names/${id}/dependencies`),
};

export const serviceTypeAPI = {
  getAll: (params) => api.get('/service-types', { params }),
  getById: (id) => api.get(`/service-types/${id}`),
  create: (data) => api.post('/service-types', data),
  update: (id, data) => api.put(`/service-types/${id}`, data),
  delete: (id, params) => api.delete(`/service-types/${id}`, { params }),
  checkDependencies: (id) => api.get(`/service-types/${id}/dependencies`),
  getHsnGst: (hsn) => api.get(`/service-types/hsn-gst/${hsn}`),
};

export const servicePeriodAPI = {
  getAll: (params) => api.get('/service-periods', { params }),
  getFinancialYears: () => api.get('/service-periods/financial-years'),
  getCurrent: () => api.get('/service-periods/current'),
};

export const teamAPI = {
  getAll: (params) => api.get('/teams', { params }),
  getReviewers: () => api.get('/teams/reviewers'),
  getById: (id) => api.get(`/teams/${id}`),
  create: (data) => api.post('/teams', data),
  update: (id, data) => api.put(`/teams/${id}`, data),
  delete: (id) => api.delete(`/teams/${id}`),
  checkDependencies: (id) => api.get(`/teams/${id}/dependencies`),
  updateCompensationPeriod: (id, data) => api.put(`/teams/${id}/compensation-period`, data),
};

export const expenseHeadAPI = {
  getAll: (params) => api.get('/expense-heads', { params }),
  getById: (id) => api.get(`/expense-heads/${id}`),
  create: (data) => api.post('/expense-heads', data),
  update: (id, data) => api.put(`/expense-heads/${id}`, data),
  delete: (id, params) => api.delete(`/expense-heads/${id}`, params ? { params } : undefined),
  checkDependencies: (id) => api.get(`/expense-heads/${id}/dependencies`),
};

export const billFromAPI = {
  getAll: (params) => api.get('/bill-froms', { params }),
  getById: (id) => api.get(`/bill-froms/${id}`),
  create: (data) => api.post('/bill-froms', data),
  update: (id, data) => api.put(`/bill-froms/${id}`, data),
  delete: (id, params) => api.delete(`/bill-froms/${id}`, params ? { params } : undefined),
  checkDependencies: (id) => api.get(`/bill-froms/${id}/dependencies`),
};

export const vendorAPI = {
  getAll: (params) => api.get('/vendors', { params }),
  getById: (id) => api.get(`/vendors/${id}`),
  create: (data) => api.post('/vendors', data),
  update: (id, data) => api.put(`/vendors/${id}`, data),
  delete: (id, params) => api.delete(`/vendors/${id}`, params ? { params } : undefined),
  checkDependencies: (id) => api.get(`/vendors/${id}/dependencies`),
};

export const recurringExpenseAPI = {
  getAll: (params) => api.get('/recurring-expenses', { params }),
  get: (id) => api.get(`/recurring-expenses/${id}`),
  create: (data) => api.post('/recurring-expenses', data),
  update: (id, data) => api.put(`/recurring-expenses/${id}`, data),
  delete: (id) => api.delete(`/recurring-expenses/${id}`),
  checkDependencies: (id) => api.get(`/recurring-expenses/${id}/dependencies`),
  validate: (data) => api.post('/recurring-expenses/validate', data),
  endAllocationPeriod: (id, allocationId, data) => api.put(`/recurring-expenses/${id}/allocations/${allocationId}/end-period`, data),
  updateAllocation: (id, allocationId, data) => api.patch(`/recurring-expenses/${id}/allocations/${allocationId}`, data),
};

export const feeMasterAPI = {
  getAll: (params) => api.get('/fee-masters', { params }),
  getForDataEntry: (params) => api.get('/fee-masters/for-data-entry', { params }),
  getCurrencies: () => api.get('/fee-masters/currencies'),
  getById: (id) => api.get(`/fee-masters/${id}`),
  create: (data) => api.post('/fee-masters', data),
  update: (id, data) => api.put(`/fee-masters/${id}`, data),
  end: (id, data) => api.put(`/fee-masters/${id}/end`, data),
  renew: (id, data) => api.post(`/fee-masters/${id}/renew`, data),
  delete: (id) => api.delete(`/fee-masters/${id}`),
};

export const revenueAPI = {
  getAll: (params) => api.get('/revenues', { params }),
  getSummary: (params) => api.get('/revenues/summary', { params }),
  getById: (id) => api.get(`/revenues/${id}`),
  create: (data) => api.post('/revenues', data),
  update: (id, data) => api.put(`/revenues/${id}`, data),
  delete: (id) => api.delete(`/revenues/${id}`),
};

export const expenseAPI = {
  getAll: (params) => api.get('/expenses', { params }),
  getSummary: (params) => api.get('/expenses/summary', { params }),
  getById: (id) => api.get(`/expenses/${id}`),
  create: (data) => api.post('/expenses', data),
  update: (id, data) => api.put(`/expenses/${id}`, data),
  delete: (id) => api.delete(`/expenses/${id}`),
};

export const reportAPI = {
  getDashboard: (params) => api.get('/reports/dashboard', { params }),
  getRevenueClientWise: (params) => api.get('/reports/revenue/client-wise', { params }),
  getRevenueReviewerWise: (params) => api.get('/reports/revenue/reviewer-wise', { params }),
  getRevenueBillingWise: (params) => api.get('/reports/revenue/billing-wise', { params }),
  getExpenseClientWise: (params) => api.get('/reports/expense/client-wise', { params }),
  getMonthlyTrend: (params) => api.get('/reports/trend/monthly', { params }),
  getProfitability: (params) => api.get('/reports/profitability/client-wise', { params }),
  getOverall: (params) => api.get('/reports/overall', { params }),
  getDrilldown: (clientId, params) => api.get(`/reports/drilldown/client/${clientId}`, { params }),
  getHeadWise: (params) => api.get('/reports/summary/head-wise', { params }),
  getFyWise: (params) => api.get('/reports/summary/fy-wise', { params }),
  getMatrix: (params) => api.get('/reports/matrix', { params }),
  getVariance: (params) => api.get('/reports/variance', { params }),
};

export default api;
