require('dotenv').config();
const express = require('express');
const cors = require('cors');
const path = require('path');

// Import routes
const authRoutes = require('./routes/auth');
const clientRoutes = require('./routes/clients');
const billingNameRoutes = require('./routes/billingNames');
const serviceTypeRoutes = require('./routes/serviceTypes');
const servicePeriodRoutes = require('./routes/servicePeriods');
const teamRoutes = require('./routes/teams');
const expenseHeadRoutes = require('./routes/expenseHeads');
const billFromRoutes = require('./routes/billFroms');
const vendorRoutes = require('./routes/vendors');
const feeMasterRoutes = require('./routes/feeMasters');
const recurringExpenseRoutes = require('./routes/recurringExpenses');
const revenueRoutes = require('./routes/revenues');
const expenseRoutes = require('./routes/expenses');
const reportRoutes = require('./routes/reports');
const userRoutes = require('./routes/users');

const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors({
  origin: process.env.FRONTEND_URL || 'http://localhost:3000',
  credentials: true,
}));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Static files for uploads
app.use('/uploads', express.static(path.join(__dirname, '../uploads')));

// API Routes
app.use('/api/auth', authRoutes);
app.use('/api/users', userRoutes);
app.use('/api/clients', clientRoutes);
app.use('/api/billing-names', billingNameRoutes);
app.use('/api/service-types', serviceTypeRoutes);
app.use('/api/service-periods', servicePeriodRoutes);
app.use('/api/teams', teamRoutes);
app.use('/api/expense-heads', expenseHeadRoutes);
app.use('/api/bill-froms', billFromRoutes);
app.use('/api/vendors', vendorRoutes);
app.use('/api/fee-masters', feeMasterRoutes);
app.use('/api/recurring-expenses', recurringExpenseRoutes);
app.use('/api/revenues', revenueRoutes);
app.use('/api/expenses', expenseRoutes);
app.use('/api/reports', reportRoutes);

// Health check
app.get('/', (req, res) => {
  res.json({
    success: true,
    message: 'JCHPL MIS backend is running',
    health: '/api/health'
  });
});

app.get('/api/health', (req, res) => {
  res.json({ status: 'OK', timestamp: new Date().toISOString() });
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({
    success: false,
    message: 'Internal server error',
    error: process.env.NODE_ENV === 'development' ? err.message : undefined
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    success: false,
    message: 'Route not found'
  });
});

const server = app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`Environment: ${process.env.NODE_ENV}`);
});

server.on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    console.error(`Port ${PORT} is already in use. Stop the existing process or set a different PORT.`);
    process.exit(1);
  }

  console.error('Failed to start server:', err.message);
  process.exit(1);
});

module.exports = app;
