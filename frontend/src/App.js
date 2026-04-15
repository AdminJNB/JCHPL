import React from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import { useAuth } from './context/AuthContext';
import Layout from './components/Layout';
import Login from './pages/Login';
import ForgotPassword from './pages/ForgotPassword';
import ResetPassword from './pages/ResetPassword';
import Dashboard from './pages/Dashboard';
import Clients from './pages/masters/Clients';
import ClientMasters from './pages/masters/ClientMasters';
import Teams from './pages/masters/Teams';
import MiscMasters from './pages/masters/MiscMasters';
import RecurringExpenses from './pages/masters/RecurringExpenses';
import Revenue from './pages/Revenue';
import Expenses from './pages/Expenses';
import Reports from './pages/Reports';
import Profile from './pages/Profile';
import Settings from './pages/Settings';

const PrivateRoute = ({ children }) => {
  const { isAuthenticated } = useAuth();
  return isAuthenticated ? children : <Navigate to="/login" replace />;
};

const PublicRoute = ({ children }) => {
  const { isAuthenticated } = useAuth();
  return !isAuthenticated ? children : <Navigate to="/" replace />;
};

function App() {
  return (
    <Routes>
      {/* Public Routes */}
      <Route path="/login" element={<PublicRoute><Login /></PublicRoute>} />
      <Route path="/forgot-password" element={<PublicRoute><ForgotPassword /></PublicRoute>} />
      <Route path="/reset-password/:token" element={<PublicRoute><ResetPassword /></PublicRoute>} />
      
      {/* Private Routes */}
      <Route path="/" element={<PrivateRoute><Layout /></PrivateRoute>}>
        <Route index element={<Dashboard />} />
        <Route path="profile" element={<Profile />} />
        <Route path="settings" element={<Settings />} />
        
        {/* Masters */}
        <Route path="masters">
            <Route path="groups" element={<Clients />} />
            <Route path="clients" element={<ClientMasters />} />
          <Route path="teams" element={<Teams />} />
          <Route path="misc" element={<MiscMasters />} />
          <Route path="recurring-expenses" element={<RecurringExpenses />} />
        </Route>
        
        {/* Transactions */}
        <Route path="revenue" element={<Revenue />} />
        <Route path="expenses" element={<Expenses />} />
        
        {/* Reports */}
        <Route path="reports" element={<Reports />} />
      </Route>
      
      {/* 404 */}
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}

export default App;
