import React, { createContext, useContext, useState, useEffect } from 'react';
import api from '../services/api';

const AuthContext = createContext(null);

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};

export const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);
  const [isAuthenticated, setIsAuthenticated] = useState(false);

  useEffect(() => {
    const token = localStorage.getItem('token');
    const savedUser = localStorage.getItem('user');
    
    if (token && savedUser) {
      setUser(JSON.parse(savedUser));
      setIsAuthenticated(true);
      api.defaults.headers.common['Authorization'] = `Bearer ${token}`;
    }
    setLoading(false);
  }, []);

  const establishSession = (token, userData) => {
    localStorage.setItem('token', token);
    localStorage.setItem('user', JSON.stringify(userData));
    api.defaults.headers.common['Authorization'] = `Bearer ${token}`;

    setUser(userData);
    setIsAuthenticated(true);
  };

  const login = async (username, password) => {
    try {
      const response = await api.post('/auth/login', { username, password });
      const { token, user: userData } = response.data.data;

      establishSession(token, userData);
      
      return { success: true };
    } catch (error) {
      return {
        success: false,
        message: error.response?.data?.message || 'Login failed'
      };
    }
  };

  const logout = async () => {
    try {
      await api.post('/auth/logout');
    } catch (error) {
      console.error('Logout error:', error);
    } finally {
      localStorage.removeItem('token');
      localStorage.removeItem('user');
      delete api.defaults.headers.common['Authorization'];
      setUser(null);
      setIsAuthenticated(false);
    }
  };

  const updateProfile = async (data) => {
    try {
      const response = await api.put('/users/profile', data);
      const updatedUser = response.data.data;
      setUser(prev => ({ ...prev, ...updatedUser }));
      localStorage.setItem('user', JSON.stringify({ ...user, ...updatedUser }));
      return { success: true };
    } catch (error) {
      return {
        success: false,
        message: error.response?.data?.message || 'Update failed'
      };
    }
  };

  const forgotPassword = async (email) => {
    try {
      await api.post('/auth/forgot-password', { email });
      return { success: true };
    } catch (error) {
      return {
        success: false,
        message: error.response?.data?.message || 'Request failed'
      };
    }
  };

  const resetPassword = async (token, password) => {
    try {
      await api.post('/auth/reset-password', { token, password });
      return { success: true };
    } catch (error) {
      return {
        success: false,
        message: error.response?.data?.message || 'Reset failed'
      };
    }
  };

  const value = {
    user,
    loading,
    isAuthenticated,
    establishSession,
    login,
    logout,
    updateProfile,
    forgotPassword,
    resetPassword,
  };

  return (
    <AuthContext.Provider value={value}>
      {!loading && children}
    </AuthContext.Provider>
  );
};
