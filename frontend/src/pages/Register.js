import React, { useState } from 'react';
import { useNavigate, Link as RouterLink } from 'react-router-dom';
import {
  Box, TextField, Button, Typography, Link,
  InputAdornment, IconButton, Alert, CircularProgress
} from '@mui/material';
import { Visibility, VisibilityOff, PersonAdd } from '@mui/icons-material';
import { authAPI } from '../services/api';
import { useAuth } from '../context/AuthContext';

const Register = () => {
  const [email, setEmail] = useState('');
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [showConfirmPassword, setShowConfirmPassword] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();
  const { establishSession } = useAuth();

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setSuccess('');

    if (password !== confirmPassword) {
      setError('Passwords do not match');
      return;
    }

    setLoading(true);

    try {
      const response = await authAPI.register({ email, username, password, confirmPassword });
      if (response.data?.success) {
        const { token, user } = response.data?.data || {};

        if (token && user) {
          establishSession(token, user);
          navigate('/');
          return;
        }

        setSuccess('Registration successful. Please sign in.');
        setTimeout(() => navigate('/login'), 1200);
      } else {
        setError(response.data?.message || 'Registration failed');
      }
    } catch (err) {
      const serverMessage = err.response?.data?.message;
      const validationError = err.response?.data?.errors?.[0]?.msg;
      setError(serverMessage || validationError || 'Registration failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Box sx={{ minHeight: '100vh', display: 'flex' }}>
      <Box sx={{
        display: { xs: 'none', md: 'flex' },
        width: '45%', flexDirection: 'column', justifyContent: 'center', alignItems: 'center',
        background: 'linear-gradient(135deg, #f5f3ff 0%, #e0e7ff 40%, #c7d2fe 100%)',
        position: 'relative', overflow: 'hidden',
      }}>
        <Box sx={{
          position: 'absolute', top: -100, right: -100, width: 350, height: 350,
          borderRadius: '50%', background: 'rgba(79, 70, 229, 0.08)',
        }} />
        <Box sx={{
          position: 'absolute', bottom: -140, left: -80, width: 420, height: 420,
          borderRadius: '50%', background: 'rgba(139, 92, 246, 0.06)',
        }} />
        <Box sx={{
          position: 'absolute', top: '30%', left: '10%', width: 180, height: 180,
          borderRadius: '50%', background: 'rgba(37, 99, 235, 0.08)',
        }} />
        <Box sx={{ position: 'relative', zIndex: 1, textAlign: 'center', px: 6 }}>
          <Box sx={{
            width: 72, height: 72, borderRadius: 4, mx: 'auto', mb: 3,
            background: 'linear-gradient(135deg, #4f46e5 0%, #818cf8 100%)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            fontSize: 26, fontWeight: 800, color: '#fff',
            boxShadow: '0 8px 30px rgba(79, 70, 229, 0.25)',
          }}>
            JC
          </Box>
          <Typography variant="h3" sx={{ color: '#1e293b', fontWeight: 800, mb: 1, letterSpacing: '-0.03em' }}>
            Join JCHPL MIS
          </Typography>
          <Typography sx={{ color: '#475569', fontSize: '1.05rem', maxWidth: 320, mx: 'auto' }}>
            Create your account to manage financial operations and reporting with ease.
          </Typography>
        </Box>
      </Box>

      <Box sx={{
        flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center',
        bgcolor: '#fafaf8', p: 3,
      }}>
        <Box sx={{ maxWidth: 420, width: '100%' }}>
          <Box sx={{ mb: 4, display: { xs: 'block', md: 'none' }, textAlign: 'center' }}>
            <Typography variant="h4" color="primary" fontWeight={800}>JCHPL MIS</Typography>
          </Box>

          <Typography variant="h5" fontWeight={700} sx={{ mb: 0.5 }}>Create your account</Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>Enter your details to register a new user.</Typography>

          {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
          {success && <Alert severity="success" sx={{ mb: 2 }}>{success}</Alert>}

          <form onSubmit={handleSubmit}>
            <TextField
              fullWidth label="Email address" value={email}
              onChange={(e) => setEmail(e.target.value)}
              margin="normal" required disabled={loading}
              type="email"
            />
            <TextField
              fullWidth label="Username" value={username}
              onChange={(e) => setUsername(e.target.value)}
              margin="normal" required disabled={loading}
            />
            <TextField
              fullWidth label="Password" type={showPassword ? 'text' : 'password'}
              value={password} onChange={(e) => setPassword(e.target.value)}
              margin="normal" required disabled={loading}
              InputProps={{
                endAdornment: (
                  <InputAdornment position="end">
                    <IconButton onClick={() => setShowPassword(!showPassword)} edge="end">
                      {showPassword ? <VisibilityOff /> : <Visibility />}
                    </IconButton>
                  </InputAdornment>
                ),
              }}
            />
            <TextField
              fullWidth label="Confirm password" type={showConfirmPassword ? 'text' : 'password'}
              value={confirmPassword} onChange={(e) => setConfirmPassword(e.target.value)}
              margin="normal" required disabled={loading}
              InputProps={{
                endAdornment: (
                  <InputAdornment position="end">
                    <IconButton onClick={() => setShowConfirmPassword(!showConfirmPassword)} edge="end">
                      {showConfirmPassword ? <VisibilityOff /> : <Visibility />}
                    </IconButton>
                  </InputAdornment>
                ),
              }}
            />
            <Button
              type="submit" fullWidth variant="contained" size="large"
              sx={{ mt: 3, mb: 2, py: 1.4, fontSize: '0.9375rem' }}
              disabled={loading}
              startIcon={loading ? <CircularProgress size={20} color="inherit" /> : <PersonAdd />}
            >
              {loading ? 'Registering...' : 'Create Account'}
            </Button>
          </form>

          <Box sx={{ textAlign: 'center' }}>
            <Typography variant="body2" sx={{ color: 'text.secondary' }}>
              Already have an account?{' '}
              <Link component={RouterLink} to="/login" underline="hover" sx={{ color: 'primary.main' }}>
                Sign in
              </Link>
            </Typography>
          </Box>
        </Box>
      </Box>
    </Box>
  );
};

export default Register;
