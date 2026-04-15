import React, { useState } from 'react';
import { Link as RouterLink } from 'react-router-dom';
import {
  Box, TextField, Button, Typography, Link,
  InputAdornment, IconButton, Alert, CircularProgress
} from '@mui/material';
import { Visibility, VisibilityOff, Login as LoginIcon } from '@mui/icons-material';
import { useAuth } from '../context/AuthContext';

const Login = () => {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const { login } = useAuth();

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);

    const result = await login(username, password);
    
    if (!result.success) {
      setError(result.message);
    }
    setLoading(false);
  };

  return (
    <Box sx={{ minHeight: '100vh', display: 'flex' }}>
      {/* Left panel - branding */}
      <Box sx={{
        display: { xs: 'none', md: 'flex' },
        width: '45%', flexDirection: 'column', justifyContent: 'center', alignItems: 'center',
        background: 'linear-gradient(135deg, #f0fdfa 0%, #ccfbf1 40%, #99f6e4 100%)',
        position: 'relative', overflow: 'hidden',
      }}>
        <Box sx={{
          position: 'absolute', top: -100, right: -100, width: 350, height: 350,
          borderRadius: '50%', background: 'rgba(13, 148, 136, 0.08)',
        }} />
        <Box sx={{
          position: 'absolute', bottom: -140, left: -80, width: 420, height: 420,
          borderRadius: '50%', background: 'rgba(225, 29, 72, 0.06)',
        }} />
        <Box sx={{
          position: 'absolute', top: '30%', left: '10%', width: 180, height: 180,
          borderRadius: '50%', background: 'rgba(8, 145, 178, 0.08)',
        }} />
        <Box sx={{ position: 'relative', zIndex: 1, textAlign: 'center', px: 6 }}>
          <Box sx={{
            width: 72, height: 72, borderRadius: 4, mx: 'auto', mb: 3,
            background: 'linear-gradient(135deg, #0d9488 0%, #5eead4 100%)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            fontSize: 26, fontWeight: 800, color: '#fff',
            boxShadow: '0 8px 30px rgba(13, 148, 136, 0.25)',
          }}>
            JC
          </Box>
          <Typography variant="h3" sx={{ color: '#1c1917', fontWeight: 800, mb: 1, letterSpacing: '-0.03em' }}>
            JCHPL MIS
          </Typography>
          <Typography sx={{ color: '#78716c', fontSize: '1.05rem', maxWidth: 320, mx: 'auto' }}>
            Financial Management &amp; Intelligence System
          </Typography>
        </Box>
      </Box>

      {/* Right panel - login form */}
      <Box sx={{
        flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center',
        bgcolor: '#fafaf8', p: 3,
      }}>
        <Box sx={{ maxWidth: 400, width: '100%' }}>
          <Box sx={{ mb: 4, display: { xs: 'block', md: 'none' }, textAlign: 'center' }}>
            <Typography variant="h4" color="primary" fontWeight={800}>JCHPL MIS</Typography>
          </Box>

          <Typography variant="h5" fontWeight={700} sx={{ mb: 0.5 }}>Welcome back</Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>Sign in to your account to continue</Typography>

          {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}

          <form onSubmit={handleSubmit}>
            <TextField
              fullWidth label="Username or Email" value={username}
              onChange={(e) => setUsername(e.target.value)}
              margin="normal" required autoFocus disabled={loading}
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
            <Button
              type="submit" fullWidth variant="contained" size="large"
              sx={{ mt: 3, mb: 2, py: 1.4, fontSize: '0.9375rem' }}
              disabled={loading}
              startIcon={loading ? <CircularProgress size={20} color="inherit" /> : <LoginIcon />}
            >
              {loading ? 'Signing in...' : 'Sign In'}
            </Button>
          </form>

          <Box sx={{ textAlign: 'center' }}>
            <Link component={RouterLink} to="/forgot-password" underline="hover" sx={{ color: 'text.secondary', '&:hover': { color: 'primary.main' } }}>
              Forgot password?
            </Link>
          </Box>

        </Box>
      </Box>
    </Box>
  );
};

export default Login;
