import React, { useMemo, useState } from 'react';
import { Outlet, useLocation, useNavigate } from 'react-router-dom';
import {
  AppBar,
  Avatar,
  Box,
  ButtonBase,
  Chip,
  CssBaseline,
  Divider,
  Drawer,
  IconButton,
  List,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Menu,
  MenuItem,
  Toolbar,
  Typography,
  useMediaQuery,
} from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import {
  Apartment,
  Assessment,
  Business,
  Dashboard as DashboardIcon,
  ExpandLess,
  ExpandMore,
  Groups,
  Menu as MenuIcon,
  MoneyOff,
  People,
  Receipt,
  Repeat,
  Settings,
  ChevronLeft,
  ChevronRight,
  AccountCircle,
  Logout,
} from '@mui/icons-material';
import { useAuth } from '../context/AuthContext';

const DRAWER_WIDTH = 252;
const COLLAPSED_WIDTH = 72;
const MOBILE_DRAWER_WIDTH = 280;

const menuItems = [
  { title: 'Dashboard', path: '/', icon: <DashboardIcon /> },
  {
    title: 'Masters',
    icon: <Apartment />,
    children: [
      { title: 'Groups', path: '/masters/groups', icon: <People /> },
      { title: 'Clients', path: '/masters/clients', icon: <Business /> },
      { title: 'Teams', path: '/masters/teams', icon: <Groups /> },
      { title: 'Misc Masters', path: '/masters/misc', icon: <Settings /> },
      { title: 'Recurring', path: '/masters/recurring-expenses', icon: <Repeat /> },
    ],
  },
  { title: 'Revenue', path: '/revenue', icon: <Receipt /> },
  { title: 'Expenses', path: '/expenses', icon: <MoneyOff /> },
  { title: 'Reports', path: '/reports', icon: <Assessment /> },
];

const Layout = () => {
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down('md'));
  const [mobileOpen, setMobileOpen] = useState(false);
  const [collapsed, setCollapsed] = useState(true);
  const [mastersOpen, setMastersOpen] = useState(true);
  const [anchorEl, setAnchorEl] = useState(null);
  const { user, logout } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();

  const handleDrawerToggle = () => setMobileOpen((previous) => !previous);
      const [pinned, setPinned] = useState(false);
  const handleSidebarEnter = () => { if (!isMobile) setCollapsed(false); };
  const handleSidebarLeave = () => { if (!isMobile && !pinned) setCollapsed(true); };
  const handleMenuOpen = (event) => setAnchorEl(event.currentTarget);
  const handleMenuClose = () => setAnchorEl(null);

  const activeTitle = useMemo(() => {
    const directMatch = menuItems.find((item) => item.path && (item.path === '/' ? location.pathname === '/' : location.pathname.startsWith(item.path)));
    if (directMatch) return directMatch.title;
    const nestedMatch = menuItems.find((item) => item.children?.some((child) => location.pathname.startsWith(child.path)));
    return nestedMatch?.children?.find((child) => location.pathname.startsWith(child.path))?.title || 'JCHPL MIS';
  }, [location.pathname]);

  const isActive = (path) => (path === '/' ? location.pathname === '/' : location.pathname.startsWith(path));
  const isMastersActive = menuItems.find((item) => item.children)?.children?.some((child) => isActive(child.path));

  const handleNavigate = (path) => {
    navigate(path);
    if (isMobile) setMobileOpen(false);
  };

  const itemStyles = (active) => ({
    minHeight: 44,
    px: 1.4,
    py: 0.65,
    mb: 0.5,
    borderRadius: 999,
    border: `1px solid ${active ? alpha(theme.palette.primary.main, 0.24) : 'transparent'}`,
    background: active
      ? `linear-gradient(135deg, ${alpha(theme.palette.primary.light, 0.7)} 0%, ${alpha(theme.palette.secondary.light, 0.6)} 100%)`
      : 'transparent',
    color: active ? theme.palette.text.primary : theme.palette.text.secondary,
    '&:hover': {
      background: active
        ? `linear-gradient(135deg, ${alpha(theme.palette.primary.light, 0.8)} 0%, ${alpha(theme.palette.secondary.light, 0.7)} 100%)`
        : alpha(theme.palette.primary.light, 0.24),
      color: theme.palette.text.primary,
    },
  });

  const iconStyles = {
    minWidth: 34,
    color: 'inherit',
    '& .MuiSvgIcon-root': {
      fontSize: '1.1rem',
    },
  };

  const sidebarContent = (width, collapsedLocal) => (
    <Box
      onMouseEnter={handleSidebarEnter}
      onMouseLeave={handleSidebarLeave}
      sx={{
        width,
        height: '100%',
        display: 'flex',
        flexDirection: 'column',
        px: 1.5,
        py: 1.2,
        position: 'relative',
        overflow: 'hidden',
        background: `
          radial-gradient(circle at 0% 0%, ${alpha(theme.palette.secondary.main, 0.14)}, transparent 26%),
          radial-gradient(circle at 100% 18%, ${alpha(theme.palette.primary.main, 0.16)}, transparent 30%),
          linear-gradient(180deg, ${alpha('#fffdfb', 0.96)} 0%, ${alpha('#fff8f1', 0.96)} 100%)
        `,
      }}
    >
      <Box
        sx={{
          px: 1.2,
          py: 1.25,
          borderRadius: 5,
          background: alpha('#ffffff', 0.84),
          border: `1px solid ${alpha(theme.palette.primary.main, 0.12)}`,
          boxShadow: '0 20px 44px -34px rgba(87, 72, 58, 0.34)',
          cursor: 'pointer',
        }}
        onClick={() => handleNavigate('/')}
      >
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.2, mb: 1 }}>
          <Box
            sx={{
              width: 40,
              height: 40,
              borderRadius: 3.5,
              display: 'grid',
              placeItems: 'center',
              color: '#ffffff',
              fontWeight: 800,
              background: `linear-gradient(135deg, ${theme.palette.primary.main} 0%, ${theme.palette.secondary.main} 100%)`,
              boxShadow: `0 18px 28px -20px ${alpha(theme.palette.primary.dark, 0.65)}`,
            }}
          >
            JC
          </Box>
          <Box sx={{ minWidth: 0 }}>
            <Typography variant="subtitle1" sx={{ lineHeight: 1.1 }}>
              JCHPL MIS
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Finance + task command
            </Typography>
          </Box>
        </Box>
        <Chip
          size="small"
          label="Pastel Future Console"
          sx={{
            height: 24,
            background: alpha(theme.palette.primary.light, 0.38),
            color: theme.palette.text.secondary,
            '& .MuiChip-label': { px: 1.2 },
          }}
        />
      </Box>

      <List sx={{ mt: 1.6, px: 0, flex: 1, overflowY: 'auto' }}>
        {menuItems.map((item) => {
          if (!item.children) {
            return (
              <ListItem key={item.path} disablePadding sx={{ display: 'block' }}>
                <ListItemButton sx={itemStyles(isActive(item.path))} onClick={() => handleNavigate(item.path)}>
                  <ListItemIcon sx={iconStyles}>{item.icon}</ListItemIcon>
                  {!collapsedLocal && (
                    <ListItemText
                      primary={item.title}
                      primaryTypographyProps={{
                        fontSize: '0.84rem',
                        fontWeight: isActive(item.path) ? 700 : 600,
                      }}
                    />
                  )}
                </ListItemButton>
              </ListItem>
            );
          }

          return (
            <Box key={item.title} sx={{ mb: 0.5 }}>
              <ListItemButton sx={itemStyles(isMastersActive)} onClick={() => setMastersOpen((previous) => !previous)}>
                <ListItemIcon sx={iconStyles}>{item.icon}</ListItemIcon>
                {!collapsedLocal && (
                  <ListItemText primary={item.title} primaryTypographyProps={{ fontSize: '0.84rem', fontWeight: 700 }} />
                )}
                {!collapsedLocal && (mastersOpen ? <ExpandLess fontSize="small" /> : <ExpandMore fontSize="small" />)}
              </ListItemButton>

              {!collapsedLocal && (
                <Box
                  sx={{
                    display: mastersOpen ? 'block' : 'none',
                    ml: 1.8,
                    mt: 0.45,
                    pl: 1.1,
                    borderLeft: `1px solid ${alpha(theme.palette.primary.main, 0.14)}`,
                  }}
                >
                  {item.children.map((child) => (
                    <ListItemButton key={child.path} sx={itemStyles(isActive(child.path))} onClick={() => handleNavigate(child.path)}>
                      <ListItemIcon sx={{ ...iconStyles, minWidth: 30 }}>{child.icon}</ListItemIcon>
                      <ListItemText
                        primary={child.title}
                        primaryTypographyProps={{
                          fontSize: '0.8rem',
                          fontWeight: isActive(child.path) ? 700 : 600,
                        }}
                      />
                    </ListItemButton>
                  ))}
                </Box>
              )}
            </Box>
          );
        })}
      </List>

      <Divider sx={{ borderColor: alpha(theme.palette.primary.main, 0.1), mb: 1.2 }} />

      <Box sx={{ position: 'absolute', right: -18, top: '50%', transform: 'translateY(-50%)', zIndex: 1400 }}>
        <IconButton
          size="small"
          onClick={() => {
            setPinned((p) => {
              const next = !p;
              if (next) setCollapsed(false);
              else setCollapsed(true);
              return next;
            });
          }}
          sx={{
            bgcolor: pinned ? theme.palette.primary.main : 'background.paper',
            color: pinned ? '#ffffff' : 'text.primary',
            boxShadow: pinned ? 3 : 2,
            border: pinned ? `1px solid ${alpha(theme.palette.primary.main, 0.18)}` : 'none',
            '&:hover': {
              bgcolor: pinned ? theme.palette.primary.dark : alpha(theme.palette.primary.main, 0.06),
            },
            transition: 'background-color 150ms ease, color 150ms ease',
          }}
        >
          {collapsed ? <ChevronRight fontSize="small" /> : <ChevronLeft fontSize="small" />}
        </IconButton>
      </Box>

      <ButtonBase
        onClick={handleMenuOpen}
        sx={{
          width: '100%',
          px: 1.2,
          py: 1.1,
          borderRadius: 4,
          display: 'flex',
          justifyContent: 'flex-start',
          gap: 1.2,
          background: alpha('#ffffff', 0.82),
          border: `1px solid ${alpha(theme.palette.primary.main, 0.12)}`,
        }}
      >
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Settings sx={{ color: 'text.secondary' }} fontSize="small" />
          {!collapsedLocal && (
            <Typography variant="body2" color="text.secondary" sx={{ fontWeight: 600 }}>Setting</Typography>
          )}
        </Box>
      </ButtonBase>
    </Box>
  );

  return (
    <Box sx={{ display: 'flex' }}>
      <CssBaseline />

      <AppBar
        position="fixed"
        sx={{
          width: { md: `calc(100% - ${collapsed ? COLLAPSED_WIDTH : DRAWER_WIDTH}px)` },
          ml: { md: `${collapsed ? COLLAPSED_WIDTH : DRAWER_WIDTH}px` },
          transition: 'width 180ms ease, margin-left 180ms ease',
        }}
      >
        <Toolbar sx={{ minHeight: '62px !important', px: { xs: 1.5, md: 2.5 } }}>
          <IconButton edge="start" onClick={handleDrawerToggle} sx={{ mr: 1.2, display: { md: 'none' } }}>
            <MenuIcon />
          </IconButton>
          <Box sx={{ flexGrow: 1, minWidth: 0 }}>
            <Typography variant="overline" sx={{ color: 'text.secondary', display: 'block', lineHeight: 1.1 }}>
              Live workspace
            </Typography>
            <Typography variant="subtitle1" noWrap>
              {activeTitle}
            </Typography>
          </Box>
          <Chip
            label="Compact Pastel Mode"
            sx={{
              display: { xs: 'none', sm: 'inline-flex' },
              background: alpha(theme.palette.secondary.light, 0.52),
              color: theme.palette.text.secondary,
            }}
          />
          <IconButton onClick={handleMenuOpen} sx={{ ml: 1 }}>
            <Avatar
              sx={{
                width: 34,
                height: 34,
                background: `linear-gradient(135deg, ${theme.palette.primary.main} 0%, ${theme.palette.secondary.main} 100%)`,
                color: '#ffffff',
                fontWeight: 800,
              }}
            >
              {(user?.name || user?.username || 'U')[0].toUpperCase()}
            </Avatar>
          </IconButton>
        </Toolbar>
      </AppBar>

      <Box component="nav" sx={{ width: { md: collapsed ? COLLAPSED_WIDTH : DRAWER_WIDTH }, flexShrink: { md: 0 } }}>
        <Drawer
          variant="temporary"
          open={mobileOpen}
          onClose={handleDrawerToggle}
          ModalProps={{ keepMounted: true }}
          sx={{
            display: { xs: 'block', md: 'none' },
            '& .MuiDrawer-paper': {
              width: MOBILE_DRAWER_WIDTH,
            },
          }}
        >
          {sidebarContent(MOBILE_DRAWER_WIDTH)}
        </Drawer>

        <Drawer
          variant="permanent"
          open
          sx={{
            display: { xs: 'none', md: 'block' },
            '& .MuiDrawer-paper': {
              width: collapsed ? COLLAPSED_WIDTH : DRAWER_WIDTH,
              transition: 'width 180ms ease',
              overflow: 'hidden',
            },
          }}
        >
          {sidebarContent(collapsed ? COLLAPSED_WIDTH : DRAWER_WIDTH, collapsed)}
        </Drawer>
      </Box>

      <Box
        component="main"
        sx={{
          flexGrow: 1,
          width: { md: `calc(100% - ${collapsed ? COLLAPSED_WIDTH : DRAWER_WIDTH}px)` },
          minHeight: '100vh',
          px: { xs: 1.5, md: 2.5 },
          pb: 2.5,
          pt: 1.2,
          position: 'relative',
        }}
      >
        <Toolbar sx={{ minHeight: '62px !important' }} />
        <Box
          sx={{
            position: 'absolute',
            inset: '12px 18px auto auto',
            width: 180,
            height: 180,
            borderRadius: '50%',
            background: alpha(theme.palette.secondary.main, 0.09),
            filter: 'blur(18px)',
            pointerEvents: 'none',
          }}
        />
        <Outlet />
      </Box>

      <Menu
        anchorEl={anchorEl}
        open={Boolean(anchorEl)}
        onClose={handleMenuClose}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
        transformOrigin={{ vertical: 'top', horizontal: 'right' }}
        slotProps={{
          paper: {
            sx: {
              minWidth: 180,
              mt: 1,
              borderRadius: 4,
              border: `1px solid ${alpha(theme.palette.primary.main, 0.12)}`,
            },
          },
        }}
      >
        <MenuItem
          onClick={() => {
            handleMenuClose();
            navigate('/profile');
          }}
        >
          <ListItemIcon>
            <AccountCircle fontSize="small" />
          </ListItemIcon>
          Profile
        </MenuItem>
        <MenuItem
          onClick={() => {
            handleMenuClose();
            navigate('/settings');
          }}
        >
          <ListItemIcon>
            <Settings fontSize="small" />
          </ListItemIcon>
          Settings
        </MenuItem>
        <Divider />
        <MenuItem
          onClick={() => {
            handleMenuClose();
            logout();
          }}
        >
          <ListItemIcon>
            <Logout fontSize="small" />
          </ListItemIcon>
          Logout
        </MenuItem>
      </Menu>
    </Box>
  );
};

export default Layout;
