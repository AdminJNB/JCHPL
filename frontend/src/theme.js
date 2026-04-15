import { alpha, createTheme } from '@mui/material/styles';

const primaryMain = '#6f9d89';
const primaryLight = '#cfe5da';
const primaryDark = '#4f6f61';
const secondaryMain = '#f1b8a7';
const secondaryLight = '#f8d9cf';
const secondaryDark = '#c98b7a';
const accentMain = '#dd9888';
const accentLight = '#f4c8bd';
const successMain = '#96bc95';
const warningMain = '#d7b680';
const infoMain = '#c5b28b';
const neutralInk = '#2d2a28';
const neutralSoft = '#7e756b';
const shellBase = '#fffaf5';

const theme = createTheme({
  palette: {
    primary: {
      main: primaryMain,
      light: primaryLight,
      dark: primaryDark,
      contrastText: '#ffffff',
    },
    secondary: {
      main: secondaryMain,
      light: secondaryLight,
      dark: secondaryDark,
      contrastText: neutralInk,
    },
    success: {
      main: successMain,
      light: '#d9ebd9',
      dark: '#6f9470',
    },
    warning: {
      main: warningMain,
      light: '#f3e4c4',
      dark: '#af8d55',
    },
    error: {
      main: accentMain,
      light: accentLight,
      dark: '#b87363',
    },
    info: {
      main: infoMain,
      light: '#eadfc7',
      dark: '#a28f67',
    },
    background: {
      default: shellBase,
      paper: 'rgba(255,255,255,0.9)',
    },
    text: {
      primary: neutralInk,
      secondary: neutralSoft,
    },
    divider: alpha(primaryMain, 0.18),
  },
  shape: {
    borderRadius: 16,
  },
  typography: {
    fontFamily: '"Aptos Narrow", "Aptos", "Segoe UI", sans-serif',
    h3: { fontWeight: 800, letterSpacing: '-0.04em' },
    h4: { fontWeight: 800, letterSpacing: '-0.03em' },
    h5: { fontWeight: 700, letterSpacing: '-0.02em' },
    h6: { fontWeight: 700, letterSpacing: '-0.02em' },
    subtitle1: { fontWeight: 600, letterSpacing: '-0.01em' },
    subtitle2: { fontWeight: 600, fontSize: '0.85rem', letterSpacing: '0.01em' },
    body1: { fontSize: '0.92rem', lineHeight: 1.55 },
    body2: { fontSize: '0.82rem', lineHeight: 1.5 },
    button: { fontWeight: 700, fontSize: '0.78rem', letterSpacing: '0.01em', textTransform: 'none' },
    overline: { fontWeight: 700, fontSize: '0.68rem', letterSpacing: '0.12em' },
  },
  components: {
    MuiCssBaseline: {
      styleOverrides: {
        body: {
          minHeight: '100vh',
          background: `
            radial-gradient(circle at 0% 0%, rgba(241, 184, 167, 0.26), transparent 28%),
            radial-gradient(circle at 100% 0%, rgba(207, 229, 218, 0.38), transparent 34%),
            radial-gradient(circle at 100% 100%, rgba(243, 228, 196, 0.25), transparent 28%),
            linear-gradient(180deg, #fffdfb 0%, ${shellBase} 100%)
          `,
          color: neutralInk,
        },
        '*': {
          boxSizing: 'border-box',
          transition: 'background-color 0.18s ease, border-color 0.18s ease, box-shadow 0.18s ease, transform 0.18s ease',
        },
        '::-webkit-scrollbar': {
          width: 6,
          height: 6,
        },
        '::-webkit-scrollbar-track': {
          background: 'transparent',
        },
        '::-webkit-scrollbar-thumb': {
          background: alpha(primaryMain, 0.35),
          borderRadius: 999,
        },
        '::-webkit-scrollbar-thumb:hover': {
          background: alpha(primaryMain, 0.48),
        },
      },
    },
    MuiAppBar: {
      styleOverrides: {
        root: {
          color: neutralInk,
          background: alpha('#fffdfb', 0.82),
          backdropFilter: 'blur(20px)',
          borderBottom: `1px solid ${alpha(primaryMain, 0.12)}`,
          boxShadow: 'none',
        },
      },
    },
    MuiDrawer: {
      styleOverrides: {
        paper: {
          background: alpha('#fffdfb', 0.92),
          backdropFilter: 'blur(24px)',
          borderRight: `1px solid ${alpha(primaryMain, 0.14)}`,
        },
      },
    },
    MuiCard: {
      styleOverrides: {
        root: {
          borderRadius: 24,
          border: `1px solid ${alpha(primaryMain, 0.14)}`,
          background: alpha('#ffffff', 0.86),
          backdropFilter: 'blur(20px)',
          boxShadow: '0 18px 40px -28px rgba(87, 72, 58, 0.34)',
        },
      },
    },
    MuiPaper: {
      styleOverrides: {
        root: {
          backgroundImage: 'none',
        },
      },
    },
    MuiButton: {
      styleOverrides: {
        root: {
          minHeight: 38,
          paddingInline: 16,
          borderRadius: 999,
          boxShadow: 'none',
        },
        containedPrimary: {
          background: `linear-gradient(135deg, ${primaryMain} 0%, ${secondaryMain} 100%)`,
          color: '#ffffff',
          '&:hover': {
            background: `linear-gradient(135deg, ${primaryDark} 0%, ${secondaryDark} 100%)`,
            boxShadow: `0 14px 28px -16px ${alpha(primaryDark, 0.65)}`,
          },
        },
        outlined: {
          borderColor: alpha(primaryMain, 0.25),
          color: neutralInk,
          '&:hover': {
            borderColor: alpha(primaryMain, 0.38),
            background: alpha(primaryLight, 0.42),
          },
        },
      },
    },
    MuiIconButton: {
      styleOverrides: {
        root: {
          borderRadius: 14,
        },
      },
    },
    MuiChip: {
      styleOverrides: {
        root: {
          borderRadius: 999,
          fontWeight: 700,
        },
      },
    },
    MuiDialog: {
      styleOverrides: {
        paper: {
          borderRadius: 28,
          background: alpha('#fffdfb', 0.94),
          backdropFilter: 'blur(26px)',
          border: `1px solid ${alpha(primaryMain, 0.14)}`,
        },
      },
    },
    MuiOutlinedInput: {
      styleOverrides: {
        root: {
          minHeight: 42,
          borderRadius: 16,
          background: alpha('#ffffff', 0.8),
          '& fieldset': {
            borderColor: alpha(primaryMain, 0.16),
          },
          '&:hover fieldset': {
            borderColor: alpha(primaryMain, 0.3),
          },
          '&.Mui-focused fieldset': {
            borderColor: primaryMain,
            borderWidth: 1.25,
          },
        },
      },
    },
    MuiInputLabel: {
      styleOverrides: {
        root: {
          fontWeight: 600,
          color: neutralSoft,
        },
      },
    },
    MuiFormControl: {
      defaultProps: {
        size: 'small',
      },
    },
    MuiTextField: {
      defaultProps: {
        size: 'small',
      },
    },
    MuiSelect: {
      defaultProps: {
        size: 'small',
      },
    },
    MuiDataGrid: {
      styleOverrides: {
        root: {
          border: 'none',
          background: 'transparent',
          '--DataGrid-rowBorderColor': alpha(primaryMain, 0.1),
          '& .MuiDataGrid-columnHeaders': {
            borderBottom: `1px solid ${alpha(primaryMain, 0.12)}`,
            background: `linear-gradient(135deg, ${alpha(primaryLight, 0.42)} 0%, ${alpha(secondaryLight, 0.42)} 100%)`,
          },
          '& .MuiDataGrid-columnHeaderTitle': {
            fontWeight: 800,
            fontSize: '0.72rem',
            letterSpacing: '0.08em',
            textTransform: 'uppercase',
            color: neutralSoft,
          },
          '& .MuiDataGrid-row:hover': {
            background: alpha(primaryLight, 0.28),
          },
          '& .MuiDataGrid-footerContainer': {
            borderTop: `1px solid ${alpha(primaryMain, 0.1)}`,
          },
        },
      },
    },
    MuiTableCell: {
      styleOverrides: {
        head: {
          fontSize: '0.72rem',
          fontWeight: 800,
          color: neutralSoft,
          letterSpacing: '0.08em',
          textTransform: 'uppercase',
          borderBottomColor: alpha(primaryMain, 0.16),
        },
        body: {
          borderBottomColor: alpha(primaryMain, 0.08),
        },
      },
    },
    MuiTab: {
      styleOverrides: {
        root: {
          minHeight: 42,
          borderRadius: 999,
          marginRight: 8,
        },
      },
    },
    MuiTabs: {
      styleOverrides: {
        indicator: {
          display: 'none',
        },
      },
    },
    MuiTooltip: {
      styleOverrides: {
        tooltip: {
          borderRadius: 12,
          background: alpha(neutralInk, 0.92),
          padding: '8px 12px',
          fontSize: '0.72rem',
        },
      },
    },
  },
});

export default theme;
