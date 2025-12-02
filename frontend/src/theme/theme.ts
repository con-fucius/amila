import { createTheme, ThemeOptions, PaletteMode } from '@mui/material/styles';

// Custom color palette with the requested green - LIGHT MODE
const lightColors = {
  primary: {
    main: '#10b981', // emerald-500 as requested
    light: '#34d399',
    lighter: 'rgba(16, 185, 129, 0.08)',
    dark: '#059669',
    contrastText: '#ffffff',
  },
  secondary: {
    main: '#1976d2',
    light: '#42a5f5',
    dark: '#1565c0',
    contrastText: '#ffffff',
  },
  success: {
    main: '#16a34a',
    light: '#22c55e',
    dark: '#15803d',
    contrastText: '#ffffff',
  },
  warning: {
    main: '#f59e0b',
    light: '#fbbf24',
    dark: '#d97706',
    contrastText: '#1a1a1a',
  },
  info: {
    main: '#0ea5e9',
    light: '#38bdf8',
    dark: '#0284c7',
    contrastText: '#ffffff',
  },
  error: {
    main: '#ef4444',
    light: '#f87171',
    dark: '#dc2626',
    contrastText: '#ffffff',
  },
  background: {
    default: '#fafafa',
    paper: '#ffffff',
  },
  text: {
    primary: '#1a1a1a',
    secondary: '#4b5563',
  },
  divider: 'rgba(2, 6, 23, 0.12)',
  grey: {
    50: '#fafafa',
    100: '#f5f5f5',
    200: '#eeeeee',
    300: '#e0e0e0',
    400: '#bdbdbd',
    500: '#9e9e9e',
    600: '#757575',
    700: '#616161',
    800: '#424242',
    900: '#212121',
  },
};

// Custom color palette - DARK MODE
const darkColors = {
  primary: {
    main: '#10b981', // Same emerald-500 green for dark mode
    light: '#34d399',
    lighter: 'rgba(16, 185, 129, 0.15)',
    dark: '#059669',
    contrastText: '#ffffff',
  },
  secondary: {
    main: '#42a5f5',
    light: '#64b5f6',
    dark: '#1976d2',
    contrastText: '#ffffff',
  },
  success: {
    main: '#22c55e',
    light: '#4ade80',
    dark: '#16a34a',
    contrastText: '#0b1220',
  },
  warning: {
    main: '#f59e0b',
    light: '#fbbf24',
    dark: '#d97706',
    contrastText: '#0b1220',
  },
  info: {
    main: '#38bdf8',
    light: '#7dd3fc',
    dark: '#0ea5e9',
    contrastText: '#0b1220',
  },
  error: {
    main: '#f87171',
    light: '#fca5a5',
    dark: '#ef4444',
    contrastText: '#0b1220',
  },
  background: {
    default: '#0f172a', // slate-900
    paper: '#1e293b', // slate-800
  },
  text: {
    primary: '#f1f5f9', // slate-100
    secondary: '#94a3b8', // slate-400
  },
  divider: 'rgba(148, 163, 184, 0.24)',
  grey: {
    50: '#f8fafc',
    100: '#f1f5f9',
    200: '#e2e8f0',
    300: '#cbd5e1',
    400: '#94a3b8',
    500: '#64748b',
    600: '#475569',
    700: '#334155',
    800: '#1e293b',
    900: '#0f172a',
  },
};

// Typography configuration with Figtree font (Phase 2.1: Reduced by 1-2px)
const typography = {
  fontFamily: '"Figtree", "Roboto", "Helvetica", "Arial", sans-serif',
  h1: {
    fontFamily: '"Figtree", "Roboto", "Helvetica", "Arial", sans-serif',
    fontWeight: 700,
    fontSize: '2.125rem', // 34px (reduced from 38px)
    lineHeight: 1.2,
  },
  h2: {
    fontFamily: '"Figtree", "Roboto", "Helvetica", "Arial", sans-serif',
    fontWeight: 600,
    fontSize: '1.625rem', // 26px (reduced from 30px)
    lineHeight: 1.3,
  },
  h3: {
    fontFamily: '"Figtree", "Roboto", "Helvetica", "Arial", sans-serif',
    fontWeight: 600,
    fontSize: '1.375rem', // 22px (reduced from 26px)
    lineHeight: 1.3,
  },
  h4: {
    fontFamily: '"Figtree", "Roboto", "Helvetica", "Arial", sans-serif',
    fontWeight: 600,
    fontSize: '1.125rem', // 18px (reduced from 22px)
    lineHeight: 1.4,
  },
  h5: {
    fontFamily: '"Figtree", "Roboto", "Helvetica", "Arial", sans-serif',
    fontWeight: 600,
    fontSize: '1rem', // 16px (reduced from 18px)
    lineHeight: 1.4,
  },
  h6: {
    fontFamily: '"Figtree", "Roboto", "Helvetica", "Arial", sans-serif',
    fontWeight: 600,
    fontSize: '0.875rem', // 14px (reduced from 16px)
    lineHeight: 1.4,
  },
  body1: {
    fontFamily: '"Figtree", "Roboto", "Helvetica", "Arial", sans-serif',
    fontWeight: 400,
    fontSize: '0.875rem', // 14px (reduced from 15px)
    lineHeight: 1.6,
    letterSpacing: '0.005em',
  },
  body2: {
    fontFamily: '"Figtree", "Roboto", "Helvetica", "Arial", sans-serif',
    fontWeight: 400,
    fontSize: '0.75rem', // 12px (reduced from 13px)
    lineHeight: 1.5,
  },
  button: {
    fontFamily: '"Figtree", "Roboto", "Helvetica", "Arial", sans-serif',
    fontWeight: 600,
    fontSize: '0.75rem', // 12px (reduced from 13px)
    textTransform: 'none' as const,
    letterSpacing: '0.02em',
  },
  caption: {
    fontFamily: '"Figtree", "Roboto", "Helvetica", "Arial", sans-serif',
    fontWeight: 400,
    fontSize: '0.625rem', // 10px (reduced from 11px)
    lineHeight: 1.4,
  },
  overline: {
    fontFamily: '"Figtree", "Roboto", "Helvetica", "Arial", sans-serif',
    fontWeight: 600,
    fontSize: '0.625rem', // 10px (reduced from 11px)
    textTransform: 'uppercase' as const,
    letterSpacing: '0.08em',
  },
};

// Component customizations for enterprise look (mode-aware)
const getComponents = (mode: PaletteMode) => ({
  MuiCssBaseline: {
    styleOverrides: `
      @import url('https://fonts.googleapis.com/css2?family=Figtree:ital,wght@0,300;0,400;0,500;0,600;0,700;0,800;0,900;1,300;1,400;1,500;1,600;1,700;1,800;1,900&display=swap');
      
      * { box-sizing: border-box; }
      html { -webkit-font-smoothing: antialiased; -moz-osx-font-smoothing: grayscale; }
      body { font-family: "Figtree", "Roboto", "Helvetica", "Arial", sans-serif; margin: 0; padding: 0; }

      /* Focus visibility (WCAG AA) */
      .MuiButtonBase-root:focus-visible,
      button:focus-visible,
      a:focus-visible,
      [role="button"]:focus-visible,
      .MuiChip-root:focus-visible {
        outline: 3px solid ${mode === 'dark' ? 'rgba(56, 189, 248, 0.6)' : 'rgba(16, 185, 129, 0.6)'};
        outline-offset: 2px;
      }

      /* Reduced motion support */
      @media (prefers-reduced-motion: reduce) {
        *, *::before, *::after { animation: none !important; transition: none !important; scroll-behavior: auto !important; }
      }

      /* Custom scrollbar styles */
      ::-webkit-scrollbar { width: 8px; height: 8px; }
      ::-webkit-scrollbar-track { background: ${mode === 'dark' ? '#1e293b' : '#f1f1f1'}; border-radius: 4px; }
      ::-webkit-scrollbar-thumb { background: ${mode === 'dark' ? '#475569' : '#c1c1c1'}; border-radius: 4px; transition: background 0.2s ease; }
      ::-webkit-scrollbar-thumb:hover { background: ${mode === 'dark' ? '#64748b' : '#a1a1a1'}; }
    `,
  },
  MuiButton: {
    styleOverrides: {
      root: {
        borderRadius: 8,
        textTransform: 'none',
        fontWeight: 600,
        padding: '8px 16px',
        transition: 'all 0.2s ease-in-out',
        '&:hover': { transform: 'translateY(-1px)', boxShadow: '0 4px 12px rgba(16, 185, 129, 0.3)' },
        '&:focus-visible': { outline: `3px solid ${mode === 'dark' ? 'rgba(56, 189, 248, 0.6)' : 'rgba(16, 185, 129, 0.6)'}`, outlineOffset: '2px' },
        variants: [],
      },
      contained: {
        boxShadow: '0 2px 8px rgba(16, 185, 129, 0.2)',
        '&:hover': { boxShadow: '0 4px 16px rgba(16, 185, 129, 0.3)' },
      },
    },
  },
  MuiIconButton: {
    styleOverrides: {
      root: {
        '&:focus-visible': { outline: `3px solid ${mode === 'dark' ? 'rgba(56, 189, 248, 0.6)' : 'rgba(16, 185, 129, 0.6)'}`, outlineOffset: '2px' },
      },
    },
  },
  MuiCard: {
    styleOverrides: {
      root: {
        borderRadius: 12,
        boxShadow: mode === 'dark' ? '0 2px 12px rgba(0, 0, 0, 0.4)' : '0 2px 12px rgba(0, 0, 0, 0.08)',
        border: mode === 'dark' ? '1px solid rgba(148, 163, 184, 0.12)' : '1px solid rgba(0, 0, 0, 0.06)',
        transition: 'all 0.2s ease-in-out',
        '&:hover': { boxShadow: mode === 'dark' ? '0 4px 20px rgba(0, 0, 0, 0.5)' : '0 4px 20px rgba(0, 0, 0, 0.12)', transform: 'translateY(-2px)' },
      },
    },
  },
  MuiPaper: {
    styleOverrides: {
      root: { borderRadius: 8, border: mode === 'dark' ? '1px solid rgba(148, 163, 184, 0.12)' : undefined },
      elevation1: { boxShadow: mode === 'dark' ? '0 2px 8px rgba(0, 0, 0, 0.4)' : '0 2px 8px rgba(0, 0, 0, 0.08)' },
      elevation2: { boxShadow: mode === 'dark' ? '0 4px 16px rgba(0, 0, 0, 0.5)' : '0 4px 16px rgba(0, 0, 0, 0.12)' },
    },
  },
  MuiTextField: {
    styleOverrides: {
      root: {
        '& .MuiOutlinedInput-root': {
          borderRadius: 8,
          transition: 'all 0.2s ease-in-out',
          '&:hover .MuiOutlinedInput-notchedOutline': { borderColor: '#34d399' },
          '&.Mui-focused .MuiOutlinedInput-notchedOutline': { borderColor: '#10b981', borderWidth: 2 },
        },
      },
    },
  },
  MuiTab: {
    styleOverrides: {
      root: {
        textTransform: 'none',
        fontWeight: 500,
        fontSize: '0.875rem',
        minHeight: 48,
        transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
        '&.Mui-selected': { color: '#10b981', fontWeight: 600 },
        '&:hover': { backgroundColor: mode === 'dark' ? 'rgba(16, 185, 129, 0.08)' : 'rgba(16, 185, 129, 0.05)' },
        variants: [],
      },
    },
  },
  MuiTabs: { styleOverrides: { indicator: { backgroundColor: '#10b981', height: 3, borderRadius: '3px 3px 0 0', transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)' } } },
  MuiChip: {
    styleOverrides: {
      root: { borderRadius: 6, fontWeight: 500 },
      filled: { '&.MuiChip-colorPrimary': { backgroundColor: '#10b981', color: '#ffffff' } },
    },
  },
  MuiAlert: {
    styleOverrides: {
      root: {
        borderRadius: 8,
        '&.MuiAlert-standardSuccess': { backgroundColor: mode === 'dark' ? 'rgba(16, 185, 129, 0.15)' : 'rgba(16, 185, 129, 0.1)', color: '#10b981', '& .MuiAlert-icon': { color: '#10b981' } },
      },
    },
  },
  MuiDrawer: { styleOverrides: { paper: { borderRight: mode === 'dark' ? '1px solid rgba(148, 163, 184, 0.12)' : '1px solid rgba(0, 0, 0, 0.08)', boxShadow: mode === 'dark' ? '2px 0 12px rgba(0, 0, 0, 0.5)' : '2px 0 12px rgba(0, 0, 0, 0.08)' } } },
  MuiAppBar: { styleOverrides: { root: { boxShadow: mode === 'dark' ? '0 2px 12px rgba(0, 0, 0, 0.5)' : '0 2px 12px rgba(0, 0, 0, 0.08)', borderBottom: mode === 'dark' ? '1px solid rgba(148, 163, 184, 0.12)' : '1px solid rgba(0, 0, 0, 0.06)' } } },
  MuiListItemText: { styleOverrides: { primary: { fontFamily: '"Figtree", "Roboto", "Helvetica", "Arial", sans-serif' }, secondary: { fontFamily: '"Figtree", "Roboto", "Helvetica", "Arial", sans-serif' } } },
  MuiListItemButton: { styleOverrides: { root: { fontFamily: '"Figtree", "Roboto", "Helvetica", "Arial", sans-serif' } } },
  MuiMenuItem: { styleOverrides: { root: { fontFamily: '"Figtree", "Roboto", "Helvetica", "Arial", sans-serif' } } },
  MuiInputBase: { styleOverrides: { input: { fontFamily: '"Figtree", "Roboto", "Helvetica", "Arial", sans-serif' } } },
  MuiInputLabel: { styleOverrides: { root: { fontFamily: '"Figtree", "Roboto", "Helvetica", "Arial", sans-serif' } } },
  MuiFormLabel: { styleOverrides: { root: { fontFamily: '"Figtree", "Roboto", "Helvetica", "Arial", sans-serif' } } },
  MuiTooltip: { styleOverrides: { tooltip: { fontFamily: '"Figtree", "Roboto", "Helvetica", "Arial", sans-serif' } } },
  MuiDialog: { styleOverrides: { paper: { borderRadius: 12 } } },
  MuiDialogTitle: { styleOverrides: { root: { fontFamily: '"Figtree", "Roboto", "Helvetica", "Arial", sans-serif', fontWeight: 600 } } },
  MuiDialogContent: { styleOverrides: { root: { fontFamily: '"Figtree", "Roboto", "Helvetica", "Arial", sans-serif' } } },
  MuiDialogContentText: { styleOverrides: { root: { fontFamily: '"Figtree", "Roboto", "Helvetica", "Arial", sans-serif' } } },
  MuiDialogActions: { styleOverrides: { root: { padding: '16px 24px' } } },
});

// Create theme factory function that accepts mode
export const createAppTheme = (mode: PaletteMode = 'light') => {
  const colors = mode === 'dark' ? darkColors : lightColors;
  
  const themeOptions: ThemeOptions = {
    palette: {
      mode,
      ...colors,
      action: {
        focus: mode === 'dark' ? 'rgba(56, 189, 248, 0.6)' : 'rgba(16, 185, 129, 0.6)',
        hoverOpacity: 0.08,
        disabledOpacity: 0.38,
      },
    },
    typography,
    components: getComponents(mode),
    shape: { borderRadius: 8 },
    spacing: 8,
    transitions: {
      duration: { shortest: 150, shorter: 200, short: 250, standard: 300, complex: 375, enteringScreen: 225, leavingScreen: 195 },
      easing: { easeInOut: 'cubic-bezier(0.4, 0, 0.2, 1)', easeOut: 'cubic-bezier(0.0, 0, 0.2, 1)', easeIn: 'cubic-bezier(0.4, 0, 1, 1)', sharp: 'cubic-bezier(0.4, 0, 0.6, 1)' },
    },
  };

  return createTheme(themeOptions);
};

// Default light theme export for backward compatibility
export const theme = createAppTheme('light');
export default theme;
