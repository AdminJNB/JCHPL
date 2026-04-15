import React from 'react';
import {
  Box,
  ButtonBase,
  Card,
  CardContent,
  Checkbox,
  FormControl,
  InputLabel,
  ListItemText,
  MenuItem,
  OutlinedInput,
  Select,
  Stack,
  ToggleButton,
  ToggleButtonGroup,
  Tooltip,
  Typography,
} from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';

export const FILTER_MENU_PROPS = {
  PaperProps: {
    sx: {
      maxHeight: 340,
      borderRadius: 4,
      mt: 0.75,
      border: (theme) => `1px solid ${alpha(theme.palette.primary.main, 0.14)}`,
      boxShadow: '0 20px 40px -28px rgba(86, 71, 57, 0.32)',
    },
  },
};

const TONES = {
  mint: {
    ring: '#6f9d89',
    soft: 'linear-gradient(135deg, rgba(207, 229, 218, 0.78) 0%, rgba(233, 243, 237, 0.92) 100%)',
    glow: 'rgba(111, 157, 137, 0.18)',
  },
  peach: {
    ring: '#f1b8a7',
    soft: 'linear-gradient(135deg, rgba(248, 217, 207, 0.82) 0%, rgba(255, 243, 238, 0.95) 100%)',
    glow: 'rgba(241, 184, 167, 0.2)',
  },
  sand: {
    ring: '#d7b680',
    soft: 'linear-gradient(135deg, rgba(243, 228, 196, 0.82) 0%, rgba(255, 248, 234, 0.95) 100%)',
    glow: 'rgba(215, 182, 128, 0.18)',
  },
  rose: {
    ring: '#dd9888',
    soft: 'linear-gradient(135deg, rgba(244, 200, 189, 0.82) 0%, rgba(255, 241, 238, 0.95) 100%)',
    glow: 'rgba(221, 152, 136, 0.18)',
  },
  sage: {
    ring: '#96bc95',
    soft: 'linear-gradient(135deg, rgba(217, 235, 217, 0.82) 0%, rgba(245, 250, 245, 0.95) 100%)',
    glow: 'rgba(150, 188, 149, 0.18)',
  },
};

const normaliseValue = (value, multi) => {
  if (multi) return Array.isArray(value) ? value : [];
  return value ?? '';
};

export const renderSelectionValue = (selected, options = [], emptyLabel = 'All') => {
  const arrayValue = Array.isArray(selected) ? selected : selected ? [selected] : [];
  if (!arrayValue.length) return emptyLabel;
  const labelMap = new Map(options.map((option) => [option.value, option.label]));
  if (arrayValue.length <= 2) {
    return arrayValue.map((value) => labelMap.get(value) || value).join(', ');
  }
  return `${arrayValue.length} selected`;
};

export const HoverActionButton = ({ icon, label, tone = 'mint', tooltip, showTooltip = false, ...props }) => {
  const theme = useTheme();
  const toneValues = TONES[tone] || TONES.mint;

  const content = (
    <ButtonBase
      {...props}
      sx={{
        minHeight: 38,
        px: 1.3,
        pr: 1.5,
        borderRadius: 999,
        border: `1px solid ${alpha(toneValues.ring, 0.25)}`,
        background: toneValues.soft,
        boxShadow: `0 14px 28px -22px ${toneValues.glow}`,
        display: 'inline-flex',
        alignItems: 'center',
        gap: 1,
        whiteSpace: 'nowrap',
        '& .hover-label': {
          maxWidth: 0,
          opacity: 0,
          overflow: 'hidden',
          transform: 'translateX(-4px)',
          transition: 'max-width 0.22s ease, opacity 0.22s ease, transform 0.22s ease',
        },
        '&:hover .hover-label, &:focus-visible .hover-label': {
          maxWidth: 160,
          opacity: 1,
          transform: 'translateX(0)',
        },
        '&:hover': {
          transform: 'translateY(-1px)',
        },
        '&:focus-visible': {
          outline: `2px solid ${alpha(theme.palette.primary.main, 0.22)}`,
          outlineOffset: 2,
        },
      }}
    >
      <Box
        sx={{
          width: 28,
          height: 28,
          borderRadius: '50%',
          display: 'grid',
          placeItems: 'center',
          background: alpha(toneValues.ring, 0.14),
          color: toneValues.ring,
        }}
      >
        {icon}
      </Box>
      <Typography className="hover-label" variant="body2" sx={{ fontWeight: 700, color: 'text.primary' }}>
        {label}
      </Typography>
    </ButtonBase>
  );

  if (showTooltip) {
    return <Tooltip title={tooltip || label}>{content}</Tooltip>;
  }
  return content;
};

export const PageHeader = ({ eyebrow = 'Module', title, subtitle, actions, chips = [] }) => (
  <Box
    sx={{
      display: 'flex',
      justifyContent: 'space-between',
      alignItems: 'flex-start',
      gap: 2,
      flexWrap: 'wrap',
      mb: 2.5,
    }}
  >
    <Box>
      <Typography variant="overline" sx={{ color: 'text.secondary', display: 'block', mb: 0.6 }}>
        {eyebrow}
      </Typography>
      <Typography variant="h4" sx={{ mb: 0.6 }}>
        {title}
      </Typography>
      {subtitle && (
        <Typography variant="body1" color="text.secondary" sx={{ maxWidth: 780 }}>
          {subtitle}
        </Typography>
      )}
      {!!chips.length && (
        <Stack direction="row" spacing={1} useFlexGap flexWrap="wrap" sx={{ mt: 1.25 }}>
          {chips.map((chip) => (
            <Box
              key={chip.label}
              sx={{
                px: 1.4,
                py: 0.55,
                borderRadius: 999,
                background: chip.background || 'rgba(255,255,255,0.78)',
                border: '1px solid rgba(111, 157, 137, 0.14)',
              }}
            >
              <Typography variant="caption" sx={{ color: chip.color || 'text.secondary', fontWeight: 700 }}>
                {chip.label}
              </Typography>
            </Box>
          ))}
        </Stack>
      )}
    </Box>
    {actions && (
      <Stack direction="row" spacing={1} useFlexGap flexWrap="wrap" justifyContent="flex-end">
        {actions}
      </Stack>
    )}
  </Box>
);

export const MetricCard = ({ eyebrow, title, value, helper, icon, tone = 'mint' }) => {
  const toneValues = TONES[tone] || TONES.mint;

  return (
    <Card
      sx={{
        height: '100%',
        position: 'relative',
        overflow: 'hidden',
        background: toneValues.soft,
        borderColor: alpha(toneValues.ring, 0.18),
        '&::after': {
          content: '""',
          position: 'absolute',
          inset: 'auto -32px -34px auto',
          width: 110,
          height: 110,
          borderRadius: '50%',
          background: alpha(toneValues.ring, 0.08),
        },
      }}
    >
      <CardContent sx={{ p: 2 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', gap: 1.5, alignItems: 'flex-start' }}>
          <Box sx={{ minWidth: 0 }}>
            <Typography variant="caption" sx={{ color: 'text.secondary', textTransform: 'uppercase', letterSpacing: '0.1em' }}>
              {eyebrow}
            </Typography>
            <Typography variant="h5" sx={{ mt: 0.6, mb: helper ? 0.5 : 0 }}>
              {value}
            </Typography>
            <Typography variant="body2" sx={{ fontWeight: 700, color: 'text.primary' }}>
              {title}
            </Typography>
            {helper && (
              <Typography variant="caption" sx={{ display: 'block', mt: 0.35, color: 'text.secondary' }}>
                {helper}
              </Typography>
            )}
          </Box>
          {icon && (
            <Box
              sx={{
                width: 38,
                height: 38,
                borderRadius: 14,
                display: 'grid',
                placeItems: 'center',
                background: alpha(toneValues.ring, 0.14),
                color: toneValues.ring,
                flexShrink: 0,
              }}
            >
              {icon}
            </Box>
          )}
        </Box>
      </CardContent>
    </Card>
  );
};

export const SectionCard = ({ title, subtitle, tone = 'mint', action, children, contentSx }) => {
  const toneValues = TONES[tone] || TONES.mint;

  return (
    <Card sx={{ overflow: 'hidden', borderColor: alpha(toneValues.ring, 0.18) }}>
      <Box
        sx={{
          px: 2,
          py: 1.5,
          borderBottom: `1px solid ${alpha(toneValues.ring, 0.12)}`,
          background: toneValues.soft,
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          gap: 2,
        }}
      >
        <Box>
          <Typography variant="subtitle1">{title}</Typography>
          {subtitle && (
            <Typography variant="body2" color="text.secondary">
              {subtitle}
            </Typography>
          )}
        </Box>
        {action}
      </Box>
      <Box sx={{ p: 2, ...contentSx }}>{children}</Box>
    </Card>
  );
};

export const FilterPanel = ({ mode, onModeChange, title = 'Filter Matrix', subtitle, onClear, children, showModeToggle = true }) => (
  <Card sx={{ mb: 2.5 }}>
    <CardContent sx={{ p: 2 }}>
      <Box
        sx={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'flex-start',
          gap: 2,
          flexWrap: 'wrap',
          mb: 2,
        }}
      >
        <Box>
          <Typography variant="subtitle1"> {title}</Typography>
          {subtitle && (
            <Typography variant="body2" color="text.secondary">
              {subtitle}
            </Typography>
          )}
        </Box>
        <Stack direction="row" spacing={1} useFlexGap flexWrap="wrap">
          {showModeToggle && (
            <ToggleButtonGroup
              exclusive
              value={mode}
              size="small"
              onChange={(_, nextMode) => nextMode && onModeChange(nextMode)}
              sx={{
                p: 0.35,
                borderRadius: 999,
                background: 'rgba(255,255,255,0.82)',
                border: '1px solid rgba(111, 157, 137, 0.14)',
                '& .MuiToggleButton-root': {
                  border: 'none',
                  borderRadius: 999,
                  px: 1.4,
                  color: 'text.secondary',
                },
                '& .Mui-selected': {
                  background: 'linear-gradient(135deg, rgba(207, 229, 218, 0.9) 0%, rgba(248, 217, 207, 0.9) 100%)',
                  color: 'text.primary',
                },
              }}
            >
              <ToggleButton value="dropdown">Dropdown View</ToggleButton>
              <ToggleButton value="full">Full Selection</ToggleButton>
            </ToggleButtonGroup>
          )}
          {onClear}
        </Stack>
      </Box>
      {children}
    </CardContent>
  </Card>
);

export const SelectionField = ({
  label,
  value,
  options = [],
  onChange,
  mode = 'dropdown',
  tone = 'mint',
  emptyLabel = 'All',
  multi = true,
  fullHeight = 200,
}) => {
  const theme = useTheme();
  const toneValues = TONES[tone] || TONES.mint;
  const resolvedValue = normaliseValue(value, multi);

  if (mode === 'full') {
    const selectedValues = multi ? resolvedValue : resolvedValue ? [resolvedValue] : [];
    const toggleValue = (nextValue) => {
      if (multi) {
        const exists = resolvedValue.includes(nextValue);
        onChange(exists ? resolvedValue.filter((valueItem) => valueItem !== nextValue) : [...resolvedValue, nextValue]);
        return;
      }
      onChange(resolvedValue === nextValue ? '' : nextValue);
    };

    return (
      <Box
        sx={{
          p: 1.25,
          minHeight: 118,
          borderRadius: 4,
          border: `1px solid ${alpha(toneValues.ring, 0.18)}`,
          background: alpha('#ffffff', 0.82),
        }}
      >
        <Typography variant="caption" sx={{ display: 'block', mb: 1, color: 'text.secondary', fontWeight: 800, letterSpacing: '0.08em' }}>
          {label}
        </Typography>
        <Stack direction="row" spacing={1} useFlexGap flexWrap="wrap" sx={{ maxHeight: fullHeight, overflow: 'auto' }}>
          <ButtonBase
            onClick={() => onChange(multi ? [] : '')}
            sx={{
              px: 1.2,
              py: 0.75,
              borderRadius: 999,
              border: `1px solid ${alpha(theme.palette.text.secondary, 0.16)}`,
              background: selectedValues.length ? alpha('#ffffff', 0.74) : alpha(toneValues.ring, 0.14),
            }}
          >
            <Typography variant="body2" sx={{ fontWeight: 700, color: 'text.secondary' }}>
              {emptyLabel}
            </Typography>
          </ButtonBase>
          {options.map((option) => {
            const selected = selectedValues.includes(option.value);
            return (
              <ButtonBase
                key={option.value}
                onClick={() => toggleValue(option.value)}
                sx={{
                  px: 1.2,
                  py: 0.75,
                  borderRadius: 999,
                  border: `1px solid ${selected ? alpha(toneValues.ring, 0.34) : alpha(toneValues.ring, 0.16)}`,
                  background: selected ? alpha(toneValues.ring, 0.14) : alpha('#ffffff', 0.74),
                }}
              >
                <Typography variant="body2" sx={{ fontWeight: 700, color: selected ? 'text.primary' : 'text.secondary' }}>
                  {option.label}
                </Typography>
              </ButtonBase>
            );
          })}
        </Stack>
      </Box>
    );
  }

  return (
    <FormControl fullWidth size="small">
      <InputLabel>{label}</InputLabel>
      <Select
        multiple={multi}
        value={resolvedValue}
        onChange={(event) => {
          const nextValue = multi
            ? (typeof event.target.value === 'string' ? event.target.value.split(',') : event.target.value)
            : event.target.value;
          onChange(nextValue);
        }}
        input={<OutlinedInput label={label} />}
        renderValue={(selected) => renderSelectionValue(selected, options, emptyLabel)}
        MenuProps={FILTER_MENU_PROPS}
      >
        {options.map((option) => (
          <MenuItem key={option.value} value={option.value}>
            {multi && <Checkbox checked={resolvedValue.includes(option.value)} size="small" />}
            <ListItemText primary={option.label} secondary={option.secondary || null} />
          </MenuItem>
        ))}
      </Select>
    </FormControl>
  );
};
