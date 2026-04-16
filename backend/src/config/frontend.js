const FRONTEND_ENV_KEYS = ['FRONTEND_URLS', 'FRONTEND_URL', 'Frontend_URL', 'CORS_ORIGIN'];
const PLACEHOLDER_ORIGIN_PATTERN = /(your-frontend|example\.com)/i;
const DEFAULT_FRONTEND_ORIGINS = ['https://jchpl-frontend.vercel.app'];
const TRUSTED_VERCEL_ORIGIN_PATTERN = /^https:\/\/jchpl-frontend(?:-[a-z0-9-]+)?\.vercel\.app$/i;

const stripWrappingQuotes = (value) => value.replace(/^['"]|['"]$/g, '');

const stripEnvAssignment = (value) => value.replace(/^(?:FRONTEND_URLS?|Frontend_URL|CORS_ORIGIN)\s*=\s*/i, '');

const extractUrlCandidate = (value) => {
  const cleaned = stripWrappingQuotes(stripEnvAssignment(String(value || '').trim()));

  if (!cleaned) {
    return '';
  }

  const match = cleaned.match(/https?:\/\/[^\s'",]+/i);
  return match ? match[0] : cleaned;
};

const normalizeOriginValue = (value) => {
  const candidate = extractUrlCandidate(value);

  if (!candidate) {
    return '';
  }

  try {
    return new URL(candidate).origin.toLowerCase();
  } catch (error) {
    return candidate.replace(/\/+$/, '').toLowerCase();
  }
};

const splitConfiguredOrigins = (value) => String(value || '')
  .split(/[\r\n,\s]+/)
  .map(normalizeOriginValue)
  .filter((origin) => origin && !PLACEHOLDER_ORIGIN_PATTERN.test(origin));

const isTrustedFrontendOrigin = (origin) => {
  const normalizedOrigin = normalizeOriginValue(origin);
  return DEFAULT_FRONTEND_ORIGINS.includes(normalizedOrigin) || TRUSTED_VERCEL_ORIGIN_PATTERN.test(normalizedOrigin);
};

const getConfiguredFrontendOrigins = (env = process.env) => {
  const origins = [
    ...DEFAULT_FRONTEND_ORIGINS,
    ...FRONTEND_ENV_KEYS.flatMap((key) => splitConfiguredOrigins(env[key])),
  ];
  return [...new Set(origins)];
};

const getPrimaryFrontendOrigin = (env = process.env) => getConfiguredFrontendOrigins(env)[0] || '';

module.exports = {
  getConfiguredFrontendOrigins,
  getPrimaryFrontendOrigin,
  isTrustedFrontendOrigin,
  normalizeOriginValue,
};
