require('dotenv').config();
const { Pool, types } = require('pg');

// Keep SQL DATE values as plain YYYY-MM-DD strings to avoid timezone shifts
// when they are serialized to JSON and rendered in the frontend.
types.setTypeParser(1082, (value) => value);

const parseBoolean = (value) => {
  if (typeof value !== 'string') {
    return Boolean(value);
  }

  return ['1', 'true', 'yes', 'on'].includes(value.trim().toLowerCase());
};

const sanitizeDatabaseUrl = (value) => {
  if (!value) {
    return '';
  }

  const trimmed = value.trim().replace(/^DATABASE_URL=/i, '');
  const postgresMatch = trimmed.match(/postgres(?:ql)?:\/\/[^/\s'"]+\/[A-Za-z0-9._-]+(?:\?[^'"\s]*)?/i);
  let normalized = postgresMatch ? postgresMatch[0] : trimmed;

  const supabasePostgresIndex = normalized.toLowerCase().lastIndexOf('/postgres');
  if (supabasePostgresIndex >= 0) {
    normalized = normalized.slice(0, supabasePostgresIndex + '/postgres'.length);
  }

  const nestedHttpIndex = normalized.search(/https?:\/\//i);
  if (nestedHttpIndex > 0) {
    normalized = normalized.slice(0, nestedHttpIndex);
  }

  return normalized.trim().replace(/^['"]|['"]$/g, '');
};

const databaseUrl = sanitizeDatabaseUrl(process.env.DATABASE_URL);
const connectionTargets = [databaseUrl, process.env.DB_HOST].filter(Boolean).join(' ').toLowerCase();
const hostedDbProviders = ['supabase.co', 'render.com', 'render.internal', 'railway.app', 'railway.internal', 'neon.tech'];
const isHostedPostgres = hostedDbProviders.some((provider) => connectionTargets.includes(provider));
const sslEnabled = process.env.DB_SSL === undefined ? isHostedPostgres : parseBoolean(process.env.DB_SSL);
const rejectUnauthorized = process.env.DB_SSL_REJECT_UNAUTHORIZED === undefined
  ? false
  : parseBoolean(process.env.DB_SSL_REJECT_UNAUTHORIZED);
const sslConfig = sslEnabled ? { rejectUnauthorized } : false;

const poolConfig = databaseUrl ? {
  connectionString: databaseUrl,
  ssl: sslConfig,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
} : {
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  ssl: sslConfig,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
};

const pool = new Pool(poolConfig);

pool.on('connect', () => {
  console.log('Connected to PostgreSQL database');
});

pool.on('error', (err) => {
  console.error('Unexpected error on idle client', err);
  process.exit(-1);
});

// Helper function to log audit trail
async function logAudit(tableName, recordId, action, oldValues, newValues, userId) {
  const query = `
    INSERT INTO audit_logs (table_name, record_id, action, old_values, new_values, user_id)
    VALUES ($1, $2, $3, $4, $5, $6)
  `;

  try {
    // Audit logging should not block a successful user action if the
    // audit table or related schema is not ready yet in a hosted deploy.
    await pool.query(query, [
      tableName,
      recordId,
      action,
      oldValues ? JSON.stringify(oldValues) : null,
      newValues ? JSON.stringify(newValues) : null,
      userId
    ]);
  } catch (error) {
    console.warn(`[AUDIT WARN] Skipped ${action} on ${tableName}: ${error.message}`);
  }
}

module.exports = { pool, logAudit, sanitizeDatabaseUrl };
