require('dotenv').config();
const { Pool, types } = require('pg');

// Keep SQL DATE values as plain YYYY-MM-DD strings to avoid timezone shifts
// when they are serialized to JSON and rendered in the frontend.
types.setTypeParser(1082, (value) => value);

const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

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
  await pool.query(query, [
    tableName,
    recordId,
    action,
    oldValues ? JSON.stringify(oldValues) : null,
    newValues ? JSON.stringify(newValues) : null,
    userId
  ]);
}

module.exports = { pool, logAudit };
