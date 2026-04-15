const bcrypt = require('bcryptjs');
const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
});

async function updateAdmin() {
  try {
    const hash = await bcrypt.hash('admin123', 10);
    const result = await pool.query(
      'UPDATE users SET password_hash = $1, can_delete = true WHERE username = $2',
      [hash, 'admin']
    );
    console.log('Admin password updated to admin123 and can_delete set to true');
    console.log('Rows affected:', result.rowCount);
  } catch (error) {
    console.error('Error:', error);
  } finally {
    await pool.end();
  }
}

updateAdmin();