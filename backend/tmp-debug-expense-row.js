require('dotenv').config();
const { Pool } = require('pg');
const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
});
(async () => {
  try {
    const id = 'dc25b6d7-7311-47ff-bcbc-242b2afd7bdf';
    const res = await pool.query(
      `SELECT id, client_id, team_id, expense_head_id, service_period_id, amount, is_active, created_at, updated_at, description FROM expenses WHERE expense_head_id = $1`,
      [id]
    );
    console.log(JSON.stringify(res.rows, null, 2));
  } catch (err) {
    console.error(err);
  } finally {
    await pool.end();
  }
})();
