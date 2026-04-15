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
    const expHeads = await pool.query('SELECT id, name, is_active FROM expense_heads ORDER BY name LIMIT 20');
    console.log('Expense heads sample:', expHeads.rows);
    const linked = await pool.query(`
      SELECT eh.id AS expense_head_id, eh.name, eh.is_active,
        COUNT(e.id) FILTER (WHERE e.expense_head_id IS NOT NULL) AS expense_count,
        COUNT(re.id) FILTER (WHERE re.expense_head_id IS NOT NULL) AS recurring_count,
        COUNT(tca.id) FILTER (WHERE tca.expense_head_id IS NOT NULL) AS alloc_count,
        COUNT(t.id) FILTER (WHERE t.expense_head_id IS NOT NULL) AS team_count
      FROM expense_heads eh
      LEFT JOIN expenses e ON e.expense_head_id = eh.id
      LEFT JOIN recurring_expenses re ON re.expense_head_id = eh.id
      LEFT JOIN team_client_allocations tca ON tca.expense_head_id = eh.id
      LEFT JOIN teams t ON t.expense_head_id = eh.id
      GROUP BY eh.id
      ORDER BY expense_count DESC, recurring_count DESC
      LIMIT 20
    `);
    console.log('Linked counts sample:', linked.rows);
  } catch (err) {
    console.error('DB error:', err);
  } finally {
    await pool.end();
  }
})();
