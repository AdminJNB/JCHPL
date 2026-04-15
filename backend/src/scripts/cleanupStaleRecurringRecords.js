require('dotenv').config();

const { pool } = require('../database/db');
const { cleanupStaleRecurringSourceRecords } = require('../utils/recurringSourceCleanup');

async function main() {
  const dbClient = await pool.connect();

  try {
    await dbClient.query('BEGIN');
    const summary = await cleanupStaleRecurringSourceRecords(dbClient);
    await dbClient.query('COMMIT');

    console.log('Stale recurring source cleanup completed.');
    console.log(JSON.stringify(summary, null, 2));
  } catch (error) {
    await dbClient.query('ROLLBACK').catch(() => {});
    console.error('Stale recurring source cleanup failed:', error.message);
    process.exitCode = 1;
  } finally {
    dbClient.release();
    await pool.end();
  }
}

main();
