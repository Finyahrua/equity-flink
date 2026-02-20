const { Pool } = require('pg');

const pool = new Pool({
  host:     'localhost',
  port:     5432,
  database: 'capital_db',
  user:     'postgres',
  password: 'krukas',
});

async function check() {
  try {
    const stateCount = await pool.query('SELECT COUNT(*) FROM capital_state');
    const logCount = await pool.query('SELECT COUNT(*) FROM capital_event_log');
    const snapCount = await pool.query('SELECT COUNT(*) FROM capital_snapshot');

    console.log(`[Counts] State: ${stateCount.rows[0].count} | Event Log: ${logCount.rows[0].count} | Snapshots: ${snapCount.rows[0].count}`);

    const sample = await pool.query('SELECT fsp_id, sector, reporting_date FROM capital_state LIMIT 1');
    if (sample.rows.length > 0) {
        console.log('--- SAMPLE ROW FOR API TEST ---');
        const r = sample.rows[0];
        console.log(`fspId=${r.fsp_id}&sector=${r.sector}&reportingDate=${r.reporting_date.toISOString().slice(0, 10)}`);
    }

    const dates = await pool.query('SELECT DISTINCT reporting_date FROM capital_state ORDER BY reporting_date DESC');
    console.log('--- REPORTING DATES ---');
    console.log(dates.rows.map(r => r.reporting_date.toISOString().slice(0, 10)).join(', '));
  } catch (err) {
    console.error('Check failed:', err.message);
  } finally {
    await pool.end();
  }
}

check();
