'use strict';

/**
 * Realtime Capital Position — REST API
 * 
 * Provides endpoints to query the materialised capital state from PostgreSQL.
 */

const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');

const app = express();
const port = process.env.PORT || 3001; // Changed from 3000 to 3001 to avoid EADDRINUSE

// ─── DB Configuration ───────────────────────────────────────────────────────
const pool = new Pool({
  host:     'localhost',
  port:     5432,
  database: 'capital_db',
  user:     'postgres',
  password: 'krukas',
});
console.log('[API] Connecting to Local Postgres at localhost:5432 with user postgres');

app.use(cors());
app.use(express.json());

// ─── Helpers ───────────────────────────────────────────────────────────────

function computeFreshness(state) {
  const now = Date.now();
  const thresholds = {
    share:    5 * 60 * 1000,   // 5 min
    reserves: 15 * 60 * 1000,  // 15 min
    dividend: 5 * 60 * 1000,   // 5 min
    deduct:   15 * 60 * 1000,  // 15 min
  };

  const getAge = (ts) => ts ? now - new Date(ts).getTime() : Infinity;

  const ages = {
    share:    getAge(state.last_share_update),
    reserves: getAge(state.last_reserves_update),
    dividend: getAge(state.last_dividends_update),
    deduct:   getAge(state.last_deductions_update),
  };

  const shareFresh    = ages.share < thresholds.share;
  const reserveFresh  = ages.reserves < thresholds.reserves;
  const dividendFresh = ages.dividend < thresholds.dividend;
  const deductFresh   = ages.deduct < thresholds.deduct;

  const allFresh = shareFresh && reserveFresh && dividendFresh && deductFresh;
  const someMissing = ages.share === Infinity || ages.reserves === Infinity || ages.dividend === Infinity || ages.deduct === Infinity;

  let status = 'LIVE';
  if (someMissing) status = 'DELAYED';
  else if (!allFresh) status = 'PARTIAL';

  return {
    status,
    refreshMetrics: {
      share:    { fresh: shareFresh,    ageSec: ages.share === Infinity ? null : Math.round(ages.share/1000) },
      reserves: { fresh: reserveFresh,  ageSec: ages.reserves === Infinity ? null : Math.round(ages.reserves/1000) },
      dividend: { fresh: dividendFresh, ageSec: ages.dividend === Infinity ? null : Math.round(ages.dividend/1000) },
      deduct:   { fresh: deductFresh,   ageSec: ages.deduct === Infinity ? null : Math.round(ages.deduct/1000) },
    }
  };
}

// ─── Endpoints ─────────────────────────────────────────────────────────────

/**
 * GET /capital-position
 * Query params: fspId, sector, reportingDate
 */
app.get('/capital-position', async (req, res) => {
  const { fspId, sector, reportingDate } = req.query;

  if (!fspId || !sector || !reportingDate) {
    return res.status(400).json({ error: 'fspId, sector, and reportingDate are required' });
  }

  try {
    const query = `
      SELECT * FROM capital_state 
      WHERE fsp_id = $1 AND sector = $2 AND reporting_date = $3
    `;
    const result = await pool.query(query, [fspId, sector, reportingDate]);

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'No data found for the given parameters' });
    }

    const state = result.rows[0];
    const freshness = computeFreshness(state);

    res.json({
      fspId:          state.fsp_id,
      sector:         state.sector,
      reportingDate:  state.reporting_date,
      asOf:           state.last_computed_at,
      
      positions: {
        shareCapital:     parseFloat(state.share_capital_total),
        reserves:         parseFloat(state.reserves_total),
        dividendsPayable: parseFloat(state.dividends_payable_total),
        deductions:       parseFloat(state.deductions_total),
        netCoreCapital:   parseFloat(state.net_core_capital)
      },

      freshnessStatus: freshness.status,
      refreshMetrics:  freshness.refreshMetrics,
      eventCount:      parseInt(state.event_count, 10)
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * GET /capital-position/summary
 * Multi-dimensional aggregation
 */
app.get('/capital-position/summary', async (req, res) => {
  const { groupBy, reportingDate } = req.query;

  if (!reportingDate) {
    return res.status(400).json({ error: 'reportingDate is required' });
  }

  let groupClause = '';
  let selectClause = '';

  if (groupBy === 'sector') {
    groupClause = 'GROUP BY sector';
    selectClause = 'sector,';
  } else if (groupBy === 'fsp') {
    groupClause = 'GROUP BY fsp_id';
    selectClause = 'fsp_id,';
  } else {
    // System-wide
    groupClause = '';
    selectClause = "'SYSTEM' as scope,";
  }

  try {
    const query = `
      SELECT 
        ${selectClause}
        SUM(share_capital_total) as share_total,
        SUM(reserves_total) as reserves_total,
        SUM(dividends_payable_total) as dividends_total,
        SUM(deductions_total) as deductions_total,
        SUM(net_core_capital) as net_total,
        MAX(last_computed_at) as last_update
      FROM capital_state
      WHERE reporting_date = $1
      ${groupClause}
    `;
    const result = await pool.query(query, [reportingDate]);
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * POST /capital-snapshot
 * Regulatory snapshots
 */
app.post('/capital-snapshot', async (req, res) => {
  const { reportingDate } = req.body;

  if (!reportingDate) {
    return res.status(400).json({ error: 'reportingDate is required' });
  }

  try {
    const insertQuery = `
      INSERT INTO capital_snapshot (
        snapshot_reporting_date, fsp_id, sector, reporting_date,
        share_capital_total, reserves_total, dividends_payable_total, deductions_total,
        net_core_capital, last_share_update, last_reserves_update, 
        last_dividends_update, last_deductions_update, last_computed_at, event_count
      )
      SELECT 
        $1 as snapshot_reporting_date, fsp_id, sector, reporting_date,
        share_capital_total, reserves_total, dividends_payable_total, deductions_total,
        net_core_capital, last_share_update, last_reserves_update, 
        last_dividends_update, last_deductions_update, last_computed_at, event_count
      FROM capital_state
      WHERE reporting_date = $1
    `;
    await pool.query(insertQuery, [reportingDate]);
    res.json({ message: `Snapshot created for ${reportingDate}` });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.listen(port, () => {
  console.log(`[API] Capital Position API listening at http://localhost:${port}`);
});
