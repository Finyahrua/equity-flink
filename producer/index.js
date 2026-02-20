'use strict';

/**
 * Realtime Capital Position — Kafka Producer Simulator
 *
 * Simulates the 4 independent bank equity data APIs posting payloads
 * to their respective Kafka topics asynchronously:
 *   share-capital           → API 1: Share Capital
 *   other-capital           → API 2: Other Capital / Reserves
 *   dividends-payable       → API 3: Dividends Payable
 *   core-capital-deductions → API 4: Core Capital Deductions
 */

const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');

// ─── Configuration ──────────────────────────────────────────────────────────
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const INTERVAL_MS  = parseInt(process.env.INTERVAL_MS || '2000', 10);

// Sample reference data
const FSPs = ['FSP001', 'FSP002', 'FSP003', 'FSP004'];
const SECTORS = ['BANKING', 'INSURANCE', 'MICROFINANCE', 'PENSION'];
const CURRENCIES = ['TZS', 'USD', 'EUR', 'GBP'];
const REPORTING_DATES = [
  new Date().toISOString().slice(0, 10),                       // today
  new Date(Date.now() - 86400000).toISOString().slice(0, 10),  // yesterday
];

const CAPITAL_CATEGORIES    = ['PAID_UP_CAPITAL', 'AUTHORIZED_CAPITAL', 'PREFERENCE_SHARES'];
const CAPITAL_SUB_CATS      = ['ORDINARY_SHARES', 'PREFERENCE_A', 'PREFERENCE_B'];
const TRANSACTION_TYPES     = ['ISSUANCE', 'BUYBACK', 'TRANSFER', 'ADJUSTMENT'];
const RESERVE_CATEGORIES    = ['STATUTORY_RESERVE', 'RETAINED_EARNINGS', 'REVALUATION_RESERVE', 'GENERAL_RESERVE'];
const RESERVE_SUB_CATS      = ['MANDATORY', 'VOLUNTARY'];
const DIVIDEND_TYPES        = ['INTERIM', 'FINAL', 'SPECIAL'];
const DEDUCTION_TYPES       = ['GOODWILL', 'INTANGIBLE_ASSETS', 'DEFERRED_TAX', 'INVESTMENT_IN_SUBS'];
const SHAREHOLDER_NAMES     = ['PENSION FUND A', 'MUTUAL FUND B', 'SOVEREIGN WEALTH FUND', 'RETAIL INVESTORS', 'INSTITUTIONAL X'];
const CLIENT_TYPES          = ['INSTITUTIONAL', 'RETAIL', 'GOVERNMENT', 'FOREIGN'];
const COUNTRIES             = ['TZ', 'KE', 'UG', 'ZA', 'GB', 'US'];

// ─── Helpers ─────────────────────────────────────────────────────────────────
function pick(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function randAmount(min = 100_000, max = 500_000_000) {
  return parseFloat((Math.random() * (max - min) + min).toFixed(2));
}

function isoDate() {
  return new Date().toISOString().slice(0, 10);
}

function fspSignature(fspId) {
  // Simulated cryptographic signature placeholder
  return `SIG_${fspId}_${Date.now()}`;
}

// ─── Payload Factories ────────────────────────────────────────────────────────

/**
 * API 1 — Share Capital payload
 */
function buildShareCapitalPayload(fspId, sector, reportingDate) {
  const currency = pick(CURRENCIES);
  const orgAmount = randAmount(1_000_000, 1_000_000_000);
  const tzsAmount = currency === 'TZS' ? orgAmount : orgAmount * (currency === 'USD' ? 2500 : currency === 'EUR' ? 2700 : 3100);

  return {
    shareCapitalData: [
      {
        reportingDate,
        capitalCategory:       pick(CAPITAL_CATEGORIES),
        capitalSubCategory:    pick(CAPITAL_SUB_CATS),
        transactionDate:       isoDate(),
        transactionType:       pick(TRANSACTION_TYPES),
        shareholderNames:      pick(SHAREHOLDER_NAMES),
        clientType:            pick(CLIENT_TYPES),
        shareholderCountry:    pick(COUNTRIES),
        numberOfShares:        Math.floor(Math.random() * 10_000_000) + 1_000,
        sharePriceBookValue:   parseFloat((Math.random() * 5000 + 100).toFixed(2)),
        currency,
        orgAmount,
        tzsAmount:             parseFloat(tzsAmount.toFixed(2)),
        sectorSnaClassification: sector,
      },
    ],
    fspInformationId: fspId,
    sender:           `${fspId}_SYSTEM`,
    signature:        fspSignature(fspId),
  };
}

/**
 * API 2 — Other Capital / Reserves payload
 */
function buildReservesPayload(fspId, sector, reportingDate) {
  const currency = pick(CURRENCIES);
  const orgAmount = randAmount(500_000, 500_000_000);
  const tzsAmount = currency === 'TZS' ? orgAmount : orgAmount * 2500;

  return {
    otherCapitalAccountData: [
      {
        reportingDate,
        transactionDate:    isoDate(),
        transactionType:    pick(TRANSACTION_TYPES),
        reserveCategory:    pick(RESERVE_CATEGORIES),
        reserveSubCategory: pick(RESERVE_SUB_CATS),
        currency,
        orgAmount,
        tzsAmount:          parseFloat(tzsAmount.toFixed(2)),
        sector:             sector,
      },
    ],
    fspInformationId: fspId,
    sender:           `${fspId}_SYSTEM`,
    signature:        fspSignature(fspId),
  };
}

/**
 * API 3 — Dividends Payable payload
 */
function buildDividendsPayload(fspId, sector, reportingDate) {
  const currency = pick(CURRENCIES);
  const orgAmount = randAmount(100_000, 200_000_000);
  const tzsAmount = currency === 'TZS' ? orgAmount : orgAmount * 2500;

  return {
    dividendsPayableData: [
      {
        reportingDate,
        transactionDate:      isoDate(),
        dividendType:         pick(DIVIDEND_TYPES),
        currency,
        orgAmount,
        tzsAmount:            parseFloat(tzsAmount.toFixed(2)),
        beneficiaryName:      pick(SHAREHOLDER_NAMES),
        beneficiaryCountry:   pick(COUNTRIES),
        beneficiaryAccNumber: `ACC${Math.floor(Math.random() * 999999999).toString().padStart(10, '0')}`,
        beneficiaryBankCode:  `BANK${Math.floor(Math.random() * 99).toString().padStart(3, '0')}`,
        sector:               sector,
      },
    ],
    fspInformationId: fspId,
    sender:           `${fspId}_SYSTEM`,
    signature:        fspSignature(fspId),
  };
}

/**
 * API 4 — Core Capital Deductions payload
 */
function buildDeductionsPayload(fspId, sector, reportingDate) {
  const currency = pick(CURRENCIES);
  const orgAmount = randAmount(50_000, 100_000_000);
  const tzsAmount = currency === 'TZS' ? orgAmount : orgAmount * 2500;

  return {
    coreCapitalDeductionsData: [
      {
        reportingDate,
        transactionDate: isoDate(),
        deductionsType:  pick(DEDUCTION_TYPES),
        currency,
        orgAmount,
        tzsAmount:       parseFloat(tzsAmount.toFixed(2)),
        sector:          sector,
      },
    ],
    fspInformationId: fspId,
    sender:           `${fspId}_SYSTEM`,
    signature:        fspSignature(fspId),
  };
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`[Producer] Connecting to Kafka at ${KAFKA_BROKER}`);

  const kafka = new Kafka({
    clientId: 'capital-producer',
    brokers:  [KAFKA_BROKER],
    retry:    { retries: 10, initialRetryTime: 3000 },
  });

  const producer = kafka.producer();

  let connected = false;
  while (!connected) {
    try {
      await producer.connect();
      connected = true;
      console.log('[Producer] Connected to Kafka ✓');
    } catch (err) {
      console.error('[Producer] Kafka not ready, retrying in 5s...', err.message);
      await new Promise(r => setTimeout(r, 5000));
    }
  }

  let msgCount = 0;

  async function sendRound() {
    // Each round: pick a random FSP + sector + reporting date, send all 4 API types
    // APIs arrive independently (one at a time per round, staggered)
    const fspId         = pick(FSPs);
    const sector        = pick(SECTORS);
    const reportingDate = pick(REPORTING_DATES);

    const messages = [
      {
        topic:   'share-capital',
        payload: buildShareCapitalPayload(fspId, sector, reportingDate),
        label:   'SHARE',
      },
      {
        topic:   'other-capital',
        payload: buildReservesPayload(fspId, sector, reportingDate),
        label:   'RESERVE',
      },
      {
        topic:   'dividends-payable',
        payload: buildDividendsPayload(fspId, sector, reportingDate),
        label:   'DIVIDEND',
      },
      {
        topic:   'core-capital-deductions',
        payload: buildDeductionsPayload(fspId, sector, reportingDate),
        label:   'DEDUCTION',
      },
    ];

    // Stagger sends to simulate asynchronous, independent API arrivals
    for (const { topic, payload, label } of messages) {
      const delay = Math.floor(Math.random() * 1000); // 0-1000 ms random delay
      await new Promise(r => setTimeout(r, delay));

      const key   = `${fspId}:${sector}:${reportingDate}`;
      const value = JSON.stringify(payload);

      await producer.send({
        topic,
        messages: [{ key, value, headers: { sourceType: label, fspId, sector, reportingDate } }],
      });

      msgCount++;
      const amount = payload.shareCapitalData?.[0]?.tzsAmount
                   ?? payload.otherCapitalAccountData?.[0]?.tzsAmount
                   ?? payload.dividendsPayableData?.[0]?.tzsAmount
                   ?? payload.coreCapitalDeductionsData?.[0]?.tzsAmount;

      console.log(
        `[${new Date().toISOString()}] #${msgCount} → topic=${topic.padEnd(26)} ` +
        `key=${key.padEnd(40)} label=${label.padEnd(10)} amount=${amount.toLocaleString('en-TZ', { minimumFractionDigits: 2 })} TZS`
      );
    }
  }

  console.log(`[Producer] Starting simulation — publishing every ${INTERVAL_MS}ms\n`);
  setInterval(sendRound, INTERVAL_MS);

  process.on('SIGINT', async () => {
    console.log('\n[Producer] Shutting down...');
    await producer.disconnect();
    process.exit(0);
  });
}

main().catch(err => {
  console.error('[Producer] Fatal error:', err);
  process.exit(1);
});
