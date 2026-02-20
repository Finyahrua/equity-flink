# Realtime Capital Position — Architecture & Implementation Blueprint

## Objective

Build a realtime regulatory capital position report that aggregates multiple asynchronous bank equity  data APIs into a continuously updated, accurate financial position per:

* Financial Service Provider (FSP)
* Market Sector
* Reporting Date

The report must display:

```
Realtime Capital Position
As of: <timestamp>

Share Capital:        X  (last update)
Reserves:             X  (last update)
Dividends Payable:    X  (last update)
Deductions:           X  (last update)

Net Core Capital:     X
```

The system must tolerate asynchronous arrivals from independent APIs while maintaining accounting correctness.

The system must be able to handle high volumes of data and provide real-time updates to the report.

See the sample payloads that apis will be pushing to kafka then you should simulate the same in this local environment in order to get the feel of how the whole pipeline works.

Apis/fsps will be submitting data via apis then we have services that will do validation and push the data to kafka.

Your objectevice is to assume the payloads have already been validated and are ready to be processed so just create something that will push to kafka and then you should create a flink job that will consume from kafka and aggregate the data and produce necessary reports and saving them to a postgres database.
---

# Data Sources

## API 1 — Share Capital

{
  "shareCapitalData": [
    {
      "reportingDate": "string",
      "capitalCategory": "string",
      "capitalSubCategory": "string",
      "transactionDate": "string",
      "transactionType": "string",
      "shareholderNames": "string",
      "clientType": "string",
      "shareholderCountry": "string",
      "numberOfShares": 0,
      "sharePriceBookValue": 0,
      "currency": "string",
      "orgAmount": 0,
      "tzsAmount": 0,
      "sectorSnaClassification": "string"
    }
  ],
  "fspInformationId" : "string",
  "sender" : "string",
  "signature": "string"
}

## API 2 — Other Capital / Reserves
{
  "otherCapitalAccountData": [
    {
      "reportingDate": "string",
      "transactionDate": "string",
      "transactionType": "string",
      "reserveCategory": "string",
      "reserveSubCategory": "string",
      "currency": "string",
      "orgAmount": 0,
      "tzsAmount": 0
    }
  ],
  "fspInformationId" : "string",
  "sender" : "string",
  "signature": "string"
}
## API 3 — Dividends Payable
{
  "dividendsPayableData": [
    {
      "reportingDate": "string",
      "transactionDate": "string",
      "dividendType": "string",
      "currency": "string",
      "orgAmount": 0,
      "tzsAmount": 0,
      "beneficiaryName": "string",
      "beneficiaryCountry": "string",
      "beneficiaryAccNumber": "string",
      "beneficiaryBankCode": "string"
    }
  ],
  "fspInformationId" : "string",
  "sender" : "string",
  "signature": "string"
}

## API 4 — Core Capital Deductions

{
  "coreCapitalDeductionsData": [
    {
      "reportingDate": "string",
      "transactionDate": "string",
      "deductionsType": "string",
      "currency": "string",
      "orgAmount": 0,
      "tzsAmount": 0
    }
  ],
  "fspInformationId" : "string",
  "sender" : "string",
  "signature": "string"
}

# Core Architectural Principle
<!-- the following are justy ideas, iterate and produce the best ones in order to achieve the desired objective stated above -->

Do NOT aggregate APIs on request.

Instead:

→ Convert each API payload into financial events
→ Continuously project events into a Capital State store
→ Serve the report from the materialized state

This ensures realtime accuracy without synchronization blocking.

---

# High‑Level Architecture

```
APIs → Event Streams → Stateful Aggregator → CapitalState Store → Realtime Report API/UI
```

---

# Event Model

Each incoming record becomes an immutable event:

```
CapitalEvent {
  sourceType        // SHARE | RESERVE | DIVIDEND | DEDUCTION
  fspId
  sector
  reportingDate
  transactionDate
  amount
  ingestionTime
}
```

---

# Capital State Model (Materialized View)

Primary key:

```
(fspId, sector, reportingDate)
```

State schema:

```
CapitalState {
  fspId
  sector
  reportingDate

  shareCapitalTotal
  reservesTotal
  dividendsPayableTotal
  deductionsTotal

  netCoreCapital

  lastShareUpdate
  lastReservesUpdate
  lastDividendsUpdate
  lastDeductionsUpdate

  lastComputedAt
}
```

---

# Aggregation Logic

Whenever an event arrives:

```
state = load(fspId, sector, reportingDate)

switch sourceType:
  SHARE:
    state.shareCapitalTotal += amount
    state.lastShareUpdate = ingestionTime

  RESERVE:
    state.reservesTotal += amount
    state.lastReservesUpdate = ingestionTime

  DIVIDEND:
    state.dividendsPayableTotal += amount
    state.lastDividendsUpdate = ingestionTime

  DEDUCTION:
    state.deductionsTotal += amount
    state.lastDeductionsUpdate = ingestionTime

state.netCoreCapital =
    state.shareCapitalTotal
  + state.reservesTotal
  - state.dividendsPayableTotal
  - state.deductionsTotal

state.lastComputedAt = now()

save(state)
```

---

# Realtime Report Query

For a given FSP + sector + reportingDate:

```
SELECT
  shareCapitalTotal,
  reservesTotal,
  dividendsPayableTotal,
  deductionsTotal,
  netCoreCapital,
  lastShareUpdate,
  lastReservesUpdate,
  lastDividendsUpdate,
  lastDeductionsUpdate,
  lastComputedAt
FROM CapitalState
WHERE fspId = ?
  AND sector = ?
  AND reportingDate = ?
```

---

# Output Rendering Logic

```
Realtime Capital Position
As of: lastComputedAt

Share Capital:        shareCapitalTotal  (lastShareUpdate)
Reserves:             reservesTotal      (lastReservesUpdate)
Dividends Payable:    dividendsTotal     (lastDividendsUpdate)
Deductions:           deductionsTotal    (lastDeductionsUpdate)

Net Core Capital:     netCoreCapital
```

---

# Multi‑Dimensional Aggregation

The same state table supports:

* Per FSP
* Per Sector
* Per Reporting Date
* System‑wide totals

Examples:

## Sector totals

```
GROUP BY sector, reportingDate
```

## FSP totals across sectors

```
GROUP BY fspId, reportingDate
```

## Entire market

```
GROUP BY reportingDate
```

---

# Technology Stack Options

## Streaming Layer

Recommended:

* Kafka


Alternative (simpler):

* Node consumers polling APIs

## Aggregation Engine

Best:
* Flink

Good:

* Node.js service
* Java Spring service

## State Store

Best realtime OLAP:

* ClickHouse
* Materialize

Good transactional:

* PostgreSQL

Low latency cache:

* Redis

---

# Recommended Production Stack
* Kafka
* Flink
* ClickHouse
* REST API service
* React dashboard


---

# Data Freshness Monitoring

Add SLA thresholds per component:

```
shareFresh   = now - lastShareUpdate   < 5 min
reserveFresh = now - lastReservesUpdate < 15 min
dividendFresh= now - lastDividendsUpdate < 5 min
deductFresh  = now - lastDeductionsUpdate < 15 min
```

Overall status:

```
if all fresh → LIVE
if some stale → PARTIAL
if missing → DELAYED
```

---

# Handling Late or Corrected Data

Events may arrive late.

Because state is keyed by reportingDate:

→ late events update correct period
→ netCoreCapital recomputes automatically

No reprocessing needed.

---

# Snapshot Mode (Regulatory)

At reporting cutoff:

```
INSERT INTO CapitalSnapshot
SELECT * FROM CapitalState
WHERE reportingDate = ?
```

This produces auditable financial statements.

---

# API Design

## Get realtime position

```
GET /capital-position?
  fspId=
  sector=
  reportingDate=
```

Response:

```
{
  asOf,
  shareCapital,
  reserves,
  dividendsPayable,
  deductions,
  netCoreCapital,
  lastUpdates:{...},
  freshnessStatus
}
```

---

# Scalability Considerations

Partition streams by:

```
fspId + reportingDate
```

This guarantees ordered updates per entity.

---

# Why This Solves Realtime Balance Sheet

* No cross‑API waiting
* No inconsistent joins
* Continuous recomputation
* Ledger‑accurate state
* Temporal transparency

The report always reflects the latest posted capital position.

---

# End State

System produces instantly:

```
Realtime Capital Position
As of: 10:05:12

Share Capital:        8.0B  (10:01)
Reserves:             5.2B  (09:58)
Dividends Payable:    0.5B  (10:00)
Deductions:           0.3B  (09:55)

Net Core Capital:     12.4B
```

Per:

* FSP
* Sector
* Market
* Reporting date

---

# Key Guarantees

* Realtime
* Accounting correct
* Asynchronous safe
* Scalable
* Auditable

---
--In the end, the report should be able to show the latest posted capital position, but te one that is run on request should use the given thresholds although the actual fsp report that can serve as the source of truth  should be the one computed at the end of day