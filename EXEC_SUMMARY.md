# Health Score Middleware: Executive Summary

## What This Is

A Python middleware that calculates a **unified Customer Health Score** for every account, monthly, by pulling data from three systems and writing a single, actionable metric back to Salesforce.

**One number. Every account. Every month.**

---

## Why It Matters

Today, account health is assessed subjectively and inconsistently. The Health Score replaces this with a data-driven model that:

- **Detects risk early** — quantitative signals from support, financial, and adoption data surface problems months before renewal
- **Captures human intelligence** — CSM-logged qualitative signals override the score in real-time when a CSM hears something the data can't yet see
- **Drives different actions by segment** — Paid Success gets human intervention; Standard Success gets 80%+ automation via Salesforce Agents
- **Directly supports CX targets** — 110% NRR, 95% GRR, 90% LRR

---

## How It Works

```
┌──────────┐   ┌──────────┐   ┌─────────────┐
│ Intercom  │   │  Looker   │   │ Salesforce  │
│ (Support) │   │(Adoption) │   │(Financial,  │
│           │   │(Platform) │   │ Signals)    │
└─────┬─────┘   └─────┬─────┘   └──────┬──────┘
      │               │                │
      └───────────────┼────────────────┘
                      ▼
            ┌─────────────────┐
            │   Scoring Engine │
            │                 │
            │ Churn Risk (60%)│
            │ + Platform (40%)│
            │ = Health Score  │
            │                 │
            │ Qualitative cap │
            │ (if signals)    │
            └────────┬────────┘
                     ▼
            ┌─────────────────┐
            │   Salesforce    │
            │ Health_Score__c │
            │ (1 record/month)│
            └─────────────────┘
```

### The Formula

**Health Score** = (60% x Churn Risk Score) + (40% x Platform Value Score)

**Churn Risk Score** = weighted average of four dimensions:

| Dimension | Weight | Source | Status |
|-----------|--------|--------|--------|
| Support Health | 30% | Intercom | Ready |
| Financial & Contract | 30% | Salesforce | Ready |
| Adoption & Engagement | 25% | Looker/Redshift | Needs cleanup |
| Relationship & Expansion | 15% | Salesforce (CSM input) | Phase 2 |

**Platform Value Score** = Existing AXP Platform Score (no changes to calculation)

### Score Tiers

| Tier | Range | Meaning |
|------|-------|---------|
| Champion | 90–100 | Expansion-ready, reference candidate |
| Healthy | 76–89 | Stable, standard cadence |
| At Risk | 60–75 | Proactive response needed |
| Critical | 0–59 | Immediate intervention required |

### Qualitative Override

CSMs log churn signals in Salesforce via a 20-second Quick-Log flow. Signals **cap** the score but never raise it:

- 1+ Moderate signal → capped at 75 (cannot be Champion)
- 1+ Critical signal → capped at 65 (forced to At Risk)
- 2+ Critical signals → capped at 55 (forced to Critical)
- Critical + Confirmed → set to 50 (highest urgency)

The qualitative modifier applies **in real-time** via Salesforce formula fields — no waiting for the monthly scoring run.

---

## What's Built

The middleware codebase is complete and structured as:

```
health-score-middleware/
├── config/
│   ├── thresholds.yaml        # Scoring thresholds by segment (configuration, not code)
│   ├── weights.yaml            # Dimension and metric weights
│   └── account_mapping.csv     # Cross-system ID mapping
├── src/
│   ├── extractors/             # API clients for Intercom, Looker, Salesforce
│   ├── scoring/                # Normaliser, dimension scoring, composite, qualitative modifier
│   ├── loaders/                # Salesforce writer + dry-run CSV output
│   └── main.py                 # Orchestrator + Lambda handler + CLI entry point
├── tests/                      # Unit + integration tests (incl. worked examples from design docs)
├── requirements.txt
└── .env.example
```

**Key design decisions:**
- **Thresholds and weights are config, not code** — when leadership changes the Support Health weight from 30% to 25%, you edit a YAML file
- **Segment-aware** — Paid and Standard have different thresholds for the same metrics
- **Missing dimensions handled gracefully** — Phase 1 runs without Relationship data; the scoring engine reweights automatically and flags coverage gaps
- **Dry-run mode** — outputs to CSV for validation before writing to Salesforce
- **Per-account resilience** — one account failing doesn't stop the run

---

## What's Needed Before Go-Live

### 1. Account Identity Resolution (BLOCKER)

The middleware joins data across Intercom, Looker, and Salesforce. A shared account identifier is required. **Investigation needed:**
- What `company_id` exists on Intercom Companies today?
- What customer identifier does Redshift use?
- Is there an existing shared key with Salesforce Account ID?

### 2. Salesforce Setup

A Salesforce admin must create:
- `Health_Score__c` custom object with 30+ fields
- `Churn_Signal__c` custom object for qualitative signals
- Connected App for OAuth authentication
- Quick-Log Screen Flow for CSM signal capture

### 3. Looker Data Availability

Confirm what Looks/Explores exist for:
- Staff/admin login counts
- Feature adoption breadth
- AXP Platform Score (current + historical)

### 4. Threshold Approval

CS leadership must review and approve the scoring thresholds in `config/thresholds.yaml` before pilot.

---

## Timeline

| Week | Milestone |
|------|-----------|
| 1 | Account identity audit + Salesforce object creation begins |
| 2 | AWS infrastructure + extractor development |
| 3 | Scoring engine tested + Salesforce loader complete |
| 4–5 | Pilot: 15 accounts scored, CSMs validate |
| 6 | Production go-live for 200+ accounts |

**Total Phase 1 effort:** ~100 hours across Engineering, SF Admin, CS Ops, and CS Leadership.

---

## Risks

| Risk | Mitigation |
|------|------------|
| No shared account identifier across systems | Week 1 audit. Fallback: fuzzy matching + human verification |
| Looker Explores don't exist yet | Adoption dimension reweighted (scored from available data + coverage flag) |
| Scores don't match CSM intuition | Dry-run validation. Thresholds are config — tune without code changes |
| Lambda timeout for 200+ accounts | Monitor in pilot. Fallback: Step Functions for parallel processing |

---

## Success Criteria

- **Phase 1:** 100% of pilot accounts scored monthly, reviewed in CSM 1:1s
- **6-month:** Standard Success 80%+ automation rate; Critical tier identified avg 120 days before renewal
- **Qualitative adoption:** 80%+ of Paid CSMs logging at least 1 signal/month within 3 months
- **Predictive value:** Signalled accounts churn at 2x+ rate of unsignalled accounts
