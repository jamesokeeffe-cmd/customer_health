# Health Score Middleware: Project Plan

## Executive Summary

This project delivers a **Python middleware** that calculates a unified Customer Health Score for 200+ accounts by extracting data from three systems (Intercom, Looker, Salesforce), applying a quantitative scoring model with qualitative signal modifiers, and writing results back to Salesforce. The middleware runs as a stateless monthly Lambda, with Looker owning historical data and Salesforce formula fields handling real-time qualitative overrides.

**The scoring model** combines a Churn Risk Score (60%) and Platform Value Score (40%) into a single 0–100 metric. The Churn Risk Score is built from four weighted dimensions — Support Health (30%), Financial & Contract (30%), Adoption & Engagement (25%), and Relationship & Expansion (15%) — each scored with segment-specific thresholds for Paid Success ($250k+ ARR) and Standard Success (<$250k ARR). A qualitative modifier layer caps the score when CSMs log churn signals in Salesforce, ensuring human intelligence overrides when data lags behind reality.

**Key outcomes:**
- Single source of truth for customer health, replacing subjective assessments
- Automated scoring for 200+ accounts on a monthly cadence
- Real-time qualitative overrides via Salesforce formula fields (no middleware dependency)
- Dry-run mode for validation before production writes
- Full test coverage including three worked examples from the design documents

**Critical blocker:** Account identity resolution across Intercom, Looker, and Salesforce. The middleware cannot function without a reliable shared key. This must be resolved before any integration testing.

---

## Phase Plan

### Phase 1: Foundation (Months 1–2)

**Goal:** Score accounts using Support Health, Financial & Contract, and Platform Value Score. Adoption & Engagement included where data quality allows. Relationship & Expansion deferred to Phase 2 — the scoring engine reweights automatically.

#### Step 1: Account Identity Audit *(Week 1 — BLOCKER)*

| Task | Owner | Deliverable |
|------|-------|-------------|
| Audit Intercom Companies API: what `company_id` and custom attributes exist on each company | Engineering | Report: identifier inventory per Intercom company |
| Audit Looker/Redshift: what customer identifier columns exist, do they reference SF Account IDs | Engineering | Report: Redshift schema + identifier mapping |
| Determine linkage strategy: SF Account ID as master key, intermediate mapping, or fuzzy match | Engineering + CS Ops | Decision document |
| Populate `config/account_mapping.csv` for pilot accounts (5 Paid + 10 Standard) | CS Ops + Engineering | Verified mapping file |

**Decision required:** How do we link accounts across three systems? Options in order of preference:
1. SF Account ID pushed into Intercom as custom attribute + referenced in Redshift (cleanest, requires upstream changes)
2. Intercom `company_id` already set to a shared key — check what it contains today
3. Automated fuzzy matching on company name/domain + human verification
4. DynamoDB mapping table maintained via admin process

**Exit criteria:** 15 pilot accounts have verified IDs across all three systems.

---

#### Step 2: Salesforce Object Setup *(Weeks 1–2, parallel with Step 1)*

| Task | Owner | Deliverable |
|------|-------|-------------|
| Create `Health_Score__c` custom object (Master-Detail to Account) | SF Admin | Object + 30+ fields per metrics map |
| Create all `Health_Score__c` fields from the metrics map CSV | SF Admin | Fields matching `Health_Score__c.*` column |
| Create `Scoring_Date__c` and `Scoring_Period__c` fields | SF Admin | Date + Text fields |
| Create `Churn_Signal__c` custom object (Master-Detail to Account) | SF Admin | Object + all signal fields |
| Create rollup fields on Account for active signal counts by severity | SF Admin | `Qual_Active_Critical__c`, `Qual_Active_Moderate__c`, `Qual_Active_Watch__c` |
| Create formula field `Final_Score__c` for real-time qualitative modifier | SF Admin | Formula: `MIN(Quantitative_Score__c, CASE(...))` |
| Create Connected App for OAuth (client credentials flow) | SF Admin | Client ID + Secret for middleware auth |
| Build Quick-Log Screen Flow for CSMs (Churn Signal capture) | SF Admin | 4-step flow: Category → Signal/Source → Confidence/Context → Save |

**Key decision — already resolved:** One record per month (append model). Each scoring run creates a new `Health_Score__c` record. Historical scores preserved natively in Salesforce.

**Exit criteria:** Health_Score__c and Churn_Signal__c objects exist in sandbox. Connected App credentials tested.

---

#### Step 3: AWS Infrastructure Setup *(Week 2)*

| Task | Owner | Deliverable |
|------|-------|-------------|
| Create Lambda function (Python 3.12 runtime, 512MB–1GB memory) with env vars for credentials (Intercom, Looker, Salesforce) | Engineering | Lambda ARN with environment variable configuration |
| Create EventBridge rule: `cron(0 6 1 * ? *)` (6am UTC, 1st of month) | Engineering | Scheduled trigger |
| Create CloudFormation template defining Lambda, EventBridge rule, and IAM role | Engineering | `deploy/cloudformation/health-score-lambda.yml` |
| IAM role: Lambda execution role with DynamoDB read/write + CloudWatch write | Engineering | IAM policy (defined in CloudFormation) |
| Configure Rollbar for error tracking (matching AHOY's universal Rollbar usage) | Engineering | Rollbar project + SDK integration |
| Configure DynamoDB operational metrics using AHOY's `{env}-custom-metrics` table pattern | Engineering | Metrics recording for run status, account counts, errors |
| Add Datadog Lambda layer for APM (matching AHOY's Datadog instrumentation) | Engineering | Datadog layer ARN in CloudFormation template |
| Create GitHub Actions workflow for pytest + linting on PR (matching AHOY's CI pattern) | Engineering | `.github/workflows/ci.yml` |

**Infrastructure pattern (AHOY-aligned):** Application code reads credentials from `os.environ` only — the infrastructure layer handles secret injection via Lambda environment variable configuration. No `boto3` Secrets Manager SDK calls in application code.

**Exit criteria:** Lambda deploys and runs (dry-run mode) from EventBridge trigger. Credentials load from environment variables. CloudFormation stack deploys successfully. Rollbar captures test errors. GitHub Actions CI passes on PR.

---

#### Step 4: Build & Test Extractors *(Weeks 2–3)*

| Task | Owner | Deliverable |
|------|-------|-------------|
| Intercom extractor: conversations by company, support metrics calculation | Engineering | `src/extractors/intercom.py` — tested against real Intercom API |
| Looker extractor: inline queries for adoption metrics + AXP Platform Score | Engineering | `src/extractors/looker.py` — tested against real Looker API |
| Salesforce extractor: financial/contract fields, qualitative signals | Engineering | `src/extractors/salesforce.py` — tested against SF sandbox |
| Validate extracted data against known values for 3–5 pilot accounts | Engineering + CSMs | Data validation report |

**Dependency:** Requires Step 1 (account mapping) and Step 3 (credentials) to test against live APIs.

**Open question:** What Looks/Explores already exist in Looker for adoption metrics and AXP Platform Score? If they don't exist, Looker configuration work must happen in parallel.

**Exit criteria:** All three extractors return valid data for pilot accounts. Unit tests pass with mock responses.

---

#### Step 5: Scoring Engine & Config *(Weeks 2–3, parallel with Step 4)*

| Task | Owner | Deliverable |
|------|-------|-------------|
| Finalise `config/thresholds.yaml` with CS leadership input | CS Ops + SVP CX | Approved threshold values per segment |
| Finalise `config/weights.yaml` | CS Ops + SVP CX | Approved dimension/metric weights |
| Normaliser: raw → 0-100 per metric with segment-aware thresholds | Engineering | `src/scoring/normaliser.py` + unit tests |
| Dimension scoring: weighted metrics → dimension score | Engineering | `src/scoring/dimensions.py` + unit tests |
| Composite scoring: dimensions → Churn Risk → Health Score | Engineering | `src/scoring/composite.py` + unit tests |
| Qualitative modifier: signal caps | Engineering | `src/scoring/qualitative.py` + unit tests |
| Missing dimension reweighting + coverage flag | Engineering | Tested in composite scoring |
| Integration tests: Three worked examples from Unified Proposal | Engineering | `tests/test_integration.py` — all pass |

**Exit criteria:** All unit tests pass. Three worked examples (Four Seasons healthy, Four Seasons + signal, Boutique Hotel At Risk) produce exact expected scores.

---

#### Step 6: Salesforce Loader & Orchestrator *(Week 3)*

| Task | Owner | Deliverable |
|------|-------|-------------|
| Salesforce writer: create `Health_Score__c` record per account per period | Engineering | `src/loaders/salesforce.py` |
| Dry-run mode: output to CSV instead of Salesforce | Engineering | CSV output with all score fields |
| Main orchestrator: extract → score → load pipeline with per-account error handling | Engineering | `src/main.py` |
| Lambda handler: Environment variable credential loading, EventBridge event parsing | Engineering | `lambda_handler()` function |

**Exit criteria:** Full pipeline runs end-to-end in dry-run mode. CSV output reviewed and validated.

---

#### Step 7: Pilot & Validation *(Weeks 4–5)*

| Task | Owner | Deliverable |
|------|-------|-------------|
| Dry-run against 15 pilot accounts (5 Paid + 10 Standard), output to CSV | Engineering | CSV with scores for review |
| CSM validation: do scores match intuition for known accounts? | CSMs + CS Ops | Validation feedback (per-account) |
| Threshold adjustment if scores don't align with CSM expectations | CS Ops + Engineering | Updated `thresholds.yaml` |
| First real run: write `Health_Score__c` records to SF sandbox | Engineering | 15 records in sandbox |
| Verify Salesforce records, reports, and formula field behaviour | SF Admin + CS Ops | Validated SF data |

**Exit criteria:** CSMs confirm scores are directionally correct for pilot accounts. No critical scoring errors.

---

#### Step 8: Production Go-Live *(Week 6)*

| Task | Owner | Deliverable |
|------|-------|-------------|
| Populate `account_mapping.csv` for all 200+ accounts | CS Ops + Engineering | Complete mapping file |
| Deploy to production Lambda | Engineering | Production deployment |
| First production run (with monitoring) | Engineering | 200+ Health_Score__c records |
| CSM training: how to read Health Score, how to log Churn Signals | CS Ops | Training session delivered |
| Monitor first 3 monthly runs closely | Engineering + CS Ops | Run reports, threshold tuning |

**Exit criteria:** Three consecutive monthly runs complete without critical failures. Scores reviewed and accepted by CS leadership.

---

### AHOY Tech Stack Alignment

The health-score-middleware has been cross-referenced with the AHOY (AXP) project infrastructure to ensure consistency with established company patterns. The following items have been aligned:

| Area | AHOY Pattern | Health Score Middleware Alignment |
|------|-------------|----------------------------------|
| **Secrets management** | Application code reads `os.environ` only; infrastructure layer handles secret injection (ECS entrypoint script or Lambda env var configuration) | Lambda env vars for all credentials — no `boto3` Secrets Manager SDK calls in application code |
| **Error tracking** | Rollbar (universal across all AHOY services) | Rollbar SDK integrated; errors reported to shared or dedicated Rollbar project |
| **Operational metrics** | DynamoDB `{env}-custom-metrics` table for recording operational data from Python Lambdas | Run status, account counts, and error metrics written to `{env}-custom-metrics` DynamoDB table |
| **Infrastructure as Code** | CloudFormation for all infrastructure definitions | `deploy/cloudformation/health-score-lambda.yml` defines Lambda, EventBridge, IAM role |
| **CI/CD** | GitHub Actions for pytest + linting on PR (supplementary CI alongside deploy pipelines) | `.github/workflows/ci.yml` runs pytest and linting on every PR |
| **APM / Observability** | Datadog Lambda layer for tracing and metrics | Datadog Lambda layer configured in CloudFormation template |

#### Open Coordination Items (Requires AHOY Team Input)

These items require confirmation or access grants from the AHOY team before infrastructure setup:

| Item | Question | Status |
|------|----------|--------|
| DynamoDB access | Can health-score-middleware write to the existing `{env}-custom-metrics` table, or does it need a dedicated table? What is the exact naming convention? | Pending |
| Rollbar project | Should health-score-middleware share the existing AHOY Rollbar project, or have a dedicated one? | Pending |
| Datadog API key | What Datadog API key / site configuration should be used for the Lambda layer? | Pending |
| Looker instance | Confirm which Looker instance and credentials are used by AHOY (for consistency) | Pending |
| AWS region | Confirm deployment region is `eu-west-1` (matching AHOY infrastructure) | Pending |

---

### Phase 2: Relationship & Automation (Months 3–4)

| Task | Deliverable |
|------|-------------|
| Add Relationship & Expansion dimension (15%) — CRM fields for QBR attendance, champion tracking, CSQL logging | Full 4-dimension Churn Risk model |
| Automated score modifier logic — signals auto-cap Health Score | Salesforce formula/automation |
| Automated playbook triggers (task creation, notifications, escalations) | Salesforce flows |
| Signal expiry automation (Watch: 60d, Moderate: 90d, Critical: manual) | Salesforce scheduled flow |
| License utilisation metric added to Adoption dimension | Looker query + scoring config |
| Automated renewal flow for Standard Healthy/Champion | Salesforce Agent workflow |
| Dashboard widgets (6 views from Qualitative Signals doc) | Salesforce reports/dashboards |

### Phase 3: AI & Calibration (Months 5–6)

| Task | Deliverable |
|------|-------------|
| Fin AI metrics added to Support Health (deflection rate, CX score) | Intercom Fin API integration |
| 30/60/90-day trend analysis in scoring (Support trend metric) | Intercom multi-window extraction |
| Scoring weight calibration against actual churn outcomes | Statistical analysis + weight tuning |
| AI-assisted signal detection from Intercom transcripts | NLP pipeline (Phase 3+) |
| Sentiment analysis on support conversations | Intercom transcript analysis |

---

## Open Questions (Must Resolve Before Building)

| # | Question | Impact | Owner |
|---|----------|--------|-------|
| 1 | **Account mapping (BLOCKER):** What identifiers exist on Intercom Companies and in Looker/Redshift today? Is there a shared key with SF Account ID? | Blocks all integration work | Engineering |
| 2 | **Looker data availability:** What Looks/Explores already exist for adoption metrics and AXP Platform Score? What's the Redshift schema? | Blocks Looker extractor | Engineering |
| 3 | **Salesforce admin capacity:** Who creates the custom objects, Connected App, and Screen Flow? | Blocks Step 2 | CS Ops / SF Admin |
| 4 | **Lambda timeout risk:** 200+ accounts × 3 API sources. Need to estimate total API calls and execution time. If >15 min, need Step Functions or batching. | Architecture decision | Engineering |
| 5 | **200+ accounts = brands or properties?** Brand-level vs property-level scoring affects mapping complexity and volume. | Scope decision | CS Ops / SVP CX |
| 6 | **Threshold approval:** Are the threshold values in `thresholds.yaml` correct? CS leadership must review and approve before pilot. | Scoring accuracy | SVP CX |
| 7 | **AHOY DynamoDB access:** Can health-score-middleware write to the existing `{env}-custom-metrics` table? What is the exact naming convention? | Blocks operational metrics setup | Engineering / AHOY team |
| 8 | **Rollbar project setup:** Shared AHOY Rollbar project or dedicated project for health-score-middleware? | Blocks error tracking setup | Engineering / AHOY team |
| 9 | **Datadog API key:** What Datadog API key and site configuration should be used for the Lambda layer? | Blocks APM setup | Engineering / AHOY team |
| 10 | **Looker instance confirmation:** Which Looker instance and service account credentials does AHOY use? | Blocks Looker extractor configuration | Engineering / AHOY team |
| 11 | **AWS region confirmation:** Confirm deployment in `eu-west-1` to match AHOY infrastructure | Blocks CloudFormation deployment | Engineering / AHOY team |

---

## Resource Requirements

| Role | Phase 1 Effort | Ongoing |
|------|---------------|---------|
| Engineering (middleware build, AWS setup, testing) | 40 hours | 2 hrs/month monitoring |
| Salesforce Admin (objects, flows, Connected App) | 35 hours | 5 hrs/month maintenance |
| CS Operations (mapping, thresholds, training, validation) | 20 hours | 2 hrs/month review |
| CS Leadership (threshold review, pilot validation, CSM reviews) | 5 hours | 2 hrs/month |

**Total Phase 1:** ~100 hours across roles, 6-week timeline.

---

## Risk Register

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| No shared account identifier across systems | High | Critical — blocks project | Investigate in Week 1. Fallback: automated fuzzy matching + human verification |
| Looker Explores don't exist for required metrics | Medium | Delays adoption scoring | Build Looker queries in parallel. Adoption dimension can be deferred (reweighted) |
| Lambda exceeds 15-minute timeout for 200+ accounts | Low | Requires architecture change | Monitor execution time in pilot. Fallback: Step Functions or batch processing |
| Threshold values produce unintuitive scores | Medium | Requires tuning | Dry-run validation with CSMs. Iterative threshold adjustment in config (no code changes) |
| CSMs don't adopt signal logging | Medium | Qualitative layer ineffective | <30 second Quick-Log flow. Manager review in 1:1s. Phase 1 = manual modifier (lower stakes) |
| Salesforce API rate limits | Low | Blocks writes | <200 accounts × standard REST = well within Enterprise limits |

---

## Resolved Decisions

| Decision | Answer | Rationale |
|----------|--------|-----------|
| Language | Python | Best library support for all three APIs |
| Execution model | Monthly AWS Lambda + EventBridge | Stateless, cost-efficient, fits monthly cadence |
| Historical data ownership | Looker | Middleware is stateless; no snapshot storage needed |
| Qualitative modifier timing | Real-time via SF formula fields | CSMs need immediate impact, not monthly delay |
| Score record model | One record per month (append) | Native SF reporting on trends, cheap storage |
| Missing dimension handling | Reweight + coverage flag | Accurate relative scoring + transparency |
| Hosting | AWS Lambda | Existing AWS footprint, ENV vars for credentials (infra-layer injection, matching AHOY pattern) |
| Error tracking | Rollbar | AHOY's universal error tracking service across all environments |
| Observability | Datadog Lambda layer | Matching AHOY's Datadog instrumentation for APM and tracing |
| Operational metrics | DynamoDB `{env}-custom-metrics` → CloudWatch | Matching AHOY's existing Python Lambda pattern for operational data |
| Infrastructure as Code | CloudFormation | AHOY uses CloudFormation for all infrastructure definitions |
| CI | GitHub Actions (pytest + linting) | Matching AHOY's supplementary CI pattern for PR validation |
