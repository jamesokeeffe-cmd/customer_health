# External Integrations

**Generated:** 2026-02-26

---

## Overview

The pipeline integrates with 4 data sources (extract), 1 destination (load), and optional infrastructure services.

```
Intercom  ──┐
Jira      ──┤  Extract    Score    Load
Looker    ──┤  ──────► ──────► ──────►  Salesforce (Health_Score__c)
Salesforce──┘
```

---

## Data Source: Intercom (Support Metrics)

**File:** `src/extractors/intercom.py`
**Auth:** Bearer token (`INTERCOM_API_TOKEN`)
**Protocol:** REST API with cursor-based pagination
**Base URL:** `https://api.intercom.io`

**Endpoints used:**
- `POST /conversations/search` — Search conversations by company ID and date range
- `GET /conversations/{id}` — Get conversation details (response times, tags)
- `GET /companies/{id}` — Get company metadata

**Metrics extracted:**
- `total_conversations`, `p1_p2_pct`, `escalation_pct`
- `median_first_response_seconds`, `median_resolution_seconds`
- `csat_score`, `csat_response_rate`

**Pagination:** Cursor-based via `pages.next.starting_after`

---

## Data Source: Jira (Bug Metrics)

**File:** `src/extractors/jira.py`
**Auth:** Basic Auth (`JIRA_EMAIL` + `JIRA_API_TOKEN`)
**Protocol:** REST API v2 with offset pagination
**Base URL:** `{JIRA_BASE_URL}/rest/api/2`

**Endpoints used:**
- `GET /search` — JQL queries for bugs by project/component

**Metrics extracted:**
- `open_bugs`, `reopened_bugs`, `critical_bugs`
- `avg_resolution_days`, `bug_trend` (new vs resolved ratio)

**Pagination:** Offset-based (`startAt` + `maxResults`)

---

## Data Source: Looker (Adoption / Platform Value)

**File:** `src/extractors/looker.py`
**Auth:** OAuth2 client credentials (`LOOKER_CLIENT_ID` + `LOOKER_CLIENT_SECRET`)
**Protocol:** Looker SDK (`looker_sdk.init40()`)
**Base URL:** `{LOOKER_BASE_URL}`

**SDK methods used:**
- `create_query()` — Build inline queries against models/views
- `run_query()` — Execute queries, return JSON
- `run_look()` — Execute saved Looks by ID

**Models/Views (hardcoded defaults):**
- Model: `alliants`
- Views: `user_sessions`, `feature_usage`, `platform_metrics`

**Metrics extracted:**
- Adoption: `dau`, `mau`, `feature_breadth`, `login_frequency`
- Platform Value Score: pillar scores across usage dimensions

---

## Data Source: Salesforce (Financial / Relationship / Qualitative)

**File:** `src/extractors/salesforce.py`
**Auth:** SOAP session via `simple_salesforce.Salesforce(username, password, security_token, domain)`
**Protocol:** SOQL queries

**Objects queried:**
- `Account` — ARR, tier, segment
- `Opportunity` — Renewal dates, close dates
- `Contract` — Contract changes count
- `Case` / Custom objects — Payment issues, QBR attendance
- `Churn_Signal__c` — Qualitative signals (critical/moderate/watch)

**Metrics extracted:**
- Financial: `days_to_renewal`, `payment_health`, `contract_changes`, `arr_trajectory_pct`, `tier_alignment`
- Relationship: `qbr_attendance`, `champion_stability`, `expansion_signals` (Phase 2 — returns None if fields missing)
- Qualitative: `critical_signals`, `moderate_signals`, `watch_signals`, `monitoring_reduction`

**Security note:** SOQL uses f-string interpolation — safe for internal IDs, not user input.

---

## Load Destination: Salesforce (Health_Score__c)

**File:** `src/loaders/salesforce.py`
**Auth:** Same Salesforce connection as extractor
**Protocol:** `simple_salesforce` upsert

**Operations:**
- Upsert `Health_Score__c` custom object per account
- Fields written: `Health_Score__c`, `Churn_Risk_Score__c`, `Tier__c`, `Coverage__c`, dimension scores, timestamp
- Dry-run mode: writes CSV to local filesystem instead

---

## Infrastructure Services

### AWS Lambda
- **Trigger:** EventBridge monthly schedule
- **Handler:** `src/main.py:lambda_handler`
- **Input:** JSON event with optional `period`, `dry_run` overrides

### AWS Secrets Manager
- **Used for:** Credential storage in production (alternative to env vars)
- **Loaded in:** `src/main.py` orchestrator init

### Rollbar (Optional)
- **File:** `src/main.py`
- **Token:** `ROLLBAR_ACCESS_TOKEN`
- **Behaviour:** Gracefully disabled if token not set; reports unhandled exceptions from Lambda handler

---

## Webhooks

None implemented. All integrations are polling-based (monthly batch ETL).
