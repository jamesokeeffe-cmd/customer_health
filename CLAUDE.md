# Health Score Middleware

## What This Is

ETL pipeline that calculates customer health scores by extracting data from Intercom, Jira, Looker, and Salesforce, scoring across multiple dimensions, and writing results back to Salesforce. Runs monthly via AWS Lambda + EventBridge, with a Streamlit dashboard for ad-hoc analysis.

## Architecture

```
Extract → Score → Load

Extractors (src/extractors/)     Scoring Engine (src/scoring/)       Loader (src/loaders/)
├── intercom.py (Support)        ├── normaliser.py (raw → 0-100)     └── salesforce.py (Health_Score__c)
├── jira.py (Bug metrics)        ├── dimensions.py (weighted dims)
├── looker.py (Adoption/PVS)     ├── composite.py (Churn Risk + HS)
└── salesforce.py (Financial)    └── qualitative.py (signal caps)

Orchestrator: src/main.py — ties it all together
Dashboard:    dashboard.py — Streamlit UI
Config:       config/ — weights.yaml, thresholds.yaml, account_mapping.csv
```

## Scoring Model

- **Health Score** = 60% Churn Risk + 40% Platform Value Score
- **Churn Risk** = 30% Support + 30% Financial + 25% Adoption + 15% Relationship
- Each metric normalised to 0-100 via green/yellow/red thresholds (segment-specific: paid vs standard)
- Missing metrics/dimensions handled by proportional weight redistribution
- Qualitative signals (Churn_Signal__c) can cap the final score downward
- Tiers: Champion (90-100), Healthy (76-89), At Risk (60-75), Critical (0-59)

## Commands

```bash
# Run tests
python3 -m pytest tests/ -v

# Run pipeline (dry-run, outputs CSV)
python3 -m src.main --dry-run

# Run dashboard
streamlit run dashboard.py
```

## Project Structure

```
config/
  weights.yaml          # Metric and dimension weights
  thresholds.yaml       # Green/yellow/red thresholds per segment
  account_mapping.csv   # Cross-system ID mapping (sf, intercom, looker, jira)
src/
  main.py               # HealthScoreOrchestrator, Lambda handler, CLI
  extractors/           # API clients (one per data source)
  scoring/              # Pure scoring logic (no I/O)
  loaders/              # Write results to Salesforce or CSV
tests/                  # pytest tests
dashboard.py            # Streamlit app
```

## Conventions

- Python 3.9+ (deployed on Lambda)
- `from __future__ import annotations` in all modules
- Scoring functions are pure (no side effects) — all I/O lives in extractors/loaders
- Missing data returns `None`, which propagates through scoring as weight redistribution
- Thresholds are segment-aware: `paid` and `standard` keys under each metric
- `lower_is_better` flag controls normalisation direction
- Extractors are initialised lazily; only those with credentials present are activated
- Tests use `unittest.mock` for API mocking; scoring tests are pure (no mocks needed)

## Environment Variables

See `.env.example` for the full list. Key groups:
- `INTERCOM_API_TOKEN`
- `JIRA_BASE_URL`, `JIRA_EMAIL`, `JIRA_API_TOKEN`
- `LOOKER_BASE_URL`, `LOOKER_CLIENT_ID`, `LOOKER_CLIENT_SECRET`
- `SF_USERNAME`, `SF_PASSWORD`, `SF_SECURITY_TOKEN`, `SF_DOMAIN`
- `ROLLBAR_ACCESS_TOKEN` (optional)

## Known Issues

- `TestExampleC.test_churn_risk_score` fails: expects 65.4 but gets 65.3 (float rounding)
- Account mapping CSV has headers only — no real accounts configured yet
- Salesforce extractor uses f-string SOQL (safe for internal IDs, not for user input)
