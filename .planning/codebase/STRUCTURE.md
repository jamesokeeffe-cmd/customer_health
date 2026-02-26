# Directory Structure

**Generated:** 2026-02-26

---

## Tree

```
health-score-middleware/
├── CLAUDE.md                        # Project instructions for AI assistants
├── dashboard.py                     # Streamlit UI for ad-hoc analysis
├── .env.example                     # Environment variable template
│
├── config/
│   ├── weights.yaml                 # Metric and dimension weights
│   ├── thresholds.yaml              # Green/yellow/red per metric per segment
│   └── account_mapping.csv          # Cross-system ID mapping (headers only)
│
├── src/
│   ├── __init__.py
│   ├── main.py                      # HealthScoreOrchestrator, lambda_handler, CLI
│   │
│   ├── extractors/
│   │   ├── __init__.py              # try/except ImportError for optional SDKs
│   │   ├── intercom.py              # IntercomExtractor (REST, cursor pagination)
│   │   ├── jira.py                  # JiraExtractor (REST, JQL, offset pagination)
│   │   ├── looker.py                # LookerExtractor (SDK, inline queries + Looks)
│   │   └── salesforce.py            # SalesforceExtractor (SOQL, financial/relationship/qualitative)
│   │
│   ├── scoring/
│   │   ├── __init__.py
│   │   ├── normaliser.py            # normalise_metric() — raw → 0-100 via thresholds
│   │   ├── dimensions.py            # score_dimension() — weighted metric average
│   │   ├── composite.py             # compute_churn_risk(), compute_health_score(), classify_tier()
│   │   └── qualitative.py           # apply_qualitative_modifier() — signal caps
│   │
│   └── loaders/
│       ├── __init__.py
│       └── salesforce.py            # SalesforceLoader (upsert Health_Score__c or CSV)
│
├── tests/
│   ├── conftest.py                  # Mocks simple_salesforce & looker_sdk via sys.modules
│   ├── test_normaliser.py           # Pure normalisation tests
│   ├── test_scoring.py              # Dimension, composite, qualitative tests
│   ├── test_integration.py          # End-to-end worked examples from design docs
│   ├── test_intercom_extractor.py   # Intercom API mocking tests
│   ├── test_jira_extractor.py       # Jira API mocking tests
│   ├── test_looker_extractor.py     # Looker SDK mocking tests
│   ├── test_salesforce_extractor.py # Salesforce SOQL mocking tests
│   ├── test_salesforce_loader.py    # Loader + CSV export tests
│   └── test_orchestrator.py         # Orchestrator init, scoring, run tests
│
└── .planning/                       # GSD planning documents (this directory)
    └── codebase/                    # Codebase mapping docs
```

## Entry Points

| Entry Point | File | Function | Trigger |
|-------------|------|----------|---------|
| Lambda | `src/main.py` | `lambda_handler(event, context)` | EventBridge (monthly) |
| CLI | `src/main.py` | `main()` via argparse | `python3 -m src.main` |
| Dashboard | `dashboard.py` | Streamlit app | `streamlit run dashboard.py` |

## Module Dependencies

```
main.py
├── extractors/intercom.py    (requests)
├── extractors/jira.py        (requests)
├── extractors/looker.py      (looker_sdk)
├── extractors/salesforce.py  (simple_salesforce)
├── scoring/normaliser.py     (pure)
├── scoring/dimensions.py     (pure, uses normaliser)
├── scoring/composite.py      (pure)
├── scoring/qualitative.py    (pure)
└── loaders/salesforce.py     (simple_salesforce)
```

## Naming Conventions

- **Modules:** `snake_case.py`
- **Classes:** `PascalCase` (e.g., `HealthScoreOrchestrator`, `IntercomExtractor`)
- **Functions:** `snake_case` (e.g., `normalise_metric`, `extract_support_metrics`)
- **Private methods:** `_prefixed` (e.g., `_extract_conversation_metrics`, `_run_inline_query`)
- **Constants:** `UPPER_SNAKE_CASE` (e.g., `TIERS`, `INTERCOM_API_BASE`)
- **Config keys:** `snake_case` in YAML
