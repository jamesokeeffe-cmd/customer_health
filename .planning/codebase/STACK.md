# Technology Stack

**Generated:** 2026-02-26

---

## Runtime

- **Language:** Python 3.9+ (deployed on AWS Lambda)
- **System Python:** 3.9.6 (macOS)
- **Future annotations:** All modules use `from __future__ import annotations` for PEP 563 compatibility (`X | None` syntax)

## Core Dependencies

| Package | Purpose | Used In |
|---------|---------|---------|
| `simple-salesforce` | Salesforce API client (SOAP auth) | `src/extractors/salesforce.py`, `src/loaders/salesforce.py` |
| `requests` | HTTP client for REST APIs | `src/extractors/intercom.py`, `src/extractors/jira.py` |
| `looker-sdk` | Looker API client (OAuth2) | `src/extractors/looker.py` |
| `pyyaml` | YAML config parsing | `src/main.py` |
| `boto3` | AWS SDK (Secrets Manager, Lambda) | `src/main.py` |
| `python-dotenv` | Local `.env` loading | `src/main.py` |
| `rollbar` | Error tracking (optional) | `src/main.py` |
| `streamlit` | Dashboard UI framework | `dashboard.py` |
| `plotly` | Interactive charts | `dashboard.py` |
| `pandas` | Data manipulation for dashboard | `dashboard.py` |
| `pytest` | Test framework | `tests/` |

## Configuration

- **`config/weights.yaml`** — Metric and dimension weights for scoring model
- **`config/thresholds.yaml`** — Green/yellow/red thresholds per metric, segment-specific (`paid` vs `standard`)
- **`config/account_mapping.csv`** — Cross-system ID mapping (sf, intercom, looker, jira account IDs)
- **`.env` / `.env.example`** — Environment variables for API credentials

## Execution Modes

1. **AWS Lambda** — `lambda_handler(event, context)` in `src/main.py`, triggered monthly via EventBridge
2. **CLI** — `python3 -m src.main --dry-run` with argparse (`--dry-run`, `--config-dir`, `--period`)
3. **Streamlit Dashboard** — `streamlit run dashboard.py` for ad-hoc analysis

## Environment Variables

**Intercom:** `INTERCOM_API_TOKEN`
**Jira:** `JIRA_BASE_URL`, `JIRA_EMAIL`, `JIRA_API_TOKEN`
**Looker:** `LOOKER_BASE_URL`, `LOOKER_CLIENT_ID`, `LOOKER_CLIENT_SECRET`
**Salesforce:** `SF_USERNAME`, `SF_PASSWORD`, `SF_SECURITY_TOKEN`, `SF_DOMAIN`
**Monitoring:** `ROLLBAR_ACCESS_TOKEN` (optional)

## Build & Test

```bash
# Run tests (no external deps needed — conftest mocks SDKs)
python3 -m pytest tests/ -v

# Run pipeline (dry-run outputs CSV)
python3 -m src.main --dry-run

# Run dashboard
streamlit run dashboard.py
```

No build step, package manager, or Dockerfile — runs directly via Python.
