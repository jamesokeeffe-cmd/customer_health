# Testing

**Generated:** 2026-02-26

---

## Overview

- **Framework:** pytest with `unittest.mock`
- **Total tests:** 194 across 9 test files
- **External SDKs:** NOT installed locally — mocked via `conftest.py`
- **Run:** `python3 -m pytest tests/ -v`

## Test Infrastructure

### conftest.py — SDK Mocking

`tests/conftest.py` mocks external SDKs at `sys.modules` level before any test imports:
```python
sys.modules["simple_salesforce"] = MagicMock()
sys.modules["looker_sdk"] = MagicMock()
sys.modules["looker_sdk.models40"] = MagicMock()
```

This allows all extractor/loader tests to run without installing `simple_salesforce` or `looker_sdk`.

## Test File Organisation

| File | Tests | Mocks? | What It Tests |
|------|-------|--------|---------------|
| `test_normaliser.py` | ~15 | No | Pure normalisation logic |
| `test_scoring.py` | ~30 | No | Dimensions, composite, qualitative, tier classification |
| `test_integration.py` | ~15 | No | End-to-end worked examples from design docs |
| `test_intercom_extractor.py` | ~10 | Yes | Intercom API parsing, pagination, metric aggregation |
| `test_jira_extractor.py` | ~15 | Yes | Jira JQL, pagination, bug metric aggregation |
| `test_looker_extractor.py` | ~30 | Yes | Looker SDK queries, JSON parsing, adoption metrics, PVS Look-based extraction, Look caching |
| `test_salesforce_extractor.py` | ~25 | Yes | SOQL queries, financial/relationship/qualitative extraction |
| `test_salesforce_loader.py` | ~15 | Yes | Health_Score__c upsert, CSV export |
| `test_orchestrator.py` | ~20 | Yes | Orchestrator init, credential loading, scoring flow |

## Fixture Patterns

**Pure functions (no fixtures needed):**
```python
# test_normaliser.py — just call the function directly
def test_green_value_returns_100():
    assert normalise_metric(2, green=2, yellow=4, red=8, lower_is_better=True) == 100.0
```

**Extractor fixtures (patch at construction):**
```python
@pytest.fixture
def extractor():
    with patch("src.extractors.salesforce.Salesforce") as MockSF:
        mock_sf = MagicMock()
        MockSF.return_value = mock_sf
        ext = SalesforceExtractor(username="u", password="p", security_token="t", domain="test")
    return ext
```

**Orchestrator fixtures:**
```python
@pytest.fixture
def orchestrator():
    orch = HealthScoreOrchestrator(config_dir="config", dry_run=False)
    orch.account_mapping = []
    return orch
```

## Mock Response Helpers

Each extractor test file defines helpers to build realistic mock API responses:

- `test_intercom_extractor.py`: `_make_conversation()`, `_mock_search_response()`, `_mock_get_response()`
- `test_salesforce_extractor.py`: `_query_result(records, total)`, `_account_record(arr, tier)`
- `test_jira_extractor.py`: `_make_issue()`, `_mock_search_response()`
- `test_looker_extractor.py`: Mock SDK `create_query()` / `run_query()` return values; `_look_cache` pre-populated for PVS tests

## Mock Setup Pattern

Extractors use `side_effect` for sequential query mocking:
```python
extractor.sf.query.side_effect = [
    _query_result([{"CloseDate": renewal_date}]),  # renewal query
    _query_result([], total=2),                      # payment query
    _query_result([], total=1),                      # contract query
    _query_result([{"ARR__c": 120000}]),             # ARR history
]
result = extractor.extract_financial_metrics("001ABC")
```

## Assertion Patterns

```python
# Exact equality
assert result["payment_health"] == 2

# None checks
assert result["days_to_renewal"] is None

# Numeric precision (1 decimal)
assert result["score"] == 75.3

# Structure validation
assert "metric_scores" in result
assert len(result) == 2

# Mock call verification
extractor.sdk.create_query.assert_called_once()
```

## Integration Tests

`test_integration.py` contains 3 worked examples from the design proposal:
- `TestExampleA` — High-performing account → Champion tier
- `TestExampleB` — Mid-range account → Healthy tier
- `TestExampleC` — Struggling account → At Risk tier

Each test class verifies the full pipeline: dimension scores → churn risk → health score → qualitative modifier → tier classification.

## Edge Cases Covered

- Boundary values (exactly at green/yellow/red thresholds)
- None propagation (missing metrics skip gracefully)
- Weight redistribution (missing dimensions redistribute proportionally)
- Division by zero (range_size == 0 → fallback to 75.0)
- Empty API responses (extractors return zero/empty metrics)
- Qualitative caps (critical signal caps at 59, moderate at 75)
