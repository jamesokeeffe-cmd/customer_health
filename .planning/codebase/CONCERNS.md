# Technical Concerns & Debt

**Generated:** 2026-02-26

---

## Summary

The codebase is architecturally sound with good test coverage (194 tests) and clean separation of concerns. Primary risks are security (SOQL injection), operational (no retries, broad exception catching), and data quality (empty account mapping, unvalidated config).

---

## CRITICAL

### 1. SOQL Injection in Salesforce Extractor

**File:** `src/extractors/salesforce.py`

9+ SOQL queries use f-string interpolation with unsanitized `sf_account_id` and `segment` values:
```python
f"WHERE AccountId = '{sf_account_id}'"
f"WHERE Success_Tier__c = '{segment}'"
```

CLAUDE.md notes this is "safe for internal IDs, not for user input." Risk is low while IDs come from internal CSV, but grows if mapping source changes.

**Fix:** Validate IDs match `^[a-zA-Z0-9]{15,18}$` before interpolation.

### 2. Empty Account Mapping CSV

**File:** `config/account_mapping.csv`

Contains only headers — zero data rows. Pipeline runs successfully but scores zero accounts. Silent success masks misconfiguration.

**Fix:** Add validation in orchestrator init; error or warn if account_mapping is empty.

---

## HIGH

### 3. Broad Exception Catching with Silent Failures

**Files:** `src/main.py`, all extractors

Pattern: `except Exception: logger.exception(...)` with no re-raise. API outages silently return empty metrics. A full Intercom outage would cause all support dimensions to be missing, reweighting scores and potentially masking problems.

**Fix:** Distinguish transient vs permanent errors; add structured error tracking.

### 4. No Retry Logic for API Calls

**Files:** All extractors

No exponential backoff or retry handling. Single transient failure (timeout, 502, rate limit) kills extraction for that account.

**Fix:** Add `tenacity` or `requests` retry adapter with 3 retries / 30s backoff.

### 5. Unvalidated Configuration

**File:** `src/main.py` (config loading)

No schema validation for `weights.yaml` or `thresholds.yaml`. Missing keys, typos, or malformed values only surface at runtime during first account scoring.

**Fix:** Add config validation at startup (check required keys, weight sums, threshold ordering).

### 6. No CSV Field Validation

**File:** `src/main.py` (account mapping loading)

`load_account_mapping()` returns raw CSV dicts. A column name typo in the CSV causes `KeyError` on first account, halting the entire run.

**Fix:** Validate required CSV columns at load time.

---

## MEDIUM

### 7. Phase 2 Relationship Metrics Always None

**File:** `src/extractors/salesforce.py`

`extract_relationship_metrics()` returns None when Phase 2 Salesforce fields don't exist. Relationship dimension (15% of Churn Risk) is always missing, systematically biasing all scores via weight redistribution.

**Fix:** Add explicit config flag for Phase 2 enablement; log clearly when disabled.

### 8. No Timeout on Salesforce Queries

**File:** `src/extractors/salesforce.py`

`simple_salesforce` client initialised without timeout. A hanging query blocks the entire pipeline.

**Fix:** Set timeout parameter on client init.

### 9. Hardcoded Looker Model/View Names and Look IDs

**File:** `src/extractors/looker.py`

Model name `alliants`, view names, and saved Look IDs (171-177) are hardcoded defaults. Different Looker instances would fail silently. Look field name constants are also placeholders pending verification against actual Look output.

**Fix:** Move to config or environment variables.

### 10. Division-by-Zero Masking in Normaliser

**File:** `src/scoring/normaliser.py`

Zero-width threshold ranges (green == yellow) silently return 75.0, masking misconfiguration.

**Fix:** Validate threshold monotonicity in config validation.

### 11. No Per-Account Error Reporting

**File:** `src/main.py`

Rollbar only at Lambda handler level. Per-account failures logged but not reported to error tracking.

**Fix:** Report per-account failures to Rollbar with account context.

---

## LOW

- Missing type hints in `tests/conftest.py` and `dashboard.py`
- No `mypy` or type checking configured
- Logging levels inconsistent across modules
- CSV writer fragile to missing keys in score results
- Intercom escalation detection relies on tag naming convention
- Lambda event schema not documented
- No dry-run output validation

---

## Recommended Priority

**Before production:** Fix #1 (SOQL injection), #2 (empty CSV validation), #4 (retries), #5 (config validation)
**Sprint 1-2:** #3 (error handling), #6 (CSV validation), #7 (Phase 2 flag), #8 (timeouts)
**Backlog:** #9-11 and low-severity items
