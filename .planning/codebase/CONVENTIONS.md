# Code Conventions

**Generated:** 2026-02-26

---

## Python Version & Imports

Every module starts with:
```python
from __future__ import annotations
```
Required for Python 3.9 compatibility with `X | None` union syntax (PEP 563).

**Import order:** Standard library → third-party → local imports.

## Type Hints

All functions have full type annotations with return types:
```python
def normalise_metric(
    value: float | None,
    green: float,
    yellow: float,
    red: float,
    lower_is_better: bool = False,
) -> float | None:
```

Orchestrator attributes use optional types: `IntercomExtractor | None`.

## Docstrings

Google-style docstrings with Summary, Args, Returns sections:
```python
def compute_health_score(...) -> dict[str, float | str | None]:
    """Compute the final health score from churn risk and platform value.

    Args:
        churn_risk_score: Churn risk score (0-100).
        platform_value_score: Platform value score (0-100) or None.
        ...

    Returns:
        Dict with keys: quantitative_score, churn_risk_component, pvs_component, ...
    """
```

## Logging

Module-level logger: `logger = logging.getLogger(__name__)`

Four levels used:
- `info()` — Normal operation state
- `warning()` — Missing data, fallback behaviour
- `error()` — Failures that don't crash the pipeline
- `exception()` — Failures with tracebacks

## Error Handling Patterns

**Extractors:** try/except with fallback values:
```python
try:
    result = self.sf.query(soql)
except Exception:
    logger.exception("Query failed for %s", account_id)
    return 0  # or {}
```

**Scoring:** None propagation with weight redistribution:
```python
if value is None:
    return None  # Metric missing, skip in dimension scoring
```

**Orchestrator:** Per-account exception handling, continues on failure:
```python
for account in self.account_mapping:
    try:
        result = self._score_account(account)
        results.append(result)
    except Exception:
        logger.exception("Failed scoring %s", account["account_name"])
```

## Data Structures

Return dicts follow consistent patterns:

**Dimension scores:** `{score, metric_scores, coverage, available_weight}`
**Composite scores:** `{score, available_dimensions, missing_dimensions, coverage_pct}`
**Health scores:** `{quantitative_score, churn_risk_component, pvs_component, ...}`

## Scoring Conventions

- Scores: 1 decimal place (e.g., 75.3)
- Percentages: 1 decimal place (e.g., 85.5%)
- Coverage: 2 decimal places (e.g., 0.95)
- Missing data = `None`, not 0
- Segment-aware: `paid` vs `standard` thresholds
- `lower_is_better` flag controls normalisation direction

## Extractor Pattern

All extractors follow the same structure:
1. Constructor takes credentials, stores client
2. Public methods return raw metric dicts (keys match config)
3. Private helper methods prefixed with `_`
4. No I/O beyond API calls
5. Initialised lazily — only if credentials present

## Configuration Loading

YAML and CSV loaded at orchestrator init time, not import time:
```python
self.weights = load_yaml(str(self.config_dir / "weights.yaml"))
self.thresholds = load_yaml(str(self.config_dir / "thresholds.yaml"))
self.account_mapping = load_account_mapping(str(self.config_dir / "account_mapping.csv"))
```

## Comments

Focus on **why**, not **what**. Only where logic isn't self-evident.
