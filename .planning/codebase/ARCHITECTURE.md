# Architecture

**Generated:** 2026-02-26

---

## System Overview

ETL pipeline that calculates customer health scores by extracting data from 4 sources, scoring across weighted dimensions, and writing results to Salesforce.

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  EXTRACT     │     │   SCORE     │     │    LOAD     │
│              │     │             │     │             │
│ Intercom   ──┼────►│ Normalise  ──┼────►│ Salesforce  │
│ Jira       ──┤     │ Dimensions ──┤     │ (upsert)    │
│ Looker     ──┤     │ Composite  ──┤     │   or        │
│ Salesforce ──┘     │ Qualitative──┘     │ CSV (dry)   │
└─────────────┘     └─────────────┘     └─────────────┘
```

## Layer Architecture

### Layer 1: Presentation
- `dashboard.py` — Streamlit UI for ad-hoc analysis and visualization

### Layer 2: Orchestration
- `src/main.py` — `HealthScoreOrchestrator` class ties everything together
  - Loads config (YAML weights/thresholds, CSV account mapping)
  - Initialises extractors lazily (only those with credentials)
  - Iterates accounts: extract → score → load
  - Entry points: Lambda handler, CLI (argparse), module import

### Layer 3: Extraction & Loading (I/O)
- `src/extractors/intercom.py` — REST API, cursor pagination
- `src/extractors/jira.py` — REST API, JQL, offset pagination
- `src/extractors/looker.py` — SDK (inline queries + saved Looks)
- `src/extractors/salesforce.py` — SOQL queries via simple_salesforce
- `src/loaders/salesforce.py` — Upsert Health_Score__c or write CSV

### Layer 4: Scoring (Pure Logic)
- `src/scoring/normaliser.py` — Raw metric → 0-100 via threshold interpolation
- `src/scoring/dimensions.py` — Weighted metric aggregation per dimension
- `src/scoring/composite.py` — Churn Risk + Health Score computation, tier classification
- `src/scoring/qualitative.py` — Signal-based caps on final score

## Data Flow (Per Account)

```
1. Extract raw metrics from each source
   intercom → {total_conversations, p1_p2_pct, median_first_response_seconds, ...}
   jira     → {open_bugs, reopened_bugs, critical_bugs, avg_resolution_days, ...}
   looker   → {dau, mau, feature_breadth, login_frequency, ...}
   salesforce → financial: {days_to_renewal, payment_health, arr_trajectory_pct, ...}
               relationship: {qbr_attendance, champion_stability, ...}
               qualitative: {critical_signals, moderate_signals, ...}

2. Normalise each metric to 0-100
   normalise_metric(value, green, yellow, red, lower_is_better) → 0-100 or None

3. Score each dimension (weighted metric average)
   score_dimension(normalised_metrics, metric_weights, segment) → {score, coverage, ...}
   - Support Health (30%)
   - Financial/Contract (30%)
   - Adoption/Engagement (25%)
   - Relationship/Expansion (15%)

4. Compute composites
   compute_churn_risk(dimension_scores, dimension_weights) → {score, coverage_pct, ...}
   compute_health_score(churn_risk=60%, platform_value=40%) → {quantitative_score, ...}

5. Apply qualitative modifier
   apply_qualitative_modifier(score, signals) → {final_score, modifier_applied, ...}
   - Critical signal → cap at 59
   - Moderate signal → cap at 75

6. Classify tier
   classify_tier(final_score) → Champion|Healthy|At Risk|Critical

7. Load results
   upsert Health_Score__c → Salesforce (or CSV in dry-run)
```

## Scoring Model

```
Health Score = 60% × Churn Risk + 40% × Platform Value Score

Churn Risk = weighted sum of:
  ├── 30% Support Health (Intercom metrics)
  ├── 30% Financial/Contract (Salesforce metrics)
  ├── 25% Adoption/Engagement (Looker metrics)
  └── 15% Relationship/Expansion (Salesforce Phase 2)

Each metric normalised via green/yellow/red thresholds:
  - Segment-specific: "paid" vs "standard"
  - lower_is_better flag controls interpolation direction
  - Missing metrics → None → proportional weight redistribution

Tiers: Champion (90-100) | Healthy (76-89) | At Risk (60-75) | Critical (0-59)
```

## Key Design Decisions

1. **Pure scoring functions** — All scoring in `src/scoring/` has zero I/O. Easy to test, reason about, and reuse.
2. **Lazy extractor init** — Only extractors with valid credentials are instantiated. Missing source = missing dimension = weight redistribution.
3. **None propagation** — Missing data flows through as `None`, not 0. Weight redistribution preserves score integrity.
4. **Segment-aware thresholds** — Paid vs standard accounts scored against different benchmarks.
5. **Qualitative caps** — Business signals can override quantitative scores downward (never upward).

## Error Handling Strategy

- **Extractors:** try/except per API call, log exception, return empty/zero metrics
- **Scoring:** None propagation, weight redistribution for missing dimensions
- **Orchestrator:** Per-account exception handling, continues to next account on failure
- **Lambda:** Top-level try/except with Rollbar reporting
