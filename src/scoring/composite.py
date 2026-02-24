from __future__ import annotations

"""Composite scoring: combine dimension scores into Churn Risk and Health Score.

Health Score = (60% × Churn Risk Score) + (40% × Platform Value Score)
Churn Risk = (30% × Support) + (30% × Financial) + (25% × Adoption) + (15% × Relationship)

Handles missing dimensions via reweighting + coverage flag.
"""

import logging

logger = logging.getLogger(__name__)

# Health Score Tiers
TIERS = [
    ("Champion", 90, 100),
    ("Healthy", 76, 89),
    ("At Risk", 60, 75),
    ("Critical", 0, 59),
]


def classify_tier(score: float) -> str:
    """Map a 0-100 score to a Health Score tier."""
    for tier_name, tier_min, tier_max in TIERS:
        if tier_min <= score <= tier_max:
            return tier_name
    return "Critical" if score < 0 else "Champion"


def compute_churn_risk(
    dimension_scores: dict[str, float | None],
    dimension_weights: dict[str, float],
) -> dict:
    """Compute the Churn Risk Score from dimension scores.

    Missing dimensions are handled by proportional reweighting.

    Args:
        dimension_scores: Score per dimension (0-100 or None if missing).
            Keys: support_health, financial_contract, adoption_engagement, relationship_expansion
        dimension_weights: Weight per dimension from config.

    Returns:
        dict with:
            score: Reweighted Churn Risk Score (0-100).
            available_dimensions: List of dimension names that had data.
            missing_dimensions: List of dimension names without data.
            coverage_pct: Percentage of total weight covered by available data.
    """
    available = {}
    missing = []
    total_available_weight = 0.0

    for dim_name, weight in dimension_weights.items():
        score = dimension_scores.get(dim_name)
        if score is not None:
            available[dim_name] = {"score": score, "weight": weight}
            total_available_weight += weight
        else:
            missing.append(dim_name)

    if not available:
        return {
            "score": None,
            "available_dimensions": [],
            "missing_dimensions": list(dimension_weights.keys()),
            "coverage_pct": 0.0,
        }

    # Reweight: scale available dimension weights to sum to 1.0
    weighted_sum = 0.0
    for dim in available.values():
        reweighted = dim["weight"] / total_available_weight
        weighted_sum += dim["score"] * reweighted

    original_total_weight = sum(dimension_weights.values())
    coverage_pct = round(
        (total_available_weight / original_total_weight) * 100, 1
    )

    return {
        "score": round(weighted_sum, 1),
        "available_dimensions": list(available.keys()),
        "missing_dimensions": missing,
        "coverage_pct": coverage_pct,
    }


def compute_health_score(
    churn_risk_score: float | None,
    platform_value_score: float | None,
    churn_risk_weight: float,
    platform_value_weight: float,
) -> dict:
    """Compute the overall Health Score.

    Health Score = (churn_risk_weight × Churn Risk) + (platform_value_weight × PVS)

    If one component is missing, the other is used at 100% weight.

    Args:
        churn_risk_score: Churn Risk Score (0-100) or None.
        platform_value_score: Platform Value Score (0-100) or None.
        churn_risk_weight: Weight for Churn Risk (default 0.60).
        platform_value_weight: Weight for Platform Value (default 0.40).

    Returns:
        dict with:
            quantitative_score: The raw quantitative health score.
            tier: Health Score tier classification.
            components: Breakdown of contributing scores.
            coverage_pct: Weight coverage percentage.
    """
    components = {}
    total_weight = 0.0
    weighted_sum = 0.0

    if churn_risk_score is not None:
        components["churn_risk"] = {
            "score": churn_risk_score,
            "weight": churn_risk_weight,
        }
        weighted_sum += churn_risk_score * churn_risk_weight
        total_weight += churn_risk_weight

    if platform_value_score is not None:
        components["platform_value"] = {
            "score": platform_value_score,
            "weight": platform_value_weight,
        }
        weighted_sum += platform_value_score * platform_value_weight
        total_weight += platform_value_weight

    if total_weight == 0:
        return {
            "quantitative_score": None,
            "tier": None,
            "components": components,
            "coverage_pct": 0.0,
        }

    # Reweight if one component is missing
    score = round(weighted_sum / total_weight, 1)
    coverage_pct = round((total_weight / (churn_risk_weight + platform_value_weight)) * 100, 1)

    return {
        "quantitative_score": score,
        "tier": classify_tier(score),
        "components": components,
        "coverage_pct": coverage_pct,
    }
