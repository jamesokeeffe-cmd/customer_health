from __future__ import annotations

"""Dimension-level scoring: normalise each metric then compute weighted dimension score.

Handles missing metrics by redistributing weights proportionally across available metrics.
"""

import logging

from src.scoring.normaliser import normalise_metric

logger = logging.getLogger(__name__)


def score_dimension(
    raw_metrics: dict[str, float | None],
    metric_weights: dict[str, float],
    thresholds: dict[str, dict],
    segment: str,
) -> dict:
    """Score a single dimension (e.g. Support Health) from raw metric values.

    Args:
        raw_metrics: Raw metric values keyed by metric name. None values = missing.
        metric_weights: Weight per metric within this dimension (should sum to 1.0).
        thresholds: Threshold config for each metric (from thresholds.yaml).
        segment: 'paid' or 'standard' — determines which threshold set to use.

    Returns:
        dict with:
            score: Weighted dimension score (0-100), or None if no metrics available.
            metric_scores: Dict of each metric's normalised score.
            coverage: Fraction of metrics that had data (0.0-1.0).
            available_weight: Sum of weights for metrics that had data.
    """
    segment_key = segment.lower()
    metric_scores = {}
    available_weight = 0.0
    weighted_sum = 0.0

    for metric_name, weight in metric_weights.items():
        raw_value = raw_metrics.get(metric_name)
        threshold_config = thresholds.get(metric_name, {})

        if not threshold_config:
            logger.warning("No threshold config for metric: %s", metric_name)
            metric_scores[metric_name] = None
            continue

        segment_thresholds = threshold_config.get(segment_key)
        if not segment_thresholds:
            logger.warning(
                "No %s thresholds for metric: %s", segment_key, metric_name
            )
            metric_scores[metric_name] = None
            continue

        lower_is_better = threshold_config.get("lower_is_better", False)

        normalised = normalise_metric(
            raw_value=raw_value,
            green=segment_thresholds["green"],
            yellow=segment_thresholds["yellow"],
            red=segment_thresholds["red"],
            lower_is_better=lower_is_better,
        )

        metric_scores[metric_name] = normalised
        if normalised is not None:
            available_weight += weight
            weighted_sum += normalised * weight

    # Reweight: if some metrics are missing, scale up available metrics proportionally
    total_metrics = len(metric_weights)
    available_metrics = sum(1 for s in metric_scores.values() if s is not None)
    coverage = available_metrics / total_metrics if total_metrics > 0 else 0.0

    if available_weight > 0:
        dimension_score = round(weighted_sum / available_weight, 1)
    else:
        dimension_score = None

    return {
        "score": dimension_score,
        "metric_scores": metric_scores,
        "coverage": round(coverage, 2),
        "available_weight": round(available_weight, 3),
    }


def score_platform_value(
    pillar_scores: dict[str, float | None],
    pillar_weights: dict[str, float],
) -> dict:
    """Score the Platform Value Score from AXP sub-pillar scores.

    The AXP Platform Score sub-pillars are already 0-100. No normalisation needed.

    Args:
        pillar_scores: Score per pillar (messaging, automations, etc.). None = missing.
        pillar_weights: Weight per pillar.

    Returns:
        dict with score, coverage, pillar_scores.
    """
    available_weight = 0.0
    weighted_sum = 0.0

    for pillar, weight in pillar_weights.items():
        score = pillar_scores.get(pillar)
        if score is not None:
            available_weight += weight
            weighted_sum += score * weight

    total = len(pillar_weights)
    available = sum(1 for s in pillar_scores.values() if s is not None)
    coverage = available / total if total > 0 else 0.0

    if available_weight > 0:
        pvs = round(weighted_sum / available_weight, 1)
    else:
        pvs = None

    return {
        "score": pvs,
        "coverage": round(coverage, 2),
        "pillar_scores": pillar_scores,
    }
