"""Tests for dimension scoring, composite scoring, and qualitative modifier."""

import pytest

from src.scoring.composite import classify_tier, compute_churn_risk, compute_health_score
from src.scoring.dimensions import score_dimension, score_platform_value
from src.scoring.qualitative import apply_qualitative_modifier


class TestClassifyTier:
    def test_champion(self):
        assert classify_tier(95) == "Champion"
        assert classify_tier(90) == "Champion"
        assert classify_tier(100) == "Champion"

    def test_healthy(self):
        assert classify_tier(76) == "Healthy"
        assert classify_tier(85) == "Healthy"
        assert classify_tier(89) == "Healthy"

    def test_at_risk(self):
        assert classify_tier(60) == "At Risk"
        assert classify_tier(70) == "At Risk"
        assert classify_tier(75) == "At Risk"

    def test_critical(self):
        assert classify_tier(0) == "Critical"
        assert classify_tier(30) == "Critical"
        assert classify_tier(59) == "Critical"


class TestQualitativeModifier:
    def test_no_signals(self):
        result = apply_qualitative_modifier(
            quantitative_score=85, critical_count=0, moderate_count=0,
            watch_count=0, has_critical_confirmed=False,
        )
        assert result["final_score"] == 85
        assert result["override_active"] is False
        assert result["modifier_applied"] is None

    def test_watch_only_no_effect(self):
        result = apply_qualitative_modifier(
            quantitative_score=85, critical_count=0, moderate_count=0,
            watch_count=3, has_critical_confirmed=False,
        )
        assert result["final_score"] == 85
        assert result["override_active"] is False

    def test_one_moderate_caps_at_75(self):
        result = apply_qualitative_modifier(
            quantitative_score=85, critical_count=0, moderate_count=1,
            watch_count=0, has_critical_confirmed=False,
        )
        assert result["final_score"] == 75
        assert result["override_active"] is True
        assert result["cap_value"] == 75

    def test_one_critical_caps_at_65(self):
        result = apply_qualitative_modifier(
            quantitative_score=82, critical_count=1, moderate_count=0,
            watch_count=0, has_critical_confirmed=False,
        )
        assert result["final_score"] == 65
        assert result["override_active"] is True

    def test_two_critical_caps_at_55(self):
        result = apply_qualitative_modifier(
            quantitative_score=78, critical_count=2, moderate_count=0,
            watch_count=0, has_critical_confirmed=False,
        )
        assert result["final_score"] == 55
        assert result["override_active"] is True

    def test_critical_confirmed_sets_to_50(self):
        result = apply_qualitative_modifier(
            quantitative_score=90, critical_count=1, moderate_count=0,
            watch_count=0, has_critical_confirmed=True,
        )
        assert result["final_score"] == 50
        assert result["override_active"] is True

    def test_low_quant_not_raised_by_cap(self):
        """If quantitative score is already below cap, cap doesn't raise it."""
        result = apply_qualitative_modifier(
            quantitative_score=48, critical_count=1, moderate_count=0,
            watch_count=0, has_critical_confirmed=False,
        )
        assert result["final_score"] == 48
        assert result["override_active"] is False

    def test_deep_critical_on_champion(self):
        """Confirmed critical should force Champion (90) down to 50."""
        result = apply_qualitative_modifier(
            quantitative_score=90, critical_count=1, moderate_count=0,
            watch_count=0, has_critical_confirmed=True,
        )
        assert result["final_score"] == 50
        assert result["cap_value"] == 50

    def test_moderate_on_at_risk_no_change(self):
        """Score at 70 (At Risk), moderate cap is 75 — no change."""
        result = apply_qualitative_modifier(
            quantitative_score=70, critical_count=0, moderate_count=1,
            watch_count=0, has_critical_confirmed=False,
        )
        assert result["final_score"] == 70
        assert result["override_active"] is False


class TestComputeChurnRisk:
    def test_all_dimensions_present(self):
        weights = {
            "support_health": 0.30,
            "financial_contract": 0.30,
            "adoption_engagement": 0.25,
            "relationship_expansion": 0.15,
        }
        scores = {
            "support_health": 85,
            "financial_contract": 95,
            "adoption_engagement": 88,
            "relationship_expansion": 92,
        }
        result = compute_churn_risk(scores, weights)
        # (0.30*85) + (0.30*95) + (0.25*88) + (0.15*92) = 25.5 + 28.5 + 22.0 + 13.8 = 89.8
        assert result["score"] == 89.8
        assert result["coverage_pct"] == 100.0
        assert result["missing_dimensions"] == []

    def test_missing_relationship(self):
        """Phase 1: Relationship dimension missing, should reweight."""
        weights = {
            "support_health": 0.30,
            "financial_contract": 0.30,
            "adoption_engagement": 0.25,
            "relationship_expansion": 0.15,
        }
        scores = {
            "support_health": 85,
            "financial_contract": 95,
            "adoption_engagement": 88,
            "relationship_expansion": None,
        }
        result = compute_churn_risk(scores, weights)
        # Available weight: 0.30 + 0.30 + 0.25 = 0.85
        # Reweighted: (85 * 0.30/0.85) + (95 * 0.30/0.85) + (88 * 0.25/0.85)
        # = (85 * 0.3529) + (95 * 0.3529) + (88 * 0.2941)
        # = 30.0 + 33.5 + 25.9 = 89.4
        assert result["score"] is not None
        assert result["missing_dimensions"] == ["relationship_expansion"]
        assert result["coverage_pct"] == 85.0

    def test_all_dimensions_missing(self):
        weights = {
            "support_health": 0.30,
            "financial_contract": 0.30,
            "adoption_engagement": 0.25,
            "relationship_expansion": 0.15,
        }
        scores = {
            "support_health": None,
            "financial_contract": None,
            "adoption_engagement": None,
            "relationship_expansion": None,
        }
        result = compute_churn_risk(scores, weights)
        assert result["score"] is None
        assert result["coverage_pct"] == 0.0


class TestComputeHealthScore:
    def test_both_components(self):
        result = compute_health_score(
            churn_risk_score=89.8,
            platform_value_score=82,
            churn_risk_weight=0.60,
            platform_value_weight=0.40,
        )
        # (0.60 * 89.8) + (0.40 * 82) = 53.88 + 32.8 = 86.68 → 86.7
        assert result["quantitative_score"] == 86.7
        assert result["tier"] == "Healthy"

    def test_missing_pvs(self):
        """If Platform Value is missing, Churn Risk is 100% of score."""
        result = compute_health_score(
            churn_risk_score=89.8,
            platform_value_score=None,
            churn_risk_weight=0.60,
            platform_value_weight=0.40,
        )
        assert result["quantitative_score"] == 89.8
        assert result["coverage_pct"] == 60.0

    def test_both_missing(self):
        result = compute_health_score(
            churn_risk_score=None,
            platform_value_score=None,
            churn_risk_weight=0.60,
            platform_value_weight=0.40,
        )
        assert result["quantitative_score"] is None


class TestScorePlatformValue:
    def test_all_pillars(self):
        pillar_scores = {
            "messaging": 80,
            "automations": 70,
            "contactless": 90,
            "requests": 60,
            "staff_adoption": 50,
        }
        pillar_weights = {
            "messaging": 0.50,
            "automations": 0.20,
            "contactless": 0.20,
            "requests": 0.05,
            "staff_adoption": 0.05,
        }
        result = score_platform_value(pillar_scores, pillar_weights)
        # (80*0.50) + (70*0.20) + (90*0.20) + (60*0.05) + (50*0.05)
        # = 40 + 14 + 18 + 3 + 2.5 = 77.5
        assert result["score"] == 77.5
        assert result["coverage"] == 1.0

    def test_partial_pillars(self):
        pillar_scores = {
            "messaging": 80,
            "automations": None,
            "contactless": 90,
            "requests": None,
            "staff_adoption": None,
        }
        pillar_weights = {
            "messaging": 0.50,
            "automations": 0.20,
            "contactless": 0.20,
            "requests": 0.05,
            "staff_adoption": 0.05,
        }
        result = score_platform_value(pillar_scores, pillar_weights)
        # Available weight: 0.50 + 0.20 = 0.70
        # (80*0.50 + 90*0.20) / 0.70 = (40 + 18) / 0.70 = 82.9
        assert result["score"] == 82.9
        assert result["coverage"] == 0.4
