"""Integration tests using worked examples from the Unified Proposal.

These tests validate the full scoring pipeline against the three examples
documented in the Customer Health Score Unified Proposal:
  A: Four Seasons ($477k ARR, Paid Success) — No qualitative signals
  B: Four Seasons — Same data + CSM hears about internal build (Confirmed Critical)
  C: Boutique Hotel Group ($85k ARR, Standard Success) — Data-driven risk
"""

import pytest

from src.scoring.composite import classify_tier, compute_churn_risk, compute_health_score
from src.scoring.qualitative import apply_qualitative_modifier


class TestExampleA:
    """Four Seasons: $477k ARR, Paid Success, no qualitative signals.

    From the Unified Proposal:
      Support Health: 85
      Financial & Contract: 95
      Adoption & Engagement: 88
      Relationship & Expansion: 92
      Churn Risk: (0.30×85) + (0.30×95) + (0.25×88) + (0.15×92) = 89.8
      Platform Value Score: 82
      Health Score: (0.60×89.8) + (0.40×82) = 86.7
      No qualitative signals → Final Score: 86.7 → Healthy
    """

    def setup_method(self):
        self.dimension_weights = {
            "support_health": 0.30,
            "financial_contract": 0.30,
            "adoption_engagement": 0.25,
            "relationship_expansion": 0.15,
        }
        self.dimension_scores = {
            "support_health": 85,
            "financial_contract": 95,
            "adoption_engagement": 88,
            "relationship_expansion": 92,
        }
        self.platform_value = 82

    def test_churn_risk_score(self):
        result = compute_churn_risk(self.dimension_scores, self.dimension_weights)
        assert result["score"] == 89.8

    def test_health_score(self):
        result = compute_health_score(
            churn_risk_score=89.8,
            platform_value_score=82,
            churn_risk_weight=0.60,
            platform_value_weight=0.40,
        )
        assert result["quantitative_score"] == 86.7

    def test_no_qualitative_modifier(self):
        result = apply_qualitative_modifier(
            quantitative_score=86.7,
            critical_count=0,
            moderate_count=0,
            watch_count=0,
            has_critical_confirmed=False,
        )
        assert result["final_score"] == 86.7
        assert result["override_active"] is False

    def test_final_tier(self):
        assert classify_tier(86.7) == "Healthy"

    def test_full_pipeline(self):
        """End-to-end: dimension scores → churn risk → health score → qualifier → tier."""
        churn_risk = compute_churn_risk(self.dimension_scores, self.dimension_weights)
        health = compute_health_score(
            churn_risk_score=churn_risk["score"],
            platform_value_score=self.platform_value,
            churn_risk_weight=0.60,
            platform_value_weight=0.40,
        )
        qual = apply_qualitative_modifier(
            quantitative_score=health["quantitative_score"],
            critical_count=0,
            moderate_count=0,
            watch_count=0,
            has_critical_confirmed=False,
        )
        assert qual["final_score"] == 86.7
        assert classify_tier(qual["final_score"]) == "Healthy"


class TestExampleB:
    """Four Seasons: Same quantitative data, but CSM hears about internal build.

    From the Unified Proposal:
      Quantitative Health Score: 86.7 (Healthy)
      Qualitative Signal: Critical + Confirmed (internal build)
      Modifier: Score set to 50
      Final Score: 50 → Critical
    """

    def test_confirmed_critical_overrides(self):
        result = apply_qualitative_modifier(
            quantitative_score=86.7,
            critical_count=1,
            moderate_count=0,
            watch_count=0,
            has_critical_confirmed=True,
        )
        assert result["final_score"] == 50
        assert result["override_active"] is True
        assert result["cap_value"] == 50

    def test_final_tier(self):
        assert classify_tier(50) == "Critical"

    def test_full_pipeline(self):
        """Same as Example A but with Critical + Confirmed signal."""
        dimension_weights = {
            "support_health": 0.30,
            "financial_contract": 0.30,
            "adoption_engagement": 0.25,
            "relationship_expansion": 0.15,
        }
        dimension_scores = {
            "support_health": 85,
            "financial_contract": 95,
            "adoption_engagement": 88,
            "relationship_expansion": 92,
        }

        churn_risk = compute_churn_risk(dimension_scores, dimension_weights)
        health = compute_health_score(
            churn_risk_score=churn_risk["score"],
            platform_value_score=82,
            churn_risk_weight=0.60,
            platform_value_weight=0.40,
        )
        qual = apply_qualitative_modifier(
            quantitative_score=health["quantitative_score"],
            critical_count=1,
            moderate_count=0,
            watch_count=0,
            has_critical_confirmed=True,
        )
        assert qual["final_score"] == 50
        assert classify_tier(qual["final_score"]) == "Critical"


class TestExampleC:
    """Boutique Hotel Group: $85k ARR, Standard Success, data-driven risk.

    From the Unified Proposal:
      Support Health: 72
      Financial & Contract: 75
      Adoption & Engagement: 55
      Relationship & Expansion: 50
      Churn Risk: (0.30×72) + (0.30×75) + (0.25×55) + (0.15×50) = 65.35
      Platform Value: 58
      Health Score: (0.60×65.35) + (0.40×58) = 62.4 (rounded)
      No qualitative signals → Final Score: 62.4 → At Risk
    """

    def setup_method(self):
        self.dimension_weights = {
            "support_health": 0.30,
            "financial_contract": 0.30,
            "adoption_engagement": 0.25,
            "relationship_expansion": 0.15,
        }
        self.dimension_scores = {
            "support_health": 72,
            "financial_contract": 75,
            "adoption_engagement": 55,
            "relationship_expansion": 50,
        }
        self.platform_value = 58

    def test_churn_risk_score(self):
        result = compute_churn_risk(self.dimension_scores, self.dimension_weights)
        # (0.30*72) + (0.30*75) + (0.25*55) + (0.15*50)
        # = 21.6 + 22.5 + 13.75 + 7.5 = 65.35
        assert result["score"] == 65.4  # rounded to 1 decimal

    def test_health_score(self):
        result = compute_health_score(
            churn_risk_score=65.4,  # as computed
            platform_value_score=58,
            churn_risk_weight=0.60,
            platform_value_weight=0.40,
        )
        # (0.60*65.4) + (0.40*58) = 39.24 + 23.2 = 62.44 → 62.4
        assert result["quantitative_score"] == 62.4

    def test_no_qualitative_modifier(self):
        result = apply_qualitative_modifier(
            quantitative_score=62.4,
            critical_count=0,
            moderate_count=0,
            watch_count=0,
            has_critical_confirmed=False,
        )
        assert result["final_score"] == 62.4
        assert result["override_active"] is False

    def test_final_tier(self):
        assert classify_tier(62.4) == "At Risk"

    def test_full_pipeline(self):
        churn_risk = compute_churn_risk(self.dimension_scores, self.dimension_weights)
        health = compute_health_score(
            churn_risk_score=churn_risk["score"],
            platform_value_score=self.platform_value,
            churn_risk_weight=0.60,
            platform_value_weight=0.40,
        )
        qual = apply_qualitative_modifier(
            quantitative_score=health["quantitative_score"],
            critical_count=0,
            moderate_count=0,
            watch_count=0,
            has_critical_confirmed=False,
        )
        assert classify_tier(qual["final_score"]) == "At Risk"


class TestMissingDimensionReweighting:
    """Test Phase 1 scenario: Relationship dimension missing."""

    def test_reweight_without_relationship(self):
        """When Relationship is missing, other three dimensions are reweighted."""
        dimension_weights = {
            "support_health": 0.30,
            "financial_contract": 0.30,
            "adoption_engagement": 0.25,
            "relationship_expansion": 0.15,
        }
        dimension_scores = {
            "support_health": 85,
            "financial_contract": 95,
            "adoption_engagement": 88,
            "relationship_expansion": None,  # Phase 2 — not available yet
        }
        result = compute_churn_risk(dimension_scores, dimension_weights)

        # Available weight: 0.85
        # (85 * 0.30/0.85) + (95 * 0.30/0.85) + (88 * 0.25/0.85)
        # = (85 * 0.3529) + (95 * 0.3529) + (88 * 0.2941)
        # = 30.0 + 33.5 + 25.9 = 89.4
        assert result["score"] is not None
        assert abs(result["score"] - 89.4) < 0.5  # Allow rounding tolerance
        assert result["coverage_pct"] == 85.0
        assert "relationship_expansion" in result["missing_dimensions"]


class TestQualitativeExamplesFromDoc:
    """Worked examples from the Qualitative Churn Signals document Section 4."""

    def test_no_signals_score_82(self):
        result = apply_qualitative_modifier(82, 0, 0, 0, False)
        assert result["final_score"] == 82

    def test_watch_only_score_82(self):
        result = apply_qualitative_modifier(82, 0, 0, 1, False)
        assert result["final_score"] == 82

    def test_moderate_caps_82_to_75(self):
        result = apply_qualitative_modifier(82, 0, 1, 0, False)
        assert result["final_score"] == 75

    def test_critical_suspected_caps_82_to_65(self):
        result = apply_qualitative_modifier(82, 1, 0, 0, False)
        assert result["final_score"] == 65

    def test_two_critical_caps_78_to_55(self):
        result = apply_qualitative_modifier(78, 2, 0, 0, False)
        assert result["final_score"] == 55

    def test_confirmed_critical_sets_90_to_50(self):
        result = apply_qualitative_modifier(90, 1, 0, 0, True)
        assert result["final_score"] == 50

    def test_low_quant_48_not_raised(self):
        result = apply_qualitative_modifier(48, 1, 0, 0, False)
        assert result["final_score"] == 48
