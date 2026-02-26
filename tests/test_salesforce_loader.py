from __future__ import annotations

"""Tests for Salesforce loader (Health_Score__c writer + dry-run CSV)."""

import csv
from unittest.mock import MagicMock, patch

import pytest

from src.loaders.salesforce import SalesforceLoader, write_dry_run_csv


@pytest.fixture
def loader():
    """SalesforceLoader with mocked simple_salesforce.Salesforce client."""
    with patch("src.loaders.salesforce.Salesforce") as MockSF:
        mock_sf = MagicMock()
        MockSF.return_value = mock_sf
        ldr = SalesforceLoader(
            username="user@example.com",
            password="pass",
            security_token="tok",
        )
    return ldr


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_scoring_result(
    *,
    churn_risk_score: float = 72.5,
    quantitative_score: float = 75.0,
    final_score: float = 70.0,
    tier: str = "At Risk",
    support_score: float = 65.0,
    financial_score: float = 80.0,
    adoption_score: float = 70.0,
    relationship_score: float | None = None,
    pvs_score: float = 68.0,
    coverage_pct: float = 85.0,
) -> dict:
    return {
        "dimension_scores": {
            "support_health": {
                "score": support_score,
                "metric_scores": {
                    "p1_p2_volume": 80,
                    "first_response_minutes": 60,
                    "close_time_hours": 70,
                    "reopen_rate_pct": 55,
                    "escalation_rate_pct": 65,
                },
                "coverage": 1.0,
            },
            "financial_contract": {
                "score": financial_score,
                "metric_scores": {
                    "days_to_renewal": 90,
                    "payment_health": 100,
                    "contract_changes": 100,
                    "arr_trajectory_pct": 50,
                    "tier_alignment": 100,
                },
                "coverage": 1.0,
            },
            "adoption_engagement": {
                "score": adoption_score,
                "metric_scores": {
                    "staff_login_trend": 75,
                    "admin_login_trend": 60,
                    "feature_breadth_pct": 80,
                    "platform_score": 65,
                    "platform_score_trend": 70,
                },
                "coverage": 1.0,
            },
            "relationship_expansion": {
                "score": relationship_score,
                "metric_scores": {},
                "coverage": 0.0,
            },
        },
        "platform_value": {
            "score": pvs_score,
            "pillar_scores": {
                "messaging": 80,
                "automations": 60,
                "contactless": 55,
                "requests": 40,
                "staff_adoption": 75,
            },
            "coverage": 1.0,
        },
        "composite": {
            "churn_risk_score": churn_risk_score,
            "quantitative_score": quantitative_score,
            "tier": tier,
            "churn_risk_detail": {},
            "health_detail": {},
        },
        "qualitative": {
            "final_score": final_score,
            "modifier_applied": -5.0,
            "cap_value": 75,
            "override_active": True,
            "critical_count": 1,
            "moderate_count": 0,
            "watch_count": 2,
        },
        "coverage_pct": coverage_pct,
    }


def _make_result_for_csv(
    account_id: str = "001ABC",
    account_name: str = "Acme",
    segment: str = "paid",
) -> dict:
    result = _make_scoring_result()
    result["account_id"] = account_id
    result["account_name"] = account_name
    result["segment"] = segment
    return result


# ---------------------------------------------------------------------------
# TestBuildRecord
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_result_maps_all_fields(self, loader):
        result = _make_scoring_result()
        record = loader._build_record("001ABC", result, "2025-02")

        assert record["Account__c"] == "001ABC"
        assert record["Scoring_Period__c"] == "2025-02"
        assert record["Churn_Risk_Score__c"] == 72.5
        assert record["Final_Score__c"] == 70.0
        assert record["Health_Tier__c"] == "At Risk"
        assert record["Support_P1P2_Volume__c"] == 80
        assert record["PVS_Messaging__c"] == 80
        assert record["Qual_Active_Critical__c"] == 1
        assert record["Qual_Override_Active__c"] is True

    def test_none_values_stripped(self, loader):
        result = _make_scoring_result(relationship_score=None)
        record = loader._build_record("001ABC", result, "2025-02")

        assert "Relationship_Expansion_Score__c" not in record

    def test_account_and_period_set(self, loader):
        result = _make_scoring_result()
        record = loader._build_record("001XYZ", result, "2024-12")

        assert record["Account__c"] == "001XYZ"
        assert record["Scoring_Period__c"] == "2024-12"

    def test_scoring_date_format(self, loader):
        result = _make_scoring_result()
        record = loader._build_record("001ABC", result, "2025-02")

        # Should be YYYY-MM-DD format
        date_str = record["Scoring_Date__c"]
        assert len(date_str) == 10
        assert date_str[4] == "-"
        assert date_str[7] == "-"

    def test_coverage_included(self, loader):
        result = _make_scoring_result(coverage_pct=92.5)
        record = loader._build_record("001ABC", result, "2025-02")

        assert record["Scoring_Coverage__c"] == 92.5

    def test_modifier_applied_included(self, loader):
        result = _make_scoring_result()
        record = loader._build_record("001ABC", result, "2025-02")

        assert record["Qual_Score_Modifier__c"] == -5.0


# ---------------------------------------------------------------------------
# TestWriteHealthScore
# ---------------------------------------------------------------------------

class TestWriteHealthScore:
    def test_creates_record_in_sf(self, loader):
        loader.sf.Health_Score__c.create.return_value = {"id": "a0XABC123", "success": True}

        result = _make_scoring_result()
        record_id = loader.write_health_score("001ABC", result, "2025-02")

        loader.sf.Health_Score__c.create.assert_called_once()
        assert record_id == "a0XABC123"

    def test_returns_record_id(self, loader):
        loader.sf.Health_Score__c.create.return_value = {"id": "a0X999", "success": True}

        result = _make_scoring_result()
        record_id = loader.write_health_score("001ABC", result, "2025-02")

        assert record_id == "a0X999"

    def test_exception_propagates(self, loader):
        loader.sf.Health_Score__c.create.side_effect = Exception("SF API error")

        with pytest.raises(Exception, match="SF API error"):
            loader.write_health_score("001ABC", _make_scoring_result(), "2025-02")


# ---------------------------------------------------------------------------
# TestWriteDryRunCsv
# ---------------------------------------------------------------------------

class TestWriteDryRunCsv:
    def test_empty_results(self, tmp_path):
        output = str(tmp_path / "empty.csv")
        path = write_dry_run_csv([], output_path=output)

        assert path == output
        # File should NOT be created for empty results
        import os
        assert not os.path.exists(output)

    def test_single_result(self, tmp_path):
        output = str(tmp_path / "scores.csv")
        results = [_make_result_for_csv()]

        path = write_dry_run_csv(results, output_path=output)

        assert path == output
        with open(output) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 1
        assert rows[0]["account_id"] == "001ABC"
        assert rows[0]["account_name"] == "Acme"

    def test_multiple_results(self, tmp_path):
        output = str(tmp_path / "scores.csv")
        results = [
            _make_result_for_csv(account_id="001", account_name="Acme"),
            _make_result_for_csv(account_id="002", account_name="Beta"),
            _make_result_for_csv(account_id="003", account_name="Gamma"),
        ]

        write_dry_run_csv(results, output_path=output)

        with open(output) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 3

    def test_correct_fieldnames(self, tmp_path):
        output = str(tmp_path / "scores.csv")
        results = [_make_result_for_csv()]

        write_dry_run_csv(results, output_path=output)

        with open(output) as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames

        expected_fields = [
            "account_id", "account_name", "segment",
            "quantitative_score", "final_score", "health_tier",
            "churn_risk_score", "platform_value_score",
            "support_health", "financial_contract",
            "adoption_engagement", "relationship_expansion",
            "coverage_pct", "qualitative_override", "modifier_applied",
        ]
        assert fieldnames == expected_fields

    def test_nested_values_flattened(self, tmp_path):
        output = str(tmp_path / "scores.csv")
        results = [_make_result_for_csv()]

        write_dry_run_csv(results, output_path=output)

        with open(output) as f:
            reader = csv.DictReader(f)
            row = next(reader)

        # These are extracted from nested dicts
        assert row["churn_risk_score"] == "72.5"
        assert row["final_score"] == "70.0"
        assert row["health_tier"] == "At Risk"
        assert row["platform_value_score"] == "68.0"

    def test_creates_parent_directory(self, tmp_path):
        output = str(tmp_path / "sub" / "dir" / "scores.csv")
        results = [_make_result_for_csv()]

        write_dry_run_csv(results, output_path=output)

        import os
        assert os.path.exists(output)
