from __future__ import annotations

"""Tests for HealthScoreOrchestrator (init, score_account, run)."""

import csv
import logging
import os
from unittest.mock import MagicMock, patch

import pytest

from src.main import HealthScoreOrchestrator, load_account_mapping, validate_config


@pytest.fixture
def orchestrator():
    """Orchestrator with real config files and empty account mapping."""
    orch = HealthScoreOrchestrator(config_dir="config", dry_run=False)
    orch.account_mapping = []
    return orch


@pytest.fixture
def orchestrator_dry_run():
    """Orchestrator in dry-run mode."""
    orch = HealthScoreOrchestrator(config_dir="config", dry_run=True)
    orch.account_mapping = []
    return orch


def _make_account(
    sf_id: str = "001ABC",
    intercom_id: str = "ic-1",
    looker_id: str = "lk-1",
    name: str = "Acme Corp",
    segment: str = "paid",
    jira_project_key: str = "",
    jira_component: str = "",
) -> dict:
    return {
        "sf_account_id": sf_id,
        "intercom_company_id": intercom_id,
        "looker_customer_id": looker_id,
        "account_name": name,
        "segment": segment,
        "jira_project_key": jira_project_key,
        "jira_component": jira_component,
    }


def _mock_extractor_results():
    """Return typical extractor mock results."""
    return {
        "support": {
            "p1_p2_volume": 3,
            "first_response_minutes": 45,
            "close_time_hours": 8,
            "reopen_rate_pct": 5,
            "escalation_rate_pct": 8,
        },
        "financial": {
            "days_to_renewal": 120,
            "payment_health": 0,
            "contract_changes": 0,
            "arr_trajectory_pct": 5.0,
            "tier_alignment": 0,
        },
        "adoption": {
            "page_visits_per_arrival": 10.0,
            "page_visits_per_arrival_trend": 5.0,
            "feature_breadth_pct": 65.0,
            "platform_score": 72,
            "platform_score_trend": 3.0,
        },
        "relationship": {
            "qbr_attendance_pct": 75.0,
            "responsiveness": None,
            "champion_stability": 200,
            "exec_engagement": None,
            "expansion_signals": 2,
        },
        "pvs": {
            "positive_sentiment_pct": 15.0,
            "response_before_target_pct": 80.0,
            "allin_conversation_pct": 22.0,
            "conversations_per_booking_pct": 25.0,
            "arrival_ciol_pct": 18.0,
            "digital_key_pct": 10.0,
            "automation_active": 1,
            "itinerary_booking_pct": 6.0,
            "page_visits_per_arrival": 90.0,
        },
        "qualitative": {
            "critical_count": 0,
            "moderate_count": 1,
            "watch_count": 0,
            "has_critical_confirmed": False,
            "signals": [],
        },
    }


# ---------------------------------------------------------------------------
# TestInitClientsFromEnv
# ---------------------------------------------------------------------------

class TestInitClientsFromEnv:
    def test_all_creds_present(self, orchestrator):
        env = {
            "INTERCOM_API_TOKEN": "ic-tok",
            "JIRA_BASE_URL": "https://jira.example.com",
            "JIRA_EMAIL": "user@example.com",
            "JIRA_API_TOKEN": "jira-tok",
            "LOOKER_BASE_URL": "https://looker.example.com",
            "LOOKER_CLIENT_ID": "lcid",
            "LOOKER_CLIENT_SECRET": "lsec",
            "SF_USERNAME": "sf-user",
            "SF_PASSWORD": "sf-pass",
            "SF_SECURITY_TOKEN": "sf-tok",
        }
        with patch.dict(os.environ, env, clear=False), \
             patch("src.main.IntercomExtractor"), \
             patch("src.main.JiraExtractor"), \
             patch("src.main.LookerExtractor"), \
             patch("src.main.SalesforceExtractor"), \
             patch("src.main.SalesforceLoader"):
            orchestrator.init_clients_from_env()

        assert orchestrator.intercom is not None
        assert orchestrator.jira is not None
        assert orchestrator.looker is not None
        assert orchestrator.sf_extractor is not None
        assert orchestrator.sf_loader is not None

    def test_no_creds(self, orchestrator):
        env = {}
        with patch.dict(os.environ, env, clear=True):
            orchestrator.init_clients_from_env()

        assert orchestrator.intercom is None
        assert orchestrator.jira is None
        assert orchestrator.looker is None
        assert orchestrator.sf_extractor is None
        assert orchestrator.sf_loader is None

    def test_intercom_only(self, orchestrator):
        env = {"INTERCOM_API_TOKEN": "ic-tok"}
        with patch.dict(os.environ, env, clear=True), \
             patch("src.main.IntercomExtractor"):
            orchestrator.init_clients_from_env()

        assert orchestrator.intercom is not None
        assert orchestrator.jira is None
        assert orchestrator.looker is None
        assert orchestrator.sf_extractor is None

    def test_sf_only(self, orchestrator):
        env = {
            "SF_USERNAME": "u",
            "SF_PASSWORD": "p",
            "SF_SECURITY_TOKEN": "t",
        }
        with patch.dict(os.environ, env, clear=True), \
             patch("src.main.SalesforceExtractor"), \
             patch("src.main.SalesforceLoader"):
            orchestrator.init_clients_from_env()

        assert orchestrator.sf_extractor is not None
        assert orchestrator.sf_loader is not None
        assert orchestrator.intercom is None

    def test_dry_run_skips_loader(self, orchestrator_dry_run):
        env = {
            "SF_USERNAME": "u",
            "SF_PASSWORD": "p",
            "SF_SECURITY_TOKEN": "t",
        }
        with patch.dict(os.environ, env, clear=True), \
             patch("src.main.SalesforceExtractor"), \
             patch("src.main.SalesforceLoader"):
            orchestrator_dry_run.init_clients_from_env()

        assert orchestrator_dry_run.sf_extractor is not None
        assert orchestrator_dry_run.sf_loader is None

    def test_jira_needs_all_three_vars(self, orchestrator):
        """Jira requires base_url, email, AND api_token."""
        # Only 2 of 3 provided
        env = {
            "JIRA_BASE_URL": "https://jira.example.com",
            "JIRA_EMAIL": "user@example.com",
        }
        with patch.dict(os.environ, env, clear=True):
            orchestrator.init_clients_from_env()

        assert orchestrator.jira is None

    def test_partial_jira_missing_email(self, orchestrator):
        env = {
            "JIRA_BASE_URL": "https://jira.example.com",
            "JIRA_API_TOKEN": "tok",
        }
        with patch.dict(os.environ, env, clear=True):
            orchestrator.init_clients_from_env()

        assert orchestrator.jira is None


# ---------------------------------------------------------------------------
# TestScoreAccount
# ---------------------------------------------------------------------------

class TestScoreAccount:
    def test_happy_path(self, orchestrator):
        data = _mock_extractor_results()
        orchestrator.intercom = MagicMock()
        orchestrator.intercom.extract_support_metrics.return_value = data["support"]
        orchestrator.looker = MagicMock()
        orchestrator.looker.extract_adoption_metrics.return_value = data["adoption"]
        orchestrator.looker.extract_platform_value_score.return_value = data["pvs"]
        orchestrator.sf_extractor = MagicMock()
        orchestrator.sf_extractor.extract_financial_metrics.return_value = data["financial"]
        orchestrator.sf_extractor.extract_relationship_metrics.return_value = data["relationship"]
        orchestrator.sf_extractor.extract_qualitative_signals.return_value = data["qualitative"]

        account = _make_account()
        result = orchestrator.score_account(account)

        assert result["account_id"] == "001ABC"
        assert result["account_name"] == "Acme Corp"
        assert result["segment"] == "paid"
        assert "composite" in result
        assert "dimension_scores" in result
        assert result["composite"]["churn_risk_score"] is not None

    def test_intercom_failure_caught(self, orchestrator):
        orchestrator.intercom = MagicMock()
        orchestrator.intercom.extract_support_metrics.side_effect = Exception("API timeout")
        orchestrator.sf_extractor = MagicMock()
        orchestrator.sf_extractor.extract_financial_metrics.return_value = _mock_extractor_results()["financial"]
        orchestrator.sf_extractor.extract_relationship_metrics.return_value = None
        orchestrator.sf_extractor.extract_qualitative_signals.return_value = _mock_extractor_results()["qualitative"]
        orchestrator.looker = None

        account = _make_account()
        # Should not raise
        result = orchestrator.score_account(account)
        assert result["account_id"] == "001ABC"

    def test_looker_failure_caught(self, orchestrator):
        orchestrator.intercom = None
        orchestrator.looker = MagicMock()
        orchestrator.looker.extract_adoption_metrics.side_effect = Exception("SDK error")
        orchestrator.looker.extract_platform_value_score.side_effect = Exception("SDK error")
        orchestrator.sf_extractor = MagicMock()
        orchestrator.sf_extractor.extract_financial_metrics.return_value = _mock_extractor_results()["financial"]
        orchestrator.sf_extractor.extract_relationship_metrics.return_value = None
        orchestrator.sf_extractor.extract_qualitative_signals.return_value = _mock_extractor_results()["qualitative"]

        account = _make_account()
        result = orchestrator.score_account(account)
        assert result is not None

    def test_sf_extractor_failure_caught(self, orchestrator):
        orchestrator.intercom = None
        orchestrator.looker = None
        orchestrator.sf_extractor = MagicMock()
        orchestrator.sf_extractor.extract_financial_metrics.side_effect = Exception("SF down")
        orchestrator.sf_extractor.extract_relationship_metrics.side_effect = Exception("SF down")
        orchestrator.sf_extractor.extract_qualitative_signals.side_effect = Exception("SF down")

        account = _make_account()
        result = orchestrator.score_account(account)
        assert result is not None

    def test_missing_extractors_none(self, orchestrator):
        """When extractors are None, extraction is skipped gracefully."""
        orchestrator.intercom = None
        orchestrator.jira = None
        orchestrator.looker = None
        orchestrator.sf_extractor = None

        account = _make_account()
        result = orchestrator.score_account(account)

        assert result["account_id"] == "001ABC"
        # With no data, scoring still completes (score may be None due to no metrics)
        assert "composite" in result
        assert "dimension_scores" in result

    def test_missing_ids_skip_extraction(self, orchestrator):
        """Empty intercom/looker IDs skip those extractors even if client is present."""
        orchestrator.intercom = MagicMock()
        orchestrator.looker = MagicMock()
        orchestrator.sf_extractor = MagicMock()
        orchestrator.sf_extractor.extract_financial_metrics.return_value = _mock_extractor_results()["financial"]
        orchestrator.sf_extractor.extract_relationship_metrics.return_value = None
        orchestrator.sf_extractor.extract_qualitative_signals.return_value = _mock_extractor_results()["qualitative"]

        account = _make_account(intercom_id="", looker_id="")
        result = orchestrator.score_account(account)

        orchestrator.intercom.extract_support_metrics.assert_not_called()
        orchestrator.looker.extract_adoption_metrics.assert_not_called()
        assert result is not None

    def test_jira_metrics_merged_into_support(self, orchestrator):
        """Jira bug metrics are merged into the support_raw dict."""
        orchestrator.intercom = MagicMock()
        orchestrator.intercom.extract_support_metrics.return_value = {
            "p1_p2_volume": 2,
            "first_response_minutes": 30,
            "close_time_hours": 5,
            "reopen_rate_pct": 3,
            "escalation_rate_pct": 5,
        }
        orchestrator.jira = MagicMock()
        orchestrator.jira.extract_bug_metrics.return_value = {
            "open_bugs_total": 7,
            "open_bugs_p1_p2": 3,
        }
        orchestrator.looker = None
        orchestrator.sf_extractor = MagicMock()
        orchestrator.sf_extractor.extract_financial_metrics.return_value = _mock_extractor_results()["financial"]
        orchestrator.sf_extractor.extract_relationship_metrics.return_value = None
        orchestrator.sf_extractor.extract_qualitative_signals.return_value = _mock_extractor_results()["qualitative"]

        account = _make_account(jira_project_key="ENG", jira_component="Acme")
        result = orchestrator.score_account(account)

        orchestrator.jira.extract_bug_metrics.assert_called_once_with(
            project_key="ENG",
            component_name="Acme",
        )
        # The scoring result should include support dimension
        assert "support_health" in result["dimension_scores"]

    def test_result_metadata(self, orchestrator):
        """Result contains account_id, account_name, segment."""
        orchestrator.intercom = None
        orchestrator.jira = None
        orchestrator.looker = None
        orchestrator.sf_extractor = None

        account = _make_account(sf_id="001XYZ", name="TestCo", segment="Standard")
        result = orchestrator.score_account(account)

        assert result["account_id"] == "001XYZ"
        assert result["account_name"] == "TestCo"
        assert result["segment"] == "standard"

    def test_default_segment(self, orchestrator):
        """Missing segment defaults to 'standard'."""
        orchestrator.intercom = None
        orchestrator.looker = None
        orchestrator.sf_extractor = None

        account = {"sf_account_id": "001", "account_name": "X"}
        result = orchestrator.score_account(account)

        assert result["segment"] == "standard"

    def test_csv_support_metrics_used(self, orchestrator):
        """When CSV support metrics are loaded, API is not called."""
        orchestrator.intercom = MagicMock()
        orchestrator.looker = None
        orchestrator.sf_extractor = None

        # Pre-load CSV metrics
        orchestrator._csv_support_metrics = {
            "acme corp": {
                "p1_p2_volume": 2,
                "first_response_minutes": 15,
                "close_time_hours": 3,
                "reopen_rate_pct": 10,
                "escalation_rate_pct": 5,
            },
        }

        account = _make_account(name="Acme Corp")
        result = orchestrator.score_account(account)

        # API should NOT be called since CSV data is available
        orchestrator.intercom.extract_support_metrics.assert_not_called()
        assert result is not None

    def test_csv_support_metrics_miss_falls_through(self, orchestrator):
        """When CSV is loaded but company not found, result has no support data."""
        orchestrator.intercom = MagicMock()
        orchestrator.looker = None
        orchestrator.sf_extractor = None

        orchestrator._csv_support_metrics = {
            "other corp": {"p1_p2_volume": 5},
        }

        account = _make_account(name="Acme Corp")
        result = orchestrator.score_account(account)

        # API should NOT be called (CSV mode is active, just no match)
        orchestrator.intercom.extract_support_metrics.assert_not_called()
        assert result is not None

    def test_load_intercom_csv(self, orchestrator, tmp_path):
        """load_intercom_csv populates _csv_support_metrics."""
        csv_file = tmp_path / "intercom.csv"
        with open(csv_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "conversation_id", "conversation_created_at",
                "conversation_first_response_at", "conversation_closed_at",
                "conversation_tags", "conversation_state", "message_type",
                "message_author_type", "message_author_companies",
            ])
            writer.writerow([
                "c1", "2026-02-10 12:00:00", "2026-02-10 12:10:00",
                "2026-02-10 14:00:00", "", "closed", "comment",
                "user", "Acme Corp",
            ])

        orchestrator.load_intercom_csv(str(csv_file))

        assert orchestrator._csv_support_metrics is not None
        assert "acme corp" in orchestrator._csv_support_metrics


# ---------------------------------------------------------------------------
# TestRun
# ---------------------------------------------------------------------------

class TestRun:
    def test_scores_all_accounts(self, orchestrator):
        orchestrator.account_mapping = [
            _make_account(sf_id="001", name="A"),
            _make_account(sf_id="002", name="B"),
        ]
        orchestrator.intercom = None
        orchestrator.jira = None
        orchestrator.looker = None
        orchestrator.sf_extractor = None

        summary = orchestrator.run(scoring_period="2025-02")

        assert summary["total_accounts"] == 2
        assert summary["scored_successfully"] == 2
        assert summary["failed"] == 0

    def test_one_fails_gracefully(self, orchestrator):
        orchestrator.account_mapping = [
            _make_account(sf_id="001", name="Good"),
            _make_account(sf_id="002", name="Bad"),
        ]
        orchestrator.intercom = None
        orchestrator.jira = None
        orchestrator.looker = None
        orchestrator.sf_extractor = None

        original_score = orchestrator.score_account

        def side_effect(account):
            if account["sf_account_id"] == "002":
                raise Exception("Scoring failed")
            return original_score(account)

        with patch.object(orchestrator, "score_account", side_effect=side_effect):
            summary = orchestrator.run(scoring_period="2025-02")

        assert summary["scored_successfully"] == 1
        assert summary["failed"] == 1
        assert summary["failures"][0]["account"] == "Bad"

    def test_dry_run_writes_csv(self, orchestrator_dry_run):
        orchestrator_dry_run.account_mapping = [
            _make_account(sf_id="001", name="Acme"),
        ]
        orchestrator_dry_run.intercom = None
        orchestrator_dry_run.jira = None
        orchestrator_dry_run.looker = None
        orchestrator_dry_run.sf_extractor = None

        with patch("src.main.write_dry_run_csv") as mock_csv:
            summary = orchestrator_dry_run.run(scoring_period="2025-02")

        mock_csv.assert_called_once()
        assert summary["dry_run"] is True

    def test_non_dry_run_writes_to_sf(self, orchestrator):
        orchestrator.account_mapping = [
            _make_account(sf_id="001", name="Acme"),
        ]
        orchestrator.intercom = None
        orchestrator.jira = None
        orchestrator.looker = None
        orchestrator.sf_extractor = None
        orchestrator.sf_loader = MagicMock()

        summary = orchestrator.run(scoring_period="2025-02")

        orchestrator.sf_loader.write_health_score.assert_called_once()
        assert summary["dry_run"] is False

    def test_default_scoring_period(self, orchestrator):
        orchestrator.account_mapping = []

        summary = orchestrator.run()

        # Should default to current month YYYY-MM
        assert len(summary["scoring_period"]) == 7
        assert summary["scoring_period"][4] == "-"

    def test_custom_scoring_period(self, orchestrator):
        orchestrator.account_mapping = []

        summary = orchestrator.run(scoring_period="2024-06")

        assert summary["scoring_period"] == "2024-06"

    def test_summary_includes_execution_time(self, orchestrator):
        orchestrator.account_mapping = []

        summary = orchestrator.run(scoring_period="2025-02")

        assert "execution_time_seconds" in summary
        assert isinstance(summary["execution_time_seconds"], float)

    def test_empty_mapping_logs_warning(self, orchestrator, caplog):
        orchestrator.account_mapping = []

        with caplog.at_level(logging.WARNING, logger="health_score"):
            orchestrator.run(scoring_period="2025-02")

        assert any("Account mapping is empty" in msg for msg in caplog.messages)


# ---------------------------------------------------------------------------
# TestLoadAccountMapping
# ---------------------------------------------------------------------------

class TestLoadAccountMapping:
    def test_valid_csv(self, tmp_path):
        csv_file = tmp_path / "mapping.csv"
        csv_file.write_text(
            "sf_account_id,intercom_company_id,looker_customer_id,account_name,segment\n"
            "001ABC000000000,ic-1,lk-1,Acme,paid\n"
        )
        rows = load_account_mapping(str(csv_file))
        assert len(rows) == 1
        assert rows[0]["sf_account_id"] == "001ABC000000000"

    def test_valid_csv_with_extra_columns(self, tmp_path):
        csv_file = tmp_path / "mapping.csv"
        csv_file.write_text(
            "sf_account_id,intercom_company_id,looker_customer_id,account_name,segment,jira_project_key\n"
            "001ABC000000000,ic-1,lk-1,Acme,paid,ENG\n"
        )
        rows = load_account_mapping(str(csv_file))
        assert len(rows) == 1
        assert rows[0]["jira_project_key"] == "ENG"

    def test_headers_only_returns_empty_list(self, tmp_path):
        csv_file = tmp_path / "mapping.csv"
        csv_file.write_text(
            "sf_account_id,intercom_company_id,looker_customer_id,account_name,segment\n"
        )
        rows = load_account_mapping(str(csv_file))
        assert rows == []

    def test_missing_required_column_raises(self, tmp_path):
        csv_file = tmp_path / "mapping.csv"
        csv_file.write_text(
            "sf_account_id,account_name,segment\n"
            "001ABC000000000,Acme,paid\n"
        )
        with pytest.raises(ValueError, match="missing required columns"):
            load_account_mapping(str(csv_file))

    def test_empty_file_raises(self, tmp_path):
        csv_file = tmp_path / "mapping.csv"
        csv_file.write_text("")
        with pytest.raises(ValueError, match="empty or unreadable"):
            load_account_mapping(str(csv_file))

    def test_error_message_lists_missing_columns(self, tmp_path):
        csv_file = tmp_path / "mapping.csv"
        csv_file.write_text("sf_account_id,account_name\n")
        with pytest.raises(ValueError, match="intercom_company_id") as exc_info:
            load_account_mapping(str(csv_file))
        # All three missing columns should be mentioned
        msg = str(exc_info.value)
        assert "looker_customer_id" in msg
        assert "segment" in msg


# ---------------------------------------------------------------------------
# TestValidateConfig
# ---------------------------------------------------------------------------

def _make_threshold(lower_is_better=True, paid=None, standard=None):
    """Build a single metric threshold entry."""
    if paid is None:
        paid = {"green": 0, "yellow": 5, "red": 10} if lower_is_better else {"green": 10, "yellow": 5, "red": 0}
    if standard is None:
        standard = paid.copy()
    return {"lower_is_better": lower_is_better, "paid": paid, "standard": standard}


def _valid_weights():
    """Return a minimal valid weights dict."""
    return {
        "health_score": {"churn_risk_weight": 0.60, "platform_value_weight": 0.40},
        "churn_risk": {
            "support_health": 0.30,
            "financial_contract": 0.30,
            "adoption_engagement": 0.25,
            "relationship_expansion": 0.15,
        },
        "support_health": {"m1": 0.5, "m2": 0.5},
        "financial_contract": {"m1": 1.0},
        "adoption_engagement": {"m1": 1.0},
        "relationship_expansion": {"m1": 1.0},
        "platform_value": {"m1": 1.0},
    }


def _valid_thresholds():
    """Return thresholds matching _valid_weights metrics."""
    return {
        "support_health": {"m1": _make_threshold(), "m2": _make_threshold()},
        "financial_contract": {"m1": _make_threshold()},
        "adoption_engagement": {"m1": _make_threshold()},
        "relationship_expansion": {"m1": _make_threshold()},
        "platform_value": {"m1": _make_threshold()},
    }


class TestValidateConfig:
    def test_valid_config_no_errors(self):
        errors = validate_config(_valid_weights(), _valid_thresholds())
        assert errors == []

    def test_real_config_no_errors(self):
        """Validate the actual project config files pass."""
        import yaml
        with open("config/weights.yaml") as f:
            weights = yaml.safe_load(f)
        with open("config/thresholds.yaml") as f:
            thresholds = yaml.safe_load(f)
        errors = validate_config(weights, thresholds)
        assert errors == [], f"Real config errors: {errors}"

    def test_missing_health_score_key(self):
        w = _valid_weights()
        del w["health_score"]
        errors = validate_config(w, _valid_thresholds())
        assert any("health_score" in e for e in errors)

    def test_missing_churn_risk_key(self):
        w = _valid_weights()
        del w["churn_risk"]
        errors = validate_config(w, _valid_thresholds())
        assert any("churn_risk" in e for e in errors)

    def test_missing_health_score_subfield(self):
        w = _valid_weights()
        del w["health_score"]["platform_value_weight"]
        errors = validate_config(w, _valid_thresholds())
        assert any("platform_value_weight" in e for e in errors)

    def test_missing_dimension_in_weights(self):
        w = _valid_weights()
        del w["support_health"]
        errors = validate_config(w, _valid_thresholds())
        assert any("missing dimension section" in e and "support_health" in e for e in errors)

    def test_missing_dimension_in_thresholds(self):
        t = _valid_thresholds()
        del t["financial_contract"]
        errors = validate_config(_valid_weights(), t)
        assert any("thresholds.yaml" in e and "financial_contract" in e for e in errors)

    def test_weight_sum_not_one(self):
        w = _valid_weights()
        w["support_health"] = {"m1": 0.3, "m2": 0.3}  # sum = 0.6
        errors = validate_config(w, _valid_thresholds())
        assert any("sum to" in e and "support_health" in e for e in errors)

    def test_weight_sum_tolerance(self):
        """Weights summing to 1.005 should pass (within 0.01 tolerance)."""
        w = _valid_weights()
        w["support_health"] = {"m1": 0.505, "m2": 0.5}
        errors = validate_config(w, _valid_thresholds())
        assert not any("sum to" in e and "support_health" in e for e in errors)

    def test_missing_metric_threshold(self):
        t = _valid_thresholds()
        del t["support_health"]["m2"]
        errors = validate_config(_valid_weights(), t)
        assert any("missing threshold" in e and "m2" in e for e in errors)

    def test_missing_lower_is_better(self):
        t = _valid_thresholds()
        del t["support_health"]["m1"]["lower_is_better"]
        errors = validate_config(_valid_weights(), t)
        assert any("lower_is_better" in e for e in errors)

    def test_missing_segment(self):
        t = _valid_thresholds()
        del t["support_health"]["m1"]["paid"]
        errors = validate_config(_valid_weights(), t)
        assert any("missing segment" in e and "paid" in e for e in errors)

    def test_missing_boundary(self):
        t = _valid_thresholds()
        del t["support_health"]["m1"]["paid"]["yellow"]
        errors = validate_config(_valid_weights(), t)
        assert any("missing 'yellow'" in e for e in errors)

    def test_lower_is_better_wrong_order(self):
        t = _valid_thresholds()
        # lower_is_better=true requires green <= yellow <= red, set reversed
        t["support_health"]["m1"]["paid"] = {"green": 10, "yellow": 5, "red": 0}
        errors = validate_config(_valid_weights(), t)
        assert any("green<=yellow<=red" in e for e in errors)

    def test_higher_is_better_wrong_order(self):
        t = _valid_thresholds()
        t["support_health"]["m1"]["lower_is_better"] = False
        # lower_is_better=false requires green >= yellow >= red, set reversed
        t["support_health"]["m1"]["paid"] = {"green": 0, "yellow": 5, "red": 10}
        t["support_health"]["m1"]["standard"] = {"green": 0, "yellow": 5, "red": 10}
        errors = validate_config(_valid_weights(), t)
        assert any("green>=yellow>=red" in e for e in errors)

    def test_missing_churn_risk_dimension_weight(self):
        w = _valid_weights()
        del w["churn_risk"]["support_health"]
        errors = validate_config(w, _valid_thresholds())
        assert any("churn_risk missing dimension weight" in e for e in errors)

    def test_multiple_errors_reported(self):
        """All errors are collected, not just the first."""
        w = _valid_weights()
        del w["health_score"]
        del w["churn_risk"]
        errors = validate_config(w, _valid_thresholds())
        assert len(errors) >= 2
