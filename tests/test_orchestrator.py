from __future__ import annotations

"""Tests for HealthScoreOrchestrator (init, score_account, run)."""

import os
from unittest.mock import MagicMock, patch

import pytest

from src.main import HealthScoreOrchestrator


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
            "messaging": 80,
            "automations": 65,
            "contactless": 55,
            "requests": 40,
            "staff_adoption": 70,
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
