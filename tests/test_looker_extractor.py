from __future__ import annotations

"""Tests for Looker extractor (Adoption, Platform Value Score)."""

import json
from unittest.mock import MagicMock, patch

import pytest

from src.extractors.looker import LookerExtractor


@pytest.fixture
def extractor():
    """LookerExtractor with mocked SDK."""
    with patch("src.extractors.looker.looker_sdk") as mock_sdk:
        mock_instance = MagicMock()
        mock_sdk.init40.return_value = mock_instance
        ext = LookerExtractor(
            base_url="https://looker.example.com",
            client_id="cid",
            client_secret="csec",
        )
    return ext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_query(query_id: int = 42) -> MagicMock:
    q = MagicMock()
    q.id = query_id
    return q


# ---------------------------------------------------------------------------
# TestRunInlineQuery
# ---------------------------------------------------------------------------

class TestRunInlineQuery:
    def test_creates_query_and_runs(self, extractor):
        mock_query = _mock_query(99)
        extractor.sdk.create_query.return_value = mock_query
        extractor.sdk.run_query.return_value = json.dumps([{"a": 1}])

        result = extractor._run_inline_query(
            model="test_model",
            view="test_view",
            fields=["test_view.field1"],
        )

        extractor.sdk.create_query.assert_called_once()
        extractor.sdk.run_query.assert_called_once_with(query_id=99, result_format="json")
        assert result == [{"a": 1}]

    def test_parses_json_string(self, extractor):
        extractor.sdk.create_query.return_value = _mock_query()
        extractor.sdk.run_query.return_value = '[{"x": 10}, {"x": 20}]'

        result = extractor._run_inline_query(
            model="m", view="v", fields=["v.x"],
        )

        assert len(result) == 2
        assert result[0]["x"] == 10

    def test_handles_list_result(self, extractor):
        """If SDK returns a list directly (not a string), use it as-is."""
        extractor.sdk.create_query.return_value = _mock_query()
        extractor.sdk.run_query.return_value = [{"y": 5}]

        result = extractor._run_inline_query(
            model="m", view="v", fields=["v.y"],
        )

        assert result == [{"y": 5}]

    def test_passes_filters_and_sorts(self, extractor):
        extractor.sdk.create_query.return_value = _mock_query()
        extractor.sdk.run_query.return_value = "[]"

        extractor._run_inline_query(
            model="m",
            view="v",
            fields=["v.f"],
            filters={"v.date": "30 days"},
            sorts=["v.f desc"],
            limit=100,
        )

        extractor.sdk.create_query.assert_called_once()
        # Verify create_query was called (WriteQuery is mocked so we can't inspect attrs)
        call_kwargs = extractor.sdk.create_query.call_args[1]
        assert "body" in call_kwargs
        # Verify run_query used the returned query id
        extractor.sdk.run_query.assert_called_once_with(query_id=42, result_format="json")


# ---------------------------------------------------------------------------
# TestCalcTrendPct
# ---------------------------------------------------------------------------

class TestCalcTrendPct:
    def test_positive_growth(self, extractor):
        assert extractor._calc_trend_pct(120, 100) == 20.0

    def test_decline(self, extractor):
        assert extractor._calc_trend_pct(80, 100) == -20.0

    def test_zero_previous(self, extractor):
        assert extractor._calc_trend_pct(50, 0) == 100.0

    def test_zero_previous_zero_current(self, extractor):
        assert extractor._calc_trend_pct(0, 0) == 0.0

    def test_no_change(self, extractor):
        assert extractor._calc_trend_pct(100, 100) == 0.0

    def test_rounding(self, extractor):
        # (33 - 30) / 30 * 100 = 10.0
        assert extractor._calc_trend_pct(33, 30) == 10.0


# ---------------------------------------------------------------------------
# TestExtractAdoptionMetrics
# ---------------------------------------------------------------------------

class TestExtractAdoptionMetrics:
    def _setup_inline_query(self, extractor, side_effect):
        """Patch _run_inline_query with a list of side effects."""
        extractor._run_inline_query = MagicMock(side_effect=side_effect)

    def test_happy_path(self, extractor):
        """All queries succeed with data."""
        call_count = [0]
        responses = [
            # Page visits per arrival: current=6.5, prev_30d=5.0
            [{"platform_score.total_page_visits_per_arrival": 6.5}],
            [{"platform_score.total_page_visits_per_arrival": 5.0}],
            # Feature breadth: 6 active of 10 total
            [{"feature_usage.active_module_count": 6, "feature_usage.total_module_count": 10}],
            # Platform score current
            [{"platform_score.score": 72}],
            # Platform score 90d ago
            [{"platform_score.score": 65}],
        ]

        def side_effect(**kwargs):
            idx = call_count[0]
            call_count[0] += 1
            return responses[idx]

        extractor._run_inline_query = MagicMock(side_effect=side_effect)

        result = extractor.extract_adoption_metrics("cust-1")

        assert result["page_visits_per_arrival"] == 6.5
        assert result["page_visits_per_arrival_trend"] == 30.0  # (6.5-5.0)/5.0*100
        assert result["feature_breadth_pct"] == 60.0  # 6/10*100
        assert result["platform_score"] == 72
        assert result["platform_score_trend"] == 7.0  # 72-65

    def test_individual_query_failure_returns_none(self, extractor):
        """When one query fails, its metric is None; others still calculated."""
        call_count = [0]

        def side_effect(**kwargs):
            idx = call_count[0]
            call_count[0] += 1
            # Fail the feature breadth query (index 2)
            if idx == 2:
                raise Exception("Looker API error")
            # Page visits per arrival
            if idx < 2:
                return [{"platform_score.total_page_visits_per_arrival": 4.0}]
            if idx == 3:
                return [{"platform_score.score": 50}]
            if idx == 4:
                return [{"platform_score.score": 45}]
            return []

        extractor._run_inline_query = MagicMock(side_effect=side_effect)

        result = extractor.extract_adoption_metrics("cust-1")

        assert result["feature_breadth_pct"] is None
        assert result["page_visits_per_arrival"] is not None

    def test_empty_results(self, extractor):
        """Empty query results produce zero/None metrics."""
        extractor._run_inline_query = MagicMock(return_value=[])

        result = extractor.extract_adoption_metrics("cust-1")

        # Page visits per arrival is 0 from empty results, trend = 0%
        assert result["page_visits_per_arrival"] == 0
        assert result["page_visits_per_arrival_trend"] == 0.0
        assert result["feature_breadth_pct"] is None
        assert result["platform_score"] is None

    def test_feature_breadth_zero_total(self, extractor):
        """Feature breadth with zero total_module_count defaults to 1 to avoid division by zero."""
        call_count = [0]

        def side_effect(**kwargs):
            idx = call_count[0]
            call_count[0] += 1
            if idx < 2:
                return [{"platform_score.total_page_visits_per_arrival": 0}]
            if idx == 2:
                return [{"feature_usage.active_module_count": 3, "feature_usage.total_module_count": 0}]
            return []

        extractor._run_inline_query = MagicMock(side_effect=side_effect)

        result = extractor.extract_adoption_metrics("cust-1")

        # total=0 → defaults to 1, so 3/1*100 = 300.0
        assert result["feature_breadth_pct"] == 300.0

    def test_all_queries_fail(self, extractor):
        """All queries raise exceptions — all metrics are None."""
        extractor._run_inline_query = MagicMock(side_effect=Exception("timeout"))

        result = extractor.extract_adoption_metrics("cust-1")

        assert result["page_visits_per_arrival"] is None
        assert result["page_visits_per_arrival_trend"] is None
        assert result["feature_breadth_pct"] is None
        assert result["platform_score"] is None
        assert result["platform_score_trend"] is None


# ---------------------------------------------------------------------------
# TestExtractPlatformValueScore
# ---------------------------------------------------------------------------

class TestExtractPlatformValueScore:
    def test_happy_path(self, extractor):
        row = {
            "platform_score.messaging_score": 85,
            "platform_score.automations_score": 70,
            "platform_score.contactless_score": 60,
            "platform_score.requests_score": 45,
            "platform_score.staff_adoption_score": 90,
        }
        extractor._run_inline_query = MagicMock(return_value=[row])

        result = extractor.extract_platform_value_score("cust-1")

        assert result["messaging"] == 85
        assert result["automations"] == 70
        assert result["contactless"] == 60
        assert result["requests"] == 45
        assert result["staff_adoption"] == 90

    def test_query_failure_returns_all_none(self, extractor):
        extractor._run_inline_query = MagicMock(side_effect=Exception("timeout"))

        result = extractor.extract_platform_value_score("cust-1")

        assert result["messaging"] is None
        assert result["automations"] is None
        assert result["contactless"] is None
        assert result["requests"] is None
        assert result["staff_adoption"] is None

    def test_empty_result(self, extractor):
        extractor._run_inline_query = MagicMock(return_value=[])

        result = extractor.extract_platform_value_score("cust-1")

        assert result["messaging"] is None
        assert result["automations"] is None

    def test_partial_pillars(self, extractor):
        """Some pillars present, others missing from row."""
        row = {
            "platform_score.messaging_score": 80,
            # others not present
        }
        extractor._run_inline_query = MagicMock(return_value=[row])

        result = extractor.extract_platform_value_score("cust-1")

        assert result["messaging"] == 80
        assert result["automations"] is None
        assert result["contactless"] is None
