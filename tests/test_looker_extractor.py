from __future__ import annotations

"""Tests for Looker extractor (Adoption, Platform Value Score)."""

import json
from unittest.mock import MagicMock, patch

import pytest

from src.extractors.looker import (
    FIELD_ALLIN_PCT,
    FIELD_ARRIVAL_CIOL_PCT,
    FIELD_AUTOMATION_VALUE,
    FIELD_CONVERSATIONS_BOOKING_PCT,
    FIELD_CUSTOMER_ID,
    FIELD_DIGITAL_KEY_PCT,
    FIELD_ITINERARY_VISITS,
    FIELD_MOBILE_KEY_PCT,
    FIELD_PAGE_VISITS_PER_ARRIVAL,
    FIELD_RESPONSE_PCT,
    FIELD_SENTIMENT_PCT,
    FIELD_TOTAL_BOOKINGS,
    LOOK_ALLIN_USAGE,
    LOOK_AUTOMATION,
    LOOK_BOOKINGS,
    LOOK_ITINERARY,
    LOOK_PAGE_VISITS,
    LOOK_RESPONSE_TIME,
    LOOK_SENTIMENT,
    LookerExtractor,
)


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
# TestGetLookData (cache behaviour)
# ---------------------------------------------------------------------------

class TestGetLookData:
    def test_caches_look_results(self, extractor):
        """Calling _get_look_data twice for the same Look only runs _run_look once."""
        extractor._run_look = MagicMock(return_value=[{"a": 1}])

        first = extractor._get_look_data(171)
        second = extractor._get_look_data(171)

        assert first == [{"a": 1}]
        assert second == [{"a": 1}]
        extractor._run_look.assert_called_once_with(171)

    def test_different_looks_not_shared(self, extractor):
        """Different Look IDs each trigger their own _run_look call."""
        extractor._run_look = MagicMock(side_effect=lambda look_id: [{"id": look_id}])

        extractor._get_look_data(171)
        extractor._get_look_data(172)

        assert extractor._run_look.call_count == 2


class TestGetCustomerRow:
    def test_finds_matching_customer(self, extractor):
        extractor._look_cache[171] = [
            {FIELD_CUSTOMER_ID: "cust-1", "val": 10},
            {FIELD_CUSTOMER_ID: "cust-2", "val": 20},
        ]

        row = extractor._get_customer_row(171, "cust-1")
        assert row == {FIELD_CUSTOMER_ID: "cust-1", "val": 10}

    def test_returns_none_for_missing_customer(self, extractor):
        extractor._look_cache[171] = [
            {FIELD_CUSTOMER_ID: "cust-1", "val": 10},
        ]

        row = extractor._get_customer_row(171, "cust-999")
        assert row is None

    def test_custom_id_field(self, extractor):
        extractor._look_cache[171] = [
            {"other_id": "abc", "val": 5},
        ]

        row = extractor._get_customer_row(171, "abc", id_field="other_id")
        assert row is not None
        assert row["val"] == 5


# ---------------------------------------------------------------------------
# TestExtractPlatformValueScore
# ---------------------------------------------------------------------------

def _make_look_data(customer_id: str = "cust-1") -> dict[int, list[dict]]:
    """Build a full set of Look data for one customer across all 7 Looks."""
    return {
        LOOK_BOOKINGS: [{
            FIELD_CUSTOMER_ID: customer_id,
            FIELD_CONVERSATIONS_BOOKING_PCT: 25.0,
            FIELD_ARRIVAL_CIOL_PCT: 18.0,
            FIELD_DIGITAL_KEY_PCT: 6.0,
            FIELD_MOBILE_KEY_PCT: 4.0,
            FIELD_TOTAL_BOOKINGS: 200,
        }],
        LOOK_ALLIN_USAGE: [{
            FIELD_CUSTOMER_ID: customer_id,
            FIELD_ALLIN_PCT: 22.0,
        }],
        LOOK_SENTIMENT: [{
            FIELD_CUSTOMER_ID: customer_id,
            FIELD_SENTIMENT_PCT: 15.0,
        }],
        LOOK_AUTOMATION: [{
            FIELD_CUSTOMER_ID: customer_id,
            FIELD_AUTOMATION_VALUE: "some_automation",
        }],
        LOOK_RESPONSE_TIME: [{
            FIELD_CUSTOMER_ID: customer_id,
            FIELD_RESPONSE_PCT: 80.0,
        }],
        LOOK_PAGE_VISITS: [{
            FIELD_CUSTOMER_ID: customer_id,
            FIELD_PAGE_VISITS_PER_ARRIVAL: 90.0,
        }],
        LOOK_ITINERARY: [{
            FIELD_CUSTOMER_ID: customer_id,
            FIELD_ITINERARY_VISITS: 12,
        }],
    }


class TestExtractPlatformValueScore:
    def test_happy_path(self, extractor):
        """All Looks return data — all 9 metrics populated."""
        extractor._look_cache = _make_look_data("cust-1")

        result = extractor.extract_platform_value_score("cust-1")

        assert result["positive_sentiment_pct"] == 15.0
        assert result["response_before_target_pct"] == 80.0
        assert result["allin_conversation_pct"] == 22.0
        assert result["conversations_per_booking_pct"] == 25.0
        assert result["arrival_ciol_pct"] == 18.0
        assert result["digital_key_pct"] == 10.0  # 6.0 + 4.0
        assert result["automation_active"] == 1
        # itinerary_booking_pct = 12 / 200 * 100 = 6.0
        assert result["itinerary_booking_pct"] == 6.0
        assert result["page_visits_per_arrival"] == 90.0

    def test_all_looks_empty_returns_all_none(self, extractor):
        """No customer rows in any Look → all None (except automation_active=0)."""
        extractor._run_look = MagicMock(return_value=[])

        result = extractor.extract_platform_value_score("cust-1")

        assert result["positive_sentiment_pct"] is None
        assert result["response_before_target_pct"] is None
        assert result["allin_conversation_pct"] is None
        assert result["conversations_per_booking_pct"] is None
        assert result["arrival_ciol_pct"] is None
        assert result["digital_key_pct"] is None
        assert result["automation_active"] == 0  # no row → 0
        assert result["itinerary_booking_pct"] is None
        assert result["page_visits_per_arrival"] is None

    def test_look_failure_returns_none_for_affected_metrics(self, extractor):
        """If a Look fetch fails, its metrics are None but others still work."""
        # Pre-cache all Looks except sentiment — that one will raise
        data = _make_look_data("cust-1")
        del data[LOOK_SENTIMENT]
        extractor._look_cache = data

        def run_look_side_effect(look_id):
            raise Exception("API timeout")

        extractor._run_look = MagicMock(side_effect=run_look_side_effect)

        result = extractor.extract_platform_value_score("cust-1")

        # Sentiment should be None (Look not cached, _run_look raises)
        assert result["positive_sentiment_pct"] is None
        # Other cached metrics should still work
        assert result["response_before_target_pct"] == 80.0
        assert result["allin_conversation_pct"] == 22.0

    def test_digital_key_sum(self, extractor):
        """digital_key_pct = digital_key_pct + mobile_key_pct."""
        extractor._look_cache = {
            LOOK_BOOKINGS: [{
                FIELD_CUSTOMER_ID: "cust-1",
                FIELD_DIGITAL_KEY_PCT: 7.0,
                FIELD_MOBILE_KEY_PCT: 3.5,
            }],
        }
        # Empty Looks for others
        extractor._run_look = MagicMock(return_value=[])

        result = extractor.extract_platform_value_score("cust-1")
        assert result["digital_key_pct"] == 10.5

    def test_digital_key_one_none(self, extractor):
        """If one key component is None, treat as 0."""
        extractor._look_cache = {
            LOOK_BOOKINGS: [{
                FIELD_CUSTOMER_ID: "cust-1",
                FIELD_DIGITAL_KEY_PCT: 8.0,
                # FIELD_MOBILE_KEY_PCT not present → None
            }],
        }
        extractor._run_look = MagicMock(return_value=[])

        result = extractor.extract_platform_value_score("cust-1")
        assert result["digital_key_pct"] == 8.0

    def test_digital_key_both_none(self, extractor):
        """If both key components are None, digital_key_pct is None."""
        extractor._look_cache = {
            LOOK_BOOKINGS: [{
                FIELD_CUSTOMER_ID: "cust-1",
                # Neither FIELD_DIGITAL_KEY_PCT nor FIELD_MOBILE_KEY_PCT present
            }],
        }
        extractor._run_look = MagicMock(return_value=[])

        result = extractor.extract_platform_value_score("cust-1")
        assert result["digital_key_pct"] is None

    def test_automation_null_value_maps_to_zero(self, extractor):
        """Automation Look row with null value → automation_active = 1 (row exists)."""
        # The field exists but value is None — row present means active per spec:
        # "null → 0, any value → 1". But get() returns None for the field.
        # Actually re-reading spec: null → 0, any value → 1
        extractor._look_cache = {
            LOOK_AUTOMATION: [{
                FIELD_CUSTOMER_ID: "cust-1",
                FIELD_AUTOMATION_VALUE: None,
            }],
        }
        extractor._run_look = MagicMock(return_value=[])

        result = extractor.extract_platform_value_score("cust-1")
        assert result["automation_active"] == 0

    def test_automation_present_maps_to_one(self, extractor):
        """Automation Look row with a value → automation_active = 1."""
        extractor._look_cache = {
            LOOK_AUTOMATION: [{
                FIELD_CUSTOMER_ID: "cust-1",
                FIELD_AUTOMATION_VALUE: "active",
            }],
        }
        extractor._run_look = MagicMock(return_value=[])

        result = extractor.extract_platform_value_score("cust-1")
        assert result["automation_active"] == 1

    def test_automation_no_row_maps_to_zero(self, extractor):
        """Customer not found in automation Look → automation_active = 0."""
        extractor._look_cache = {
            LOOK_AUTOMATION: [{
                FIELD_CUSTOMER_ID: "other-customer",
                FIELD_AUTOMATION_VALUE: "something",
            }],
        }
        extractor._run_look = MagicMock(return_value=[])

        result = extractor.extract_platform_value_score("cust-1")
        assert result["automation_active"] == 0

    def test_itinerary_booking_pct_calculation(self, extractor):
        """itinerary_booking_pct = itinerary_visits / total_bookings * 100."""
        extractor._look_cache = {
            LOOK_BOOKINGS: [{
                FIELD_CUSTOMER_ID: "cust-1",
                FIELD_TOTAL_BOOKINGS: 500,
            }],
            LOOK_ITINERARY: [{
                FIELD_CUSTOMER_ID: "cust-1",
                FIELD_ITINERARY_VISITS: 25,
            }],
        }
        extractor._run_look = MagicMock(return_value=[])

        result = extractor.extract_platform_value_score("cust-1")
        assert result["itinerary_booking_pct"] == 5.0  # 25/500*100

    def test_itinerary_zero_bookings_returns_none(self, extractor):
        """Zero total_bookings → itinerary_booking_pct is None (avoid div by zero)."""
        extractor._look_cache = {
            LOOK_BOOKINGS: [{
                FIELD_CUSTOMER_ID: "cust-1",
                FIELD_TOTAL_BOOKINGS: 0,
            }],
            LOOK_ITINERARY: [{
                FIELD_CUSTOMER_ID: "cust-1",
                FIELD_ITINERARY_VISITS: 10,
            }],
        }
        extractor._run_look = MagicMock(return_value=[])

        result = extractor.extract_platform_value_score("cust-1")
        assert result["itinerary_booking_pct"] is None

    def test_look_caching_across_metrics(self, extractor):
        """Look 171 (bookings) is used by multiple metrics — only fetched once."""
        call_results = {
            LOOK_BOOKINGS: [{
                FIELD_CUSTOMER_ID: "cust-1",
                FIELD_CONVERSATIONS_BOOKING_PCT: 20,
                FIELD_TOTAL_BOOKINGS: 100,
            }],
            LOOK_ITINERARY: [{
                FIELD_CUSTOMER_ID: "cust-1",
                FIELD_ITINERARY_VISITS: 5,
            }],
        }

        def run_look(look_id):
            return call_results.get(look_id, [])

        extractor._run_look = MagicMock(side_effect=run_look)

        extractor.extract_platform_value_score("cust-1")

        # Look 171 should only be called once even though used for bookings + itinerary
        look_171_calls = [
            c for c in extractor._run_look.call_args_list if c[0][0] == LOOK_BOOKINGS
        ]
        assert len(look_171_calls) == 1
