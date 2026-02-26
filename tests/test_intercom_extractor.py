from __future__ import annotations

"""Tests for Intercom support metrics extractor."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.extractors.intercom import IntercomExtractor


@pytest.fixture
def extractor():
    return IntercomExtractor(api_token="fake-token", lookback_days=30)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_conversation(
    *,
    priority: str = "not_priority",
    tags: list[str] | None = None,
    created_at: int = 1700000000,
    first_contact_reply_at: int | None = None,
    last_close_at: int | None = None,
    median_time_to_reply: int | None = None,
    count_reopens: int = 0,
    state: str = "closed",
    assignee_type: str | None = None,
) -> dict:
    tag_objs = [{"name": t} for t in (tags or [])]
    conv = {
        "priority": priority,
        "tags": {"tags": tag_objs},
        "created_at": created_at,
        "state": state,
        "statistics": {
            "first_contact_reply_at": first_contact_reply_at,
            "last_close_at": last_close_at,
            "median_time_to_reply": median_time_to_reply,
            "count_reopens": count_reopens,
        },
    }
    if assignee_type:
        conv["assignee"] = {"type": assignee_type}
    return conv


def _mock_search_response(conversations: list[dict], next_cursor: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = 200
    pages = {}
    if next_cursor:
        pages["next"] = next_cursor
    resp.json.return_value = {
        "conversations": conversations,
        "pages": pages,
    }
    resp.raise_for_status = MagicMock()
    return resp


def _mock_get_response(data: list[dict], next_page: str | dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = 200
    pages = {}
    if next_page:
        pages["next"] = next_page
    resp.json.return_value = {
        "data": data,
        "pages": pages,
    }
    resp.raise_for_status = MagicMock()
    return resp


def _mock_companies_response(data: list[dict], next_page: str | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = 200
    pages = {}
    if next_page:
        pages["next"] = next_page
    resp.json.return_value = {
        "data": data,
        "pages": pages,
    }
    resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# TestExtractConversationMetrics
# ---------------------------------------------------------------------------

class TestExtractConversationMetrics:
    """Tests for _extract_conversation_metrics (single conversation parsing)."""

    def test_p1_by_priority_field(self, extractor):
        conv = _make_conversation(priority="priority")
        result = extractor._extract_conversation_metrics(conv)
        assert result["is_p1_p2"] is True

    def test_not_p1_by_default_priority(self, extractor):
        conv = _make_conversation(priority="not_priority")
        result = extractor._extract_conversation_metrics(conv)
        assert result["is_p1_p2"] is False

    def test_p1_by_p1_tag(self, extractor):
        conv = _make_conversation(tags=["p1"])
        result = extractor._extract_conversation_metrics(conv)
        assert result["is_p1_p2"] is True

    def test_p1_by_p2_tag(self, extractor):
        conv = _make_conversation(tags=["P2"])
        result = extractor._extract_conversation_metrics(conv)
        assert result["is_p1_p2"] is True

    def test_p1_by_urgent_tag(self, extractor):
        conv = _make_conversation(tags=["urgent"])
        result = extractor._extract_conversation_metrics(conv)
        assert result["is_p1_p2"] is True

    def test_p1_by_critical_tag(self, extractor):
        conv = _make_conversation(tags=["Critical"])
        result = extractor._extract_conversation_metrics(conv)
        assert result["is_p1_p2"] is True

    def test_first_response_time_calculated(self, extractor):
        conv = _make_conversation(
            created_at=1000,
            first_contact_reply_at=1300,
        )
        result = extractor._extract_conversation_metrics(conv)
        assert result["first_response_seconds"] == 300

    def test_first_response_time_none_when_missing(self, extractor):
        conv = _make_conversation(created_at=1000, first_contact_reply_at=None)
        result = extractor._extract_conversation_metrics(conv)
        assert result["first_response_seconds"] is None

    def test_time_to_close_from_last_close(self, extractor):
        conv = _make_conversation(
            created_at=1000,
            last_close_at=4600,
        )
        result = extractor._extract_conversation_metrics(conv)
        assert result["time_to_close_seconds"] == 3600

    def test_reopen_detected(self, extractor):
        conv = _make_conversation(count_reopens=2)
        result = extractor._extract_conversation_metrics(conv)
        assert result["is_reopened"] is True

    def test_no_reopen(self, extractor):
        conv = _make_conversation(count_reopens=0)
        result = extractor._extract_conversation_metrics(conv)
        assert result["is_reopened"] is False

    def test_escalation_by_tag(self, extractor):
        conv = _make_conversation(tags=["escalated"])
        result = extractor._extract_conversation_metrics(conv)
        assert result["is_escalated"] is True

    def test_escalation_by_escalation_tag(self, extractor):
        conv = _make_conversation(tags=["escalation"])
        result = extractor._extract_conversation_metrics(conv)
        assert result["is_escalated"] is True

    def test_no_escalation(self, extractor):
        conv = _make_conversation(tags=["general"], state="closed")
        result = extractor._extract_conversation_metrics(conv)
        assert result["is_escalated"] is False

    def test_missing_statistics_field(self, extractor):
        conv = {"priority": "not_priority", "tags": {"tags": []}}
        result = extractor._extract_conversation_metrics(conv)
        assert result["is_reopened"] is False
        assert result["first_response_seconds"] is None

    def test_missing_tags_field(self, extractor):
        conv = {"priority": "not_priority", "statistics": {"count_reopens": 0}}
        result = extractor._extract_conversation_metrics(conv)
        assert result["is_p1_p2"] is False


# ---------------------------------------------------------------------------
# TestExtractSupportMetrics
# ---------------------------------------------------------------------------

class TestExtractSupportMetrics:
    """Tests for extract_support_metrics (aggregated company metrics)."""

    def test_no_conversations_returns_zeros(self, extractor):
        with patch.object(extractor.session, "post", return_value=_mock_search_response([])):
            result = extractor.extract_support_metrics("company-123")

        assert result["p1_p2_volume"] == 0
        assert result["first_response_minutes"] == 0
        assert result["close_time_hours"] == 0
        assert result["reopen_rate_pct"] == 0
        assert result["escalation_rate_pct"] == 0

    def test_happy_path_all_metrics(self, extractor):
        convos = [
            _make_conversation(
                priority="priority",
                created_at=1000,
                first_contact_reply_at=1600,  # 600s = 10min
                last_close_at=4600,           # 3600s = 1hr
                count_reopens=1,
                tags=["escalated"],
            ),
            _make_conversation(
                priority="not_priority",
                created_at=2000,
                first_contact_reply_at=2300,  # 300s = 5min
                last_close_at=9200,           # 7200s = 2hr
                count_reopens=0,
            ),
        ]
        with patch.object(extractor.session, "post", return_value=_mock_search_response(convos)):
            result = extractor.extract_support_metrics("company-123")

        assert result["p1_p2_volume"] == 1
        # median of [600, 300] / 60 = 450/60 = 7.5
        assert result["first_response_minutes"] == 7.5
        # median of [3600, 7200] / 3600 = 5400/3600 = 1.5
        assert result["close_time_hours"] == 1.5
        # 1 of 2 reopened = 50%
        assert result["reopen_rate_pct"] == 50.0
        # 1 of 2 escalated = 50%
        assert result["escalation_rate_pct"] == 50.0

    def test_median_first_response(self, extractor):
        # created_at must be nonzero so first_response_seconds > 0
        convos = [
            _make_conversation(created_at=1000, first_contact_reply_at=1120),   # 120s
            _make_conversation(created_at=1000, first_contact_reply_at=1600),   # 600s
            _make_conversation(created_at=1000, first_contact_reply_at=2200),   # 1200s
        ]
        with patch.object(extractor.session, "post", return_value=_mock_search_response(convos)):
            result = extractor.extract_support_metrics("company-123")

        # median of [120, 600, 1200] = 600 / 60 = 10.0
        assert result["first_response_minutes"] == 10.0

    def test_reopen_rate_calculation(self, extractor):
        convos = [
            _make_conversation(count_reopens=1, created_at=100, first_contact_reply_at=200),
            _make_conversation(count_reopens=0, created_at=100, first_contact_reply_at=200),
            _make_conversation(count_reopens=0, created_at=100, first_contact_reply_at=200),
            _make_conversation(count_reopens=3, created_at=100, first_contact_reply_at=200),
        ]
        with patch.object(extractor.session, "post", return_value=_mock_search_response(convos)):
            result = extractor.extract_support_metrics("company-456")

        assert result["reopen_rate_pct"] == 50.0

    def test_as_of_date_parameter(self, extractor):
        """as_of_date controls the time window for conversation search."""
        fixed_date = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        mock_resp = _mock_search_response([])
        with patch.object(extractor.session, "post", return_value=mock_resp) as mock_post:
            extractor.extract_support_metrics("company-123", as_of_date=fixed_date)

        call_args = mock_post.call_args
        query = call_args[1]["json"] if "json" in call_args[1] else call_args[0][1]
        # until_ts should be the as_of_date
        until_value = query["query"]["value"][2]["value"]
        assert until_value == int(fixed_date.timestamp())


# ---------------------------------------------------------------------------
# TestGetConversationsForCompany
# ---------------------------------------------------------------------------

class TestGetConversationsForCompany:
    """Tests for _get_conversations_for_company (pagination via cursor)."""

    def test_single_page(self, extractor):
        convos = [_make_conversation(), _make_conversation()]
        with patch.object(extractor.session, "post", return_value=_mock_search_response(convos)):
            result = extractor._get_conversations_for_company("c1", 1000, 2000)

        assert len(result) == 2

    def test_pagination_with_cursor(self, extractor):
        page1 = _mock_search_response(
            [_make_conversation()],
            next_cursor={"starting_after": "cursor-abc"},
        )
        page2 = _mock_search_response([_make_conversation()])

        with patch.object(extractor.session, "post", side_effect=[page1, page2]):
            result = extractor._get_conversations_for_company("c1", 1000, 2000)

        assert len(result) == 2

    def test_empty_result(self, extractor):
        with patch.object(extractor.session, "post", return_value=_mock_search_response([])):
            result = extractor._get_conversations_for_company("c1", 1000, 2000)

        assert result == []


# ---------------------------------------------------------------------------
# TestGetCompanies
# ---------------------------------------------------------------------------

class TestGetCompanies:
    """Tests for get_companies (paginated list)."""

    def test_single_page(self, extractor):
        companies = [{"id": "c1", "name": "Acme"}]
        with patch.object(
            extractor.session, "get",
            return_value=_mock_companies_response(companies),
        ):
            result = extractor.get_companies()

        assert len(result) == 1
        assert result[0]["name"] == "Acme"

    def test_paginated(self, extractor):
        page1 = _mock_companies_response(
            [{"id": "c1"}],
            next_page="https://api.intercom.io/companies?page=2",
        )
        page2 = _mock_companies_response([{"id": "c2"}])

        with patch.object(extractor.session, "get", side_effect=[page1, page2]):
            result = extractor.get_companies()

        assert len(result) == 2

    def test_empty(self, extractor):
        with patch.object(
            extractor.session, "get",
            return_value=_mock_companies_response([]),
        ):
            result = extractor.get_companies()

        assert result == []
