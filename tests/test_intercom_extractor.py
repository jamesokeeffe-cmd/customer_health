from __future__ import annotations

"""Tests for Intercom support metrics extractor."""

import textwrap
from datetime import datetime, timezone
from pathlib import Path
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


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def _write_csv(tmp_path: Path, rows: list[dict]) -> Path:
    """Write a CSV file from a list of dicts and return the path."""
    csv_path = tmp_path / "conversations.csv"
    if not rows:
        csv_path.write_text("")
        return csv_path

    import csv as csv_mod

    with open(csv_path, "w", newline="") as f:
        writer = csv_mod.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return csv_path


def _make_csv_row(
    *,
    conversation_id: str = "conv-1",
    conversation_created_at: str = "2026-02-10 12:00:00",
    conversation_first_response_at: str = "2026-02-10 12:10:00",
    conversation_closed_at: str = "2026-02-10 14:00:00",
    conversation_tags: str = "",
    conversation_state: str = "closed",
    message_type: str = "comment",
    message_author_type: str = "user",
    message_author_companies: str = "Acme Corp",
) -> dict:
    return {
        "conversation_id": conversation_id,
        "conversation_created_at": conversation_created_at,
        "conversation_first_response_at": conversation_first_response_at,
        "conversation_closed_at": conversation_closed_at,
        "conversation_tags": conversation_tags,
        "conversation_state": conversation_state,
        "message_type": message_type,
        "message_author_type": message_author_type,
        "message_author_companies": message_author_companies,
    }


# ---------------------------------------------------------------------------
# TestParseCsvDatetime
# ---------------------------------------------------------------------------

class TestParseCsvDatetime:
    def test_valid_datetime(self):
        result = IntercomExtractor._parse_csv_datetime("2026-02-10 12:00:00")
        assert result == datetime(2026, 2, 10, 12, 0, 0, tzinfo=timezone.utc)

    def test_empty_string(self):
        assert IntercomExtractor._parse_csv_datetime("") is None

    def test_none_like(self):
        assert IntercomExtractor._parse_csv_datetime("  ") is None

    def test_invalid_format(self):
        assert IntercomExtractor._parse_csv_datetime("not-a-date") is None


class TestParseTags:
    def test_comma_separated(self):
        assert IntercomExtractor._parse_tags("p1, urgent, escalated") == [
            "p1", "urgent", "escalated",
        ]

    def test_empty(self):
        assert IntercomExtractor._parse_tags("") == []

    def test_single_tag(self):
        assert IntercomExtractor._parse_tags("p1") == ["p1"]


class TestParseCompanies:
    def test_single(self):
        assert IntercomExtractor._parse_companies("Acme Corp") == ["Acme Corp"]

    def test_multiple(self):
        assert IntercomExtractor._parse_companies("Acme Corp, Beta Inc") == [
            "Acme Corp", "Beta Inc",
        ]

    def test_empty(self):
        assert IntercomExtractor._parse_companies("") == []


# ---------------------------------------------------------------------------
# TestLoadSupportMetricsFromCsv
# ---------------------------------------------------------------------------

class TestLoadSupportMetricsFromCsv:
    """Tests for CSV-based support metrics extraction."""

    AS_OF = datetime(2026, 2, 27, 0, 0, 0, tzinfo=timezone.utc)

    def test_happy_path(self, tmp_path):
        """Conversations grouped by company with correct metrics."""
        rows = [
            # Conv 1: Acme Corp, P1, 10min response, 2hr close, reopened
            _make_csv_row(
                conversation_id="c1",
                conversation_created_at="2026-02-10 12:00:00",
                conversation_first_response_at="2026-02-10 12:10:00",
                conversation_closed_at="2026-02-10 14:00:00",
                conversation_tags="p1",
                message_type="comment",
                message_author_type="user",
                message_author_companies="Acme Corp",
            ),
            # c1 reopen message
            _make_csv_row(
                conversation_id="c1",
                conversation_created_at="2026-02-10 12:00:00",
                conversation_first_response_at="2026-02-10 12:10:00",
                conversation_closed_at="2026-02-10 14:00:00",
                conversation_tags="p1",
                message_type="assign_and_reopen",
                message_author_type="admin",
                message_author_companies="",
            ),
            # Conv 2: Acme Corp, normal, 5min response, 1hr close
            _make_csv_row(
                conversation_id="c2",
                conversation_created_at="2026-02-15 10:00:00",
                conversation_first_response_at="2026-02-15 10:05:00",
                conversation_closed_at="2026-02-15 11:00:00",
                message_author_companies="Acme Corp",
            ),
        ]
        csv_path = _write_csv(tmp_path, rows)

        result = IntercomExtractor.load_support_metrics_from_csv(
            str(csv_path), lookback_days=30, as_of_date=self.AS_OF,
        )

        assert "acme corp" in result
        metrics = result["acme corp"]
        assert metrics["p1_p2_volume"] == 1
        # median of [600, 300] seconds / 60 = 7.5 min
        assert metrics["first_response_minutes"] == 7.5
        # median of [7200, 3600] seconds / 3600 = 1.5 hr
        assert metrics["close_time_hours"] == 1.5
        # 1 of 2 conversations reopened = 50%
        assert metrics["reopen_rate_pct"] == 50.0
        assert metrics["escalation_rate_pct"] == 0.0

    def test_multiple_companies(self, tmp_path):
        """Conversations are correctly split between companies."""
        rows = [
            _make_csv_row(conversation_id="c1", message_author_companies="Acme Corp"),
            _make_csv_row(conversation_id="c2", message_author_companies="Beta Inc"),
            _make_csv_row(conversation_id="c3", message_author_companies="Acme Corp"),
        ]
        csv_path = _write_csv(tmp_path, rows)

        result = IntercomExtractor.load_support_metrics_from_csv(
            str(csv_path), lookback_days=30, as_of_date=self.AS_OF,
        )

        assert len(result) == 2
        assert result["acme corp"]["p1_p2_volume"] == 0
        assert result["beta inc"]["p1_p2_volume"] == 0

    def test_date_filtering(self, tmp_path):
        """Conversations outside the lookback window are excluded."""
        rows = [
            # Within window (27 days ago from 2026-02-27)
            _make_csv_row(
                conversation_id="c1",
                conversation_created_at="2026-02-10 12:00:00",
                message_author_companies="Acme Corp",
            ),
            # Outside window (60 days ago)
            _make_csv_row(
                conversation_id="c2",
                conversation_created_at="2025-12-29 12:00:00",
                message_author_companies="Acme Corp",
            ),
        ]
        csv_path = _write_csv(tmp_path, rows)

        result = IntercomExtractor.load_support_metrics_from_csv(
            str(csv_path), lookback_days=30, as_of_date=self.AS_OF,
        )

        # Only 1 conversation within window
        assert "acme corp" in result
        # p1_p2_volume is from the one in-window conversation
        assert result["acme corp"]["p1_p2_volume"] == 0

    def test_empty_csv(self, tmp_path):
        """Empty CSV returns empty dict."""
        import csv as csv_mod

        csv_path = tmp_path / "empty.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv_mod.writer(f)
            writer.writerow([
                "conversation_id", "conversation_created_at",
                "conversation_first_response_at", "conversation_closed_at",
                "conversation_tags", "conversation_state", "message_type",
                "message_author_type", "message_author_companies",
            ])

        result = IntercomExtractor.load_support_metrics_from_csv(
            str(csv_path), lookback_days=30, as_of_date=self.AS_OF,
        )
        assert result == {}

    def test_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            IntercomExtractor.load_support_metrics_from_csv(
                str(tmp_path / "nonexistent.csv"),
            )

    def test_escalation_from_tags(self, tmp_path):
        """Escalation detected from conversation tags."""
        rows = [
            _make_csv_row(
                conversation_id="c1",
                conversation_tags="escalated",
                message_author_companies="Acme Corp",
            ),
            _make_csv_row(
                conversation_id="c2",
                conversation_tags="escalation",
                message_author_companies="Acme Corp",
            ),
            _make_csv_row(
                conversation_id="c3",
                message_author_companies="Acme Corp",
            ),
        ]
        csv_path = _write_csv(tmp_path, rows)

        result = IntercomExtractor.load_support_metrics_from_csv(
            str(csv_path), lookback_days=30, as_of_date=self.AS_OF,
        )

        # 2 of 3 escalated = 66.7%
        assert result["acme corp"]["escalation_rate_pct"] == 66.7

    def test_reopen_detection(self, tmp_path):
        """Reopens detected from message_type field."""
        rows = [
            # Conv with reopen
            _make_csv_row(conversation_id="c1", message_author_companies="Acme Corp"),
            _make_csv_row(
                conversation_id="c1",
                message_type="assign_and_reopen",
                message_author_type="admin",
                message_author_companies="",
            ),
            # Conv without reopen
            _make_csv_row(conversation_id="c2", message_author_companies="Acme Corp"),
        ]
        csv_path = _write_csv(tmp_path, rows)

        result = IntercomExtractor.load_support_metrics_from_csv(
            str(csv_path), lookback_days=30, as_of_date=self.AS_OF,
        )

        assert result["acme corp"]["reopen_rate_pct"] == 50.0

    def test_company_name_case_insensitive(self, tmp_path):
        """Company names are lowercased for consistent lookup."""
        rows = [
            _make_csv_row(conversation_id="c1", message_author_companies="ACME Corp"),
            _make_csv_row(conversation_id="c2", message_author_companies="acme corp"),
        ]
        csv_path = _write_csv(tmp_path, rows)

        result = IntercomExtractor.load_support_metrics_from_csv(
            str(csv_path), lookback_days=30, as_of_date=self.AS_OF,
        )

        # Both should be under the same key
        assert len(result) == 1
        assert "acme corp" in result

    def test_admin_messages_ignored_for_company(self, tmp_path):
        """Admin messages don't contribute to company assignment."""
        rows = [
            _make_csv_row(
                conversation_id="c1",
                message_author_type="user",
                message_author_companies="Acme Corp",
            ),
            _make_csv_row(
                conversation_id="c1",
                message_author_type="admin",
                message_author_companies="Internal Co",
            ),
        ]
        csv_path = _write_csv(tmp_path, rows)

        result = IntercomExtractor.load_support_metrics_from_csv(
            str(csv_path), lookback_days=30, as_of_date=self.AS_OF,
        )

        assert "acme corp" in result
        assert "internal co" not in result

    def test_no_first_response(self, tmp_path):
        """Missing first_response_at yields 0 response time."""
        rows = [
            _make_csv_row(
                conversation_id="c1",
                conversation_first_response_at="",
                message_author_companies="Acme Corp",
            ),
        ]
        csv_path = _write_csv(tmp_path, rows)

        result = IntercomExtractor.load_support_metrics_from_csv(
            str(csv_path), lookback_days=30, as_of_date=self.AS_OF,
        )

        assert result["acme corp"]["first_response_minutes"] == 0

    def test_p1_p2_detection_multiple_tags(self, tmp_path):
        """P1/P2 detected from various tag names."""
        for tag in ["p1", "P2", "urgent", "critical", "priority"]:
            rows = [
                _make_csv_row(
                    conversation_id=f"c-{tag}",
                    conversation_tags=tag,
                    message_author_companies="Acme Corp",
                ),
            ]
            csv_path = _write_csv(tmp_path, rows)

            result = IntercomExtractor.load_support_metrics_from_csv(
                str(csv_path), lookback_days=30, as_of_date=self.AS_OF,
            )

            assert result["acme corp"]["p1_p2_volume"] == 1, f"Failed for tag: {tag}"
