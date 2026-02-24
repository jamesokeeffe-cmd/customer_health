from __future__ import annotations

"""Tests for Jira bug metrics extractor."""

from unittest.mock import MagicMock, patch

import pytest

from src.extractors.jira import JiraExtractor


@pytest.fixture
def extractor():
    return JiraExtractor(
        base_url="https://test.atlassian.net",
        email="user@example.com",
        api_token="fake-token",
    )


def _make_issue(priority_name: str) -> dict:
    return {"fields": {"priority": {"name": priority_name}}}


def _mock_search_response(issues: list[dict], total: int | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {
        "issues": issues,
        "total": total if total is not None else len(issues),
        "startAt": 0,
        "maxResults": 100,
    }
    resp.raise_for_status = MagicMock()
    return resp


class TestExtractBugMetrics:
    def test_no_open_bugs(self, extractor):
        """No open bugs returns zeroes."""
        with patch.object(extractor.session, "post", return_value=_mock_search_response([])):
            result = extractor.extract_bug_metrics("ENG", "Acme Corp")

        assert result == {"open_bugs_total": 0, "open_bugs_p1_p2": 0}

    def test_mixed_priorities(self, extractor):
        """Mixed priority bugs correctly count P1/P2."""
        issues = [
            _make_issue("Highest"),    # P1/P2
            _make_issue("High"),       # P1/P2
            _make_issue("Medium"),     # not P1/P2
            _make_issue("Low"),        # not P1/P2
            _make_issue("Critical"),   # P1/P2
            _make_issue("Blocker"),    # P1/P2
            _make_issue("Lowest"),     # not P1/P2
        ]
        with patch.object(extractor.session, "post", return_value=_mock_search_response(issues)):
            result = extractor.extract_bug_metrics("ENG", "Acme Corp")

        assert result["open_bugs_total"] == 7
        assert result["open_bugs_p1_p2"] == 4

    def test_all_p1_p2(self, extractor):
        """All high-priority bugs."""
        issues = [
            _make_issue("Critical"),
            _make_issue("Blocker"),
            _make_issue("Highest"),
        ]
        with patch.object(extractor.session, "post", return_value=_mock_search_response(issues)):
            result = extractor.extract_bug_metrics("PROJ", "Customer X")

        assert result["open_bugs_total"] == 3
        assert result["open_bugs_p1_p2"] == 3

    def test_no_p1_p2(self, extractor):
        """All low-priority bugs."""
        issues = [
            _make_issue("Medium"),
            _make_issue("Low"),
            _make_issue("Lowest"),
        ]
        with patch.object(extractor.session, "post", return_value=_mock_search_response(issues)):
            result = extractor.extract_bug_metrics("PROJ", "Customer Y")

        assert result["open_bugs_total"] == 3
        assert result["open_bugs_p1_p2"] == 0

    def test_component_name_in_jql(self, extractor):
        """Component name is included in the JQL query."""
        mock_resp = _mock_search_response([])
        with patch.object(extractor.session, "post", return_value=mock_resp) as mock_post:
            extractor.extract_bug_metrics("ENG", "Acme Corp")

        call_args = mock_post.call_args
        jql = call_args[1]["json"]["jql"] if "json" in call_args[1] else call_args[0][1]["jql"]
        assert 'component = "Acme Corp"' in jql
        assert 'project = "ENG"' in jql

    def test_missing_priority_field(self, extractor):
        """Issues with no priority field are not counted as P1/P2."""
        issues = [
            {"fields": {"priority": None}},
            {"fields": {}},
        ]
        with patch.object(extractor.session, "post", return_value=_mock_search_response(issues)):
            result = extractor.extract_bug_metrics("ENG", "Acme Corp")

        assert result["open_bugs_total"] == 2
        assert result["open_bugs_p1_p2"] == 0

    def test_pagination(self, extractor):
        """Handles multiple pages of results."""
        page1_issues = [_make_issue("High")] * 3
        page2_issues = [_make_issue("Low")] * 2

        page1_resp = MagicMock()
        page1_resp.json.return_value = {
            "issues": page1_issues,
            "total": 5,
            "startAt": 0,
            "maxResults": 3,
        }
        page1_resp.raise_for_status = MagicMock()

        page2_resp = MagicMock()
        page2_resp.json.return_value = {
            "issues": page2_issues,
            "total": 5,
            "startAt": 3,
            "maxResults": 3,
        }
        page2_resp.raise_for_status = MagicMock()

        with patch.object(extractor.session, "post", side_effect=[page1_resp, page2_resp]):
            result = extractor.extract_bug_metrics("ENG", "Acme Corp")

        assert result["open_bugs_total"] == 5
        assert result["open_bugs_p1_p2"] == 3

    def test_case_insensitive_priority_matching(self, extractor):
        """Priority names are matched case-insensitively."""
        issues = [
            _make_issue("HIGH"),
            _make_issue("critical"),
            _make_issue("BLOCKER"),
            _make_issue("medium"),
        ]
        with patch.object(extractor.session, "post", return_value=_mock_search_response(issues)):
            result = extractor.extract_bug_metrics("ENG", "Test")

        assert result["open_bugs_p1_p2"] == 3


class TestSearchIssues:
    def test_builds_correct_url(self, extractor):
        """Search URL is built from base_url."""
        mock_resp = _mock_search_response([])
        with patch.object(extractor.session, "post", return_value=mock_resp) as mock_post:
            extractor._search_issues("project = TEST", fields=["priority"])

        mock_post.assert_called_once()
        url = mock_post.call_args[0][0]
        assert url == "https://test.atlassian.net/rest/api/3/search"

    def test_trailing_slash_stripped(self):
        """Base URL trailing slash is handled."""
        ext = JiraExtractor(
            base_url="https://test.atlassian.net/",
            email="user@example.com",
            api_token="fake",
        )
        mock_resp = _mock_search_response([])
        with patch.object(ext.session, "post", return_value=mock_resp) as mock_post:
            ext._search_issues("project = TEST", fields=["priority"])

        url = mock_post.call_args[0][0]
        assert url == "https://test.atlassian.net/rest/api/3/search"
