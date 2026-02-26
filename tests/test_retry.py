from __future__ import annotations

"""Tests for retry helpers (mount_retry_adapter, retry_on_transient)."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from src.extractors.retry import mount_retry_adapter, retry_on_transient


# ---------------------------------------------------------------------------
# TestMountRetryAdapter
# ---------------------------------------------------------------------------

class TestMountRetryAdapter:
    def test_mounts_adapters_on_session(self):
        session = requests.Session()
        mount_retry_adapter(session)

        https_adapter = session.get_adapter("https://example.com")
        http_adapter = session.get_adapter("http://example.com")

        assert https_adapter.max_retries.total == 3
        assert http_adapter.max_retries.total == 3

    def test_custom_total(self):
        session = requests.Session()
        mount_retry_adapter(session, total=5)

        adapter = session.get_adapter("https://example.com")
        assert adapter.max_retries.total == 5

    def test_custom_backoff_factor(self):
        session = requests.Session()
        mount_retry_adapter(session, backoff_factor=2.0)

        adapter = session.get_adapter("https://example.com")
        assert adapter.max_retries.backoff_factor == 2.0

    def test_custom_status_forcelist(self):
        session = requests.Session()
        custom_statuses = frozenset({503, 504})
        mount_retry_adapter(session, status_forcelist=custom_statuses)

        adapter = session.get_adapter("https://example.com")
        assert set(adapter.max_retries.status_forcelist) == {503, 504}

    def test_default_status_forcelist(self):
        session = requests.Session()
        mount_retry_adapter(session)

        adapter = session.get_adapter("https://example.com")
        assert set(adapter.max_retries.status_forcelist) == {429, 500, 502, 503, 504}


# ---------------------------------------------------------------------------
# TestRetryOnTransient
# ---------------------------------------------------------------------------

class TestRetryOnTransient:
    def test_succeeds_first_try(self):
        call_count = 0

        @retry_on_transient(max_retries=3, backoff_factor=0)
        def fn():
            nonlocal call_count
            call_count += 1
            return "ok"

        assert fn() == "ok"
        assert call_count == 1

    @patch("src.extractors.retry.time.sleep")
    def test_retries_on_exception(self, mock_sleep):
        call_count = 0

        @retry_on_transient(max_retries=3, backoff_factor=0)
        def fn():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("transient")
            return "ok"

        assert fn() == "ok"
        assert call_count == 3
        assert mock_sleep.call_count == 2

    @patch("src.extractors.retry.time.sleep")
    def test_raises_after_max_retries(self, mock_sleep):
        @retry_on_transient(max_retries=3, backoff_factor=0)
        def fn():
            raise ConnectionError("always fails")

        with pytest.raises(ConnectionError, match="always fails"):
            fn()

        assert mock_sleep.call_count == 2  # sleeps between attempts 1-2 and 2-3

    @patch("src.extractors.retry.time.sleep")
    def test_exponential_backoff(self, mock_sleep):
        call_count = 0

        @retry_on_transient(max_retries=4, backoff_factor=1.0)
        def fn():
            nonlocal call_count
            call_count += 1
            if call_count < 4:
                raise ConnectionError("transient")
            return "ok"

        fn()
        # Backoff: 1*2^0=1s, 1*2^1=2s, 1*2^2=4s
        delays = [call.args[0] for call in mock_sleep.call_args_list]
        assert delays == [1.0, 2.0, 4.0]

    @patch("src.extractors.retry.time.sleep")
    def test_only_retries_specified_exceptions(self, mock_sleep):
        """Non-matching exceptions are raised immediately without retry."""
        call_count = 0

        @retry_on_transient(
            max_retries=3,
            backoff_factor=0,
            transient_exceptions=(ConnectionError,),
        )
        def fn():
            nonlocal call_count
            call_count += 1
            raise ValueError("not transient")

        with pytest.raises(ValueError, match="not transient"):
            fn()

        assert call_count == 1  # no retries
        assert mock_sleep.call_count == 0

    @patch("src.extractors.retry.time.sleep")
    def test_preserves_function_metadata(self, mock_sleep):
        @retry_on_transient(max_retries=2, backoff_factor=0)
        def my_function():
            """My docstring."""
            return 42

        assert my_function.__name__ == "my_function"
        assert my_function.__doc__ == "My docstring."

    @patch("src.extractors.retry.time.sleep")
    def test_works_with_method(self, mock_sleep):
        """Retry works correctly on instance methods."""
        class MyClient:
            def __init__(self):
                self.call_count = 0

            @retry_on_transient(max_retries=3, backoff_factor=0)
            def fetch(self):
                self.call_count += 1
                if self.call_count < 2:
                    raise ConnectionError("transient")
                return "data"

        client = MyClient()
        assert client.fetch() == "data"
        assert client.call_count == 2


# ---------------------------------------------------------------------------
# TestExtractorRetryIntegration
# ---------------------------------------------------------------------------

class TestExtractorRetryIntegration:
    """Verify retry is wired into extractor constructors."""

    def test_intercom_session_has_retry(self):
        from src.extractors.intercom import IntercomExtractor
        ext = IntercomExtractor(api_token="test-token")
        adapter = ext.session.get_adapter("https://api.intercom.io")
        assert adapter.max_retries.total == 3

    def test_jira_session_has_retry(self):
        from src.extractors.jira import JiraExtractor
        ext = JiraExtractor(
            base_url="https://jira.example.com",
            email="test@example.com",
            api_token="test-token",
        )
        adapter = ext.session.get_adapter("https://jira.example.com")
        assert adapter.max_retries.total == 3

    def test_salesforce_mounts_retry_on_session(self):
        """When SF client has a real session, retry adapter is mounted."""
        mock_session = requests.Session()
        with patch("src.extractors.salesforce.Salesforce") as MockSF:
            mock_sf = MagicMock()
            mock_sf.session = mock_session
            MockSF.return_value = mock_sf

            from src.extractors.salesforce import SalesforceExtractor
            SalesforceExtractor(
                username="user",
                password="pass",
                security_token="tok",
            )

        adapter = mock_session.get_adapter("https://example.com")
        assert adapter.max_retries.total == 3

    def test_salesforce_skips_retry_without_session(self):
        """When SF client has no session attr (mock), no error is raised."""
        with patch("src.extractors.salesforce.Salesforce") as MockSF:
            mock_sf = MagicMock(spec=[])  # no attributes
            MockSF.return_value = mock_sf

            from src.extractors.salesforce import SalesforceExtractor
            # Should not raise
            SalesforceExtractor(
                username="user",
                password="pass",
                security_token="tok",
            )
