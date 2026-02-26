from __future__ import annotations

"""Tests for Salesforce extractor (Financial, Relationship, Qualitative)."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.extractors.salesforce import SalesforceExtractor


@pytest.fixture
def extractor():
    """SalesforceExtractor with a mocked simple_salesforce.Salesforce client."""
    with patch("src.extractors.salesforce.Salesforce") as MockSF:
        mock_sf = MagicMock()
        MockSF.return_value = mock_sf
        ext = SalesforceExtractor(
            username="user@example.com",
            password="pass",
            security_token="tok",
        )
    return ext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _query_result(records: list[dict], total: int | None = None) -> dict:
    return {
        "totalSize": total if total is not None else len(records),
        "records": records,
    }


def _account_record(arr: float = 100000, tier: str = "Paid") -> dict:
    return {
        "Id": "001ABC",
        "ARR__c": arr,
        "Success_Tier__c": tier,
    }


# ---------------------------------------------------------------------------
# TestExtractFinancialMetrics
# ---------------------------------------------------------------------------

class TestExtractFinancialMetrics:
    def test_happy_path_all_metrics(self, extractor):
        extractor.sf.Account.get.return_value = _account_record(arr=150000, tier="Paid")

        # Renewal query
        renewal_date = "2025-08-15"
        renewal_result = _query_result([{"CloseDate": renewal_date}])

        # Payment, contract, ARR history queries
        payment_result = _query_result([], total=2)
        contract_result = _query_result([], total=1)
        arr_history = _query_result([{"ARR__c": 120000}])

        extractor.sf.query.side_effect = [
            renewal_result,
            payment_result,
            contract_result,
            arr_history,
        ]

        result = extractor.extract_financial_metrics("001ABC")

        assert result["days_to_renewal"] is not None
        assert result["payment_health"] == 2
        assert result["contract_changes"] == 1
        assert result["arr_trajectory_pct"] == 25.0  # (150k - 120k) / 120k * 100
        assert result["tier_alignment"] == 0  # 150k < 200k

    def test_no_renewal_opportunity(self, extractor):
        extractor.sf.Account.get.return_value = _account_record()
        extractor.sf.query.side_effect = [
            _query_result([]),          # No renewal
            _query_result([], total=0), # payment
            _query_result([], total=0), # contract
            _query_result([]),          # arr history
        ]

        result = extractor.extract_financial_metrics("001ABC")
        assert result["days_to_renewal"] is None

    def test_payment_query_failure_graceful(self, extractor):
        extractor.sf.Account.get.return_value = _account_record()

        def query_side_effect(q):
            if "Payment_Record__c" in q:
                raise Exception("SOQL error")
            if "Renewal" in q:
                return _query_result([])
            if "Downgrade" in q or "Amendment" in q:
                return _query_result([], total=0)
            if "Account_History__c" in q:
                return _query_result([])
            return _query_result([], total=0)

        extractor.sf.query.side_effect = query_side_effect

        result = extractor.extract_financial_metrics("001ABC")
        assert result["payment_health"] == 0  # falls back to 0

    def test_contract_query_failure_graceful(self, extractor):
        extractor.sf.Account.get.return_value = _account_record()

        def query_side_effect(q):
            if "Downgrade" in q or "Amendment" in q:
                raise Exception("SOQL error")
            if "Renewal" in q:
                return _query_result([])
            if "Payment_Record__c" in q:
                return _query_result([], total=0)
            if "Account_History__c" in q:
                return _query_result([])
            return _query_result([], total=0)

        extractor.sf.query.side_effect = query_side_effect

        result = extractor.extract_financial_metrics("001ABC")
        assert result["contract_changes"] == 0

    def test_arr_trajectory_calculation(self, extractor):
        extractor.sf.Account.get.return_value = _account_record(arr=200000)
        extractor.sf.query.side_effect = [
            _query_result([]),          # renewal
            _query_result([], total=0), # payment
            _query_result([], total=0), # contract
            _query_result([{"ARR__c": 250000}]),  # old ARR was higher
        ]

        result = extractor.extract_financial_metrics("001ABC")
        # (200k - 250k) / 250k * 100 = -20%
        assert result["arr_trajectory_pct"] == -20.0

    def test_arr_trajectory_zero_old_arr(self, extractor):
        extractor.sf.Account.get.return_value = _account_record(arr=100000)
        extractor.sf.query.side_effect = [
            _query_result([]),
            _query_result([], total=0),
            _query_result([], total=0),
            _query_result([{"ARR__c": 0}]),
        ]

        result = extractor.extract_financial_metrics("001ABC")
        assert result["arr_trajectory_pct"] == 0.0

    def test_tier_alignment_misaligned(self, extractor):
        """ARR > 200k on Standard = misaligned."""
        extractor.sf.Account.get.return_value = _account_record(arr=300000, tier="Standard")
        extractor.sf.query.side_effect = [
            _query_result([]),
            _query_result([], total=0),
            _query_result([], total=0),
            _query_result([]),
        ]

        result = extractor.extract_financial_metrics("001ABC")
        assert result["tier_alignment"] == 1

    def test_tier_alignment_aligned(self, extractor):
        """ARR <= 200k or Paid tier = aligned."""
        extractor.sf.Account.get.return_value = _account_record(arr=300000, tier="Paid")
        extractor.sf.query.side_effect = [
            _query_result([]),
            _query_result([], total=0),
            _query_result([], total=0),
            _query_result([]),
        ]

        result = extractor.extract_financial_metrics("001ABC")
        assert result["tier_alignment"] == 0

    def test_arr_history_query_failure(self, extractor):
        extractor.sf.Account.get.return_value = _account_record()

        def query_side_effect(q):
            if "Account_History__c" in q:
                raise Exception("SOQL error")
            if "Renewal" in q:
                return _query_result([])
            if "Payment_Record__c" in q:
                return _query_result([], total=0)
            if "Downgrade" in q or "Amendment" in q:
                return _query_result([], total=0)
            return _query_result([], total=0)

        extractor.sf.query.side_effect = query_side_effect

        result = extractor.extract_financial_metrics("001ABC")
        assert result["arr_trajectory_pct"] == 0.0


# ---------------------------------------------------------------------------
# TestExtractRelationshipMetrics
# ---------------------------------------------------------------------------

class TestExtractRelationshipMetrics:
    def test_happy_path(self, extractor):
        now_iso = datetime.now(timezone.utc).isoformat()
        extractor.sf.query.side_effect = [
            _query_result([], total=4),   # QBR total
            _query_result([], total=3),   # QBR attended
            _query_result([{             # Champion
                "Contact": {"LastModifiedDate": now_iso}
            }]),
            _query_result([], total=2),   # CSQLs
        ]

        result = extractor.extract_relationship_metrics("001ABC")

        assert result is not None
        assert result["qbr_attendance_pct"] == 75.0  # 3/4
        assert result["champion_stability"] is not None
        assert result["expansion_signals"] == 2

    def test_returns_none_when_fields_dont_exist(self, extractor):
        extractor.sf.query.side_effect = Exception("Object not found")

        result = extractor.extract_relationship_metrics("001ABC")
        assert result is None

    def test_no_qbrs_scheduled(self, extractor):
        extractor.sf.query.side_effect = [
            _query_result([], total=0),   # QBR total = 0
            _query_result([], total=0),   # QBR attended
            _query_result([]),            # No champion
            _query_result([], total=0),   # CSQLs
        ]

        result = extractor.extract_relationship_metrics("001ABC")
        assert result["qbr_attendance_pct"] is None  # 0/0 = None

    def test_no_champion(self, extractor):
        extractor.sf.query.side_effect = [
            _query_result([], total=2),
            _query_result([], total=1),
            _query_result([]),             # No champion record
            _query_result([], total=0),
        ]

        result = extractor.extract_relationship_metrics("001ABC")
        assert result["champion_stability"] is None

    def test_csql_query_failure(self, extractor):
        """CSQL query failure returns 0 expansion signals."""
        call_count = [0]
        qbr_responses = [
            _query_result([], total=1),
            _query_result([], total=1),
            _query_result([]),
        ]

        def query_side_effect(q):
            if "CSQL__c" in q:
                raise Exception("Object not found")
            result = qbr_responses[call_count[0]]
            call_count[0] += 1
            return result

        extractor.sf.query.side_effect = query_side_effect

        result = extractor.extract_relationship_metrics("001ABC")
        assert result["expansion_signals"] == 0


# ---------------------------------------------------------------------------
# TestExtractQualitativeSignals
# ---------------------------------------------------------------------------

class TestExtractQualitativeSignals:
    def test_mixed_signals(self, extractor):
        signals = [
            {"Severity__c": "Critical", "Confidence__c": "Suspected", "Status__c": "Active"},
            {"Severity__c": "Moderate", "Confidence__c": "Confirmed", "Status__c": "Active"},
            {"Severity__c": "Watch", "Confidence__c": "Suspected", "Status__c": "Active"},
            {"Severity__c": "Critical", "Confidence__c": "Confirmed", "Status__c": "Monitoring"},
        ]
        extractor.sf.query.return_value = _query_result(signals)

        result = extractor.extract_qualitative_signals("001ABC")

        assert result["critical_count"] == 1
        # 1 active moderate + 1 monitoring critical (reduced to moderate)
        assert result["moderate_count"] == 2
        assert result["watch_count"] == 1
        assert result["has_critical_confirmed"] is False  # only Active ones count

    def test_no_signals(self, extractor):
        extractor.sf.query.return_value = _query_result([])

        result = extractor.extract_qualitative_signals("001ABC")

        assert result["critical_count"] == 0
        assert result["moderate_count"] == 0
        assert result["watch_count"] == 0
        assert result["has_critical_confirmed"] is False
        assert result["signals"] == []

    def test_confirmed_critical_detection(self, extractor):
        signals = [
            {"Severity__c": "Critical", "Confidence__c": "Confirmed", "Status__c": "Active"},
        ]
        extractor.sf.query.return_value = _query_result(signals)

        result = extractor.extract_qualitative_signals("001ABC")

        assert result["has_critical_confirmed"] is True
        assert result["critical_count"] == 1

    def test_monitoring_severity_reduction(self, extractor):
        """Monitoring signals are reduced by one tier."""
        signals = [
            {"Severity__c": "Critical", "Confidence__c": "Suspected", "Status__c": "Monitoring"},
            {"Severity__c": "Moderate", "Confidence__c": "Suspected", "Status__c": "Monitoring"},
        ]
        extractor.sf.query.return_value = _query_result(signals)

        result = extractor.extract_qualitative_signals("001ABC")

        # Monitoring Critical → counts as Moderate
        # Monitoring Moderate → counts as Watch
        assert result["critical_count"] == 0
        assert result["moderate_count"] == 1  # from monitoring critical
        assert result["watch_count"] == 1     # from monitoring moderate

    def test_signals_list_returned(self, extractor):
        signals = [
            {"Id": "s1", "Severity__c": "Watch", "Status__c": "Active"},
        ]
        extractor.sf.query.return_value = _query_result(signals)

        result = extractor.extract_qualitative_signals("001ABC")
        assert result["signals"] == signals


# ---------------------------------------------------------------------------
# TestGetAllAccounts
# ---------------------------------------------------------------------------

class TestGetAllAccounts:
    def test_no_filter(self, extractor):
        accounts = [
            {"Id": "001", "Name": "Acme", "ARR__c": 100000, "Success_Tier__c": "Paid"},
            {"Id": "002", "Name": "Beta", "ARR__c": 50000, "Success_Tier__c": "Standard"},
        ]
        extractor.sf.query_all.return_value = _query_result(accounts)

        result = extractor.get_all_accounts()

        assert len(result) == 2
        # Verify no WHERE clause
        call_args = extractor.sf.query_all.call_args[0][0]
        assert "WHERE" not in call_args

    def test_segment_filter(self, extractor):
        accounts = [
            {"Id": "001", "Name": "Acme", "ARR__c": 100000, "Success_Tier__c": "Paid"},
        ]
        extractor.sf.query_all.return_value = _query_result(accounts)

        result = extractor.get_all_accounts(segment="Paid")

        assert len(result) == 1
        call_args = extractor.sf.query_all.call_args[0][0]
        assert "WHERE Success_Tier__c = 'Paid'" in call_args

    def test_empty_result(self, extractor):
        extractor.sf.query_all.return_value = _query_result([])

        result = extractor.get_all_accounts()
        assert result == []


# ---------------------------------------------------------------------------
# TestConstructorAuthModes
# ---------------------------------------------------------------------------

class TestConstructorAuthModes:
    def test_access_token_auth(self):
        with patch("src.extractors.salesforce.Salesforce") as MockSF:
            SalesforceExtractor(
                instance_url="https://na1.salesforce.com",
                access_token="session-id-123",
            )
            MockSF.assert_called_once_with(
                instance_url="https://na1.salesforce.com",
                session_id="session-id-123",
            )

    def test_client_id_auth(self):
        with patch("src.extractors.salesforce.Salesforce") as MockSF:
            SalesforceExtractor(
                username="user@example.com",
                password="pass",
                security_token="tok",
                client_id="cid",
                client_secret="csec",
            )
            MockSF.assert_called_once_with(
                username="user@example.com",
                password="pass",
                security_token="tok",
                domain="login",
                consumer_key="cid",
                consumer_secret="csec",
            )

    def test_basic_auth(self):
        with patch("src.extractors.salesforce.Salesforce") as MockSF:
            SalesforceExtractor(
                username="user@example.com",
                password="pass",
                security_token="tok",
            )
            MockSF.assert_called_once_with(
                username="user@example.com",
                password="pass",
                security_token="tok",
                domain="login",
            )

    def test_missing_security_token_defaults_empty(self):
        with patch("src.extractors.salesforce.Salesforce") as MockSF:
            SalesforceExtractor(
                username="user@example.com",
                password="pass",
            )
            MockSF.assert_called_once_with(
                username="user@example.com",
                password="pass",
                security_token="",
                domain="login",
            )
