from __future__ import annotations

"""Salesforce extractor for Financial & Contract, Relationship, and Qualitative Signal data.

Reads from:
- Account: ARR, Success Tier, custom fields
- Opportunity: Renewal dates, contract changes
- Churn_Signal__c: Active qualitative signals (for modifier)
- Relationship fields (Phase 2): QBR attendance, champion tracking, etc.
"""

import logging
from datetime import datetime, timezone

from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


class SalesforceExtractor:
    def __init__(
        self,
        username: str | None = None,
        password: str | None = None,
        security_token: str | None = None,
        domain: str = "login",
        client_id: str | None = None,
        client_secret: str | None = None,
        instance_url: str | None = None,
        access_token: str | None = None,
    ):
        if access_token and instance_url:
            self.sf = Salesforce(
                instance_url=instance_url,
                session_id=access_token,
            )
        elif client_id and client_secret:
            self.sf = Salesforce(
                username=username,
                password=password,
                security_token=security_token or "",
                domain=domain,
                consumer_key=client_id,
                consumer_secret=client_secret,
            )
        else:
            self.sf = Salesforce(
                username=username,
                password=password,
                security_token=security_token or "",
                domain=domain,
            )

    def extract_financial_metrics(self, sf_account_id: str) -> dict:
        """Extract Financial & Contract metrics for one Account.

        Returns:
            dict with keys:
                days_to_renewal, payment_health, contract_changes,
                arr_trajectory_pct, tier_alignment
        """
        # Get Account fields
        account = self.sf.Account.get(sf_account_id)
        current_arr = float(account.get("ARR__c") or 0)
        success_tier = account.get("Success_Tier__c", "Standard")

        # Days to renewal: find next open Renewal Opportunity
        renewal_query = (
            f"SELECT CloseDate FROM Opportunity "
            f"WHERE AccountId = '{sf_account_id}' "
            f"AND StageName != 'Closed Lost' "
            f"AND Type = 'Renewal' "
            f"ORDER BY CloseDate ASC LIMIT 1"
        )
        renewals = self.sf.query(renewal_query)
        days_to_renewal = None
        if renewals["totalSize"] > 0:
            close_date_str = renewals["records"][0]["CloseDate"]
            close_date = datetime.strptime(close_date_str, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
            days_to_renewal = (close_date - datetime.now(timezone.utc)).days

        # Payment health: count failed payments in last 12 months
        payment_query = (
            f"SELECT COUNT() FROM Payment_Record__c "
            f"WHERE Account__c = '{sf_account_id}' "
            f"AND Status__c = 'Failed' "
            f"AND Payment_Date__c = LAST_N_MONTHS:12"
        )
        try:
            payment_result = self.sf.query(payment_query)
            payment_failures = payment_result["totalSize"]
        except Exception:
            logger.warning("Payment records query failed for %s — assuming 0", sf_account_id)
            payment_failures = 0

        # Contract changes: module removals or term shortenings in last 12 months
        contract_query = (
            f"SELECT COUNT() FROM Opportunity "
            f"WHERE AccountId = '{sf_account_id}' "
            f"AND (Type = 'Downgrade' OR Type = 'Amendment') "
            f"AND CloseDate = LAST_N_MONTHS:12 "
            f"AND StageName = 'Closed Won'"
        )
        try:
            contract_result = self.sf.query(contract_query)
            contract_changes = contract_result["totalSize"]
        except Exception:
            logger.warning("Contract changes query failed for %s — assuming 0", sf_account_id)
            contract_changes = 0

        # ARR trajectory: compare current ARR to 12 months ago
        arr_history_query = (
            f"SELECT ARR__c FROM Account_History__c "
            f"WHERE Account__c = '{sf_account_id}' "
            f"AND Snapshot_Date__c = LAST_N_MONTHS:12 "
            f"ORDER BY Snapshot_Date__c ASC LIMIT 1"
        )
        arr_trajectory_pct = 0.0
        try:
            arr_history = self.sf.query(arr_history_query)
            if arr_history["totalSize"] > 0:
                old_arr = float(arr_history["records"][0].get("ARR__c") or 0)
                if old_arr > 0:
                    arr_trajectory_pct = round(
                        ((current_arr - old_arr) / old_arr) * 100, 1
                    )
        except Exception:
            logger.warning("ARR history query failed for %s — using 0%%", sf_account_id)

        # Tier alignment: flag if ARR > $200k but on Standard Success
        tier_misaligned = (
            1 if current_arr > 200000 and success_tier == "Standard" else 0
        )

        return {
            "days_to_renewal": days_to_renewal,
            "payment_health": payment_failures,
            "contract_changes": contract_changes,
            "arr_trajectory_pct": arr_trajectory_pct,
            "tier_alignment": tier_misaligned,
        }

    def extract_relationship_metrics(self, sf_account_id: str) -> dict | None:
        """Extract Relationship & Expansion metrics (Phase 2 — CSM input fields).

        Returns None if Phase 2 fields don't exist yet, signalling to the scoring
        engine to reweight this dimension.
        """
        try:
            # QBR attendance
            qbr_query = (
                f"SELECT COUNT() FROM Event "
                f"WHERE AccountId = '{sf_account_id}' "
                f"AND Type = 'QBR' "
                f"AND ActivityDate = LAST_N_MONTHS:12"
            )
            qbr_total = self.sf.query(qbr_query)["totalSize"]

            qbr_attended_query = (
                f"SELECT COUNT() FROM Event "
                f"WHERE AccountId = '{sf_account_id}' "
                f"AND Type = 'QBR' "
                f"AND Attended__c = true "
                f"AND ActivityDate = LAST_N_MONTHS:12"
            )
            qbr_attended = self.sf.query(qbr_attended_query)["totalSize"]
            qbr_attendance_pct = (
                round((qbr_attended / qbr_total) * 100, 1) if qbr_total > 0 else None
            )

            # Champion stability: days since champion contact role changed
            champion_query = (
                f"SELECT Contact.LastModifiedDate FROM AccountContactRelation "
                f"WHERE AccountId = '{sf_account_id}' "
                f"AND Roles = 'Champion' "
                f"ORDER BY Contact.LastModifiedDate DESC LIMIT 1"
            )
            champion_result = self.sf.query(champion_query)
            champion_stability = None
            if champion_result["totalSize"] > 0:
                last_modified = champion_result["records"][0]["Contact"]["LastModifiedDate"]
                mod_date = datetime.fromisoformat(last_modified.replace("Z", "+00:00"))
                champion_stability = (datetime.now(timezone.utc) - mod_date).days

            # Expansion signals: count of open CSQLs
            csql_query = (
                f"SELECT COUNT() FROM CSQL__c "
                f"WHERE Account__c = '{sf_account_id}' "
                f"AND Status__c = 'Open'"
            )
            expansion_count = 0
            try:
                expansion_count = self.sf.query(csql_query)["totalSize"]
            except Exception:
                pass

            return {
                "qbr_attendance_pct": qbr_attendance_pct,
                "responsiveness": None,  # Requires CSM input field — Phase 2
                "champion_stability": champion_stability,
                "exec_engagement": None,  # Requires CSM input field — Phase 2
                "expansion_signals": expansion_count,
            }

        except Exception:
            logger.info(
                "Relationship metrics not available for %s — Phase 2 fields may not exist",
                sf_account_id,
            )
            return None

    def extract_qualitative_signals(self, sf_account_id: str) -> dict:
        """Extract active Churn Signal records for qualitative modifier calculation.

        Returns:
            dict with keys:
                critical_count, moderate_count, watch_count,
                has_critical_confirmed, signals (list of signal records)
        """
        query = (
            f"SELECT Id, Signal_Category__c, Signal_Detail__c, Severity__c, "
            f"Confidence__c, Status__c, Date_Observed__c "
            f"FROM Churn_Signal__c "
            f"WHERE Account__c = '{sf_account_id}' "
            f"AND Status__c IN ('Active', 'Monitoring')"
        )
        result = self.sf.query(query)
        signals = result.get("records", [])

        active_signals = [s for s in signals if s.get("Status__c") == "Active"]

        critical_count = sum(
            1 for s in active_signals if s.get("Severity__c") == "Critical"
        )
        moderate_count = sum(
            1 for s in active_signals if s.get("Severity__c") == "Moderate"
        )
        watch_count = sum(
            1 for s in active_signals if s.get("Severity__c") == "Watch"
        )
        has_critical_confirmed = any(
            s.get("Severity__c") == "Critical" and s.get("Confidence__c") == "Confirmed"
            for s in active_signals
        )

        # Monitoring signals: reduced one tier
        monitoring_signals = [s for s in signals if s.get("Status__c") == "Monitoring"]
        monitoring_critical_as_moderate = sum(
            1 for s in monitoring_signals if s.get("Severity__c") == "Critical"
        )
        monitoring_moderate_as_watch = sum(
            1 for s in monitoring_signals if s.get("Severity__c") == "Moderate"
        )

        # Add monitoring signals at reduced severity
        moderate_count += monitoring_critical_as_moderate
        watch_count += monitoring_moderate_as_watch

        return {
            "critical_count": critical_count,
            "moderate_count": moderate_count,
            "watch_count": watch_count,
            "has_critical_confirmed": has_critical_confirmed,
            "signals": signals,
        }

    def get_all_accounts(self, segment: str | None = None) -> list[dict]:
        """Fetch all Account records for scoring.

        Args:
            segment: Optional filter — 'Paid' or 'Standard'. None = all.

        Returns:
            List of Account dicts with Id, Name, ARR__c, Success_Tier__c.
        """
        where_clause = ""
        if segment:
            where_clause = f"WHERE Success_Tier__c = '{segment}'"

        query = (
            f"SELECT Id, Name, ARR__c, Success_Tier__c "
            f"FROM Account {where_clause} "
            f"ORDER BY Name"
        )
        result = self.sf.query_all(query)
        return result.get("records", [])
