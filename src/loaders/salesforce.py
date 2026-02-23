"""Salesforce loader: writes Health_Score__c records.

Creates one Health_Score__c record per Account per scoring period (append model).
Also supports dry-run mode: outputs to CSV instead of writing to Salesforce.
"""

import csv
import logging
from datetime import datetime, timezone
from pathlib import Path

from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


class SalesforceLoader:
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

    def write_health_score(
        self, sf_account_id: str, scoring_result: dict, scoring_period: str
    ) -> str:
        """Create a Health_Score__c record in Salesforce.

        Args:
            sf_account_id: The Salesforce Account ID.
            scoring_result: Full scoring result dict from the orchestrator.
            scoring_period: e.g. "2025-02" for monthly cadence.

        Returns:
            The ID of the created Health_Score__c record.
        """
        record = self._build_record(sf_account_id, scoring_result, scoring_period)

        result = self.sf.Health_Score__c.create(record)
        record_id = result.get("id")
        logger.info(
            "Created Health_Score__c %s for Account %s period %s",
            record_id, sf_account_id, scoring_period,
        )
        return record_id

    def _build_record(
        self, sf_account_id: str, scoring_result: dict, scoring_period: str
    ) -> dict:
        """Build the Health_Score__c field dict from scoring results."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Dimension scores
        dims = scoring_result.get("dimension_scores", {})
        support = dims.get("support_health", {})
        financial = dims.get("financial_contract", {})
        adoption = dims.get("adoption_engagement", {})
        relationship = dims.get("relationship_expansion", {})

        # Metric-level scores
        support_metrics = support.get("metric_scores", {})
        financial_metrics = financial.get("metric_scores", {})
        adoption_metrics = adoption.get("metric_scores", {})

        # Platform value
        pvs = scoring_result.get("platform_value", {})
        pvs_pillars = pvs.get("pillar_scores", {})

        # Composite scores
        composite = scoring_result.get("composite", {})
        qual = scoring_result.get("qualitative", {})

        record = {
            "Account__c": sf_account_id,
            "Scoring_Date__c": now,
            "Scoring_Period__c": scoring_period,
            # Support Health metrics
            "Support_P1P2_Volume__c": support_metrics.get("p1_p2_volume"),
            "Support_First_Response__c": support_metrics.get("first_response_minutes"),
            "Support_Close_Time__c": support_metrics.get("close_time_hours"),
            "Support_Reopen_Rate__c": support_metrics.get("reopen_rate_pct"),
            "Support_Escalation_Rate__c": support_metrics.get("escalation_rate_pct"),
            # Financial metrics
            "Financial_Days_To_Renewal__c": financial_metrics.get("days_to_renewal"),
            "Financial_Payment_Health__c": financial_metrics.get("payment_health"),
            "Financial_Contract_Changes__c": financial_metrics.get("contract_changes"),
            "Financial_ARR_Trajectory__c": financial_metrics.get("arr_trajectory_pct"),
            "Financial_Tier_Alignment__c": financial_metrics.get("tier_alignment"),
            # Adoption metrics
            "Adoption_Login_Trend__c": adoption_metrics.get("staff_login_trend"),
            "Adoption_Admin_Logins__c": adoption_metrics.get("admin_login_trend"),
            "Adoption_Feature_Breadth__c": adoption_metrics.get("feature_breadth_pct"),
            "Adoption_Platform_Score__c": adoption_metrics.get("platform_score"),
            "Adoption_Platform_Trend__c": adoption_metrics.get("platform_score_trend"),
            # Platform Value Score pillars
            "PVS_Messaging__c": pvs_pillars.get("messaging"),
            "PVS_Automations__c": pvs_pillars.get("automations"),
            "PVS_Contactless__c": pvs_pillars.get("contactless"),
            "PVS_Requests__c": pvs_pillars.get("requests"),
            "PVS_Staff_Adoption__c": pvs_pillars.get("staff_adoption"),
            # Dimension-level scores
            "Support_Health_Score__c": support.get("score"),
            "Financial_Contract_Score__c": financial.get("score"),
            "Adoption_Engagement_Score__c": adoption.get("score"),
            "Relationship_Expansion_Score__c": relationship.get("score"),
            "Platform_Value_Score__c": pvs.get("score"),
            # Composite scores
            "Churn_Risk_Score__c": composite.get("churn_risk_score"),
            "Quantitative_Score__c": composite.get("quantitative_score"),
            "Final_Score__c": qual.get("final_score"),
            "Health_Tier__c": composite.get("tier"),
            # Qualitative modifier
            "Qual_Active_Critical__c": qual.get("critical_count", 0),
            "Qual_Active_Moderate__c": qual.get("moderate_count", 0),
            "Qual_Active_Watch__c": qual.get("watch_count", 0),
            "Qual_Override_Active__c": qual.get("override_active", False),
            "Qual_Score_Modifier__c": qual.get("modifier_applied"),
            # Coverage
            "Scoring_Coverage__c": scoring_result.get("coverage_pct"),
        }

        # Remove None values to avoid SF API errors on non-nillable fields
        return {k: v for k, v in record.items() if v is not None}


def write_dry_run_csv(
    results: list[dict],
    output_path: str = "output/health_scores.csv",
) -> str:
    """Write scoring results to a CSV file for review (dry-run mode).

    Args:
        results: List of scoring result dicts (one per account).
        output_path: Path for the output CSV file.

    Returns:
        The output file path.
    """
    if not results:
        logger.warning("No results to write to CSV")
        return output_path

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Flatten nested dicts for CSV
    rows = []
    for r in results:
        flat = {
            "account_id": r.get("account_id"),
            "account_name": r.get("account_name"),
            "segment": r.get("segment"),
            "quantitative_score": r.get("composite", {}).get("quantitative_score"),
            "final_score": r.get("qualitative", {}).get("final_score"),
            "health_tier": r.get("composite", {}).get("tier"),
            "churn_risk_score": r.get("composite", {}).get("churn_risk_score"),
            "platform_value_score": r.get("platform_value", {}).get("score"),
            "support_health": r.get("dimension_scores", {}).get("support_health", {}).get("score"),
            "financial_contract": r.get("dimension_scores", {}).get("financial_contract", {}).get("score"),
            "adoption_engagement": r.get("dimension_scores", {}).get("adoption_engagement", {}).get("score"),
            "relationship_expansion": r.get("dimension_scores", {}).get("relationship_expansion", {}).get("score"),
            "coverage_pct": r.get("coverage_pct"),
            "qualitative_override": r.get("qualitative", {}).get("override_active"),
            "modifier_applied": r.get("qualitative", {}).get("modifier_applied"),
        }
        rows.append(flat)

    fieldnames = list(rows[0].keys())

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    logger.info("Dry-run CSV written to %s (%d accounts)", output_path, len(rows))
    return output_path
