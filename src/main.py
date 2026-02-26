from __future__ import annotations

"""Health Score Middleware Orchestrator.

Extract → Score → Load pipeline:
1. Load config (weights, thresholds, account mapping)
2. For each account: extract data from Intercom, Looker, Salesforce
3. Score each dimension, compute composite, apply qualitative modifier
4. Write Health_Score__c to Salesforce (or CSV in dry-run mode)

Execution: Monthly via AWS Lambda + EventBridge.
"""

import csv
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml

from src.extractors.intercom import IntercomExtractor
from src.extractors.jira import JiraExtractor
from src.extractors.looker import LookerExtractor
from src.extractors.salesforce import SalesforceExtractor
from src.loaders.salesforce import SalesforceLoader, write_dry_run_csv
from src.scoring.composite import classify_tier, compute_churn_risk, compute_health_score
from src.scoring.dimensions import score_dimension
from src.scoring.qualitative import apply_qualitative_modifier

logger = logging.getLogger("health_score")


def load_yaml(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def load_account_mapping(path: str) -> list[dict]:
    """Load cross-system account ID mapping from CSV.

    Expected columns: sf_account_id, intercom_company_id, looker_customer_id,
                      account_name, segment
    """
    rows = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


class HealthScoreOrchestrator:
    def __init__(
        self,
        config_dir: str = "config",
        dry_run: bool = False,
    ):
        self.dry_run = dry_run
        self.config_dir = Path(config_dir)

        # Load config
        self.weights = load_yaml(str(self.config_dir / "weights.yaml"))
        self.thresholds = load_yaml(str(self.config_dir / "thresholds.yaml"))
        self.account_mapping = load_account_mapping(
            str(self.config_dir / "account_mapping.csv")
        )

        # Extractors and loader are initialised lazily via init_clients()
        self.intercom: IntercomExtractor | None = None
        self.jira: JiraExtractor | None = None
        self.looker: LookerExtractor | None = None
        self.sf_extractor: SalesforceExtractor | None = None
        self.sf_loader: SalesforceLoader | None = None

    def init_clients(
        self,
        intercom_token: str,
        looker_base_url: str,
        looker_client_id: str,
        looker_client_secret: str,
        sf_username: str,
        sf_password: str,
        sf_token: str,
        sf_domain: str = "login",
    ):
        """Initialise API clients. Call this after loading credentials."""
        self.intercom = IntercomExtractor(api_token=intercom_token)
        self.looker = LookerExtractor.from_credentials(
            base_url=looker_base_url,
            client_id=looker_client_id,
            client_secret=looker_client_secret,
        )
        self.sf_extractor = SalesforceExtractor(
            username=sf_username,
            password=sf_password,
            security_token=sf_token,
            domain=sf_domain,
        )
        if not self.dry_run:
            self.sf_loader = SalesforceLoader(
                username=sf_username,
                password=sf_password,
                security_token=sf_token,
                domain=sf_domain,
            )

    def init_clients_from_env(self):
        """Initialise API clients from environment variables.

        Only initialises extractors whose credentials are fully present.
        Logs which extractors are available at startup.
        """
        available = []

        # Intercom
        intercom_token = os.environ.get("INTERCOM_API_TOKEN")
        if intercom_token:
            self.intercom = IntercomExtractor(api_token=intercom_token)
            available.append("Intercom")

        # Jira
        jira_base_url = os.environ.get("JIRA_BASE_URL")
        jira_email = os.environ.get("JIRA_EMAIL")
        jira_api_token = os.environ.get("JIRA_API_TOKEN")
        if jira_base_url and jira_email and jira_api_token:
            self.jira = JiraExtractor(
                base_url=jira_base_url,
                email=jira_email,
                api_token=jira_api_token,
            )
            available.append("Jira")

        # Looker
        looker_base_url = os.environ.get("LOOKER_BASE_URL")
        looker_client_id = os.environ.get("LOOKER_CLIENT_ID")
        looker_client_secret = os.environ.get("LOOKER_CLIENT_SECRET")
        if looker_base_url and looker_client_id and looker_client_secret:
            self.looker = LookerExtractor.from_credentials(
                base_url=looker_base_url,
                client_id=looker_client_id,
                client_secret=looker_client_secret,
            )
            available.append("Looker")

        # Salesforce
        sf_username = os.environ.get("SF_USERNAME")
        sf_password = os.environ.get("SF_PASSWORD")
        sf_token = os.environ.get("SF_SECURITY_TOKEN")
        sf_domain = os.environ.get("SF_DOMAIN", "login")
        if sf_username and sf_password and sf_token:
            self.sf_extractor = SalesforceExtractor(
                username=sf_username,
                password=sf_password,
                security_token=sf_token,
                domain=sf_domain,
            )
            if not self.dry_run:
                self.sf_loader = SalesforceLoader(
                    username=sf_username,
                    password=sf_password,
                    security_token=sf_token,
                    domain=sf_domain,
                )
            available.append("Salesforce")

        if available:
            logger.info("Extractors available: %s", ", ".join(available))
        else:
            logger.warning("No extractors configured — check environment variables")

    def score_account(self, account: dict) -> dict:
        """Run the full scoring pipeline for a single account.

        Args:
            account: Dict with sf_account_id, intercom_company_id,
                     looker_customer_id, account_name, segment.

        Returns:
            Full scoring result dict.
        """
        sf_id = account["sf_account_id"]
        intercom_id = account.get("intercom_company_id", "")
        looker_id = account.get("looker_customer_id", "")
        segment = account.get("segment", "standard").lower()
        account_name = account.get("account_name", sf_id)

        logger.info("Scoring account: %s (%s) [%s]", account_name, sf_id, segment)

        # ----- EXTRACT -----
        # Support Health (Intercom)
        support_raw = {}
        if self.intercom and intercom_id:
            try:
                support_raw = self.intercom.extract_support_metrics(intercom_id)
            except Exception:
                logger.exception("Intercom extraction failed for %s", account_name)

        # Support Health — Jira bug metrics (merged into support_raw)
        jira_project_key = account.get("jira_project_key", "")
        jira_component = account.get("jira_component", "")
        if self.jira and jira_project_key and jira_component:
            try:
                jira_metrics = self.jira.extract_bug_metrics(
                    project_key=jira_project_key,
                    component_name=jira_component,
                )
                support_raw.update(jira_metrics)
            except Exception:
                logger.exception("Jira extraction failed for %s", account_name)

        # Adoption & Engagement + Platform Value (Looker)
        adoption_raw = {}
        pvs_raw = {}
        if self.looker and looker_id:
            try:
                adoption_raw = self.looker.extract_adoption_metrics(looker_id)
            except Exception:
                logger.exception("Looker adoption extraction failed for %s", account_name)
            try:
                pvs_raw = self.looker.extract_platform_value_score(looker_id)
            except Exception:
                logger.exception("Looker PVS extraction failed for %s", account_name)

        # Financial & Contract (Salesforce)
        financial_raw = {}
        if self.sf_extractor:
            try:
                financial_raw = self.sf_extractor.extract_financial_metrics(sf_id)
            except Exception:
                logger.exception("SF financial extraction failed for %s", account_name)

        # Relationship & Expansion (Salesforce — Phase 2)
        relationship_raw = None
        if self.sf_extractor:
            try:
                relationship_raw = self.sf_extractor.extract_relationship_metrics(sf_id)
            except Exception:
                logger.info("Relationship metrics not available for %s", account_name)

        # Qualitative Signals (Salesforce)
        qual_data = {"critical_count": 0, "moderate_count": 0, "watch_count": 0,
                     "has_critical_confirmed": False, "signals": []}
        if self.sf_extractor:
            try:
                qual_data = self.sf_extractor.extract_qualitative_signals(sf_id)
            except Exception:
                logger.exception("SF qualitative extraction failed for %s", account_name)

        # ----- SCORE -----
        result = self._compute_scores(
            support_raw=support_raw,
            financial_raw=financial_raw,
            adoption_raw=adoption_raw,
            relationship_raw=relationship_raw,
            pvs_raw=pvs_raw,
            qual_data=qual_data,
            segment=segment,
        )

        result["account_id"] = sf_id
        result["account_name"] = account_name
        result["segment"] = segment

        return result

    def _compute_scores(
        self,
        support_raw: dict,
        financial_raw: dict,
        adoption_raw: dict,
        relationship_raw: dict | None,
        pvs_raw: dict,
        qual_data: dict,
        segment: str,
    ) -> dict:
        """Run the scoring engine on extracted data."""
        thresholds = self.thresholds
        weights = self.weights

        # Score each Churn Risk dimension
        support_result = score_dimension(
            raw_metrics=support_raw,
            metric_weights=weights["support_health"],
            thresholds=thresholds["support_health"],
            segment=segment,
        )

        financial_result = score_dimension(
            raw_metrics=financial_raw,
            metric_weights=weights["financial_contract"],
            thresholds=thresholds["financial_contract"],
            segment=segment,
        )

        adoption_result = score_dimension(
            raw_metrics=adoption_raw,
            metric_weights=weights["adoption_engagement"],
            thresholds=thresholds["adoption_engagement"],
            segment=segment,
        )

        relationship_result = {"score": None, "metric_scores": {}, "coverage": 0.0}
        if relationship_raw is not None:
            relationship_result = score_dimension(
                raw_metrics=relationship_raw,
                metric_weights=weights["relationship_expansion"],
                thresholds=thresholds["relationship_expansion"],
                segment=segment,
            )

        # Platform Value Score (normalised via score_dimension like other dimensions)
        pvs_result = score_dimension(
            raw_metrics=pvs_raw,
            metric_weights=weights["platform_value"],
            thresholds=thresholds["platform_value"],
            segment=segment,
        )

        # Churn Risk composite
        dimension_scores = {
            "support_health": support_result["score"],
            "financial_contract": financial_result["score"],
            "adoption_engagement": adoption_result["score"],
            "relationship_expansion": relationship_result["score"],
        }
        churn_risk = compute_churn_risk(
            dimension_scores=dimension_scores,
            dimension_weights=weights["churn_risk"],
        )

        # Health Score composite
        health = compute_health_score(
            churn_risk_score=churn_risk["score"],
            platform_value_score=pvs_result["score"],
            churn_risk_weight=weights["health_score"]["churn_risk_weight"],
            platform_value_weight=weights["health_score"]["platform_value_weight"],
        )

        # Qualitative modifier
        quantitative_score = health["quantitative_score"] or 0
        qual_result = apply_qualitative_modifier(
            quantitative_score=quantitative_score,
            critical_count=qual_data.get("critical_count", 0),
            moderate_count=qual_data.get("moderate_count", 0),
            watch_count=qual_data.get("watch_count", 0),
            has_critical_confirmed=qual_data.get("has_critical_confirmed", False),
        )

        # Final tier (after qualifier)
        final_tier = classify_tier(qual_result["final_score"])

        # Coverage: weighted average of dimension coverage and PVS coverage
        dim_coverages = [
            support_result["coverage"],
            financial_result["coverage"],
            adoption_result["coverage"],
            relationship_result.get("coverage", 0.0),
        ]
        dim_weights_list = list(weights["churn_risk"].values())
        churn_coverage = sum(c * w for c, w in zip(dim_coverages, dim_weights_list))
        overall_coverage = round(
            (churn_coverage * weights["health_score"]["churn_risk_weight"])
            + (pvs_result["coverage"] * weights["health_score"]["platform_value_weight"]),
            2,
        ) * 100

        return {
            "dimension_scores": {
                "support_health": support_result,
                "financial_contract": financial_result,
                "adoption_engagement": adoption_result,
                "relationship_expansion": relationship_result,
            },
            "platform_value": pvs_result,
            "composite": {
                "churn_risk_score": churn_risk["score"],
                "quantitative_score": health["quantitative_score"],
                "tier": final_tier,
                "churn_risk_detail": churn_risk,
                "health_detail": health,
            },
            "qualitative": {
                "final_score": qual_result["final_score"],
                "modifier_applied": qual_result["modifier_applied"],
                "cap_value": qual_result["cap_value"],
                "override_active": qual_result["override_active"],
                "critical_count": qual_data.get("critical_count", 0),
                "moderate_count": qual_data.get("moderate_count", 0),
                "watch_count": qual_data.get("watch_count", 0),
            },
            "coverage_pct": round(overall_coverage, 1),
        }

    def run(self, scoring_period: str | None = None) -> dict:
        """Run the full scoring pipeline for all mapped accounts.

        Args:
            scoring_period: e.g. "2025-02". Defaults to current month.

        Returns:
            Run summary dict.
        """
        if not scoring_period:
            scoring_period = datetime.now(timezone.utc).strftime("%Y-%m")

        start_time = time.time()
        results = []
        failures = []

        logger.info(
            "Starting Health Score run for period %s (%d accounts)",
            scoring_period, len(self.account_mapping),
        )

        for account in self.account_mapping:
            account_name = account.get("account_name", account.get("sf_account_id", "unknown"))
            try:
                result = self.score_account(account)
                results.append(result)

                # Write to Salesforce (or skip in dry-run)
                if not self.dry_run and self.sf_loader:
                    self.sf_loader.write_health_score(
                        sf_account_id=account["sf_account_id"],
                        scoring_result=result,
                        scoring_period=scoring_period,
                    )

            except Exception as e:
                logger.exception("Failed to score account: %s", account_name)
                failures.append({"account": account_name, "error": str(e)})

        # Dry-run: write all results to CSV
        if self.dry_run:
            output_path = write_dry_run_csv(results)
            logger.info("Dry-run complete. Results written to %s", output_path)

        elapsed = round(time.time() - start_time, 1)
        summary = {
            "scoring_period": scoring_period,
            "total_accounts": len(self.account_mapping),
            "scored_successfully": len(results),
            "failed": len(failures),
            "failures": failures,
            "execution_time_seconds": elapsed,
            "dry_run": self.dry_run,
        }

        logger.info(
            "Run complete: %d/%d scored, %d failed, %.1fs elapsed",
            len(results), len(self.account_mapping), len(failures), elapsed,
        )

        return summary


# --- Rollbar helper ---

def _init_rollbar(environment: str):
    """Initialise Rollbar error reporting. Gracefully disabled if token not set."""
    token = os.environ.get("ROLLBAR_ACCESS_TOKEN")
    if not token:
        logger.info("Rollbar disabled (ROLLBAR_ACCESS_TOKEN not set)")
        return
    try:
        import rollbar
        rollbar.init(access_token=token, environment=environment)
        logger.info("Rollbar initialised for environment: %s", environment)
    except ImportError:
        logger.warning("rollbar package not installed — error reporting disabled")


# --- AWS Lambda handler ---

def lambda_handler(event, context):
    """AWS Lambda entry point. Triggered by EventBridge monthly schedule."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    environment = os.environ.get("ENVIRONMENT", "production")
    _init_rollbar(environment)

    dry_run = event.get("dry_run", False)
    scoring_period = event.get("scoring_period")

    orchestrator = HealthScoreOrchestrator(dry_run=dry_run)
    orchestrator.init_clients_from_env()

    try:
        summary = orchestrator.run(scoring_period=scoring_period)
    except Exception:
        logger.exception("Lambda execution failed")
        try:
            import rollbar
            rollbar.report_exc_info()
        except (ImportError, Exception):
            pass
        raise

    if summary["failed"] > 0:
        logger.error("Run completed with %d failures", summary["failed"])

    return summary


# --- CLI entry point ---

def main():
    """CLI entry point for local development and dry-run testing."""
    import argparse

    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("Loaded .env file")
    except ImportError:
        pass

    parser = argparse.ArgumentParser(description="Health Score Middleware")
    parser.add_argument("--dry-run", action="store_true", help="Output to CSV instead of Salesforce")
    parser.add_argument("--config-dir", default="config", help="Path to config directory")
    parser.add_argument("--period", help="Scoring period (YYYY-MM). Defaults to current month.")
    args = parser.parse_args()

    # Ensure output directory exists for log file
    os.makedirs("output", exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f"output/run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        ],
    )

    environment = os.environ.get("ENVIRONMENT", "dev")
    _init_rollbar(environment)

    orchestrator = HealthScoreOrchestrator(
        config_dir=args.config_dir,
        dry_run=args.dry_run,
    )
    orchestrator.init_clients_from_env()

    summary = orchestrator.run(scoring_period=args.period)
    print(json.dumps(summary, indent=2))

    if summary["failed"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
