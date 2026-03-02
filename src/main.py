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
from src.scoring.composite import classify_tier, compute_churn_risk, compute_health_score
from src.scoring.dimensions import score_dimension
from src.scoring.qualitative import apply_qualitative_modifier

try:
    from src.extractors.salesforce import SalesforceExtractor
except ImportError:
    SalesforceExtractor = None  # type: ignore[assignment,misc]

from src.loaders.salesforce import SalesforceLoader, write_dry_run_csv
logger = logging.getLogger("health_score")

# Dimension sections that must appear in both weights.yaml and thresholds.yaml
_SCORED_DIMENSIONS = [
    "support_health",
    "financial_contract",
    "adoption_engagement",
    "relationship_expansion",
    "platform_value",
]

_SEGMENTS = ["paid", "standard"]


def validate_config(weights: dict, thresholds: dict) -> list[str]:
    """Validate weights and thresholds config, returning a list of error messages.

    Checks:
    - Required top-level keys in weights (health_score, churn_risk, dimensions)
    - health_score has churn_risk_weight and platform_value_weight
    - Each scored dimension exists in both weights and thresholds
    - Metric weights within each dimension sum to ~1.0
    - Each metric in weights has a matching threshold entry
    - Each threshold entry has lower_is_better, paid, and standard segments
    - Each segment has green, yellow, red values
    - Threshold ordering matches lower_is_better direction
    """
    errors = []

    # --- weights top-level keys ---
    for key in ["health_score", "churn_risk"]:
        if key not in weights:
            errors.append(f"weights.yaml missing required key: '{key}'")

    hs = weights.get("health_score", {})
    for field in ["churn_risk_weight", "platform_value_weight"]:
        if field not in hs:
            errors.append(f"weights.yaml health_score missing '{field}'")

    # --- churn_risk dimension weights ---
    cr = weights.get("churn_risk", {})
    for dim in _SCORED_DIMENSIONS[:4]:  # first 4 are churn risk dimensions
        if dim not in cr:
            errors.append(f"weights.yaml churn_risk missing dimension weight: '{dim}'")

    # --- each scored dimension ---
    for dim in _SCORED_DIMENSIONS:
        if dim not in weights:
            errors.append(f"weights.yaml missing dimension section: '{dim}'")
            continue
        if dim not in thresholds:
            errors.append(f"thresholds.yaml missing dimension section: '{dim}'")
            continue

        metric_weights = weights[dim]
        dim_thresholds = thresholds[dim]

        # Metric weights should sum to ~1.0
        weight_sum = sum(metric_weights.values())
        if abs(weight_sum - 1.0) > 0.01:
            errors.append(
                f"weights.yaml '{dim}' metric weights sum to {weight_sum:.3f}, expected ~1.0"
            )

        # Each metric needs a threshold entry
        for metric_name in metric_weights:
            if metric_name not in dim_thresholds:
                errors.append(
                    f"thresholds.yaml '{dim}' missing threshold for metric: '{metric_name}'"
                )
                continue

            tc = dim_thresholds[metric_name]

            if "lower_is_better" not in tc:
                errors.append(
                    f"thresholds.yaml '{dim}.{metric_name}' missing 'lower_is_better'"
                )

            lower_is_better = tc.get("lower_is_better", False)

            for seg in _SEGMENTS:
                if seg not in tc:
                    errors.append(
                        f"thresholds.yaml '{dim}.{metric_name}' missing segment: '{seg}'"
                    )
                    continue

                seg_t = tc[seg]
                for boundary in ["green", "yellow", "red"]:
                    if boundary not in seg_t:
                        errors.append(
                            f"thresholds.yaml '{dim}.{metric_name}.{seg}' "
                            f"missing '{boundary}'"
                        )

                if all(b in seg_t for b in ["green", "yellow", "red"]):
                    g, y, r = seg_t["green"], seg_t["yellow"], seg_t["red"]
                    if lower_is_better:
                        if not (g <= y <= r):
                            errors.append(
                                f"thresholds.yaml '{dim}.{metric_name}.{seg}': "
                                f"lower_is_better=true requires green<=yellow<=red, "
                                f"got {g}, {y}, {r}"
                            )
                    else:
                        if not (g >= y >= r):
                            errors.append(
                                f"thresholds.yaml '{dim}.{metric_name}.{seg}': "
                                f"lower_is_better=false requires green>=yellow>=red, "
                                f"got {g}, {y}, {r}"
                            )

    return errors


def load_yaml(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


_REQUIRED_CSV_COLUMNS = {
    "sf_account_id",
    "intercom_company_id",
    "looker_customer_id",
    "account_name",
    "segment",
}


def load_account_mapping(path: str) -> list[dict]:
    """Load cross-system account ID mapping from CSV.

    Expected columns: sf_account_id, intercom_company_id, looker_customer_id,
                      account_name, segment

    Raises:
        ValueError: If required columns are missing from the CSV header.
    """
    rows = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError(f"Account mapping CSV is empty or unreadable: {path}")
        actual = set(reader.fieldnames)
        missing = _REQUIRED_CSV_COLUMNS - actual
        if missing:
            raise ValueError(
                f"Account mapping CSV missing required columns: {sorted(missing)}. "
                f"Found: {sorted(actual)}"
            )
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

        # Validate config consistency
        config_errors = validate_config(self.weights, self.thresholds)
        if config_errors:
            raise ValueError(
                f"Config validation failed with {len(config_errors)} error(s):\n"
                + "\n".join(f"  - {e}" for e in config_errors)
            )

        # Extractors and loader are initialised lazily via init_clients()
        self.intercom: IntercomExtractor | None = None
        self.jira: JiraExtractor | None = None
        self.looker: LookerExtractor | None = None
        self.sf_extractor: SalesforceExtractor | None = None
        self.sf_loader: SalesforceLoader | None = None

        # Pre-loaded CSV support metrics (keyed by lowercase company name)
        self._csv_support_metrics: dict[str, dict] | None = None

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
        looker_timeout = int(os.environ.get("LOOKERSDK_TIMEOUT", "300"))
        if looker_base_url and looker_client_id and looker_client_secret:
            self.looker = LookerExtractor.from_credentials(
                base_url=looker_base_url,
                client_id=looker_client_id,
                client_secret=looker_client_secret,
                timeout=looker_timeout,
            )
            available.append("Looker")

        # Salesforce
        sf_username = os.environ.get("SF_USERNAME")
        sf_password = os.environ.get("SF_PASSWORD")
        sf_token = os.environ.get("SF_SECURITY_TOKEN")
        sf_domain = os.environ.get("SF_DOMAIN", "login")
        if SalesforceExtractor and sf_username and sf_password and sf_token:
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

    def load_intercom_csv(self, csv_path: str, lookback_days: int = 30) -> None:
        """Pre-load support metrics from an Intercom conversation CSV export.

        When loaded, CSV metrics are used in score_account() instead of API calls.
        """
        self._csv_support_metrics = IntercomExtractor.load_support_metrics_from_csv(
            csv_path=csv_path,
            lookback_days=lookback_days,
        )
        logger.info(
            "Loaded Intercom CSV export: %d companies with support data",
            len(self._csv_support_metrics),
        )

    def score_account(self, account: dict) -> dict:
        """Run the full scoring pipeline for a single account.

        Args:
            account: Dict with sf_account_id, intercom_company_id,
                     looker_customer_id, account_name, segment.

        Returns:
            Full scoring result dict.
        """
        sf_id = account["sf_account_id"]
        # intercom_internal_id is the Intercom-assigned ID used for conversation searches;
        # intercom_company_id is the custom external ID (Brand:<uuid>).
        intercom_id = account.get("intercom_internal_id", "") or account.get("intercom_company_id", "")
        looker_id = account.get("looker_customer_id", "")
        segment = account.get("segment", "standard").lower()
        account_name = account.get("account_name", sf_id)

        logger.info("Scoring account: %s (%s) [%s]", account_name, sf_id, segment)

        # ----- EXTRACT -----
        # Support Health (Intercom — CSV export preferred, API fallback)
        support_raw = {}
        if self._csv_support_metrics is not None:
            # Look up by lowercase account name
            csv_key = account_name.lower()
            if csv_key in self._csv_support_metrics:
                support_raw = self._csv_support_metrics[csv_key]
                logger.debug("Support metrics from CSV for %s", account_name)
            else:
                logger.debug("No CSV support data for %s", account_name)
        elif self.intercom and intercom_id:
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

        if not self.account_mapping:
            logger.warning(
                "Account mapping is empty — no accounts to score. "
                "Populate %s with account rows.",
                self.config_dir / "account_mapping.csv",
            )

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
    intercom_csv = event.get("intercom_export_path")

    orchestrator = HealthScoreOrchestrator(dry_run=dry_run)
    orchestrator.init_clients_from_env()

    if intercom_csv:
        orchestrator.load_intercom_csv(intercom_csv)

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
    parser.add_argument(
        "--intercom-export",
        metavar="CSV_PATH",
        help="Path to Intercom conversation CSV export for support metrics",
    )
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

    if args.intercom_export:
        orchestrator.load_intercom_csv(args.intercom_export)

    summary = orchestrator.run(scoring_period=args.period)
    print(json.dumps(summary, indent=2))

    if summary["failed"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
