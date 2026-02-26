from __future__ import annotations

"""Looker API extractor for Adoption & Engagement metrics and Platform Value Score.

Queries Looker Explores/Looks for:
- Staff login counts (current 30d, 60d ago, 90d ago)
- Admin login counts (same windows)
- Feature adoption breadth
- License utilisation
- AXP Platform Score (current + historical)
"""

import logging

import looker_sdk
from looker_sdk import models40 as models

logger = logging.getLogger(__name__)


class LookerExtractor:
    def __init__(self, base_url: str, client_id: str, client_secret: str):
        self.sdk = looker_sdk.init40()
        # Override settings if provided (for non-ini-file config)
        self.sdk.auth.settings.base_url = base_url
        self.sdk.auth.settings.client_id = client_id
        self.sdk.auth.settings.client_secret = client_secret

    @classmethod
    def from_credentials(cls, base_url: str, client_id: str, client_secret: str):
        """Create extractor from explicit credentials (for Lambda/Secrets Manager)."""
        import os
        os.environ["LOOKERSDK_BASE_URL"] = base_url
        os.environ["LOOKERSDK_CLIENT_ID"] = client_id
        os.environ["LOOKERSDK_CLIENT_SECRET"] = client_secret
        instance = cls.__new__(cls)
        instance.sdk = looker_sdk.init40()
        return instance

    def _run_inline_query(
        self,
        model: str,
        view: str,
        fields: list[str],
        filters: dict[str, str] | None = None,
        sorts: list[str] | None = None,
        limit: int = 500,
    ) -> list[dict]:
        """Run an inline query against a Looker Explore and return results as dicts."""
        query = self.sdk.create_query(
            body=models.WriteQuery(
                model=model,
                view=view,
                fields=fields,
                filters=filters or {},
                sorts=sorts or [],
                limit=str(limit),
            )
        )
        result = self.sdk.run_query(query_id=query.id, result_format="json")
        import json
        return json.loads(result) if isinstance(result, str) else result

    def _run_look(self, look_id: int) -> list[dict]:
        """Run a saved Look and return results as dicts."""
        result = self.sdk.run_look(look_id=look_id, result_format="json")
        import json
        return json.loads(result) if isinstance(result, str) else result

    def _calc_trend_pct(self, current: float, previous: float) -> float:
        """Calculate percentage change between two values."""
        if previous == 0:
            return 100.0 if current > 0 else 0.0
        return round(((current - previous) / previous) * 100, 1)

    def extract_adoption_metrics(
        self,
        looker_customer_id: str,
        model_name: str = "alliants",
        login_view: str = "user_sessions",
        feature_view: str = "feature_usage",
        customer_field: str = "customer_id",
    ) -> dict:
        """Extract Adoption & Engagement metrics for one customer.

        Returns:
            dict with keys:
                staff_login_trend (% change 30d vs prior 30d)
                admin_login_trend (% change)
                feature_breadth_pct (% of modules active)
                platform_score (current AXP score)
                platform_score_trend (change over 90d)
        """
        # Staff login counts across three 30-day windows
        staff_logins = {}
        for period, date_filter in [
            ("current", "30 days"),
            ("prev_30d", "30 days ago for 30 days"),
            ("prev_60d", "60 days ago for 30 days"),
        ]:
            try:
                rows = self._run_inline_query(
                    model=model_name,
                    view=login_view,
                    fields=[
                        f"{login_view}.{customer_field}",
                        f"{login_view}.unique_staff_logins",
                    ],
                    filters={
                        f"{login_view}.{customer_field}": looker_customer_id,
                        f"{login_view}.session_date": date_filter,
                    },
                )
                staff_logins[period] = (
                    rows[0].get(f"{login_view}.unique_staff_logins", 0)
                    if rows else 0
                )
            except Exception:
                logger.exception("Failed to fetch staff logins for period %s", period)
                staff_logins[period] = None

        # Admin login counts
        admin_logins = {}
        for period, date_filter in [
            ("current", "30 days"),
            ("prev_30d", "30 days ago for 30 days"),
        ]:
            try:
                rows = self._run_inline_query(
                    model=model_name,
                    view=login_view,
                    fields=[
                        f"{login_view}.{customer_field}",
                        f"{login_view}.unique_admin_logins",
                    ],
                    filters={
                        f"{login_view}.{customer_field}": looker_customer_id,
                        f"{login_view}.session_date": date_filter,
                        f"{login_view}.user_role": "admin,manager",
                    },
                )
                admin_logins[period] = (
                    rows[0].get(f"{login_view}.unique_admin_logins", 0)
                    if rows else 0
                )
            except Exception:
                logger.exception("Failed to fetch admin logins for period %s", period)
                admin_logins[period] = None

        # Feature adoption breadth
        feature_breadth = None
        try:
            rows = self._run_inline_query(
                model=model_name,
                view=feature_view,
                fields=[
                    f"{feature_view}.{customer_field}",
                    f"{feature_view}.active_module_count",
                    f"{feature_view}.total_module_count",
                ],
                filters={
                    f"{feature_view}.{customer_field}": looker_customer_id,
                    f"{feature_view}.activity_date": "30 days",
                },
            )
            if rows:
                active = rows[0].get(f"{feature_view}.active_module_count", 0) or 0
                total = rows[0].get(f"{feature_view}.total_module_count", 1) or 1
                feature_breadth = round((active / total) * 100, 1)
        except Exception:
            logger.exception("Failed to fetch feature breadth")

        # AXP Platform Score (current + 90d ago)
        platform_current = None
        platform_90d_ago = None
        try:
            rows = self._run_inline_query(
                model=model_name,
                view="platform_score",
                fields=[
                    "platform_score.customer_id",
                    "platform_score.score",
                ],
                filters={
                    "platform_score.customer_id": looker_customer_id,
                    "platform_score.score_date": "1 day",
                },
            )
            if rows:
                platform_current = rows[0].get("platform_score.score", 0)
        except Exception:
            logger.exception("Failed to fetch current platform score")

        try:
            rows = self._run_inline_query(
                model=model_name,
                view="platform_score",
                fields=[
                    "platform_score.customer_id",
                    "platform_score.score",
                ],
                filters={
                    "platform_score.customer_id": looker_customer_id,
                    "platform_score.score_date": "90 days ago for 1 day",
                },
            )
            if rows:
                platform_90d_ago = rows[0].get("platform_score.score", 0)
        except Exception:
            logger.exception("Failed to fetch 90d-ago platform score")

        # Calculate trends
        staff_trend = None
        if staff_logins.get("current") is not None and staff_logins.get("prev_30d") is not None:
            staff_trend = self._calc_trend_pct(
                staff_logins["current"], staff_logins["prev_30d"]
            )

        admin_trend = None
        if admin_logins.get("current") is not None and admin_logins.get("prev_30d") is not None:
            admin_trend = self._calc_trend_pct(
                admin_logins["current"], admin_logins["prev_30d"]
            )

        platform_trend = None
        if platform_current is not None and platform_90d_ago is not None:
            platform_trend = round(platform_current - platform_90d_ago, 1)

        return {
            "staff_login_trend": staff_trend,
            "admin_login_trend": admin_trend,
            "feature_breadth_pct": feature_breadth,
            "platform_score": platform_current,
            "platform_score_trend": platform_trend,
        }

    def extract_platform_value_score(
        self,
        looker_customer_id: str,
        model_name: str = "alliants",
    ) -> dict:
        """Extract the AXP Platform Value Score sub-pillar scores.

        Returns:
            dict with keys: messaging, automations, contactless, requests, staff_adoption
        """
        try:
            rows = self._run_inline_query(
                model=model_name,
                view="platform_score",
                fields=[
                    "platform_score.customer_id",
                    "platform_score.messaging_score",
                    "platform_score.automations_score",
                    "platform_score.contactless_score",
                    "platform_score.requests_score",
                    "platform_score.staff_adoption_score",
                ],
                filters={
                    "platform_score.customer_id": looker_customer_id,
                    "platform_score.score_date": "1 day",
                },
            )
            if rows:
                row = rows[0]
                return {
                    "messaging": row.get("platform_score.messaging_score"),
                    "automations": row.get("platform_score.automations_score"),
                    "contactless": row.get("platform_score.contactless_score"),
                    "requests": row.get("platform_score.requests_score"),
                    "staff_adoption": row.get("platform_score.staff_adoption_score"),
                }
        except Exception:
            logger.exception("Failed to fetch platform value sub-scores")

        return {
            "messaging": None,
            "automations": None,
            "contactless": None,
            "requests": None,
            "staff_adoption": None,
        }
