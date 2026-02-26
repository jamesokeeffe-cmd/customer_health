from __future__ import annotations

"""Looker API extractor for Adoption & Engagement metrics and Platform Value Score.

Queries Looker Explores/Looks for:
- Page visits per arrival (current 30d + prior 30d for trend)
- Feature adoption breadth
- License utilisation
- AXP Platform Score (current + historical)
- Platform Value Score metrics (from saved Looks 171-177)
"""

import logging

import looker_sdk
from looker_sdk import models40 as models

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Look IDs for Platform Value Score metrics
# ---------------------------------------------------------------------------
LOOK_BOOKINGS = 171
LOOK_ALLIN_USAGE = 172
LOOK_SENTIMENT = 173
LOOK_AUTOMATION = 174
LOOK_RESPONSE_TIME = 175
LOOK_PAGE_VISITS = 176
LOOK_ITINERARY = 177

# ---------------------------------------------------------------------------
# Look field name constants (update these to match actual Look output columns)
# ---------------------------------------------------------------------------
FIELD_CUSTOMER_ID = "customer_id"
FIELD_SENTIMENT_PCT = "positive_sentiment_pct"
FIELD_RESPONSE_PCT = "response_before_target_pct"
FIELD_ALLIN_PCT = "allin_conversation_pct"
FIELD_CONVERSATIONS_BOOKING_PCT = "conversations_per_booking_pct"
FIELD_ARRIVAL_CIOL_PCT = "arrival_ciol_pct"
FIELD_DIGITAL_KEY_PCT = "digital_key_pct"
FIELD_MOBILE_KEY_PCT = "mobile_key_pct"
FIELD_AUTOMATION_VALUE = "automation_value"
FIELD_PAGE_VISITS_PER_ARRIVAL = "page_visits_per_arrival"
FIELD_ITINERARY_VISITS = "itinerary_visits"
FIELD_TOTAL_BOOKINGS = "total_bookings"


class LookerExtractor:
    def __init__(self, base_url: str, client_id: str, client_secret: str):
        self.sdk = looker_sdk.init40()
        # Override settings if provided (for non-ini-file config)
        self.sdk.auth.settings.base_url = base_url
        self.sdk.auth.settings.client_id = client_id
        self.sdk.auth.settings.client_secret = client_secret
        self._look_cache: dict[int, list[dict]] = {}

    @classmethod
    def from_credentials(cls, base_url: str, client_id: str, client_secret: str):
        """Create extractor from explicit credentials (for Lambda/Secrets Manager)."""
        import os
        os.environ["LOOKERSDK_BASE_URL"] = base_url
        os.environ["LOOKERSDK_CLIENT_ID"] = client_id
        os.environ["LOOKERSDK_CLIENT_SECRET"] = client_secret
        instance = cls.__new__(cls)
        instance.sdk = looker_sdk.init40()
        instance._look_cache = {}
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
        feature_view: str = "feature_usage",
        customer_field: str = "customer_id",
    ) -> dict:
        """Extract Adoption & Engagement metrics for one customer.

        Returns:
            dict with keys:
                page_visits_per_arrival (avg page visits per arrival, current 30d)
                page_visits_per_arrival_trend (% change 30d vs prior 30d)
                feature_breadth_pct (% of modules active)
                platform_score (current AXP score)
                platform_score_trend (change over 90d)
        """
        # Page visits per arrival (current 30d + prior 30d for trend)
        pvpa = {}
        for period, date_filter in [
            ("current", "30 days"),
            ("prev_30d", "30 days ago for 30 days"),
        ]:
            try:
                rows = self._run_inline_query(
                    model=model_name,
                    view="platform_score",
                    fields=[
                        "platform_score.customer_id",
                        "platform_score.total_page_visits_per_arrival",
                    ],
                    filters={
                        "platform_score.customer_id": looker_customer_id,
                        "platform_score.score_date": date_filter,
                    },
                )
                pvpa[period] = (
                    rows[0].get("platform_score.total_page_visits_per_arrival", 0)
                    if rows else 0
                )
            except Exception:
                logger.exception("Failed to fetch page visits per arrival for period %s", period)
                pvpa[period] = None

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
        pvpa_current = pvpa.get("current")
        pvpa_trend = None
        if pvpa_current is not None and pvpa.get("prev_30d") is not None:
            pvpa_trend = self._calc_trend_pct(
                pvpa_current, pvpa["prev_30d"]
            )

        platform_trend = None
        if platform_current is not None and platform_90d_ago is not None:
            platform_trend = round(platform_current - platform_90d_ago, 1)

        return {
            "page_visits_per_arrival": pvpa_current,
            "page_visits_per_arrival_trend": pvpa_trend,
            "feature_breadth_pct": feature_breadth,
            "platform_score": platform_current,
            "platform_score_trend": platform_trend,
        }

    # ------------------------------------------------------------------
    # Look cache helpers (each Look returns all customers; run once)
    # ------------------------------------------------------------------

    def _get_look_data(self, look_id: int) -> list[dict]:
        """Return cached Look results, fetching on first access."""
        if look_id not in self._look_cache:
            self._look_cache[look_id] = self._run_look(look_id)
        return self._look_cache[look_id]

    def _get_customer_row(
        self, look_id: int, customer_id: str, id_field: str = FIELD_CUSTOMER_ID
    ) -> dict | None:
        """Find a single customer's row in a Look's results."""
        for row in self._get_look_data(look_id):
            if str(row.get(id_field, "")) == str(customer_id):
                return row
        return None

    # ------------------------------------------------------------------
    # Platform Value Score — raw metrics from saved Looks
    # ------------------------------------------------------------------

    def extract_platform_value_score(
        self,
        looker_customer_id: str,
    ) -> dict:
        """Extract Platform Value Score raw metrics from saved Looks.

        Returns:
            dict with 9 metric keys matching config/weights.yaml platform_value,
            each value a raw number (or None if unavailable).
        """
        metrics: dict[str, float | None] = {
            "positive_sentiment_pct": None,
            "response_before_target_pct": None,
            "allin_conversation_pct": None,
            "conversations_per_booking_pct": None,
            "arrival_ciol_pct": None,
            "digital_key_pct": None,
            "automation_active": None,
            "itinerary_booking_pct": None,
            "page_visits_per_arrival": None,
        }

        # --- Look 173: Sentiment ---
        try:
            row = self._get_customer_row(LOOK_SENTIMENT, looker_customer_id)
            if row:
                metrics["positive_sentiment_pct"] = row.get(FIELD_SENTIMENT_PCT)
        except Exception:
            logger.exception("Failed to fetch sentiment (Look %s)", LOOK_SENTIMENT)

        # --- Look 175: Response time ---
        try:
            row = self._get_customer_row(LOOK_RESPONSE_TIME, looker_customer_id)
            if row:
                metrics["response_before_target_pct"] = row.get(FIELD_RESPONSE_PCT)
        except Exception:
            logger.exception("Failed to fetch response time (Look %s)", LOOK_RESPONSE_TIME)

        # --- Look 172: All-in conversation usage ---
        try:
            row = self._get_customer_row(LOOK_ALLIN_USAGE, looker_customer_id)
            if row:
                metrics["allin_conversation_pct"] = row.get(FIELD_ALLIN_PCT)
        except Exception:
            logger.exception("Failed to fetch all-in usage (Look %s)", LOOK_ALLIN_USAGE)

        # --- Look 171: Bookings — multiple metrics ---
        try:
            bookings_row = self._get_customer_row(LOOK_BOOKINGS, looker_customer_id)
            if bookings_row:
                metrics["conversations_per_booking_pct"] = bookings_row.get(
                    FIELD_CONVERSATIONS_BOOKING_PCT
                )
                metrics["arrival_ciol_pct"] = bookings_row.get(FIELD_ARRIVAL_CIOL_PCT)

                # Digital key = digital_key_pct + mobile_key_pct
                dk = bookings_row.get(FIELD_DIGITAL_KEY_PCT)
                mk = bookings_row.get(FIELD_MOBILE_KEY_PCT)
                if dk is not None or mk is not None:
                    metrics["digital_key_pct"] = (dk or 0) + (mk or 0)
        except Exception:
            logger.exception("Failed to fetch bookings (Look %s)", LOOK_BOOKINGS)

        # --- Look 174: Automation ---
        try:
            row = self._get_customer_row(LOOK_AUTOMATION, looker_customer_id)
            if row:
                val = row.get(FIELD_AUTOMATION_VALUE)
                # null → 0, any non-null value → 1
                metrics["automation_active"] = 0 if val is None else 1
            else:
                metrics["automation_active"] = 0
        except Exception:
            logger.exception("Failed to fetch automation (Look %s)", LOOK_AUTOMATION)

        # --- Look 177 / Look 171: Itinerary booking % ---
        try:
            itin_row = self._get_customer_row(LOOK_ITINERARY, looker_customer_id)
            bookings_row_for_itin = self._get_customer_row(
                LOOK_BOOKINGS, looker_customer_id
            )
            itin_visits = (
                itin_row.get(FIELD_ITINERARY_VISITS) if itin_row else None
            )
            total_bookings = (
                bookings_row_for_itin.get(FIELD_TOTAL_BOOKINGS)
                if bookings_row_for_itin
                else None
            )
            if itin_visits is not None and total_bookings and total_bookings > 0:
                metrics["itinerary_booking_pct"] = round(
                    (itin_visits / total_bookings) * 100, 2
                )
        except Exception:
            logger.exception(
                "Failed to fetch itinerary (Looks %s/%s)", LOOK_ITINERARY, LOOK_BOOKINGS
            )

        # --- Look 176: Page visits per arrival ---
        try:
            row = self._get_customer_row(LOOK_PAGE_VISITS, looker_customer_id)
            if row:
                metrics["page_visits_per_arrival"] = row.get(
                    FIELD_PAGE_VISITS_PER_ARRIVAL
                )
        except Exception:
            logger.exception("Failed to fetch page visits (Look %s)", LOOK_PAGE_VISITS)

        return metrics
