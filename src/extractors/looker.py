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

from src.extractors.retry import retry_on_transient

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
# Look field name constants (verified against actual Look output 2026-02-27)
# ---------------------------------------------------------------------------
# Each Look uses a different brand ID field name
FIELD_ID_BOOKINGS = "booking_summary.brand_id"              # Look 171
FIELD_ID_ALLIN = "allin_usage.brand_id"                     # Look 172
FIELD_ID_SENTIMENT = "conversation_sentiment.brand_id"      # Look 173
FIELD_ID_AUTOMATION = "flow_sends.brand_id"                 # Look 174
FIELD_ID_RESPONSE = "conversation_items_union_new.brand_id" # Look 175
FIELD_ID_PAGE_VISITS = "rudder_active_users.brand_id"       # Look 176
FIELD_ID_ITINERARY = "recommends_main.brand_id"             # Look 177

# Metric value fields
FIELD_SENTIMENT_PCT = "conversation_sentiment.percent_positive_conversations"
FIELD_RESPONSE_PCT = "conversation_items_union_new.percent_messages_within_target"
FIELD_ALLIN_PCT = "allin_usage.percent_of_messages_with_allin"
FIELD_CONVERSATIONS_BOOKING_PCT = "booking_summary.m_perc_of_bookings_with_messages"
FIELD_ARRIVAL_CIOL_PCT = "booking_summary.m_perc_of_bookings_with_checkin"
FIELD_DIGITAL_KEY_PCT = "booking_summary.m_perc_of_bookings_with_aw"    # Apple Wallet key
FIELD_MOBILE_KEY_PCT = "booking_summary.m_perc_of_bookings_with_mk"     # BLE mobile key
FIELD_AUTOMATION_VALUE = "flow_sends.delivery_method"
FIELD_TOTAL_BOOKINGS = "booking_summary.m_total_bookings"
FIELD_PAGE_VISITS_RAW = "rudder_active_users.dynamic_ranking_metric"
FIELD_ITINERARY_VISITS = "recommends_main.logged_in_users_digi_it_m"


class LookerExtractor:
    def __init__(self, base_url: str, client_id: str, client_secret: str, timeout: int = 300):
        self.timeout = timeout
        self.sdk = looker_sdk.init40()
        # Override settings if provided (for non-ini-file config)
        self.sdk.auth.settings.base_url = base_url
        self.sdk.auth.settings.client_id = client_id
        self.sdk.auth.settings.client_secret = client_secret
        self._look_cache: dict[int, list[dict]] = {}

    @classmethod
    def from_credentials(
        cls,
        base_url: str,
        client_id: str,
        client_secret: str,
        timeout: int = 300,
    ):
        """Create extractor from explicit credentials (for Lambda/Secrets Manager)."""
        import os
        os.environ["LOOKERSDK_BASE_URL"] = base_url
        os.environ["LOOKERSDK_CLIENT_ID"] = client_id
        os.environ["LOOKERSDK_CLIENT_SECRET"] = client_secret
        os.environ["LOOKERSDK_TIMEOUT"] = str(timeout)
        instance = cls.__new__(cls)
        instance.timeout = timeout
        instance.sdk = looker_sdk.init40()
        instance._look_cache = {}
        return instance

    @retry_on_transient(max_retries=3, backoff_factor=1.0)
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
        opts = {"timeout": self.timeout}
        query = self.sdk.create_query(
            body=models.WriteQuery(
                model=model,
                view=view,
                fields=fields,
                filters=filters or {},
                sorts=sorts or [],
                limit=str(limit),
            ),
            transport_options=opts,
        )
        result = self.sdk.run_query(
            query_id=query.id,
            result_format="json",
            transport_options=opts,
        )
        import json
        return json.loads(result) if isinstance(result, str) else result

    @retry_on_transient(max_retries=3, backoff_factor=1.0)
    def _run_look(self, look_id: int) -> list[dict]:
        """Run a saved Look and return results as dicts."""
        result = self.sdk.run_look(
            look_id=str(look_id),
            result_format="json",
            transport_options={"timeout": self.timeout},
        )
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
        **_kwargs,
    ) -> dict:
        """Extract Adoption & Engagement metrics for one customer.

        Uses saved Looks (via the Look cache) instead of inline queries.
        page_visits_per_arrival is derived from Look 176 / Look 171.
        Other metrics (feature_breadth, platform_score, trends) require
        Looker Explores that are not yet configured — returns None for those.

        Returns:
            dict with keys:
                page_visits_per_arrival, page_visits_per_arrival_trend,
                feature_breadth_pct, platform_score, platform_score_trend
        """
        page_visits_per_arrival = None

        try:
            pv_row = self._get_customer_row(
                LOOK_PAGE_VISITS, looker_customer_id, id_field=FIELD_ID_PAGE_VISITS,
            )
            bookings_row = self._get_customer_row(
                LOOK_BOOKINGS, looker_customer_id, id_field=FIELD_ID_BOOKINGS,
            )
            raw_visits = pv_row.get(FIELD_PAGE_VISITS_RAW) if pv_row else None
            total_bookings = (
                bookings_row.get(FIELD_TOTAL_BOOKINGS)
                if bookings_row
                else None
            )
            if raw_visits is not None and total_bookings and total_bookings > 0:
                page_visits_per_arrival = round(raw_visits / total_bookings, 2)
        except Exception:
            logger.exception("Failed to fetch page visits per arrival from Looks")

        return {
            "page_visits_per_arrival": page_visits_per_arrival,
            "page_visits_per_arrival_trend": None,  # needs prior-period Look
            "feature_breadth_pct": None,             # needs Explore
            "platform_score": None,                  # needs Explore
            "platform_score_trend": None,            # needs Explore
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
        self, look_id: int, customer_id: str, id_field: str,
    ) -> dict | None:
        """Find a single customer's row in a Look's results."""
        for row in self._get_look_data(look_id):
            if str(row.get(id_field, "")) == str(customer_id):
                return row
        return None

    # ------------------------------------------------------------------
    # Platform Value Score — raw metrics from saved Looks
    # ------------------------------------------------------------------

    @staticmethod
    def _to_pct(value: float | None) -> float | None:
        """Convert a 0.0-1.0 decimal to a 0-100 percentage."""
        if value is None:
            return None
        return round(value * 100, 2)

    def extract_platform_value_score(
        self,
        looker_customer_id: str,
    ) -> dict:
        """Extract Platform Value Score raw metrics from saved Looks.

        Returns:
            dict with 9 metric keys matching config/weights.yaml platform_value,
            each value a percentage (0-100) or raw number, or None if unavailable.
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

        # --- Look 173: Sentiment (decimal → %) ---
        try:
            row = self._get_customer_row(
                LOOK_SENTIMENT, looker_customer_id, id_field=FIELD_ID_SENTIMENT,
            )
            if row:
                metrics["positive_sentiment_pct"] = self._to_pct(
                    row.get(FIELD_SENTIMENT_PCT)
                )
        except Exception:
            logger.exception("Failed to fetch sentiment (Look %s)", LOOK_SENTIMENT)

        # --- Look 175: Response time (decimal → %) ---
        try:
            row = self._get_customer_row(
                LOOK_RESPONSE_TIME, looker_customer_id, id_field=FIELD_ID_RESPONSE,
            )
            if row:
                metrics["response_before_target_pct"] = self._to_pct(
                    row.get(FIELD_RESPONSE_PCT)
                )
        except Exception:
            logger.exception("Failed to fetch response time (Look %s)", LOOK_RESPONSE_TIME)

        # --- Look 172: All-in conversation usage (decimal → %) ---
        try:
            row = self._get_customer_row(
                LOOK_ALLIN_USAGE, looker_customer_id, id_field=FIELD_ID_ALLIN,
            )
            if row:
                metrics["allin_conversation_pct"] = self._to_pct(
                    row.get(FIELD_ALLIN_PCT)
                )
        except Exception:
            logger.exception("Failed to fetch all-in usage (Look %s)", LOOK_ALLIN_USAGE)

        # --- Look 171: Bookings — multiple metrics (decimals → %) ---
        try:
            bookings_row = self._get_customer_row(
                LOOK_BOOKINGS, looker_customer_id, id_field=FIELD_ID_BOOKINGS,
            )
            if bookings_row:
                metrics["conversations_per_booking_pct"] = self._to_pct(
                    bookings_row.get(FIELD_CONVERSATIONS_BOOKING_PCT)
                )
                metrics["arrival_ciol_pct"] = self._to_pct(
                    bookings_row.get(FIELD_ARRIVAL_CIOL_PCT)
                )

                # Digital key = Apple Wallet key + BLE mobile key
                # (brands only use one or the other)
                dk = bookings_row.get(FIELD_DIGITAL_KEY_PCT)
                mk = bookings_row.get(FIELD_MOBILE_KEY_PCT)
                if dk is not None or mk is not None:
                    metrics["digital_key_pct"] = self._to_pct((dk or 0) + (mk or 0))
        except Exception:
            logger.exception("Failed to fetch bookings (Look %s)", LOOK_BOOKINGS)

        # --- Look 174: Automation (presence check) ---
        try:
            row = self._get_customer_row(
                LOOK_AUTOMATION, looker_customer_id, id_field=FIELD_ID_AUTOMATION,
            )
            if row:
                val = row.get(FIELD_AUTOMATION_VALUE)
                # null → 0, any non-null value → 1
                metrics["automation_active"] = 0 if val is None else 1
            else:
                metrics["automation_active"] = 0
        except Exception:
            logger.exception("Failed to fetch automation (Look %s)", LOOK_AUTOMATION)

        # --- Look 177 / Look 171: Itinerary booking % ---
        # TODO: Re-enable once Look 177 performance is fixed (times out at 300s)
        # try:
        #     itin_row = self._get_customer_row(
        #         LOOK_ITINERARY, looker_customer_id, id_field=FIELD_ID_ITINERARY,
        #     )
        #     bookings_row_for_itin = self._get_customer_row(
        #         LOOK_BOOKINGS, looker_customer_id, id_field=FIELD_ID_BOOKINGS,
        #     )
        #     itin_visits = (
        #         itin_row.get(FIELD_ITINERARY_VISITS) if itin_row else None
        #     )
        #     total_bookings = (
        #         bookings_row_for_itin.get(FIELD_TOTAL_BOOKINGS)
        #         if bookings_row_for_itin
        #         else None
        #     )
        #     if itin_visits is not None and total_bookings and total_bookings > 0:
        #         metrics["itinerary_booking_pct"] = round(
        #             (itin_visits / total_bookings) * 100, 2
        #         )
        # except Exception:
        #     logger.exception(
        #         "Failed to fetch itinerary (Looks %s/%s)", LOOK_ITINERARY, LOOK_BOOKINGS
        #     )
        logger.info("Skipping Look 177 (itinerary) — temporarily disabled due to timeout")

        # --- Look 176 / Look 171: Page visits per arrival ---
        try:
            row = self._get_customer_row(
                LOOK_PAGE_VISITS, looker_customer_id, id_field=FIELD_ID_PAGE_VISITS,
            )
            bookings_row_for_pv = self._get_customer_row(
                LOOK_BOOKINGS, looker_customer_id, id_field=FIELD_ID_BOOKINGS,
            )
            raw_visits = row.get(FIELD_PAGE_VISITS_RAW) if row else None
            total_bookings_pv = (
                bookings_row_for_pv.get(FIELD_TOTAL_BOOKINGS)
                if bookings_row_for_pv
                else None
            )
            if raw_visits is not None and total_bookings_pv and total_bookings_pv > 0:
                metrics["page_visits_per_arrival"] = round(
                    raw_visits / total_bookings_pv, 2
                )
        except Exception:
            logger.exception("Failed to fetch page visits (Look %s)", LOOK_PAGE_VISITS)

        return metrics
