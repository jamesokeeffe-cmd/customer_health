"""Intercom API extractor for Support Health metrics.

Pulls conversation data per company for a 30-day window and calculates:
- P1/P2 ticket volume
- Median first response time (minutes)
- Median close time (hours)
- Reopen rate (%)
- Escalation rate (%)
"""

import logging
import statistics
from datetime import datetime, timedelta, timezone

import requests

logger = logging.getLogger(__name__)

INTERCOM_API_BASE = "https://api.intercom.io"


class IntercomExtractor:
    def __init__(self, api_token: str, lookback_days: int = 30):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        })
        self.lookback_days = lookback_days

    def _get_paginated(self, url: str, params: dict | None = None) -> list[dict]:
        """Fetch all pages from a cursor-paginated Intercom endpoint."""
        results = []
        params = params or {}

        while True:
            resp = self.session.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            if "conversations" in data:
                results.extend(data["conversations"])
            elif "data" in data:
                results.extend(data["data"])

            pages = data.get("pages", {})
            next_page = pages.get("next")
            if next_page:
                if isinstance(next_page, dict):
                    url = next_page.get("url", next_page.get("starting_after", ""))
                    params = {}
                else:
                    url = next_page
                    params = {}
            else:
                break

        return results

    def get_companies(self) -> list[dict]:
        """Fetch all Intercom companies with their IDs and custom attributes."""
        url = f"{INTERCOM_API_BASE}/companies"
        params = {"per_page": 50}
        companies = []

        while True:
            resp = self.session.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            companies.extend(data.get("data", []))

            pages = data.get("pages", {})
            next_page = pages.get("next")
            if next_page:
                url = next_page if isinstance(next_page, str) else next_page.get("url", "")
                params = {}
            else:
                break

        return companies

    def _get_conversations_for_company(
        self, company_id: str, since_ts: int, until_ts: int
    ) -> list[dict]:
        """Search conversations for a specific company within a date range."""
        url = f"{INTERCOM_API_BASE}/conversations/search"
        conversations = []
        next_starting_after = None

        while True:
            query = {
                "query": {
                    "operator": "AND",
                    "value": [
                        {
                            "field": "company.id",
                            "operator": "=",
                            "value": company_id,
                        },
                        {
                            "field": "statistics.last_close_at",
                            "operator": ">",
                            "value": since_ts,
                        },
                        {
                            "field": "created_at",
                            "operator": "<",
                            "value": until_ts,
                        },
                    ],
                },
            }
            if next_starting_after:
                query["pagination"] = {"starting_after": next_starting_after}

            resp = self.session.post(url, json=query)
            resp.raise_for_status()
            data = resp.json()
            conversations.extend(data.get("conversations", []))

            pages = data.get("pages", {})
            next_cursor = pages.get("next", {})
            if isinstance(next_cursor, dict) and next_cursor.get("starting_after"):
                next_starting_after = next_cursor["starting_after"]
            else:
                break

        return conversations

    def _extract_conversation_metrics(self, conversation: dict) -> dict:
        """Extract relevant metrics from a single conversation."""
        stats = conversation.get("statistics", {})
        tags = [t.get("name", "") for t in conversation.get("tags", {}).get("tags", [])]

        priority = conversation.get("priority", "not_priority")
        # Determine if P1/P2 by priority field or tags
        is_p1_p2 = priority in ("priority",) or any(
            t.lower() in ("p1", "p2", "priority", "urgent", "critical") for t in tags
        )

        first_response_secs = stats.get("first_contact_reply_at")
        created_at = conversation.get("created_at")
        if first_response_secs and created_at:
            first_response_time = first_response_secs - created_at
        else:
            first_response_time = None

        time_to_close = stats.get("median_time_to_reply")
        last_close = stats.get("last_close_at")
        if last_close and created_at:
            time_to_close = last_close - created_at

        is_reopened = stats.get("count_reopens", 0) > 0

        # Escalation: conversation was assigned to a human after bot, or has escalation tag
        is_escalated = any(
            t.lower() in ("escalated", "escalation") for t in tags
        ) or conversation.get("state") == "open" and conversation.get("assignee", {}).get("type") == "admin"

        return {
            "is_p1_p2": is_p1_p2,
            "first_response_seconds": first_response_time,
            "time_to_close_seconds": time_to_close,
            "is_reopened": is_reopened,
            "is_escalated": is_escalated,
        }

    def extract_support_metrics(
        self, intercom_company_id: str, as_of_date: datetime | None = None
    ) -> dict:
        """Extract all Support Health metrics for one company.

        Returns:
            dict with keys matching Support Health metric names:
                p1_p2_volume, first_response_minutes, close_time_hours,
                reopen_rate_pct, escalation_rate_pct
        """
        now = as_of_date or datetime.now(timezone.utc)
        since = now - timedelta(days=self.lookback_days)
        since_ts = int(since.timestamp())
        until_ts = int(now.timestamp())

        conversations = self._get_conversations_for_company(
            intercom_company_id, since_ts, until_ts
        )

        if not conversations:
            logger.warning(
                "No conversations found for company %s in the last %d days",
                intercom_company_id, self.lookback_days,
            )
            return {
                "p1_p2_volume": 0,
                "first_response_minutes": 0,
                "close_time_hours": 0,
                "reopen_rate_pct": 0,
                "escalation_rate_pct": 0,
            }

        metrics = [self._extract_conversation_metrics(c) for c in conversations]
        total = len(metrics)

        p1_p2_count = sum(1 for m in metrics if m["is_p1_p2"])

        response_times = [
            m["first_response_seconds"]
            for m in metrics
            if m["first_response_seconds"] is not None and m["first_response_seconds"] > 0
        ]
        median_response_minutes = (
            statistics.median(response_times) / 60 if response_times else 0
        )

        close_times = [
            m["time_to_close_seconds"]
            for m in metrics
            if m["time_to_close_seconds"] is not None and m["time_to_close_seconds"] > 0
        ]
        median_close_hours = (
            statistics.median(close_times) / 3600 if close_times else 0
        )

        reopen_count = sum(1 for m in metrics if m["is_reopened"])
        reopen_rate = (reopen_count / total * 100) if total > 0 else 0

        escalation_count = sum(1 for m in metrics if m["is_escalated"])
        escalation_rate = (escalation_count / total * 100) if total > 0 else 0

        return {
            "p1_p2_volume": p1_p2_count,
            "first_response_minutes": round(median_response_minutes, 1),
            "close_time_hours": round(median_close_hours, 1),
            "reopen_rate_pct": round(reopen_rate, 1),
            "escalation_rate_pct": round(escalation_rate, 1),
        }
