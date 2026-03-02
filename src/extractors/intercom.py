from __future__ import annotations

"""Intercom API extractor for Support Health metrics.

Pulls conversation data per company for a 30-day window and calculates:
- P1/P2 ticket volume
- Median first response time (minutes)
- Median close time (hours)
- Reopen rate (%)
- Escalation rate (%)

Supports two modes:
  1. API mode: live Intercom API queries (limited by lack of company filter)
  2. CSV mode: Intercom conversation export (recommended for production)
"""

import csv
import logging
import statistics
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from src.extractors.retry import mount_retry_adapter

logger = logging.getLogger(__name__)

INTERCOM_API_BASE = "https://api.intercom.io"
CONTACT_BATCH_SIZE = 15  # Max contact IDs per conversation search query


class IntercomExtractor:
    DEFAULT_TIMEOUT = 30  # seconds per HTTP request

    def __init__(self, api_token: str, lookback_days: int = 30, timeout: int | None = None):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        })
        mount_retry_adapter(self.session)
        self.lookback_days = lookback_days
        self.timeout = timeout or self.DEFAULT_TIMEOUT

    def _get_paginated(self, url: str, params: dict | None = None) -> list[dict]:
        """Fetch all pages from a cursor-paginated Intercom endpoint."""
        results = []
        params = params or {}
        base_url = url  # preserve for cursor-based pagination

        while True:
            resp = self.session.get(url, params=params, timeout=self.timeout)
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
                    next_url = next_page.get("url")
                    if next_url:
                        url = next_url
                        params = {}
                    else:
                        # Cursor-based pagination: reuse base URL with starting_after param
                        url = base_url
                        params = {"starting_after": next_page["starting_after"]}
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
            resp = self.session.get(url, params=params, timeout=self.timeout)
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

    def _get_contacts_for_company(self, company_id: str) -> list[str]:
        """Fetch all contact IDs for a company (paginated GET)."""
        url = f"{INTERCOM_API_BASE}/companies/{company_id}/contacts"
        contacts = self._get_paginated(url)
        return [c["id"] for c in contacts if "id" in c]

    def _search_conversation_batch(
        self, contact_ids: list[str], since_ts: int, until_ts: int
    ) -> list[dict]:
        """Search conversations for a batch of contact IDs (paginated)."""
        url = f"{INTERCOM_API_BASE}/conversations/search"
        conversations: list[dict] = []
        next_starting_after = None

        # Build contact filter — single filter or OR group
        if len(contact_ids) == 1:
            contact_filter: dict = {
                "field": "contact_ids",
                "operator": "~",
                "value": contact_ids[0],
            }
        else:
            contact_filter = {
                "operator": "OR",
                "value": [
                    {"field": "contact_ids", "operator": "~", "value": cid}
                    for cid in contact_ids
                ],
            }

        while True:
            query: dict = {
                "query": {
                    "operator": "AND",
                    "value": [
                        contact_filter,
                        {
                            "field": "created_at",
                            "operator": "<",
                            "value": until_ts,
                        },
                        {
                            "field": "statistics.last_close_at",
                            "operator": ">",
                            "value": since_ts,
                        },
                    ],
                },
            }
            if next_starting_after:
                query["pagination"] = {"starting_after": next_starting_after}

            resp = self.session.post(url, json=query, timeout=self.timeout)
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

    def _search_conversations_by_contacts(
        self, contact_ids: list[str], since_ts: int, until_ts: int
    ) -> list[dict]:
        """Search conversations by contact IDs in batches, with deduplication."""
        seen_ids: set[str] = set()
        conversations: list[dict] = []

        for i in range(0, len(contact_ids), CONTACT_BATCH_SIZE):
            batch = contact_ids[i : i + CONTACT_BATCH_SIZE]
            batch_convos = self._search_conversation_batch(batch, since_ts, until_ts)
            for conv in batch_convos:
                conv_id = conv.get("id")
                if conv_id and conv_id not in seen_ids:
                    seen_ids.add(conv_id)
                    conversations.append(conv)

        return conversations

    def _get_conversations_for_company(
        self, company_id: str, since_ts: int, until_ts: int
    ) -> list[dict]:
        """Fetch conversations for a company via contact-based search.

        Two-step approach:
        1. List all contacts for the company
        2. Search conversations by contact IDs (in batches of 15)
        """
        contact_ids = self._get_contacts_for_company(company_id)
        if not contact_ids:
            logger.info("No contacts found for company %s", company_id)
            return []

        logger.info(
            "Found %d contacts for company %s, searching conversations in %d batches",
            len(contact_ids),
            company_id,
            (len(contact_ids) + CONTACT_BATCH_SIZE - 1) // CONTACT_BATCH_SIZE,
        )
        return self._search_conversations_by_contacts(
            contact_ids, since_ts, until_ts
        )

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

    # ------------------------------------------------------------------
    # CSV-based extraction (Intercom conversation export)
    # ------------------------------------------------------------------

    _P1_P2_TAGS = {"p1", "p2", "priority", "urgent", "critical"}
    _ESCALATION_TAGS = {"escalated", "escalation"}

    @staticmethod
    def _parse_csv_datetime(value: str) -> datetime | None:
        """Parse a UTC datetime string from an Intercom CSV export."""
        if not value or not value.strip():
            return None
        try:
            return datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=timezone.utc,
            )
        except ValueError:
            return None

    @staticmethod
    def _parse_tags(tags_str: str) -> list[str]:
        """Parse a comma-separated tags string into a list of tag names."""
        if not tags_str or not tags_str.strip():
            return []
        return [t.strip() for t in tags_str.split(",") if t.strip()]

    @staticmethod
    def _parse_companies(companies_str: str) -> list[str]:
        """Parse the message_author_companies field into a list of company names."""
        if not companies_str or not companies_str.strip():
            return []
        return [c.strip() for c in companies_str.split(",") if c.strip()]

    @classmethod
    def load_support_metrics_from_csv(
        cls,
        csv_path: str,
        lookback_days: int = 30,
        as_of_date: datetime | None = None,
    ) -> dict[str, dict]:
        """Load and compute support metrics from an Intercom conversation CSV export.

        Reads the export CSV (one row per message), groups by conversation_id,
        joins to companies via message_author_companies, filters by date range,
        and computes the same 5 support metrics as the API-based method.

        Args:
            csv_path: Path to the Intercom conversation CSV export.
            lookback_days: Number of days to look back from as_of_date.
            as_of_date: Reference date (defaults to now UTC).

        Returns:
            Dict mapping company name (lowercased) → support metrics dict with keys:
                p1_p2_volume, first_response_minutes, close_time_hours,
                reopen_rate_pct, escalation_rate_pct
        """
        now = as_of_date or datetime.now(timezone.utc)
        since = now - timedelta(days=lookback_days)

        path = Path(csv_path)
        if not path.exists():
            raise FileNotFoundError(f"Intercom CSV export not found: {csv_path}")

        # Pass 1: read CSV and group messages by conversation_id
        conversations: dict[str, dict] = {}
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                raise ValueError(f"CSV is empty or unreadable: {csv_path}")

            for row in reader:
                conv_id = row.get("conversation_id", "").strip()
                if not conv_id:
                    continue

                if conv_id not in conversations:
                    conversations[conv_id] = {
                        "created_at": cls._parse_csv_datetime(
                            row.get("conversation_created_at", ""),
                        ),
                        "first_response_at": cls._parse_csv_datetime(
                            row.get("conversation_first_response_at", ""),
                        ),
                        "closed_at": cls._parse_csv_datetime(
                            row.get("conversation_closed_at", ""),
                        ),
                        "tags": cls._parse_tags(row.get("conversation_tags", "")),
                        "state": row.get("conversation_state", ""),
                        "companies": set(),
                        "has_reopen": False,
                    }

                # Detect reopens from message_type
                msg_type = row.get("message_type", "").strip().lower()
                if msg_type in ("assign_and_reopen", "reopen"):
                    conversations[conv_id]["has_reopen"] = True

                # Collect companies from customer (non-admin) message authors
                author_type = row.get("message_author_type", "").strip().lower()
                if author_type in ("user", "lead", "contact"):
                    for company in cls._parse_companies(
                        row.get("message_author_companies", ""),
                    ):
                        conversations[conv_id]["companies"].add(company.lower())

        # Pass 2: assign conversations to companies and filter by date range
        company_conversations: dict[str, list[dict]] = {}
        for conv_id, conv in conversations.items():
            created = conv["created_at"]
            if created is None:
                continue
            if created < since or created > now:
                continue

            for company_name in conv["companies"]:
                company_conversations.setdefault(company_name, []).append(conv)

        # Pass 3: compute metrics per company
        result: dict[str, dict] = {}
        for company_name, convos in company_conversations.items():
            result[company_name] = cls._compute_csv_support_metrics(convos)

        logger.info(
            "Loaded Intercom CSV: %d conversations, %d companies, date range %s to %s",
            len(conversations),
            len(result),
            since.strftime("%Y-%m-%d"),
            now.strftime("%Y-%m-%d"),
        )
        return result

    @classmethod
    def _compute_csv_support_metrics(cls, conversations: list[dict]) -> dict:
        """Compute support metrics from a list of parsed conversation dicts."""
        if not conversations:
            return {
                "p1_p2_volume": 0,
                "first_response_minutes": 0,
                "close_time_hours": 0,
                "reopen_rate_pct": 0,
                "escalation_rate_pct": 0,
            }

        total = len(conversations)
        p1_p2_count = 0
        response_times: list[float] = []
        close_times: list[float] = []
        reopen_count = 0
        escalation_count = 0

        for conv in conversations:
            tags_lower = [t.lower() for t in conv["tags"]]

            # P1/P2 detection
            if any(t in cls._P1_P2_TAGS for t in tags_lower):
                p1_p2_count += 1

            # First response time
            created = conv["created_at"]
            first_resp = conv["first_response_at"]
            if created and first_resp and first_resp > created:
                response_times.append(
                    (first_resp - created).total_seconds(),
                )

            # Close time
            closed = conv["closed_at"]
            if created and closed and closed > created:
                close_times.append((closed - created).total_seconds())

            # Reopens
            if conv["has_reopen"]:
                reopen_count += 1

            # Escalation
            if any(t in cls._ESCALATION_TAGS for t in tags_lower):
                escalation_count += 1

        median_response_minutes = (
            statistics.median(response_times) / 60 if response_times else 0
        )
        median_close_hours = (
            statistics.median(close_times) / 3600 if close_times else 0
        )
        reopen_rate = (reopen_count / total * 100) if total > 0 else 0
        escalation_rate = (escalation_count / total * 100) if total > 0 else 0

        return {
            "p1_p2_volume": p1_p2_count,
            "first_response_minutes": round(median_response_minutes, 1),
            "close_time_hours": round(median_close_hours, 1),
            "reopen_rate_pct": round(reopen_rate, 1),
            "escalation_rate_pct": round(escalation_rate, 1),
        }
