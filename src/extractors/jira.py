from __future__ import annotations

"""Jira API extractor for bug ticket metrics.

Pulls open bug data from a Jira project filtered by Component (customer identifier).
Returns:
- open_bugs_total: count of all open bugs for the component
- open_bugs_p1_p2: count of high-priority open bugs (Highest, High, Critical, Blocker)
"""

import logging

import requests

logger = logging.getLogger(__name__)

# Jira priority names considered P1/P2
P1_P2_PRIORITIES = {"highest", "high", "critical", "blocker"}


class JiraExtractor:
    def __init__(self, base_url: str, email: str, api_token: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.auth = (email, api_token)
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
        })

    def _search_issues(
        self, jql: str, fields: list[str], max_results: int = 100
    ) -> list[dict]:
        """Paginated JQL search via POST /rest/api/3/search."""
        url = f"{self.base_url}/rest/api/3/search"
        start_at = 0
        all_issues = []

        while True:
            payload = {
                "jql": jql,
                "fields": fields,
                "startAt": start_at,
                "maxResults": max_results,
            }
            resp = self.session.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()

            issues = data.get("issues", [])
            all_issues.extend(issues)

            total = data.get("total", 0)
            start_at += len(issues)
            if start_at >= total or not issues:
                break

        return all_issues

    def extract_bug_metrics(
        self, project_key: str, component_name: str
    ) -> dict:
        """Extract open bug metrics for a project component.

        Args:
            project_key: Jira project key (e.g. "ENG").
            component_name: Component name used to identify the customer.

        Returns:
            dict with open_bugs_total and open_bugs_p1_p2.
        """
        jql = (
            f'project = "{project_key}" '
            f'AND issuetype = Bug '
            f'AND component = "{component_name}" '
            f'AND status NOT IN (Done, Closed, Resolved)'
        )

        logger.info(
            "Querying Jira bugs: project=%s component=%s",
            project_key, component_name,
        )

        issues = self._search_issues(jql, fields=["priority"])

        open_total = len(issues)
        open_p1_p2 = 0

        for issue in issues:
            priority = issue.get("fields", {}).get("priority")
            if priority:
                priority_name = priority.get("name", "").lower()
                if priority_name in P1_P2_PRIORITIES:
                    open_p1_p2 += 1

        logger.info(
            "Jira bugs for %s/%s: total=%d, p1_p2=%d",
            project_key, component_name, open_total, open_p1_p2,
        )

        return {
            "open_bugs_total": open_total,
            "open_bugs_p1_p2": open_p1_p2,
        }
