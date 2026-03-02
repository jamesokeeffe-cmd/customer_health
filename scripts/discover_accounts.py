"""Discovery script to fetch customer lists from Intercom and Looker.

Outputs CSVs that can be used to manually match customers across systems
and populate config/account_mapping.csv for the first dry-run.

Usage:
    python3 scripts/discover_accounts.py
"""
from __future__ import annotations

import csv
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Allow running as `python3 scripts/discover_accounts.py` from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def discover_intercom() -> int:
    """Fetch all Intercom companies and write to CSV. Returns count."""
    token = os.getenv("INTERCOM_API_TOKEN")
    if not token:
        print("  INTERCOM_API_TOKEN not set — skipping Intercom discovery")
        return 0

    from src.extractors.intercom import IntercomExtractor

    extractor = IntercomExtractor(api_token=token)
    print("  Fetching Intercom companies...")
    companies = extractor.get_companies()

    out_path = OUTPUT_DIR / "intercom_companies.csv"
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "intercom_id", "company_id", "name", "created_at", "plan",
            "monthly_spend", "sf_account_id", "segment",
        ])
        for c in companies:
            created_ts = c.get("created_at")
            created_str = ""
            if created_ts:
                created_str = datetime.fromtimestamp(
                    created_ts, tz=timezone.utc,
                ).strftime("%Y-%m-%d")

            plan = c.get("plan", {})
            plan_name = plan.get("name", "") if isinstance(plan, dict) else str(plan)
            custom = c.get("custom_attributes", {})

            writer.writerow([
                c.get("id", ""),
                c.get("company_id", ""),
                c.get("name", ""),
                created_str,
                plan_name,
                c.get("monthly_spend", custom.get("monthly_spend", "")),
                custom.get("Salesforce Account ID", ""),
                custom.get("Segment", ""),
            ])

    print(f"  Wrote {len(companies)} companies to {out_path}")
    return len(companies)


def discover_looker() -> int:
    """Fetch Look 171 (bookings) and write brand list to CSV. Returns count."""
    base_url = os.getenv("LOOKER_BASE_URL")
    client_id = os.getenv("LOOKER_CLIENT_ID")
    client_secret = os.getenv("LOOKER_CLIENT_SECRET")

    if not all([base_url, client_id, client_secret]):
        print("  LOOKER_* credentials not fully set — skipping Looker discovery")
        return 0

    from src.extractors.looker import (
        FIELD_ID_BOOKINGS,
        FIELD_TOTAL_BOOKINGS,
        LOOK_BOOKINGS,
        LookerExtractor,
    )

    extractor = LookerExtractor.from_credentials(
        base_url=base_url,
        client_id=client_id,
        client_secret=client_secret,
    )
    print(f"  Fetching Look {LOOK_BOOKINGS} (bookings)...")
    rows = extractor._get_look_data(LOOK_BOOKINGS)

    out_path = OUTPUT_DIR / "looker_brands.csv"
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["brand_id", "total_bookings"])
        for row in rows:
            brand_id = row.get(FIELD_ID_BOOKINGS, "")
            bookings = row.get(FIELD_TOTAL_BOOKINGS, "")
            writer.writerow([brand_id, bookings])

    print(f"  Wrote {len(rows)} brands to {out_path}")
    return len(rows)


def main():
    print("=== Account Discovery ===\n")

    print("[Intercom]")
    intercom_count = discover_intercom()

    print("\n[Looker]")
    looker_count = discover_looker()

    print(f"\n=== Summary ===")
    print(f"  Intercom companies: {intercom_count}")
    print(f"  Looker brands:      {looker_count}")
    print()
    print("Next steps:")
    print("  1. Review output/intercom_companies.csv and output/looker_brands.csv")
    print("  2. Match companies by name/booking volume")
    print("  3. Populate config/account_mapping.csv with matched rows")
    print("  4. Run: python3 -m src.main --dry-run")


if __name__ == "__main__":
    main()
