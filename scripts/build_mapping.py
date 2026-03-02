"""Build account_mapping.csv by matching Intercom companies to Looker brands.

Intercom company_id format: "Brand:<uuid>"
Looker brand_id format: "<uuid>"

Strips the "Brand:" prefix and joins on UUID.

Usage:
    python3 scripts/build_mapping.py
"""
from __future__ import annotations

import csv
import html
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "output"
CONFIG_DIR = PROJECT_ROOT / "config"

# Internal/test brands to exclude from the mapping
EXCLUDE_NAMES = {
    "Alliants",
    "Training",
    "Pre Production",
    "Production",
    "1LiVNG Demo",
    "1LiVNG Demo 3 [Sentral Demo]",
    "NewLiving Demo 4",
    "365 Beta AXP",
    "AIL Test",
    "OHIP Test",
    "Alliants Track & Trace",
    "Alliants Wallet Keys",
    "Resorts World - Key Testing",
    "UAT - Loews",
    "Boutique Homes",
    "Kevin's Crib",
    "Survicate Dummy Company",
    "CleanSuite",
}


def main():
    # Load Intercom companies (keyed by brand UUID extracted from company_id)
    intercom = {}
    with open(OUTPUT_DIR / "intercom_companies.csv") as f:
        for row in csv.DictReader(f):
            cid = row["company_id"]
            if cid.startswith("Brand:"):
                uuid = cid[len("Brand:"):]
                intercom[uuid] = {
                    "name": html.unescape(row["name"].removeprefix("Brand:").strip()),
                    "intercom_id": row["intercom_id"],
                    "company_id": cid,
                    "sf_account_id": row.get("sf_account_id", ""),
                    "segment": row.get("segment", ""),
                }

    # Load Looker brands
    looker = {}
    with open(OUTPUT_DIR / "looker_brands.csv") as f:
        for row in csv.DictReader(f):
            brand_id = row["brand_id"]
            bookings = row["total_bookings"]
            looker[brand_id] = int(bookings) if bookings else 0

    # Match: UUIDs present in both systems
    matched = set(intercom.keys()) & set(looker.keys())

    # Filter out test/internal brands
    filtered = []
    excluded = []
    for uuid in sorted(matched, key=lambda u: looker.get(u, 0), reverse=True):
        info = intercom[uuid]
        if info["name"] in EXCLUDE_NAMES:
            excluded.append(info["name"])
            continue
        filtered.append((uuid, info, looker[uuid]))

    # Write account_mapping.csv
    mapping_path = CONFIG_DIR / "account_mapping.csv"
    with open(mapping_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "sf_account_id", "intercom_company_id", "intercom_internal_id",
            "looker_customer_id", "account_name", "segment",
            "jira_project_key", "jira_component",
        ])
        for uuid, info, bookings in filtered:
            segment = info["segment"].lower() if info["segment"] else "paid"
            # Map Intercom segment values to our paid/standard
            if segment not in ("paid", "standard"):
                segment = "paid"

            writer.writerow([
                info["sf_account_id"],
                info["company_id"],
                info["intercom_id"],
                uuid,
                info["name"],
                segment,
                "",
                "",
            ])

    # Summary
    only_intercom = set(intercom.keys()) - set(looker.keys())
    only_looker = set(looker.keys()) - set(intercom.keys())

    sf_count = sum(1 for _, info, _ in filtered if info["sf_account_id"])

    print(f"=== Account Mapping ===\n")
    print(f"  Intercom companies:  {len(intercom)}")
    print(f"  Looker brands:       {len(looker)}")
    print(f"  Matched (both):      {len(matched)}")
    print(f"  Excluded (internal): {len(excluded)} — {', '.join(excluded)}")
    print(f"  Written to mapping:  {len(filtered)}")
    print(f"  With SF Account ID:  {sf_count}")
    print(f"  Only in Intercom:    {len(only_intercom)}")
    print(f"  Only in Looker:      {len(only_looker)}")
    print(f"\nWrote: {mapping_path}")
    print(f"\nTop 10 by bookings:")
    for uuid, info, bookings in filtered[:10]:
        sf_flag = " [SF]" if info["sf_account_id"] else ""
        print(f"  {info['name']:40s} {bookings:>8,} bookings{sf_flag}")

    if only_looker:
        print(f"\nLooker-only brands (no Intercom match):")
        for uuid in sorted(only_looker):
            print(f"  {uuid}  ({looker[uuid]:,} bookings)")

    print(f"\nNext: review config/account_mapping.csv, fill in jira columns, then:")
    print(f"  python3 -m src.main --dry-run")


if __name__ == "__main__":
    main()
