#!/usr/bin/env python3
"""Simple script to verify synced data in the database."""

import sqlite3
import sys
from datetime import datetime


def verify_data(db_path: str = "data/portfolio_report.db"):
    """Verify data in the database."""
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        print("=" * 80)
        print("PORTFOLIO REPORTING DATA VERIFICATION")
        print("=" * 80)
        print()

        # Companies
        cursor.execute("SELECT COUNT(*) as count FROM companies")
        companies_count = cursor.fetchone()["count"]
        print(f"📊 Companies: {companies_count}")

        # Power plants
        cursor.execute("SELECT COUNT(*) as count FROM power_plants")
        plants_count = cursor.fetchone()["count"]
        print(f"🏭 Power plants: {plants_count}")

        # Production days
        cursor.execute("""
            SELECT COUNT(*) as count,
                   MIN(date) as min_date,
                   MAX(date) as max_date,
                   SUM(volume) as total_volume,
                   SUM(revenue_nok) as total_revenue_nok,
                   SUM(revenue_eur) as total_revenue_eur
            FROM production_days
        """)
        prod = cursor.fetchone()
        print(f"⚡ Production days: {prod['count']:,}")
        if prod["count"] > 0:
            print(f"   └─ Date range: {prod['min_date']} to {prod['max_date']}")
            if prod["total_volume"]:
                print(f"   └─ Total volume: {prod['total_volume']:,.0f} MWh")
            if prod["total_revenue_nok"]:
                print(f"   └─ Total revenue NOK: {prod['total_revenue_nok']:,.0f}")
            if prod["total_revenue_eur"]:
                print(f"   └─ Total revenue EUR: {prod['total_revenue_eur']:,.0f}")

        # Market prices
        cursor.execute("""
            SELECT COUNT(*) as count,
                   MIN(timestamp) as min_time,
                   MAX(timestamp) as max_time,
                   COUNT(DISTINCT price_area) as areas
            FROM market_prices
        """)
        prices = cursor.fetchone()
        print(f"💰 Market prices: {prices['count']:,}")
        if prices["count"] > 0:
            print(f"   └─ Time range: {prices['min_time']} to {prices['max_time']}")
            print(f"   └─ Price areas: {prices['areas']}")

        # Downtime events
        cursor.execute("""
            SELECT COUNT(*) as count,
                   MIN(start_time) as min_time,
                   MAX(start_time) as max_time
            FROM downtime_events
        """)
        downtime = cursor.fetchone()
        print(f"🔧 Downtime events: {downtime['count']}")
        if downtime["count"] > 0 and downtime["min_time"]:
            print(f"   └─ Time range: {downtime['min_time']} to {downtime['max_time']}")

        # Work items
        cursor.execute("""
            SELECT COUNT(*) as count,
                   COUNT(DISTINCT status) as statuses,
                   MIN(created_at) as min_time,
                   MAX(created_at) as max_time
            FROM work_items
        """)
        work_items = cursor.fetchone()
        print(f"📝 Work items: {work_items['count']}")
        if work_items["count"] > 0:
            print(f"   └─ Created range: {work_items['min_time']} to {work_items['max_time']}")
            print(f"   └─ Statuses: {work_items['statuses']}")

        # Budgets
        cursor.execute("""
            SELECT COUNT(*) as count,
                   MIN(month) as min_month,
                   MAX(month) as max_month,
                   SUM(volume) as total_volume,
                   SUM(revenue) as total_revenue
            FROM budgets
        """)
        budgets = cursor.fetchone()
        print(f"📊 Budgets: {budgets['count']}")
        if budgets["count"] > 0:
            print(f"   └─ Month range: {budgets['min_month']} to {budgets['max_month']}")
            if budgets["total_volume"]:
                print(f"   └─ Total budgeted volume: {budgets['total_volume']:,.0f} MWh")
            if budgets["total_revenue"]:
                print(f"   └─ Total budgeted revenue: {budgets['total_revenue']:,.0f}")

        # Sync metadata
        print()
        print("─" * 80)
        print("SYNC METADATA")
        print("─" * 80)
        cursor.execute("""
            SELECT entity_type, last_sync_at, last_sync_success, error_message
            FROM sync_metadata
            ORDER BY last_sync_at DESC
        """)
        for row in cursor:
            status = "✅" if row["last_sync_success"] else "❌"
            print(f"{status} {row['entity_type']:20s} - {row['last_sync_at']}")
            if row["error_message"]:
                print(f"   └─ Error: {row['error_message']}")

        print()
        print("=" * 80)

        conn.close()

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else "data/portfolio_report.db"
    verify_data(db_path)
