#!/usr/bin/env python3
"""Simple script to verify synced data in the database."""

import sqlite3
import sys


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
        cursor.execute("""
            SELECT COUNT(*) as count,
                   COUNT(DISTINCT company_id) as companies,
                   SUM(capacity_mw) as total_capacity,
                   AVG(capacity_mw) as avg_capacity,
                   MIN(commissioned_date) as oldest_commissioned,
                   MAX(commissioned_date) as newest_commissioned
            FROM power_plants
        """)
        plants = cursor.fetchone()
        print(f"🏭 Power plants: {plants['count']}")
        if plants["count"] > 0:
            print(f"   └─ Companies: {plants['companies']}")
            if plants["total_capacity"]:
                print(f"   └─ Total capacity: {plants['total_capacity']:,.1f} MW (avg: {plants['avg_capacity']:,.1f} MW)")
            if plants["oldest_commissioned"]:
                print(f"   └─ Commissioned: {plants['oldest_commissioned']} to {plants['newest_commissioned']}")

            # Breakdown by asset class type
            cursor.execute("""
                SELECT asset_class_type, COUNT(*) as count, SUM(capacity_mw) as total_capacity
                FROM power_plants
                GROUP BY asset_class_type
                ORDER BY count DESC
            """)
            print(f"   └─ By asset class:")
            for row in cursor:
                capacity_str = f" ({row['total_capacity']:,.1f} MW)" if row['total_capacity'] else ""
                print(f"      • {row['asset_class_type']}: {row['count']}{capacity_str}")

            # Data quality check
            cursor.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(*) - COUNT(capacity_mw) as missing_capacity,
                    COUNT(*) - COUNT(commissioned_date) as missing_commissioned,
                    COUNT(*) - COUNT(asset_class_type) as missing_type,
                    COUNT(*) - COUNT(latitude) as missing_lat,
                    COUNT(*) - COUNT(longitude) as missing_lng
                FROM power_plants
            """)
            quality = cursor.fetchone()
            if quality["missing_capacity"] > 0 or quality["missing_commissioned"] > 0 or quality["missing_type"] > 0 or quality["missing_lat"] > 0 or quality["missing_lng"] > 0:
                print(f"   └─ Missing metadata:")
                if quality["missing_capacity"] > 0:
                    print(f"      • Capacity: {quality['missing_capacity']} plants")
                if quality["missing_commissioned"] > 0:
                    print(f"      • Commissioned date: {quality['missing_commissioned']} plants")
                if quality["missing_type"] > 0:
                    print(f"      • Asset class type: {quality['missing_type']} plants")
                if quality["missing_lat"] > 0 or quality["missing_lng"] > 0:
                    print(f"      • Coordinates: {max(quality['missing_lat'], quality['missing_lng'])} plants")
            else:
                print(f"   └─ ✓ All plants have complete metadata")

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
                   COUNT(DISTINCT price_area) as areas,
                   SUM(price_nok) as total_price_nok,
                   SUM(price_eur) as total_price_eur,
                   AVG(price_nok) as avg_price_nok,
                   AVG(price_eur) as avg_price_eur
            FROM market_prices
        """)
        prices = cursor.fetchone()
        print(f"💰 Market prices: {prices['count']:,}")
        if prices["count"] > 0:
            print(f"   └─ Time range: {prices['min_time']} to {prices['max_time']}")
            print(f"   └─ Price areas: {prices['areas']}")
            if prices["total_price_nok"]:
                print(f"   └─ Total price NOK: {prices['total_price_nok']:,.0f} (avg: {prices['avg_price_nok']:,.2f})")
            if prices["total_price_eur"]:
                print(f"   └─ Total price EUR: {prices['total_price_eur']:,.0f} (avg: {prices['avg_price_eur']:,.2f})")

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

        # Downtime days
        cursor.execute("""
            SELECT COUNT(*) as count,
                   MIN(date) as min_date,
                   MAX(date) as max_date,
                   SUM(volume) as total_volume,
                   SUM(cost_nok) as total_cost_nok,
                   SUM(cost_eur) as total_cost_eur
            FROM downtime_days
        """)
        downtime_days = cursor.fetchone()
        print(f"📅 Downtime days: {downtime_days['count']}")
        if downtime_days["count"] > 0:
            print(f"   └─ Date range: {downtime_days['min_date']} to {downtime_days['max_date']}")
            if downtime_days["total_volume"]:
                print(f"   └─ Total lost volume: {downtime_days['total_volume']:,.0f} MWh")
            if downtime_days["total_cost_nok"]:
                print(f"   └─ Total cost NOK: {downtime_days['total_cost_nok']:,.0f}")
            if downtime_days["total_cost_eur"]:
                print(f"   └─ Total cost EUR: {downtime_days['total_cost_eur']:,.0f}")

            # Top reasons
            cursor.execute("""
                SELECT reason, COUNT(*) as count, SUM(volume) as total_volume
                FROM downtime_days
                WHERE reason IS NOT NULL
                GROUP BY reason
                ORDER BY count DESC
                LIMIT 5
            """)
            reasons = cursor.fetchall()
            if reasons:
                print(f"   └─ Top reasons:")
                for row in reasons:
                    vol_str = f" ({row['total_volume']:,.0f} MWh)" if row['total_volume'] else ""
                    print(f"      • {row['reason']}: {row['count']}{vol_str}")

        # Downtime periods
        cursor.execute("""
            SELECT COUNT(*) as count,
                   MIN(timestamp) as min_time,
                   MAX(timestamp) as max_time,
                   SUM(volume) as total_volume,
                   SUM(cost_nok) as total_cost_nok,
                   SUM(cost_eur) as total_cost_eur
            FROM downtime_periods
        """)
        downtime_periods = cursor.fetchone()
        print(f"⏱️  Downtime periods: {downtime_periods['count']:,}")
        if downtime_periods["count"] > 0:
            print(
                f"   └─ Time range: {downtime_periods['min_time']} to {downtime_periods['max_time']}"
            )
            if downtime_periods["total_volume"]:
                print(f"   └─ Total lost volume: {downtime_periods['total_volume']:,.0f} MWh")
            if downtime_periods["total_cost_nok"]:
                print(f"   └─ Total cost NOK: {downtime_periods['total_cost_nok']:,.0f}")
            if downtime_periods["total_cost_eur"]:
                print(f"   └─ Total cost EUR: {downtime_periods['total_cost_eur']:,.0f}")

            # Top components
            cursor.execute("""
                SELECT component, COUNT(*) as count, SUM(hours) as total_hours
                FROM downtime_periods
                WHERE component IS NOT NULL
                GROUP BY component
                ORDER BY count DESC
                LIMIT 5
            """)
            components = cursor.fetchall()
            if components:
                print(f"   └─ Top components:")
                for row in components:
                    hours_str = f" ({row['total_hours']:,.0f}h)" if row['total_hours'] else ""
                    print(f"      • {row['component']}: {row['count']}{hours_str}")

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

            # Component breakdown
            cursor.execute("""
                SELECT component, COUNT(*) as count
                FROM work_items
                WHERE component IS NOT NULL
                GROUP BY component
                ORDER BY count DESC
                LIMIT 10
            """)
            components = cursor.fetchall()
            if components:
                print(f"   └─ Top components:")
                for row in components:
                    print(f"      • {row['component']}: {row['count']}")

        # Budgets
        cursor.execute("""
            SELECT COUNT(*) as count,
                   MIN(month) as min_month,
                   MAX(month) as max_month,
                   SUM(volume) as total_volume,
                   SUM(revenue_nok) as total_revenue_nok,
                   SUM(revenue_eur) as total_revenue_eur
            FROM budgets
        """)
        budgets = cursor.fetchone()
        print(f"📊 Budgets: {budgets['count']}")
        if budgets["count"] > 0:
            print(f"   └─ Month range: {budgets['min_month']} to {budgets['max_month']}")
            if budgets["total_volume"]:
                print(f"   └─ Total budgeted volume: {budgets['total_volume']:,.0f} MWh")
            if budgets["total_revenue_nok"]:
                print(f"   └─ Total budgeted revenue NOK: {budgets['total_revenue_nok']:,.0f}")
            if budgets["total_revenue_eur"]:
                print(f"   └─ Total budgeted revenue EUR: {budgets['total_revenue_eur']:,.0f}")

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

    except sqlite3.OperationalError as e:
        if "locked" in str(e).lower():
            print("\n" + "=" * 80, file=sys.stderr)
            print("ERROR: Database is locked by another process", file=sys.stderr)
            print("=" * 80, file=sys.stderr)
            print("\nCommon causes:", file=sys.stderr)
            print("  - Power BI or other BI tool has the database open", file=sys.stderr)
            print("  - A sync process is currently running", file=sys.stderr)
            print("  - Database file is open in another application", file=sys.stderr)
            print("\nSolution:", file=sys.stderr)
            print("  1. Close Power BI and any other applications accessing the database", file=sys.stderr)
            print("  2. Stop any running sync processes", file=sys.stderr)
            print("  3. Try running this script again", file=sys.stderr)
            print("\n" + "=" * 80, file=sys.stderr)
        else:
            print(f"Database error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else "data/portfolio_report.db"
    verify_data(db_path)
