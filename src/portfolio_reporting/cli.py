"""Command-line interface for portfolio reporting."""
import argparse
import logging
import sys
from pathlib import Path

from .sync import SyncCoordinator
from .utils.config import load_config, validate_config
from .utils.logging_config import setup_logging


logger = logging.getLogger(__name__)


def main():
    """Main entry point for CLI."""
    parser = argparse.ArgumentParser(
        description="Portfolio Reporting - Fetch data from Kaia Solutions Portal API for PowerBI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full refresh (fetch all data)
  python -m portfolio_reporting --mode full

  # Incremental update (fetch only new data since last sync)
  python -m portfolio_reporting --mode incremental

  # Use custom config file
  python -m portfolio_reporting --config /path/to/config.yaml

  # Set custom date range
  python -m portfolio_reporting --start-date 2024-01-01 --end-date 2024-12-31
        """,
    )

    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="full",
        help="Sync mode: 'full' replaces all data, 'incremental' fetches only new data (default: full)",
    )

    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)",
    )

    parser.add_argument(
        "--start-date",
        help="Start date for data fetch (YYYY-MM-DD format, overrides config)",
    )

    parser.add_argument(
        "--end-date",
        help="End date for data fetch (YYYY-MM-DD format, overrides config)",
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level (overrides config)",
    )

    args = parser.parse_args()

    try:
        # Load and validate configuration
        config = load_config(args.config)
        validate_config(config)

        # Override config with command-line arguments
        if args.start_date:
            config.setdefault("data", {})["start_date"] = args.start_date

        if args.end_date:
            config.setdefault("data", {})["end_date"] = args.end_date

        if args.log_level:
            config.setdefault("logging", {})["level"] = args.log_level

        # Setup logging
        setup_logging(config)

        logger.info("=" * 80)
        logger.info("Portfolio Reporting - Data Sync Started")
        logger.info(f"Mode: {args.mode}")
        logger.info(f"Config: {args.config}")
        logger.info(f"Database: {config['database']['path']}")
        logger.info("=" * 80)

        # Run sync
        coordinator = SyncCoordinator(config)
        stats = coordinator.sync_all(mode=args.mode)

        # Print summary
        logger.info("=" * 80)
        logger.info("Sync Summary:")
        for entity_type, count in stats.items():
            logger.info(f"  {entity_type}: {count} records")
        logger.info("=" * 80)
        logger.info("Data sync completed successfully!")

        print("\n✓ Sync completed successfully!")
        print(f"  Database: {config['database']['path']}")
        print(f"  Total records synced: {sum(stats.values())}")

        return 0

    except FileNotFoundError as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        return 1

    except ValueError as e:
        print(f"\n✗ Configuration error: {e}", file=sys.stderr)
        return 1

    except KeyboardInterrupt:
        print("\n\n✗ Sync interrupted by user", file=sys.stderr)
        logger.warning("Sync interrupted by user")
        return 130

    except Exception as e:
        print(f"\n✗ Error during sync: {e}", file=sys.stderr)
        logger.exception("Error during sync")
        return 1


if __name__ == "__main__":
    sys.exit(main())
