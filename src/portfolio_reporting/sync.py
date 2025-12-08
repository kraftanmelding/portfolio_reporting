"""Sync coordinator for data synchronization."""

import logging
from datetime import datetime
from typing import Any

from .api.client import APIClient
from .database.handler import DatabaseHandler
from .fetchers.budgets import BudgetsFetcher
from .fetchers.companies import CompaniesFetcher
from .fetchers.market_prices import MarketPricesFetcher
from .fetchers.om_data import OMDataFetcher
from .fetchers.power_plants import PowerPlantsFetcher
from .fetchers.production import ProductionFetcher
from .fetchers.production_periods import ProductionPeriodsFetcher

logger = logging.getLogger(__name__)


class SyncCoordinator:
    """Coordinates data synchronization between API and database."""

    def __init__(self, config: dict[str, Any]):
        """Initialize sync coordinator.

        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.api_client = APIClient(
            base_url=config["api"]["base_url"],
            api_key=config["api"]["api_key"],
            timeout=config["api"].get("timeout", 30),
            retry_attempts=config["api"].get("retry_attempts", 3),
        )
        self.db_handler = DatabaseHandler(config["database"]["path"])

        # Initialize fetchers
        self.companies_fetcher = CompaniesFetcher(self.api_client)
        self.power_plants_fetcher = PowerPlantsFetcher(self.api_client)
        self.production_fetcher = ProductionFetcher(self.api_client)
        self.production_periods_fetcher = ProductionPeriodsFetcher(self.api_client)
        self.market_prices_fetcher = MarketPricesFetcher(self.api_client)
        self.om_fetcher = OMDataFetcher(self.api_client)
        self.budgets_fetcher = BudgetsFetcher(self.api_client)

    def sync_all(self, mode: str = "full") -> dict[str, int]:
        """Sync all data from API to database.

        Args:
            mode: Sync mode ('full' or 'incremental')

        Returns:
            Dictionary with counts of synced records by type
        """
        logger.info(f"Starting {mode} sync of all data")
        stats = {}

        try:
            self.db_handler.connect()
            self.db_handler.initialize_schema()

            # Get date range from config
            start_date = self.config.get("data", {}).get("start_date")
            end_date = self.config.get("data", {}).get("end_date")

            # Sync companies
            if self.config.get("data", {}).get("fetch_companies", True):
                stats["companies"] = self._sync_companies(mode)

            # Sync power plants (we need this list for production data)
            power_plants = []
            if self.config.get("data", {}).get("fetch_power_plants", True):
                power_plants, count = self._sync_power_plants(mode)
                stats["power_plants"] = count

            # Sync production data (requires power plants list)
            if self.config.get("data", {}).get("fetch_production", True):
                stats["production"] = self._sync_production(
                    mode, power_plants, start_date, end_date
                )

            # Sync production periods (hourly data, requires power plants list)
            if self.config.get("data", {}).get("fetch_production_periods", True):
                stats["production_periods"] = self._sync_production_periods(
                    mode, power_plants, start_date, end_date
                )

            # Sync market prices
            if self.config.get("data", {}).get("fetch_market_prices", True):
                stats["market_prices"] = self._sync_market_prices(mode, start_date, end_date)

            # Sync downtime events
            if self.config.get("data", {}).get("fetch_downtime_events", True):
                stats["downtime_events"] = self._sync_downtime_events(mode, start_date, end_date)

            # Sync downtime days (requires power plants list)
            if self.config.get("data", {}).get("fetch_downtime_days", True):
                stats["downtime_days"] = self._sync_downtime_days(
                    mode, power_plants, start_date, end_date
                )

            # Sync downtime periods (requires power plants list)
            if self.config.get("data", {}).get("fetch_downtime_periods", True):
                stats["downtime_periods"] = self._sync_downtime_periods(
                    mode, power_plants, start_date, end_date
                )

            # Sync work items (requires power plants list)
            if self.config.get("data", {}).get("fetch_work_items", True):
                stats["work_items"] = self._sync_work_items(
                    mode, power_plants, start_date, end_date
                )

            # Sync budgets (requires power plants list)
            if self.config.get("data", {}).get("fetch_budgets", True):
                stats["budgets"] = self._sync_budgets(mode, power_plants, start_date, end_date)

            logger.info(f"Sync completed successfully: {stats}")
            return stats

        except Exception as e:
            logger.error(f"Error during sync: {e}")
            raise

        finally:
            self.db_handler.disconnect()
            self.api_client.close()

    def _sync_companies(self, mode: str) -> int:
        """Sync companies data.

        NOTE: Companies is a small reference table that always performs a full sync
        regardless of mode. This is more efficient than tracking incremental changes
        for small datasets.

        Args:
            mode: Sync mode (ignored - always performs full sync)

        Returns:
            Number of records synced
        """
        logger.info("Syncing companies (full sync)")

        try:
            companies = self.companies_fetcher.fetch()
            count = self.db_handler.upsert_companies(companies)
            self.db_handler.update_sync_metadata("companies", success=True)
            return count

        except Exception as e:
            self.db_handler.update_sync_metadata("companies", success=False, error_message=str(e))
            raise

    def _sync_power_plants(self, mode: str) -> tuple:
        """Sync power plants data.

        NOTE: Power plants is a small reference table that always performs a full sync
        regardless of mode. This is more efficient than tracking incremental changes
        for small datasets.

        Args:
            mode: Sync mode (ignored - always performs full sync)

        Returns:
            Tuple of (power_plants_list, count)
        """
        logger.info("Syncing power plants (full sync)")

        try:
            power_plants = self.power_plants_fetcher.fetch()
            count = self.db_handler.upsert_power_plants(power_plants)
            self.db_handler.update_sync_metadata("power_plants", success=True)
            return power_plants, count

        except Exception as e:
            self.db_handler.update_sync_metadata(
                "power_plants", success=False, error_message=str(e)
            )
            raise

    def _sync_production(
        self,
        mode: str,
        power_plants: list[dict[str, Any]],
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> int:
        """Sync production data.

        Args:
            mode: Sync mode
            power_plants: List of power plant dictionaries
            from_date: Start date filter
            to_date: End date filter

        Returns:
            Number of records synced
        """
        logger.info("Syncing production data")

        if not power_plants:
            logger.warning("No power plants available, skipping production sync")
            return 0

        try:
            # For incremental mode, use last sync time as from_date
            if mode == "incremental" and not from_date:
                last_sync = self.db_handler.get_last_sync_time("production")
                if last_sync:
                    from_date = last_sync.split("T")[0]  # Convert to YYYY-MM-DD

            production_data = self.production_fetcher.fetch_all_production_days(
                power_plants=power_plants,
                from_date=from_date,
                to_date=to_date,
            )

            # Get UUID to ID mapping from database (after power plants are inserted)
            uuid_to_id = self.db_handler.get_power_plant_uuid_to_id_mapping()
            logger.debug(f"UUID to ID mapping from database: {uuid_to_id}")

            if production_data:
                logger.debug(f"Sample production record keys: {production_data[0].keys()}")

            for record in production_data:
                # Map power_plant_uuid to power_plant_id
                if "power_plant_uuid" in record:
                    plant_uuid = record["power_plant_uuid"]
                    record["power_plant_id"] = uuid_to_id.get(plant_uuid)
                    if not record["power_plant_id"]:
                        logger.warning(f"Could not find database ID for UUID {plant_uuid}")
                # Try other possible field names
                elif "power_plant" in record and isinstance(record["power_plant"], dict):
                    plant_uuid = record["power_plant"].get("uuid")
                    record["power_plant_id"] = uuid_to_id.get(plant_uuid)

            logger.debug(
                f"After mapping, sample record: {production_data[0] if production_data else 'No data'}"
            )

            count = self.db_handler.upsert_production_days(production_data)
            self.db_handler.update_sync_metadata("production", success=True)
            return count

        except Exception as e:
            self.db_handler.update_sync_metadata("production", success=False, error_message=str(e))
            raise

    def _sync_production_periods(
        self,
        mode: str,
        power_plants: list[dict[str, Any]],
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> int:
        """Sync production periods data (hourly production).

        Args:
            mode: Sync mode
            power_plants: List of power plant dictionaries
            from_date: Start date filter
            to_date: End date filter

        Returns:
            Number of records synced
        """
        logger.info("Syncing production periods data")

        if not power_plants:
            logger.warning("No power plants available, skipping production periods sync")
            return 0

        try:
            # For incremental mode, use last sync time as from_date
            if mode == "incremental" and not from_date:
                last_sync = self.db_handler.get_last_sync_time("production_periods")
                if last_sync:
                    from_date = last_sync.split("T")[0]  # Convert to YYYY-MM-DD

            production_periods_data = self.production_periods_fetcher.fetch_all_production_periods(
                power_plants=power_plants,
                timestamp_from=from_date,
                timestamp_to=to_date,
            )

            # Get UUID to ID mapping from database (after power plants are inserted)
            uuid_to_id = self.db_handler.get_power_plant_uuid_to_id_mapping()

            for record in production_periods_data:
                # Map power_plant_uuid to power_plant_id
                if "power_plant_uuid" in record:
                    plant_uuid = record["power_plant_uuid"]
                    record["power_plant_id"] = uuid_to_id.get(plant_uuid)
                    if not record["power_plant_id"]:
                        logger.warning(f"Could not find database ID for UUID {plant_uuid}")

            count = self.db_handler.upsert_production_periods(production_periods_data)
            self.db_handler.update_sync_metadata("production_periods", success=True)
            return count

        except Exception as e:
            self.db_handler.update_sync_metadata(
                "production_periods", success=False, error_message=str(e)
            )
            raise

    def _sync_market_prices(
        self,
        mode: str,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> int:
        """Sync market prices data.

        Args:
            mode: Sync mode
            from_date: Start date filter
            to_date: End date filter

        Returns:
            Number of records synced
        """
        logger.info("Syncing market prices")

        try:
            # For incremental mode, use last sync time as from_date
            if mode == "incremental" and not from_date:
                last_sync = self.db_handler.get_last_sync_time("market_prices")
                if last_sync:
                    from_date = last_sync.split("T")[0]

            # If to_date not specified, use today
            if not to_date:
                to_date = datetime.utcnow().strftime("%Y-%m-%d")

            prices = self.market_prices_fetcher.fetch(from_date=from_date, to_date=to_date)
            count = self.db_handler.upsert_market_prices(prices)
            self.db_handler.update_sync_metadata("market_prices", success=True)
            return count

        except Exception as e:
            self.db_handler.update_sync_metadata(
                "market_prices", success=False, error_message=str(e)
            )
            raise

    def _sync_downtime_events(
        self,
        mode: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> int:
        """Sync downtime events data.

        Args:
            mode: Sync mode
            start_date: Start date filter
            end_date: End date filter

        Returns:
            Number of records synced
        """
        logger.info("Syncing downtime events")

        try:
            # For incremental mode, use last sync time as start_date
            if mode == "incremental" and not start_date:
                last_sync = self.db_handler.get_last_sync_time("downtime_events")
                if last_sync:
                    start_date = last_sync.split("T")[0]

            events = self.om_fetcher.fetch_downtime_events(start_date=start_date, end_date=end_date)

            # Map UUID to ID for database insertion
            uuid_to_id = self.db_handler.get_power_plant_uuid_to_id_mapping()
            for event in events:
                if "power_plant_uuid" in event:
                    event["power_plant_id"] = uuid_to_id.get(event["power_plant_uuid"])

            count = self.db_handler.upsert_downtime_events(events)
            self.db_handler.update_sync_metadata("downtime_events", success=True)
            return count

        except Exception as e:
            self.db_handler.update_sync_metadata(
                "downtime_events", success=False, error_message=str(e)
            )
            raise

    def _sync_downtime_days(
        self,
        mode: str,
        power_plants: list[dict[str, Any]],
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> int:
        """Sync downtime days data.

        Args:
            mode: Sync mode
            power_plants: List of power plant dictionaries
            start_date: Start date filter
            end_date: End date filter

        Returns:
            Number of records synced
        """
        logger.info("Syncing downtime days")

        if not power_plants:
            logger.warning("No power plants available, skipping downtime days sync")
            return 0

        try:
            # For incremental mode, use last sync time as start_date
            if mode == "incremental" and not start_date:
                last_sync = self.db_handler.get_last_sync_time("downtime_days")
                if last_sync:
                    start_date = last_sync.split("T")[0]

            days = self.om_fetcher.fetch_all_downtime_days(
                power_plants=power_plants,
                from_date=start_date,
                to_date=end_date,
            )

            # Map UUID to ID for database insertion
            uuid_to_id = self.db_handler.get_power_plant_uuid_to_id_mapping()
            for day in days:
                if "power_plant_uuid" in day:
                    day["power_plant_id"] = uuid_to_id.get(day["power_plant_uuid"])

            count = self.db_handler.upsert_downtime_days(days)
            self.db_handler.update_sync_metadata("downtime_days", success=True)
            return count

        except Exception as e:
            self.db_handler.update_sync_metadata(
                "downtime_days", success=False, error_message=str(e)
            )
            raise

    def _sync_downtime_periods(
        self,
        mode: str,
        power_plants: list[dict[str, Any]],
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> int:
        """Sync downtime periods data.

        Args:
            mode: Sync mode
            power_plants: List of power plant dictionaries
            start_date: Start date filter (converted to timestamp)
            end_date: End date filter (converted to timestamp)

        Returns:
            Number of records synced
        """
        logger.info("Syncing downtime periods")

        if not power_plants:
            logger.warning("No power plants available, skipping downtime periods sync")
            return 0

        try:
            # For incremental mode, use last sync time as start_date
            if mode == "incremental" and not start_date:
                last_sync = self.db_handler.get_last_sync_time("downtime_periods")
                if last_sync:
                    start_date = last_sync.split("T")[0]

            # Convert dates to timestamps for periods endpoint
            timestamp_from = f"{start_date}T00:00:00" if start_date else None
            timestamp_to = f"{end_date}T23:59:59" if end_date else None

            periods = self.om_fetcher.fetch_all_downtime_periods(
                power_plants=power_plants,
                timestamp_from=timestamp_from,
                timestamp_to=timestamp_to,
            )

            # Map UUID to ID for database insertion
            uuid_to_id = self.db_handler.get_power_plant_uuid_to_id_mapping()
            for period in periods:
                if "power_plant_uuid" in period:
                    period["power_plant_id"] = uuid_to_id.get(period["power_plant_uuid"])

            count = self.db_handler.upsert_downtime_periods(periods)
            self.db_handler.update_sync_metadata("downtime_periods", success=True)
            return count

        except Exception as e:
            self.db_handler.update_sync_metadata(
                "downtime_periods", success=False, error_message=str(e)
            )
            raise

    def _sync_work_items(
        self,
        mode: str,
        power_plants: list[dict[str, Any]],
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> int:
        """Sync work items data.

        Args:
            mode: Sync mode
            power_plants: List of power plant dictionaries
            start_date: Start date filter
            end_date: End date filter

        Returns:
            Number of records synced
        """
        logger.info("Syncing work items")

        if not power_plants:
            logger.warning("No power plants available, skipping work items sync")
            return 0

        try:
            # For incremental mode, use last sync time as start_date
            if mode == "incremental" and not start_date:
                last_sync = self.db_handler.get_last_sync_time("work_items")
                if last_sync:
                    start_date = last_sync.split("T")[0]

            items = self.om_fetcher.fetch_all_work_items(
                power_plants=power_plants,
                start_date=start_date,
                end_date=end_date,
            )

            # Map UUID to ID for database insertion
            uuid_to_id = self.db_handler.get_power_plant_uuid_to_id_mapping()
            for item in items:
                if "power_plant_uuid" in item:
                    item["power_plant_id"] = uuid_to_id.get(item["power_plant_uuid"])

            count = self.db_handler.upsert_work_items(items)
            self.db_handler.update_sync_metadata("work_items", success=True)
            return count

        except Exception as e:
            self.db_handler.update_sync_metadata("work_items", success=False, error_message=str(e))
            raise

    def _sync_budgets(
        self,
        mode: str,
        power_plants: list[dict[str, Any]],
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> int:
        """Sync budget data.

        Args:
            mode: Sync mode
            power_plants: List of power plant dictionaries
            start_date: Start date filter
            end_date: End date filter

        Returns:
            Number of records synced
        """
        logger.info("Syncing budgets")

        if not power_plants:
            logger.warning("No power plants available, skipping budgets sync")
            return 0

        try:
            # For incremental mode, use last sync time as start_date
            if mode == "incremental" and not start_date:
                last_sync = self.db_handler.get_last_sync_time("budgets")
                if last_sync:
                    start_date = last_sync.split("T")[0]

            budgets = self.budgets_fetcher.fetch_all_budgets(
                power_plants=power_plants,
                from_date=start_date,
                to_date=end_date,
            )

            # Map UUID to ID for database insertion
            uuid_to_id = self.db_handler.get_power_plant_uuid_to_id_mapping()
            for budget in budgets:
                if "power_plant_uuid" in budget:
                    budget["power_plant_id"] = uuid_to_id.get(budget["power_plant_uuid"])

            count = self.db_handler.upsert_budgets(budgets)
            self.db_handler.update_sync_metadata("budgets", success=True)
            return count

        except Exception as e:
            self.db_handler.update_sync_metadata("budgets", success=False, error_message=str(e))
            raise
