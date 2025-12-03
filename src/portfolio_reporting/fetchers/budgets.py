"""Fetcher for budget data."""

import logging
from typing import Any

from ..utils import split_date_range_by_year
from .base import BaseFetcher

logger = logging.getLogger(__name__)


class BudgetsFetcher(BaseFetcher):
    """Fetcher for budget data from Kaia Solutions Portal API."""

    def fetch_budgets(
        self,
        power_plant_uuid: str,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch budgets for a specific power plant.

        Args:
            power_plant_uuid: Power plant UUID (required)
            from_date: Start date (YYYY-MM-DD format)
            to_date: End date (YYYY-MM-DD format)
            limit: Maximum number of months to fetch (default 120, max 360)

        Returns:
            List of budget dictionaries
        """
        logger.debug(f"Fetching budgets for power plant {power_plant_uuid}")

        try:
            params = {"power_plant_uuid": power_plant_uuid}
            if from_date:
                params["from"] = from_date
            if to_date:
                params["to"] = to_date
            if limit:
                params["limit"] = limit

            response = self.api_client.get("/api/v2/budgets", params=params)

            # The response might be a list or a dict with a 'data' key
            if isinstance(response, list):
                budgets = response
            elif isinstance(response, dict) and "data" in response:
                budgets = response["data"]
            else:
                budgets = [response] if response else []

            logger.debug(f"Fetched {len(budgets)} budget records for {power_plant_uuid}")
            return budgets

        except Exception as e:
            logger.error(f"Error fetching budgets for {power_plant_uuid}: {e}")
            raise

    def fetch_all_budgets(
        self,
        power_plants: list[dict[str, Any]],
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch budgets for all power plants.

        Args:
            power_plants: List of power plant dictionaries with 'uuid' field
            from_date: Start date (YYYY-MM-DD format)
            to_date: End date (YYYY-MM-DD format)
            limit: Maximum number of months to fetch per plant

        Returns:
            List of all budget dictionaries
        """
        logger.info(f"Fetching budgets for {len(power_plants)} power plants")

        # Split date range into yearly chunks to avoid API timeouts
        date_chunks = split_date_range_by_year(from_date, to_date)
        logger.info(f"Split date range into {len(date_chunks)} yearly chunks")

        all_budgets = []

        for plant in power_plants:
            uuid = plant.get("uuid")
            if not uuid:
                logger.warning(f"Power plant missing UUID: {plant.get('name', 'Unknown')}")
                continue

            # Fetch budgets for each yearly chunk
            for chunk_start, chunk_end in date_chunks:
                try:
                    logger.debug(f"Fetching budgets for {uuid} from {chunk_start} to {chunk_end}")
                    budgets = self.fetch_budgets(
                        power_plant_uuid=uuid,
                        from_date=chunk_start,
                        to_date=chunk_end,
                        limit=limit,
                    )
                    all_budgets.extend(budgets)

                except Exception as e:
                    logger.warning(
                        f"Failed to fetch budgets for {uuid} ({chunk_start} to {chunk_end}): {e}"
                    )
                    # Continue with other chunks/plants even if one fails
                    continue

        logger.info(f"Fetched total of {len(all_budgets)} budget records")
        return all_budgets
