"""Fetcher for power plant data."""

import logging
from typing import Any

from .base import BaseFetcher

logger = logging.getLogger(__name__)


class PowerPlantsFetcher(BaseFetcher):
    """Fetcher for power plant data from Kaia Solutions Portal API."""

    def fetch(self) -> list[dict[str, Any]]:
        """Fetch all power plants.

        Returns:
            List of power plant dictionaries
        """
        logger.info("Fetching power plants from API")

        try:
            # Use v2 API for more detailed power plant information
            response = self.api_client.get("/api/v2/power_plants")

            # The response might be a list or a dict with a 'data' key
            if isinstance(response, list):
                power_plants = response
            elif isinstance(response, dict) and "data" in response:
                power_plants = response["data"]
            else:
                power_plants = [response] if response else []

            logger.info(f"Fetched {len(power_plants)} power plants")
            return power_plants

        except Exception as e:
            logger.error(f"Error fetching power plants: {e}")
            raise

    def fetch_time_series(self, uuid: str, params: dict[str, Any]) -> dict[str, Any]:
        """Fetch time series data for a specific power plant.

        Args:
            uuid: Power plant UUID
            params: Query parameters (e.g., start_date, end_date, resolution)

        Returns:
            Time series data dictionary
        """
        logger.info(f"Fetching time series for power plant {uuid}")

        try:
            endpoint = f"/api/v1/power_plants/{uuid}/time_series"
            response = self.api_client.get(endpoint, params=params)

            logger.debug(f"Fetched time series data for {uuid}")
            return response

        except Exception as e:
            logger.error(f"Error fetching time series for {uuid}: {e}")
            raise
