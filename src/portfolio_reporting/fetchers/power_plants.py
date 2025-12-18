"""Fetcher for power plant data."""

import logging
from typing import Any

from .base import BaseFetcher

logger = logging.getLogger(__name__)


class PowerPlantsFetcher(BaseFetcher):
    """Fetcher for power plant data from Kaia Solutions Portal API."""

    def fetch(self) -> list[dict[str, Any]]:
        """Fetch all power plants and transform to database schema.

        Returns:
            List of power plant dictionaries with fields mapped to database schema
        """
        logger.info("Fetching power plants from API")

        try:
            # Use v2 API with reporting=true for portfolio reporting fields
            response = self.api_client.get("/api/v2/power_plants", params={"reporting": "true"})

            # The response might be a list or a dict with a 'data' key
            if isinstance(response, list):
                power_plants = response
            elif isinstance(response, dict) and "data" in response:
                power_plants = response["data"]
            else:
                power_plants = [response] if response else []

            # Transform API fields to database schema
            transformed = []
            for plant in power_plants:
                # Extract country name from dict if needed
                country_value = plant.get("country")
                if isinstance(country_value, dict):
                    country = country_value.get("name")
                else:
                    country = country_value

                transformed.append(
                    {
                        "id": plant.get("power_plant_id"),  # From reporting=true
                        "uuid": plant.get("uuid"),
                        "name": plant.get("name"),
                        "company_id": plant.get("company_id"),
                        "portfolio_name": plant.get("portfolio_name"),  # From reporting=true
                        "asset_class_type": plant.get(
                            "asset_class"
                        ),  # API: asset_class → DB: asset_class_type
                        "capacity_mw": plant.get(
                            "installed_effect"
                        ),  # API: installed_effect → DB: capacity_mw
                        "price_area": plant.get("price_area"),
                        "country": country,
                        "latitude": plant.get("lat"),  # API: lat → DB: latitude
                        "longitude": plant.get("lng"),  # API: lng → DB: longitude
                        "commissioned_date": plant.get(
                            "commissioning_date"
                        ),  # API: commissioning_date
                        "created_at": plant.get("created_at"),
                        "updated_at": plant.get("updated_at"),
                    }
                )

            logger.info(f"Fetched {len(transformed)} power plants")
            return transformed

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
