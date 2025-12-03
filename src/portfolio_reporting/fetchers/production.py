"""Fetcher for production data."""
import logging
from typing import Any, Dict, List, Optional

from .base import BaseFetcher


logger = logging.getLogger(__name__)


class ProductionFetcher(BaseFetcher):
    """Fetcher for production data from Captiva Portal API."""

    def fetch_production_days(
        self,
        power_plant_uuid: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch daily production data for a specific power plant.

        Args:
            power_plant_uuid: Power plant UUID (required)
            from_date: Start date (YYYY-MM-DD format)
            to_date: End date (YYYY-MM-DD format)

        Returns:
            List of production day dictionaries
        """
        logger.debug(f"Fetching production days for power plant {power_plant_uuid}")

        try:
            params = {"power_plant_uuid": power_plant_uuid}
            if from_date:
                params["from_date"] = from_date
            if to_date:
                params["to_date"] = to_date

            response = self.api_client.get("/api/v2/production/days", params=params)

            # The response might be a list or a dict with a 'data' key
            if isinstance(response, list):
                production_data = response
            elif isinstance(response, dict) and "data" in response:
                production_data = response["data"]
            else:
                production_data = [response] if response else []

            logger.debug(f"Fetched {len(production_data)} production day records for {power_plant_uuid}")
            return production_data

        except Exception as e:
            logger.error(f"Error fetching production days for {power_plant_uuid}: {e}")
            raise

    def fetch_all_production_days(
        self,
        power_plants: List[Dict[str, Any]],
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch production data for all power plants.

        Args:
            power_plants: List of power plant dictionaries with 'uuid' field
            from_date: Start date (YYYY-MM-DD format)
            to_date: End date (YYYY-MM-DD format)

        Returns:
            List of all production day dictionaries
        """
        logger.info(f"Fetching production days for {len(power_plants)} power plants")

        all_production_data = []

        for plant in power_plants:
            uuid = plant.get("uuid")
            if not uuid:
                logger.warning(f"Power plant missing UUID: {plant.get('name', 'Unknown')}")
                continue

            try:
                production_data = self.fetch_production_days(
                    power_plant_uuid=uuid,
                    from_date=from_date,
                    to_date=to_date,
                )
                all_production_data.extend(production_data)

            except Exception as e:
                logger.warning(f"Failed to fetch production for {uuid}: {e}")
                # Continue with other power plants even if one fails
                continue

        logger.info(f"Fetched total of {len(all_production_data)} production day records")
        return all_production_data
