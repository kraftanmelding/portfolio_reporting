"""Fetcher for production data."""

import logging
from typing import Any

from ..utils import split_date_range_by_year
from .base import BaseFetcher

logger = logging.getLogger(__name__)


class ProductionFetcher(BaseFetcher):
    """Fetcher for production data from Kaia Solutions Portal API."""

    def fetch_production_days(
        self,
        power_plant_uuid: str,
        from_date: str | None = None,
        to_date: str | None = None,
        currency: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch daily production data for a specific power plant.

        Args:
            power_plant_uuid: Power plant UUID (required)
            from_date: Start date (YYYY-MM-DD format)
            to_date: End date (YYYY-MM-DD format)
            currency: Currency code (NOK or EUR)

        Returns:
            List of production day dictionaries
        """
        logger.debug(
            f"Fetching production days for power plant {power_plant_uuid} (currency: {currency})"
        )

        try:
            params = {"power_plant_uuid": power_plant_uuid}
            if from_date:
                params["from_date"] = from_date
            if to_date:
                params["to_date"] = to_date
            if currency:
                params["currency"] = currency

            response = self.api_client.get("/api/v2/production/days", params=params)

            # The response might be a list or a dict with a 'data' key
            if isinstance(response, list):
                production_data = response
            elif isinstance(response, dict) and "data" in response:
                production_data = response["data"]
            else:
                production_data = [response] if response else []

            # Add currency field to each record
            for record in production_data:
                record["currency"] = currency if currency else "NOK"

            logger.debug(
                f"Fetched {len(production_data)} production day records for {power_plant_uuid} (currency: {currency})"
            )
            return production_data

        except Exception as e:
            logger.error(f"Error fetching production days for {power_plant_uuid}: {e}")
            raise

    def fetch_all_production_days(
        self,
        power_plants: list[dict[str, Any]],
        from_date: str | None = None,
        to_date: str | None = None,
        currencies: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch production data for all power plants in multiple currencies.

        Args:
            power_plants: List of power plant dictionaries with 'uuid' field
            from_date: Start date (YYYY-MM-DD format)
            to_date: End date (YYYY-MM-DD format)
            currencies: List of currency codes (e.g., ['NOK', 'EUR']). Defaults to ['NOK', 'EUR']

        Returns:
            List of all production day dictionaries
        """
        if currencies is None:
            currencies = ["NOK", "EUR"]

        # Split date range into yearly chunks to avoid API limit of 365 records
        date_chunks = split_date_range_by_year(from_date, to_date)
        logger.info(
            f"Fetching production days for {len(power_plants)} power plants in {len(currencies)} currencies"
        )
        logger.info(f"Split date range into {len(date_chunks)} yearly chunks")

        all_production_data = []

        for plant in power_plants:
            uuid = plant.get("uuid")
            if not uuid:
                logger.warning(f"Power plant missing UUID: {plant.get('name', 'Unknown')}")
                continue

            for currency in currencies:
                # Fetch production for each yearly chunk
                for chunk_start, chunk_end in date_chunks:
                    try:
                        production_data = self.fetch_production_days(
                            power_plant_uuid=uuid,
                            from_date=chunk_start,
                            to_date=chunk_end,
                            currency=currency,
                        )
                        all_production_data.extend(production_data)

                    except Exception as e:
                        logger.warning(
                            f"Failed to fetch production for {uuid} in {currency} ({chunk_start} to {chunk_end}): {e}"
                        )
                        # Continue with other chunks/currencies/plants even if one fails
                        continue

        logger.info(f"Fetched total of {len(all_production_data)} production day records")
        return all_production_data
