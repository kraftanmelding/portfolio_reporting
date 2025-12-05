"""Fetcher for production periods data (hourly production)."""

import logging
from typing import Any

from ..utils import split_date_range_by_year
from .base import BaseFetcher

logger = logging.getLogger(__name__)


class ProductionPeriodsFetcher(BaseFetcher):
    """Fetcher for hourly production periods data from Kaia Solutions Portal API."""

    def fetch_production_periods(
        self,
        power_plant_uuid: str,
        timestamp_from: str | None = None,
        timestamp_to: str | None = None,
        currency: str = "NOK",
    ) -> list[dict[str, Any]]:
        """Fetch production periods (hourly) for a specific power plant.

        Args:
            power_plant_uuid: Power plant UUID (required)
            timestamp_from: Start timestamp (ISO8601 format)
            timestamp_to: End timestamp (ISO8601 format)
            currency: Currency for revenue (NOK or EUR)

        Returns:
            List of production period dictionaries
        """
        logger.debug(f"Fetching production periods for {power_plant_uuid} in {currency}")

        try:
            params = {"power_plant_uuid": power_plant_uuid, "currency": currency}
            if timestamp_from:
                params["timestamp_from"] = timestamp_from
            if timestamp_to:
                params["timestamp_to"] = timestamp_to

            response = self.api_client.get("/api/v2/production_periods", params=params)

            # The response might be a list or a dict with a 'data' key
            if isinstance(response, list):
                periods = response
            elif isinstance(response, dict) and "data" in response:
                periods = response["data"]
            else:
                periods = [response] if response else []

            logger.debug(f"Fetched {len(periods)} production periods for {power_plant_uuid}")
            return periods

        except Exception as e:
            logger.error(f"Error fetching production periods for {power_plant_uuid}: {e}")
            raise

    def fetch_all_production_periods(
        self,
        power_plants: list[dict[str, Any]],
        timestamp_from: str | None = None,
        timestamp_to: str | None = None,
        currencies: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch production periods for all power plants in multiple currencies.

        Args:
            power_plants: List of power plant dictionaries with 'uuid' field
            timestamp_from: Start timestamp (ISO8601 format)
            timestamp_to: End timestamp (ISO8601 format)
            currencies: List of currencies to fetch (default: ["NOK", "EUR"])

        Returns:
            List of all production period dictionaries
        """
        if currencies is None:
            currencies = ["NOK", "EUR"]

        logger.info(
            f"Fetching production periods for {len(power_plants)} power plants in {len(currencies)} currencies"
        )

        # Split date range into yearly chunks to avoid API timeouts
        date_chunks = split_date_range_by_year(timestamp_from, timestamp_to)
        logger.info(f"Split date range into {len(date_chunks)} yearly chunks")

        all_periods = []

        for plant in power_plants:
            uuid = plant.get("uuid")
            if not uuid:
                logger.warning(f"Power plant missing UUID: {plant.get('name', 'Unknown')}")
                continue

            # Fetch production periods for each currency
            for currency in currencies:
                # Fetch periods for each yearly chunk
                for chunk_start, chunk_end in date_chunks:
                    try:
                        logger.debug(
                            f"Fetching production periods for {uuid} in {currency} from {chunk_start} to {chunk_end}"
                        )
                        periods = self.fetch_production_periods(
                            power_plant_uuid=uuid,
                            timestamp_from=chunk_start,
                            timestamp_to=chunk_end,
                            currency=currency,
                        )
                        # Add currency field to each record for grouping later
                        for period in periods:
                            period["currency"] = currency
                            period["power_plant_uuid"] = uuid
                        all_periods.extend(periods)

                    except Exception as e:
                        logger.warning(
                            f"Failed to fetch production periods for {uuid} in {currency} ({chunk_start} to {chunk_end}): {e}"
                        )
                        # Continue with other chunks/plants even if one fails
                        continue

        logger.info(f"Fetched total of {len(all_periods)} production periods")
        return all_periods
