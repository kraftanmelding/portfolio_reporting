"""Fetcher for market price data."""

import logging
from typing import Any

from ..utils import split_date_range_by_year
from .base import BaseFetcher

logger = logging.getLogger(__name__)


class MarketPricesFetcher(BaseFetcher):
    """Fetcher for market price data from Kaia Solutions Portal API."""

    def fetch(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        price_areas: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch market prices.

        Args:
            from_date: Start date (YYYY-MM-DD format, required)
            to_date: End date (YYYY-MM-DD format, required)
            price_areas: Filter by price areas (e.g., ['NO1', 'NO2'])

        Returns:
            List of market price dictionaries
        """
        logger.info("Fetching market prices from API")

        # Market prices endpoint requires both from_date and to_date
        if not from_date or not to_date:
            logger.warning("Market prices require both from_date and to_date, skipping")
            return []

        # Split date range into yearly chunks to avoid API timeouts
        date_chunks = split_date_range_by_year(from_date, to_date)
        logger.info(f"Split date range into {len(date_chunks)} yearly chunks")

        all_prices = []

        for chunk_start, chunk_end in date_chunks:
            try:
                logger.debug(f"Fetching market prices from {chunk_start} to {chunk_end}")
                params = {
                    "from_date": chunk_start,
                    "to_date": chunk_end,
                }
                if price_areas:
                    params["price_areas[]"] = price_areas

                response = self.api_client.get("/api/v1/market_prices", params=params)

                # The response might be a list or a dict with a 'data' key
                if isinstance(response, list):
                    prices = response
                elif isinstance(response, dict) and "data" in response:
                    prices = response["data"]
                else:
                    prices = [response] if response else []

                # Transform API fields to database schema
                # API returns: nok_mwh, eur_mwh
                # DB expects: price_nok, price_eur
                for price_record in prices:
                    price_record["price_nok"] = price_record.get("nok_mwh")
                    price_record["price_eur"] = price_record.get("eur_mwh")

                all_prices.extend(prices)
                logger.debug(
                    f"Fetched {len(prices)} records for period {chunk_start} to {chunk_end}"
                )

            except Exception as e:
                logger.error(
                    f"Error fetching market prices for period {chunk_start} to {chunk_end}: {e}"
                )
                raise

        logger.info(f"Fetched total of {len(all_prices)} market price records")
        return all_prices
