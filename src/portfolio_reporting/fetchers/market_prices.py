"""Fetcher for market price data."""
import logging
from typing import Any, Dict, List, Optional

from .base import BaseFetcher


logger = logging.getLogger(__name__)


class MarketPricesFetcher(BaseFetcher):
    """Fetcher for market price data from Kaia Solutions Portal API."""

    def fetch(
        self,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        price_areas: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
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

        try:
            params = {
                "from_date": from_date,
                "to_date": to_date,
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

            logger.info(f"Fetched {len(prices)} market price records")
            return prices

        except Exception as e:
            logger.error(f"Error fetching market prices: {e}")
            raise
