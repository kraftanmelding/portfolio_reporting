"""Fetcher for company data."""

import logging
from typing import Any

from .base import BaseFetcher

logger = logging.getLogger(__name__)


class CompaniesFetcher(BaseFetcher):
    """Fetcher for company data from Kaia Solutions Portal API."""

    def fetch(self) -> list[dict[str, Any]]:
        """Fetch all companies.

        Returns:
            List of company dictionaries
        """
        logger.info("Fetching companies from API")

        try:
            # Fetch from v1 API
            response = self.api_client.get("/api/v1/companies")

            # The response might be a list or a dict with a 'data' key
            if isinstance(response, list):
                companies = response
            elif isinstance(response, dict) and "data" in response:
                companies = response["data"]
            else:
                companies = [response] if response else []

            logger.info(f"Fetched {len(companies)} companies")
            return companies

        except Exception as e:
            logger.error(f"Error fetching companies: {e}")
            raise
