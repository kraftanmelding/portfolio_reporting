"""Base fetcher class for data retrieval."""
import logging
from typing import Any, Dict, List, Optional

from ..api.client import APIClient


logger = logging.getLogger(__name__)


class BaseFetcher:
    """Base class for data fetchers."""

    def __init__(self, api_client: APIClient):
        """Initialize fetcher.

        Args:
            api_client: API client instance
        """
        self.api_client = api_client

    def fetch(self, **kwargs) -> List[Dict[str, Any]]:
        """Fetch data from API.

        Args:
            **kwargs: Additional parameters for fetching

        Returns:
            List of data dictionaries

        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError("Subclasses must implement fetch method")
