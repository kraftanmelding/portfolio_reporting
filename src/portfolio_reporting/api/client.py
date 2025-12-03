"""API client for Kaia Solutions Portal API."""
import logging
import time
from typing import Any, Dict, Optional
from urllib.parse import urljoin

import requests


logger = logging.getLogger(__name__)


class APIClient:
    """Client for interacting with Kaia Solutions Portal API."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        timeout: int = 30,
        retry_attempts: int = 3,
    ):
        """Initialize API client.

        Args:
            base_url: Base URL of the Kaia Solutions Portal API
            api_key: API key for authentication
            timeout: Request timeout in seconds
            retry_attempts: Number of retry attempts for failed requests
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request with retry logic.

        Args:
            method: HTTP method (GET, POST, PUT, etc.)
            endpoint: API endpoint (e.g., '/api/v1/power_plants')
            params: Query parameters
            json: JSON body for POST/PUT requests

        Returns:
            Response JSON data

        Raises:
            requests.exceptions.RequestException: If request fails after retries
        """
        url = urljoin(self.base_url, endpoint)
        attempt = 0

        while attempt < self.retry_attempts:
            try:
                logger.debug(
                    f"Making {method} request to {url} (attempt {attempt + 1}/{self.retry_attempts})"
                )
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                logger.debug(f"Request successful: {method} {url}")
                return response.json()

            except requests.exceptions.HTTPError as e:
                if response.status_code in [401, 403]:
                    # Don't retry authentication errors
                    logger.error(f"Authentication error: {e}")
                    raise
                logger.warning(
                    f"HTTP error on attempt {attempt + 1}: {e}"
                )
                attempt += 1
                if attempt < self.retry_attempts:
                    time.sleep(2**attempt)  # Exponential backoff
                else:
                    raise

            except requests.exceptions.RequestException as e:
                logger.warning(
                    f"Request error on attempt {attempt + 1}: {e}"
                )
                attempt += 1
                if attempt < self.retry_attempts:
                    time.sleep(2**attempt)
                else:
                    raise

        raise requests.exceptions.RequestException(
            f"Failed after {self.retry_attempts} attempts"
        )

    def get(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make GET request.

        Args:
            endpoint: API endpoint
            params: Query parameters

        Returns:
            Response JSON data
        """
        return self._make_request("GET", endpoint, params=params)

    def post(
        self,
        endpoint: str,
        json: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make POST request.

        Args:
            endpoint: API endpoint
            json: JSON body
            params: Query parameters

        Returns:
            Response JSON data
        """
        return self._make_request("POST", endpoint, params=params, json=json)

    def put(
        self,
        endpoint: str,
        json: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make PUT request.

        Args:
            endpoint: API endpoint
            json: JSON body
            params: Query parameters

        Returns:
            Response JSON data
        """
        return self._make_request("PUT", endpoint, params=params, json=json)

    def close(self):
        """Close the session."""
        self.session.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
