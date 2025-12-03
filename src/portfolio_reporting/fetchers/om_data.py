"""Fetcher for O&M (Operations & Maintenance) data."""

import logging
from typing import Any

from ..utils import split_date_range_by_year
from .base import BaseFetcher

logger = logging.getLogger(__name__)


class OMDataFetcher(BaseFetcher):
    """Fetcher for O&M data from Kaia Solutions Portal API."""

    def fetch_downtime_events(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        power_plant_uuid: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch downtime events.

        Args:
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            power_plant_uuid: Filter by specific power plant UUID

        Returns:
            List of downtime event dictionaries
        """
        logger.info("Fetching downtime events from API")

        # Split date range into yearly chunks to avoid API timeouts
        date_chunks = split_date_range_by_year(start_date, end_date)
        logger.info(f"Split date range into {len(date_chunks)} yearly chunks")

        all_events = []

        for chunk_start, chunk_end in date_chunks:
            try:
                logger.debug(f"Fetching downtime events from {chunk_start} to {chunk_end}")
                params = {}
                if chunk_start:
                    params["start_date"] = chunk_start
                if chunk_end:
                    params["end_date"] = chunk_end
                if power_plant_uuid:
                    params["power_plant_uuid"] = power_plant_uuid

                response = self.api_client.get("/api/v2/downtime_events", params=params)

                # The response might be a list or a dict with a 'data' key
                if isinstance(response, list):
                    events = response
                elif isinstance(response, dict) and "data" in response:
                    events = response["data"]
                else:
                    events = [response] if response else []

                all_events.extend(events)
                logger.debug(
                    f"Fetched {len(events)} events for period {chunk_start} to {chunk_end}"
                )

            except Exception as e:
                logger.error(
                    f"Error fetching downtime events for period {chunk_start} to {chunk_end}: {e}"
                )
                raise

        logger.info(f"Fetched total of {len(all_events)} downtime events")
        return all_events

    def fetch_scheduled_downtime_events(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        power_plant_uuid: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch scheduled downtime events.

        Args:
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            power_plant_uuid: Filter by specific power plant UUID

        Returns:
            List of scheduled downtime event dictionaries
        """
        logger.info("Fetching scheduled downtime events from API")

        try:
            params = {}
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
            if power_plant_uuid:
                params["power_plant_uuid"] = power_plant_uuid

            response = self.api_client.get("/api/v2/scheduled_downtime_events", params=params)

            # The response might be a list or a dict with a 'data' key
            if isinstance(response, list):
                events = response
            elif isinstance(response, dict) and "data" in response:
                events = response["data"]
            else:
                events = [response] if response else []

            logger.info(f"Fetched {len(events)} scheduled downtime events")
            return events

        except Exception as e:
            logger.error(f"Error fetching scheduled downtime events: {e}")
            raise

    def fetch_work_items(
        self,
        power_plant_uuid: str,
        start_date: str | None = None,
        end_date: str | None = None,
        status: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch work items for a specific power plant.

        Args:
            power_plant_uuid: Power plant UUID (required)
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            status: Filter by status (e.g., 'open', 'closed')
            limit: Maximum number of items to fetch (API default is 100)

        Returns:
            List of work item dictionaries
        """
        logger.debug(f"Fetching work items for power plant {power_plant_uuid}")

        try:
            params = {"power_plant_uuid": power_plant_uuid}
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
            if status:
                params["status"] = status
            if limit:
                params["limit"] = limit

            response = self.api_client.get("/api/v2/work_items", params=params)

            # The response might be a list or a dict with a 'data' key
            if isinstance(response, list):
                items = response
            elif isinstance(response, dict) and "data" in response:
                items = response["data"]
            else:
                items = [response] if response else []

            logger.debug(f"Fetched {len(items)} work items for {power_plant_uuid}")
            return items

        except Exception as e:
            logger.error(f"Error fetching work items for {power_plant_uuid}: {e}")
            raise

    def fetch_all_work_items(
        self,
        power_plants: list[dict[str, Any]],
        start_date: str | None = None,
        end_date: str | None = None,
        status: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch work items for all power plants.

        Args:
            power_plants: List of power plant dictionaries with 'uuid' field
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            status: Filter by status (e.g., 'open', 'closed')
            limit: Maximum number of items to fetch per plant

        Returns:
            List of all work item dictionaries
        """
        logger.info(f"Fetching work items for {len(power_plants)} power plants")

        # Split date range into yearly chunks to avoid API timeouts
        date_chunks = split_date_range_by_year(start_date, end_date)
        logger.info(f"Split date range into {len(date_chunks)} yearly chunks")

        all_items = []

        for plant in power_plants:
            uuid = plant.get("uuid")
            if not uuid:
                logger.warning(f"Power plant missing UUID: {plant.get('name', 'Unknown')}")
                continue

            # Fetch work items for each yearly chunk
            for chunk_start, chunk_end in date_chunks:
                try:
                    logger.debug(
                        f"Fetching work items for {uuid} from {chunk_start} to {chunk_end}"
                    )
                    items = self.fetch_work_items(
                        power_plant_uuid=uuid,
                        start_date=chunk_start,
                        end_date=chunk_end,
                        status=status,
                        limit=limit,
                    )
                    all_items.extend(items)

                except Exception as e:
                    logger.warning(
                        f"Failed to fetch work items for {uuid} ({chunk_start} to {chunk_end}): {e}"
                    )
                    # Continue with other chunks/plants even if one fails
                    continue

        logger.info(f"Fetched total of {len(all_items)} work items")
        return all_items
