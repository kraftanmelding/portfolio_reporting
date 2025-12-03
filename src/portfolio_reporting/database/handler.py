"""Database handler for SQLite operations."""

import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from .schema import SCHEMA_SQL

logger = logging.getLogger(__name__)


class DatabaseHandler:
    """Handler for SQLite database operations."""

    def __init__(self, db_path: str):
        """Initialize database handler.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn: sqlite3.Connection | None = None

    def connect(self):
        """Establish database connection."""
        logger.info(f"Connecting to database: {self.db_path}")
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row  # Enable column access by name
        logger.info("Database connection established")

    def disconnect(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

    def initialize_schema(self):
        """Create all tables and indexes."""
        if not self.conn:
            raise RuntimeError("Database not connected")

        logger.info("Initializing database schema")
        cursor = self.conn.cursor()
        cursor.executescript(SCHEMA_SQL)
        self.conn.commit()
        logger.info("Database schema initialized successfully")

    def upsert_companies(self, companies: list[dict[str, Any]]) -> int:
        """Insert or update companies.

        Args:
            companies: List of company dictionaries

        Returns:
            Number of companies processed
        """
        if not self.conn:
            raise RuntimeError("Database not connected")

        cursor = self.conn.cursor()
        count = 0

        for company in companies:
            cursor.execute(
                """
                INSERT INTO companies (id, name, description, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    description = excluded.description,
                    updated_at = excluded.updated_at
                """,
                (
                    company.get("id"),
                    company.get("name"),
                    company.get("description"),
                    company.get("created_at"),
                    datetime.utcnow().isoformat(),
                ),
            )
            count += 1

        self.conn.commit()
        logger.info(f"Upserted {count} companies")
        return count

    def upsert_power_plants(self, power_plants: list[dict[str, Any]]) -> int:
        """Insert or update power plants.

        Args:
            power_plants: List of power plant dictionaries

        Returns:
            Number of power plants processed
        """
        if not self.conn:
            raise RuntimeError("Database not connected")

        cursor = self.conn.cursor()
        count = 0

        for plant in power_plants:
            cursor.execute(
                """
                INSERT INTO power_plants (
                    id, uuid, name, company_id, power_plant_type,
                    capacity_mw, latitude, longitude, commissioned_date,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(uuid) DO UPDATE SET
                    id = excluded.id,
                    name = excluded.name,
                    company_id = excluded.company_id,
                    power_plant_type = excluded.power_plant_type,
                    capacity_mw = excluded.capacity_mw,
                    latitude = excluded.latitude,
                    longitude = excluded.longitude,
                    commissioned_date = excluded.commissioned_date,
                    updated_at = excluded.updated_at
                """,
                (
                    plant.get("id"),
                    plant.get("uuid"),
                    plant.get("name"),
                    plant.get("company_id"),
                    plant.get("power_plant_type"),
                    plant.get("capacity_mw"),
                    plant.get("latitude"),
                    plant.get("longitude"),
                    plant.get("commissioned_date"),
                    plant.get("created_at"),
                    datetime.utcnow().isoformat(),
                ),
            )
            count += 1

        self.conn.commit()
        logger.info(f"Upserted {count} power plants")
        return count

    def upsert_production_days(self, production_data: list[dict[str, Any]]) -> int:
        """Insert or update production days.

        Args:
            production_data: List of production day dictionaries

        Returns:
            Number of records processed
        """
        if not self.conn:
            raise RuntimeError("Database not connected")

        cursor = self.conn.cursor()
        count = 0

        for record in production_data:
            cursor.execute(
                """
                INSERT INTO production_days (
                    power_plant_id, date, volume, revenue, currency,
                    forecasted_volume, cap_theoretical_volume,
                    full_load_count, no_load_count, operational_count,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(power_plant_id, date, currency) DO UPDATE SET
                    volume = excluded.volume,
                    revenue = excluded.revenue,
                    forecasted_volume = excluded.forecasted_volume,
                    cap_theoretical_volume = excluded.cap_theoretical_volume,
                    full_load_count = excluded.full_load_count,
                    no_load_count = excluded.no_load_count,
                    operational_count = excluded.operational_count,
                    updated_at = excluded.updated_at
                """,
                (
                    record.get("power_plant_id"),
                    record.get("date"),
                    record.get("volume"),
                    record.get("revenue"),
                    record.get("currency", "NOK"),
                    record.get("forecasted_volume"),
                    record.get("cap_theoretical_volume"),
                    record.get("full_load_count"),
                    record.get("no_load_count"),
                    record.get("operational_count"),
                    datetime.utcnow().isoformat(),
                    datetime.utcnow().isoformat(),
                ),
            )
            count += 1

        self.conn.commit()
        logger.info(f"Upserted {count} production day records")
        return count

    def upsert_market_prices(self, prices: list[dict[str, Any]]) -> int:
        """Insert or update market prices.

        Args:
            prices: List of market price dictionaries

        Returns:
            Number of records processed
        """
        if not self.conn:
            raise RuntimeError("Database not connected")

        cursor = self.conn.cursor()
        count = 0

        for price in prices:
            cursor.execute(
                """
                INSERT INTO market_prices (
                    price_area, timestamp, price, currency,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(price_area, timestamp) DO UPDATE SET
                    price = excluded.price,
                    currency = excluded.currency,
                    updated_at = excluded.updated_at
                """,
                (
                    price.get("price_area"),
                    price.get("timestamp"),
                    price.get("price"),
                    price.get("currency", "NOK"),
                    datetime.utcnow().isoformat(),
                    datetime.utcnow().isoformat(),
                ),
            )
            count += 1

        self.conn.commit()
        logger.info(f"Upserted {count} market price records")
        return count

    def upsert_downtime_events(self, events: list[dict[str, Any]]) -> int:
        """Insert or update downtime events.

        Args:
            events: List of downtime event dictionaries

        Returns:
            Number of records processed
        """
        if not self.conn:
            raise RuntimeError("Database not connected")

        cursor = self.conn.cursor()
        count = 0

        for event in events:
            cursor.execute(
                """
                INSERT INTO downtime_events (
                    id, power_plant_id, start_time, end_time, duration_hours,
                    reason, event_type, lost_production_kwh,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    end_time = excluded.end_time,
                    duration_hours = excluded.duration_hours,
                    reason = excluded.reason,
                    event_type = excluded.event_type,
                    lost_production_kwh = excluded.lost_production_kwh,
                    updated_at = excluded.updated_at
                """,
                (
                    event.get("id"),
                    event.get("power_plant_id"),
                    event.get("start_time"),
                    event.get("end_time"),
                    event.get("duration_hours"),
                    event.get("reason"),
                    event.get("event_type"),
                    event.get("lost_production_kwh"),
                    event.get("created_at"),
                    datetime.utcnow().isoformat(),
                ),
            )
            count += 1

        self.conn.commit()
        logger.info(f"Upserted {count} downtime events")
        return count

    def upsert_work_items(self, items: list[dict[str, Any]]) -> int:
        """Insert or update work items.

        Args:
            items: List of work item dictionaries

        Returns:
            Number of records processed
        """
        if not self.conn:
            raise RuntimeError("Database not connected")

        cursor = self.conn.cursor()
        count = 0

        for item in items:
            cursor.execute(
                """
                INSERT INTO work_items (
                    id, power_plant_id, title, description, status,
                    priority, assigned_to, due_date, completed_at,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    title = excluded.title,
                    description = excluded.description,
                    status = excluded.status,
                    priority = excluded.priority,
                    assigned_to = excluded.assigned_to,
                    due_date = excluded.due_date,
                    completed_at = excluded.completed_at,
                    updated_at = excluded.updated_at
                """,
                (
                    item.get("id"),
                    item.get("power_plant_id"),
                    item.get("title"),
                    item.get("description"),
                    item.get("status"),
                    item.get("priority"),
                    item.get("assigned_to"),
                    item.get("due_date"),
                    item.get("completed_at"),
                    item.get("created_at"),
                    datetime.utcnow().isoformat(),
                ),
            )
            count += 1

        self.conn.commit()
        logger.info(f"Upserted {count} work items")
        return count

    def update_sync_metadata(
        self,
        entity_type: str,
        success: bool = True,
        error_message: str | None = None,
    ):
        """Update sync metadata for incremental updates.

        Args:
            entity_type: Type of entity synced (e.g., 'power_plants', 'production')
            success: Whether the sync was successful
            error_message: Error message if sync failed
        """
        if not self.conn:
            raise RuntimeError("Database not connected")

        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO sync_metadata (
                entity_type, last_sync_at, last_sync_success, error_message, updated_at
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(entity_type) DO UPDATE SET
                last_sync_at = excluded.last_sync_at,
                last_sync_success = excluded.last_sync_success,
                error_message = excluded.error_message,
                updated_at = excluded.updated_at
            """,
            (
                entity_type,
                datetime.utcnow().isoformat(),
                success,
                error_message,
                datetime.utcnow().isoformat(),
            ),
        )
        self.conn.commit()

    def get_last_sync_time(self, entity_type: str) -> str | None:
        """Get last successful sync time for entity type.

        Args:
            entity_type: Type of entity

        Returns:
            ISO format timestamp of last sync, or None if never synced
        """
        if not self.conn:
            raise RuntimeError("Database not connected")

        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT last_sync_at
            FROM sync_metadata
            WHERE entity_type = ? AND last_sync_success = 1
            """,
            (entity_type,),
        )
        row = cursor.fetchone()
        return row["last_sync_at"] if row else None

    def get_power_plant_uuid_to_id_mapping(self) -> dict[str, int]:
        """Get mapping of power plant UUIDs to database IDs.

        Returns:
            Dictionary mapping UUID to database ID
        """
        if not self.conn:
            raise RuntimeError("Database not connected")

        cursor = self.conn.cursor()
        cursor.execute("SELECT id, uuid FROM power_plants")
        rows = cursor.fetchall()
        return {row["uuid"]: row["id"] for row in rows}

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
