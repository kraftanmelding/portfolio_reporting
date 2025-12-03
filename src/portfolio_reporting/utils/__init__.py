"""Utility functions for portfolio reporting."""

from datetime import date, datetime


def split_date_range_by_year(
    start_date: str | date | None, end_date: str | date | None
) -> list[tuple[str, str]]:
    """Split a date range into yearly chunks.

    Args:
        start_date: Start date in YYYY-MM-DD format or datetime.date object
        end_date: End date in YYYY-MM-DD format or datetime.date object

    Returns:
        List of (start_date, end_date) tuples, one per year

    Example:
        >>> split_date_range_by_year('2023-06-15', '2025-03-20')
        [
            ('2023-06-15', '2023-12-31'),
            ('2024-01-01', '2024-12-31'),
            ('2025-01-01', '2025-03-20')
        ]
    """
    if not start_date or not end_date:
        return [(start_date, end_date)]

    # Convert to datetime if string
    if isinstance(start_date, str):
        start = datetime.strptime(start_date, "%Y-%m-%d")
    else:
        start = datetime.combine(start_date, datetime.min.time())

    if isinstance(end_date, str):
        end = datetime.strptime(end_date, "%Y-%m-%d")
    else:
        end = datetime.combine(end_date, datetime.min.time())

    if start >= end:
        return [(start_date, end_date)]

    chunks = []
    current_start = start

    while current_start <= end:
        # End of current year or the final end_date, whichever is earlier
        year_end = datetime(current_start.year, 12, 31)
        current_end = min(year_end, end)

        chunks.append(
            (
                current_start.strftime("%Y-%m-%d"),
                current_end.strftime("%Y-%m-%d"),
            )
        )

        # Move to start of next year
        current_start = datetime(current_start.year + 1, 1, 1)

    return chunks
