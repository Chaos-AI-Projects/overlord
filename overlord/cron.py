"""Cron expression parser and evaluator.

Supports standard 5-field cron expressions:
    minute hour day_of_month month day_of_week

Field syntax:
    *        — every value
    N        — exact value
    N-M      — range (inclusive)
    N-M/S    — range with step
    */S      — every S values
    N,M,...  — list of values or sub-expressions
"""

from datetime import datetime, timedelta
from typing import Optional


class CronError(Exception):
    """Raised for invalid cron expressions."""


# Inclusive ranges for each field (minute, hour, dom, month, dow).
_FIELD_RANGES = [
    (0, 59),   # minute
    (0, 23),   # hour
    (1, 31),   # day of month
    (1, 12),   # month
    (0, 6),    # day of week (0=Sunday … 6=Saturday)
]

_MONTH_NAMES = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

_DOW_NAMES = {
    "sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
}


def _resolve_name(token: str, field_index: int) -> str:
    """Replace 3-letter month/dow names with their numeric equivalents."""
    lower = token.lower()
    if field_index == 3:  # month
        if lower in _MONTH_NAMES:
            return str(_MONTH_NAMES[lower])
    elif field_index == 4:  # dow
        if lower in _DOW_NAMES:
            return str(_DOW_NAMES[lower])
    return token


def _parse_field(field: str, field_index: int) -> set[int]:
    """Parse a single cron field into a set of matching integer values."""
    lo, hi = _FIELD_RANGES[field_index]
    result: set[int] = set()

    for part in field.split(","):
        part = _resolve_name(part.strip(), field_index)

        if "/" in part:
            range_part, step_str = part.split("/", 1)
            step = int(step_str)
            if step <= 0:
                raise CronError(f"Step must be positive: {field}")
            if range_part == "*":
                start, end = lo, hi
            elif "-" in range_part:
                a, b = range_part.split("-", 1)
                start = int(_resolve_name(a, field_index))
                end = int(_resolve_name(b, field_index))
            else:
                start, end = int(range_part), hi
            for v in range(start, end + 1, step):
                result.add(v)
        elif part == "*":
            result.update(range(lo, hi + 1))
        elif "-" in part:
            a, b = part.split("-", 1)
            a, b = int(_resolve_name(a, field_index)), int(_resolve_name(b, field_index))
            if a > b:
                raise CronError(f"Invalid range {a}-{b} in field {field_index}")
            result.update(range(a, b + 1))
        else:
            result.add(int(part))

    # Validate all values are within the allowed range.
    for v in result:
        if v < lo or v > hi:
            raise CronError(
                f"Value {v} out of range [{lo}, {hi}] for field index {field_index}"
            )
    return result


class CronExpression:
    """Parsed cron expression that can test if a given time matches."""

    def __init__(self, expression: str):
        self.expression = expression
        fields = expression.strip().split()
        if len(fields) != 5:
            raise CronError(
                f"Expected 5 fields (minute hour dom month dow), got {len(fields)}: "
                f"{expression!r}"
            )
        self.minutes = _parse_field(fields[0], 0)
        self.hours = _parse_field(fields[1], 1)
        self.days_of_month = _parse_field(fields[2], 2)
        self.months = _parse_field(fields[3], 3)
        self.days_of_week = _parse_field(fields[4], 4)

    def matches(self, dt: datetime) -> bool:
        """Return True if the datetime matches this cron expression.

        Uses the standard cron convention: if both day-of-month and
        day-of-week are restricted (not *), the job runs when *either*
        field matches.  If only one is restricted, it must match.
        """
        if dt.minute not in self.minutes:
            return False
        if dt.hour not in self.hours:
            return False
        if dt.month not in self.months:
            return False

        dom_restricted = self.days_of_month != set(range(1, 32))
        dow_restricted = self.days_of_week != set(range(0, 7))
        # Python: isoweekday() returns 1=Monday..7=Sunday; convert to 0=Sun..6=Sat
        py_dow = dt.isoweekday() % 7

        if dom_restricted and dow_restricted:
            return dt.day in self.days_of_month or py_dow in self.days_of_week
        if dom_restricted:
            return dt.day in self.days_of_month
        if dow_restricted:
            return py_dow in self.days_of_week
        return True

    def next_occurrence(self, after: datetime, max_years: int = 2) -> Optional[datetime]:
        """Find the next minute >= after that matches this expression.

        Searches up to max_years into the future. Returns None if no match
        is found (e.g. an expression that can never fire like '0 0 31 2 *').
        """
        # Truncate to the current minute, then advance by one minute.
        dt = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
        deadline = after + timedelta(days=max_years * 366)

        while dt <= deadline:
            if dt.month not in self.months:
                # Skip to the first day of the next month.
                if dt.month == 12:
                    dt = dt.replace(year=dt.year + 1, month=1, day=1, hour=0, minute=0)
                else:
                    dt = dt.replace(month=dt.month + 1, day=1, hour=0, minute=0)
                continue

            # Check day match (same logic as matches()).
            dom_restricted = self.days_of_month != set(range(1, 32))
            dow_restricted = self.days_of_week != set(range(0, 7))
            py_dow = dt.isoweekday() % 7

            day_ok = True
            if dom_restricted and dow_restricted:
                day_ok = dt.day in self.days_of_month or py_dow in self.days_of_week
            elif dom_restricted:
                day_ok = dt.day in self.days_of_month
            elif dow_restricted:
                day_ok = py_dow in self.days_of_week

            if not day_ok:
                dt = dt.replace(hour=0, minute=0) + timedelta(days=1)
                continue

            if dt.hour not in self.hours:
                dt = dt.replace(minute=0) + timedelta(hours=1)
                continue

            if dt.minute not in self.minutes:
                dt += timedelta(minutes=1)
                continue

            return dt

        return None

    def __repr__(self) -> str:
        return f"CronExpression({self.expression!r})"
