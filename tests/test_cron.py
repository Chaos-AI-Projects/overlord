"""Tests for the cron expression parser and evaluator."""

from datetime import datetime

import pytest

from overlord.cron import CronError, CronExpression


class TestParsing:
    def test_every_minute(self):
        cron = CronExpression("* * * * *")
        assert cron.minutes == set(range(0, 60))

    def test_specific_values(self):
        cron = CronExpression("30 14 1 6 3")
        assert cron.minutes == {30}
        assert cron.hours == {14}
        assert cron.days_of_month == {1}
        assert cron.months == {6}
        assert cron.days_of_week == {3}

    def test_range(self):
        cron = CronExpression("0-5 * * * *")
        assert cron.minutes == {0, 1, 2, 3, 4, 5}

    def test_step(self):
        cron = CronExpression("*/15 * * * *")
        assert cron.minutes == {0, 15, 30, 45}

    def test_range_with_step(self):
        cron = CronExpression("1-10/3 * * * *")
        assert cron.minutes == {1, 4, 7, 10}

    def test_list(self):
        cron = CronExpression("0,15,30,45 * * * *")
        assert cron.minutes == {0, 15, 30, 45}

    def test_month_names(self):
        cron = CronExpression("0 0 1 jan,jun *")
        assert cron.months == {1, 6}

    def test_dow_names(self):
        cron = CronExpression("0 0 * * mon,fri")
        assert cron.days_of_week == {1, 5}

    def test_invalid_field_count(self):
        with pytest.raises(CronError, match="Expected 5 fields"):
            CronExpression("* * *")

    def test_value_out_of_range(self):
        with pytest.raises(CronError, match="out of range"):
            CronExpression("61 * * * *")

    def test_invalid_range(self):
        with pytest.raises(CronError, match="Invalid range"):
            CronExpression("10-5 * * * *")


class TestMatching:
    def test_every_minute_matches(self):
        cron = CronExpression("* * * * *")
        assert cron.matches(datetime(2026, 4, 11, 10, 30))

    def test_specific_minute_matches(self):
        cron = CronExpression("30 * * * *")
        assert cron.matches(datetime(2026, 4, 11, 10, 30))
        assert not cron.matches(datetime(2026, 4, 11, 10, 31))

    def test_every_five_minutes(self):
        cron = CronExpression("*/5 * * * *")
        assert cron.matches(datetime(2026, 1, 1, 0, 0))
        assert cron.matches(datetime(2026, 1, 1, 0, 5))
        assert not cron.matches(datetime(2026, 1, 1, 0, 3))

    def test_specific_time(self):
        cron = CronExpression("0 9 * * *")
        assert cron.matches(datetime(2026, 4, 11, 9, 0))
        assert not cron.matches(datetime(2026, 4, 11, 10, 0))

    def test_day_of_month(self):
        cron = CronExpression("0 0 15 * *")
        assert cron.matches(datetime(2026, 4, 15, 0, 0))
        assert not cron.matches(datetime(2026, 4, 14, 0, 0))

    def test_day_of_week(self):
        # 2026-04-13 is a Monday (isoweekday=1, our dow=1)
        cron = CronExpression("0 0 * * 1")  # Monday
        assert cron.matches(datetime(2026, 4, 13, 0, 0))
        assert not cron.matches(datetime(2026, 4, 14, 0, 0))  # Tuesday

    def test_sunday_is_zero(self):
        # 2026-04-12 is a Sunday
        cron = CronExpression("0 0 * * 0")
        assert cron.matches(datetime(2026, 4, 12, 0, 0))

    def test_both_dom_and_dow_restricted(self):
        # Standard cron: if both restricted, either match triggers.
        # 2026-04-15 is a Wednesday (dow=3), day 15
        cron = CronExpression("0 0 1 * 3")  # 1st of month OR Wednesday
        assert cron.matches(datetime(2026, 4, 15, 0, 0))  # Wednesday
        assert cron.matches(datetime(2026, 4, 1, 0, 0))   # 1st

    def test_month_filter(self):
        cron = CronExpression("0 0 1 6 *")
        assert cron.matches(datetime(2026, 6, 1, 0, 0))
        assert not cron.matches(datetime(2026, 7, 1, 0, 0))


class TestNextOccurrence:
    def test_next_minute(self):
        cron = CronExpression("* * * * *")
        now = datetime(2026, 4, 11, 10, 30, 15)
        nxt = cron.next_occurrence(now)
        assert nxt == datetime(2026, 4, 11, 10, 31)

    def test_next_hour(self):
        cron = CronExpression("0 * * * *")
        now = datetime(2026, 4, 11, 10, 30)
        nxt = cron.next_occurrence(now)
        assert nxt == datetime(2026, 4, 11, 11, 0)

    def test_next_day(self):
        cron = CronExpression("0 9 * * *")
        now = datetime(2026, 4, 11, 10, 0)
        nxt = cron.next_occurrence(now)
        assert nxt == datetime(2026, 4, 12, 9, 0)

    def test_next_month(self):
        cron = CronExpression("0 0 1 * *")
        now = datetime(2026, 4, 15, 0, 0)
        nxt = cron.next_occurrence(now)
        assert nxt == datetime(2026, 5, 1, 0, 0)

    def test_specific_dow(self):
        # Next Monday after 2026-04-11 (Saturday) is 2026-04-13
        cron = CronExpression("0 9 * * 1")
        now = datetime(2026, 4, 11, 10, 0)
        nxt = cron.next_occurrence(now)
        assert nxt == datetime(2026, 4, 13, 9, 0)
