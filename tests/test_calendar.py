from datetime import date

from ninebar import calendar as cal


def test_slots_per_local_day_follow_dst():
    assert cal.expected_slots(date(2013, 11, 5)) == 144
    assert cal.expected_slots(date(2013, 10, 27)) == 150   # clocks go back
    assert cal.expected_slots(date(2014, 3, 30)) == 138    # clocks go forward


def test_week_is_iso_monday():
    assert cal.week_start(date(2013, 11, 1)) == date(2013, 10, 28)
    assert cal.week_start(date(2013, 11, 4)) == date(2013, 11, 4)
    assert cal.week_start(date(2013, 11, 10)) == date(2013, 11, 4)


def test_fortnight_is_anchored_on_friday_and_cuts_through_weeks():
    anchor = date(2013, 11, 1)
    assert cal.cycle_start(date(2013, 11, 14), anchor) == date(2013, 11, 1)
    assert cal.cycle_start(date(2013, 11, 15), anchor) == date(2013, 11, 15)
    assert cal.cycle_start(date(2013, 10, 31), anchor) == date(2013, 10, 18)  # before the anchor still works
    weeks = {cal.week_start(d) for d in cal.cycle_days(anchor)}
    assert len(weeks) == 3   # tail of one ISO week, one full week, head of the next
