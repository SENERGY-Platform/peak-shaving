"""
   Copyright 2026 InfAI (CC SES)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import datetime
import tempfile
import unittest

from unittest import mock

import pandas as pd
from operator_lib.util.config import MissingConfigValueError

from history import NoHistoryError
from main import CustomConfig, Operator
from shaving import lowest_limit

UTC = datetime.timezone.utc
STEP = pd.Timedelta(30, "s")


def load_profile(start, days, peak_minutes=30):
    # a value every 30 s: 1000 W, and 3000 W from noon on
    index = pd.date_range(start, periods=days * 2880, freq="30s")
    power = [3000.0 if time.hour == 12 and time.minute < peak_minutes else 1000.0 for time in index]
    return pd.Series(power, index=index)


def one_peak(start, days):
    # a value every 30 s: 1000 W, and 3000 W for a single quarter hour at noon of the second day
    history = pd.Series(1000.0, index=pd.date_range(start, periods=days * 2880, freq="30s"))
    noon = pd.Timestamp(start) + pd.Timedelta(1, "d") + pd.Timedelta(12, "h")
    history[noon:noon + pd.Timedelta(14.5, "min")] = 3000.0
    return history


def frame(series):
    # as the reader returns it: the UTC time without a timezone, and the power
    return pd.DataFrame({"time": series.index, "power": series.to_numpy()})


class Battery:
    # the battery simulation: integrates the power set at each control, within its capacity and powers
    def __init__(self, capacity, max_capacity, max_power):
        self.capacity, self.max_capacity, self.max_power = capacity, max_capacity, max_power
        self.power, self.time = 0.0, None

    def control(self, time, power):
        if self.time is not None:
            self.capacity += (time - self.time) / pd.Timedelta(1, "h") * self.power
        self.capacity = min(max(self.capacity, 0), self.max_capacity)
        self.time = time
        self.power = min(max(power, -self.max_power), self.max_power)
        if (self.power > 0 and self.capacity > self.max_capacity - 10) or (self.power < 0 and self.capacity < 10):
            self.power = 0.0
        return self.capacity


class TestPeakShaving(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.dir.cleanup()

    def start(self, history=None, reader=None, init_phase_length="2", max_capacity="500", max_power="5000",
              energy_reserve="0"):
        operator = Operator()
        operator.produce = lambda value: None
        operator.data_path = self.dir.name
        if reader is None:
            reader = mock.patch("main.read_input_history", side_effect=NoHistoryError("no device")) if history is None \
                else mock.patch("main.read_input_history", return_value=frame(history))
        with reader as self.read:
            operator.init(None, None, None, None, None, None,
                          config=CustomConfig({"init_phase_length": init_phase_length, "init_phase_level": "d",
                                               "max_capacity": max_capacity, "max_charging_power": max_power,
                                               "max_discharging_power": max_power,
                                               "energy_reserve": energy_reserve}))
        return operator

    def power(self, operator, time, value):
        return operator.run({"power": value}, "consumption_device", "device", pd.Timestamp(time).to_pydatetime())

    def capacity(self, operator, time, value):
        return operator.run({"capacity": value}, "battery", "operator", pd.Timestamp(time).to_pydatetime())

    def test_reads_two_weeks_of_the_power(self):
        self.start(history=load_profile("2026-06-01", 1))
        self.read.assert_called_once_with("power", pd.Timedelta(14, "d"))

    def test_discharges_down_to_the_limit_set_from_the_history(self):
        # 480 Wh cover a quarter hour of 3000 W down to 1080 W
        operator = self.start(history=one_peak("2026-06-01", 3))
        self.capacity(operator, "2026-06-03 23:59:40+00:00", 480.0)
        self.power(operator, "2026-06-03 23:59:45+00:00", 1000.0)
        result = self.power(operator, "2026-06-04 00:00:00+00:00", 3000.0)
        self.assertAlmostEqual(-1920, result["battery_power"], places=3)
        self.assertEqual(("yes", "2026-06-04T00:00:00.000Z", ""),
                         (result["trigger_battery"], result["timestamp"], result["initial_phase"]))

    def test_sets_the_limit_without_the_energy_reserve(self):
        # 90 % of 480 Wh cover a quarter hour of 3000 W down to 1272 W
        operator = self.start(history=one_peak("2026-06-01", 3), energy_reserve="10")
        self.capacity(operator, "2026-06-03 23:59:40+00:00", 480.0)
        self.power(operator, "2026-06-03 23:59:45+00:00", 1000.0)
        result = self.power(operator, "2026-06-04 00:00:00+00:00", 3000.0)
        self.assertAlmostEqual(-1728, result["battery_power"], places=3)

    def test_controls_the_battery_at_once_when_the_load_jumps(self):
        operator = self.start(history=one_peak("2026-06-01", 3))
        self.capacity(operator, "2026-06-03 23:59:40+00:00", 480.0)
        results = [self.power(operator, time, value) for time, value in [
            ("2026-06-03 23:59:45+00:00", 1000.0), ("2026-06-04 00:00:00+00:00", 1000.0),
            ("2026-06-04 00:00:20+00:00", 1000.0), ("2026-06-04 00:00:40+00:00", 3000.0)]]
        self.assertEqual(["no", "yes", "no", "yes"], [result["trigger_battery"] for result in results])
        self.assertAlmostEqual(80, results[1]["battery_power"], places=3)
        self.assertAlmostEqual(-1920, results[3]["battery_power"], places=3)

    def test_does_not_use_the_battery_before_it_reported_its_capacity(self):
        operator = self.start(history=one_peak("2026-06-01", 3))
        self.power(operator, "2026-06-03 23:59:45+00:00", 1000.0)
        result = self.power(operator, "2026-06-04 00:00:00+00:00", 3000.0)
        self.assertEqual((0, "yes"), (result["battery_power"], result["trigger_battery"]))

    def test_controls_the_battery_at_least_once_a_minute_with_the_time_in_utc(self):
        operator = self.start(init_phase_length="0")
        times = pd.date_range("2026-10-25 00:58:00", periods=6, freq="20s", tz=UTC)
        results = [self.power(operator, time, 1000.0) for time in times]
        # the first value ends the initial phase, the second is the first control
        self.assertEqual(["no", "yes", "no", "no", "no", "yes"], [result["trigger_battery"] for result in results])
        self.assertEqual("2026-10-25T00:59:40.000Z", results[-1]["timestamp"])

    def test_keeps_every_quarter_hour_of_the_grid_below_the_limit_of_the_history(self):
        history = load_profile("2026-06-01", 3)
        operator = self.start(history=history, max_capacity="2000", energy_reserve="10")
        battery = Battery(1990.0, 2000.0, 5000.0)
        grid = []
        for time, value in load_profile("2026-06-04", 1).items():
            result = self.power(operator, time.tz_localize(UTC), value)
            if result["trigger_battery"] == "yes":
                self.capacity(operator, time.tz_localize(UTC), battery.control(time, result["battery_power"]))
            # the value holds until the next one, with the power the battery took on at it
            grid.append(value + battery.power)
        quarter_hours = pd.Series(grid, index=load_profile("2026-06-04", 1).index).resample("15min").mean()
        limit = lowest_limit(history.resample("15min").mean(), 1980 * 0.9, 5000, 5000)
        self.assertLess(limit, 1500)
        self.assertLessEqual(quarter_hours.max(), limit * 1.01)

    def test_a_history_as_long_as_the_initial_phase_ends_it_with_the_first_value(self):
        operator = self.start(history=load_profile("2026-06-01", 3))
        self.assertEqual({"battery_power": 0, "timestamp": "2026-06-04T00:00:30.000Z", "trigger_battery": "no", "initial_phase": ""},
                         self.power(operator, "2026-06-04 00:00:30+00:00", 1000.0))

    def test_without_history_the_initial_phase_starts_with_the_first_value(self):
        operator = self.start()
        result = self.power(operator, "2026-06-04 10:00:00+00:00", 1000.0)
        self.assertEqual("no", result["trigger_battery"])
        self.assertIn("2026-06-06T10:00:00", result["initial_phase"])

    def test_starts_without_history_where_timescale_cannot_provide_it(self):
        for error in [NoHistoryError("input power comes from 2 devices"), MissingConfigValueError("no TS_CONN")]:
            with self.subTest(error=type(error).__name__):
                self.dir.cleanup()
                self.dir = tempfile.TemporaryDirectory()
                operator = self.start(reader=mock.patch("main.read_input_history", side_effect=error))
                result = self.power(operator, "2026-06-04 10:00:00+00:00", 1000.0)
                self.assertIn("2026-06-06T10:00:00", result["initial_phase"])

    def test_does_not_start_when_timescale_cannot_be_read(self):
        with self.assertRaises(ConnectionError):
            self.start(reader=mock.patch("main.read_input_history", side_effect=ConnectionError("no connection")))

    def test_leaves_out_live_values_that_the_history_already_holds(self):
        history = load_profile("2026-06-01", 3)
        operator = self.start(history=history)
        last = history.index[-1].tz_localize(UTC)
        self.assertIsNone(self.power(operator, last, 9000.0))
        self.assertIsNone(self.power(operator, last - STEP, 9000.0))
        self.assertIsNotNone(self.power(operator, last + STEP, 1000.0))

    def test_refuses_a_battery_without_capacity_or_power(self):
        with self.assertRaises(ValueError):
            CustomConfig({"max_capacity": "0"})

    def test_takes_the_default_for_an_empty_battery_value(self):
        self.assertEqual(500.0, CustomConfig({"max_capacity": ""}).max_capacity)

    def test_holds_back_ten_percent_of_the_energy_by_default(self):
        self.assertEqual(10.0, CustomConfig({}).energy_reserve)
        self.assertEqual(10.0, CustomConfig({"energy_reserve": ""}).energy_reserve)

    def test_refuses_an_energy_reserve_outside_0_to_below_100_percent(self):
        for value in ["-1", "100"]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                CustomConfig({"energy_reserve": value})


if __name__ == '__main__':
    unittest.main()
