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

import unittest

import pandas as pd

from shaving import Shaver, lowest_limit

HIGH = 100000


def at(text):
    return pd.Timestamp(f"2026-06-01 {text}")


class TestLowestLimit(unittest.TestCase):
    def test_lets_the_battery_cover_a_peak_with_its_energy(self):
        # at 1000 W the quarter hour of 3000 W takes 2000 W for 0.25 h, all of the 500 Wh
        self.assertAlmostEqual(1000, lowest_limit([1000, 3000, 1000], 500, HIGH, HIGH), places=3)

    def test_is_held_up_by_the_discharging_power(self):
        self.assertAlmostEqual(2500, lowest_limit([1000, 3000], 10000, HIGH, 500), places=3)

    def test_recharges_between_peaks_with_what_the_load_leaves_below_the_limit(self):
        # after the first peak 500 - (3000 - P)/4 Wh are left, (P - 1000)/4 Wh are charged, the second peak takes
        # (3000 - P)/4 again: P = 5000/3
        self.assertAlmostEqual(5000 / 3, lowest_limit([3000, 1000, 3000], 500, HIGH, HIGH), places=3)

    def test_recharges_no_faster_than_the_charging_power(self):
        # 400 W for 0.25 h recharge 100 Wh: 600 Wh for two peaks of (3000 - P)/4, P = 1800
        self.assertAlmostEqual(1800, lowest_limit([3000, 1000, 3000], 500, 400, HIGH), places=3)

    def test_is_none_without_quarter_hours(self):
        self.assertIsNone(lowest_limit([], 500, HIGH, HIGH))


class TestShaver(unittest.TestCase):
    def shaver(self, limit=2000, capacity=5000, max_power=5000):
        shaver = Shaver(10000, max_power, max_power, pd.Timedelta(14, "d"))
        shaver.limit = limit
        shaver.capacity = capacity
        return shaver

    def decide(self, shaver, values):
        # the values up to the last one, then the decision with the last one as the load
        for time, load in values:
            shaver.add(at(time), load)
        time, load = values[-1]
        return shaver.decide(at(time), load)

    def test_discharges_by_what_the_load_exceeds_the_limit(self):
        self.assertEqual(-1000, self.decide(self.shaver(), [("10:00", 3000)]))

    def test_also_covers_what_the_quarter_hour_has_drawn_above_the_limit_so_far(self):
        # 250 Wh in the first 5 minutes leave 250 Wh of the 500 Wh for the remaining 10 minutes: 1500 W
        self.assertAlmostEqual(-1500, self.decide(self.shaver(), [("10:00", 3000), ("10:05", 3000)]))

    def test_counts_the_battery_power_set_into_the_quarter_hour(self):
        shaver = self.shaver()
        shaver.add(at("10:00"), 3000)
        shaver.battery_power = -1000
        # 2000 W for 5 minutes leave 333 Wh for 10 minutes: 2000 W
        self.assertAlmostEqual(-1000, self.decide(shaver, [("10:05", 3000)]))

    def test_charges_up_to_the_limit(self):
        self.assertEqual(800, self.decide(self.shaver(), [("10:00", 1200)]))

    def test_charges_no_more_than_up_to_the_limit_after_a_low_start_of_the_quarter_hour(self):
        self.assertEqual(800, self.decide(self.shaver(), [("10:00", 500), ("10:10", 1200)]))

    def test_plans_for_the_next_quarter_hour_in_the_last_minute_of_one(self):
        self.assertEqual(-1000, self.decide(self.shaver(), [("10:00", 4000), ("10:14:30", 3000)]))

    def test_keeps_to_the_powers_of_the_battery(self):
        self.assertEqual(-500, self.decide(self.shaver(max_power=500), [("10:00", 3000)]))
        self.assertEqual(500, self.decide(self.shaver(max_power=500), [("10:00", 1000)]))

    def test_does_not_discharge_a_nearly_empty_battery(self):
        self.assertEqual(0, self.decide(self.shaver(capacity=5), [("10:00", 3000)]))

    def test_does_not_charge_a_nearly_full_battery(self):
        self.assertEqual(0, self.decide(self.shaver(capacity=9995), [("10:00", 1000)]))

    def test_does_not_use_the_battery_before_it_reported_its_capacity(self):
        self.assertEqual(0, self.decide(self.shaver(capacity=None), [("10:00", 3000)]))

    def test_holds_a_value_no_longer_than_five_minutes(self):
        shaver = self.shaver()
        for time, load in [("10:00", 4000), ("10:20", 1000), ("10:30", 1000)]:
            shaver.add(at(time), load)
        # held for the whole gap, the quarter hours would be 4000 W and 2000 W
        self.assertEqual([at("10:00"), at("10:15")], [start for start, _ in shaver.means])
        for (_, mean), expected in zip(shaver.means, [4000, 1000]):
            self.assertAlmostEqual(expected, mean, places=6)

    def test_sets_the_limit_from_the_quarter_hours_of_the_history_length(self):
        shaver = Shaver(500, HIGH, HIGH, pd.Timedelta(1, "h"))
        values = [3000.0] * 4 + [1000.0, 3000.0, 1000.0, 1000.0, 1000.0]
        shaver.learn(pd.Series(values, index=pd.date_range("2026-06-01 08:00", periods=len(values), freq="15min")))
        # the last value starts a quarter hour of its own, so the hour before it holds 1000, 3000, 1000, 1000 W
        self.assertAlmostEqual(lowest_limit([1000, 3000, 1000, 1000], 480, HIGH, HIGH), shaver.limit, places=3)

    def test_sets_the_limit_without_the_energy_reserve(self):
        shaver = Shaver(500, HIGH, HIGH, pd.Timedelta(1, "d"), energy_reserve=0.1)
        values = [1000.0, 3000.0, 1000.0, 1000.0]
        shaver.learn(pd.Series(values, index=pd.date_range("2026-06-01 08:00", periods=len(values), freq="15min")))
        # 90 % of 480 Wh shave the quarter hour of 3000 W down to 3000 - 432 * 4 W
        self.assertAlmostEqual(1272, shaver.limit, places=3)

    def test_uses_the_energy_reserve_within_a_quarter_hour(self):
        shaver = Shaver(10000, 5000, 5000, pd.Timedelta(14, "d"), energy_reserve=0.1)
        shaver.limit, shaver.capacity = 2000, 500
        shaver.add(at("10:00"), 3000)
        self.assertEqual(-1000, shaver.decide(at("10:00"), 3000))


if __name__ == '__main__':
    unittest.main()
