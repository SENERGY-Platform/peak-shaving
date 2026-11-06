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

import numpy as np
import pandas as pd

HOUR = pd.Timedelta(1, "h")
# The grid draw that is billed: the mean of each quarter hour.
INTERVAL = pd.Timedelta(15, "min")
# A value holds until the next one, but no longer than this; a longer gap counts as no data.
MAX_HOLD = pd.Timedelta(5, "min")
# The battery simulation neither charges within this much of full nor discharges within this much of empty, in Wh.
RESERVE = 10


def lowest_limit(means, usable, max_charging_power, max_discharging_power):
    """
    The lowest limit in W that a battery of `usable` Wh, full at the start, would have kept the mean load of every
    quarter hour in `means` below: above the limit it covers the excess, below it it charges with what is left up
    to the limit. None without means.

    A quarter hour beyond the discharging power raises the limit for the whole history. Lower, the limit would not
    lower the peak of that day, would cost energy on every other day, and in simulations let the battery run empty.
    """
    means = np.asarray(means, dtype=float)
    if means.size == 0:
        return None
    hours = INTERVAL / HOUR

    def holds(limit):
        charge = usable
        for mean in means:
            excess = mean - limit
            if excess > 0:
                if excess > max_discharging_power or excess * hours > charge:
                    return False
                charge -= excess * hours
            else:
                charge = min(usable, charge + min(-excess, max_charging_power) * hours)
        return True

    # The highest mean always holds; bisected to a fraction of a watt.
    low, high = 0.0, float(means.max())
    for _ in range(40):
        middle = (low + high) / 2
        if holds(middle):
            high = middle
        else:
            low = middle
    return high


class Shaver:
    """
    Keeps the mean grid draw of each quarter hour below a limit: the lowest limit the battery would have held over
    the history with the share `energy_reserve` of its energy held back, which covers a day that needs more than any
    in the history. Within a quarter hour the battery covers what the energy drawn so far leaves above the limit,
    and charges up to the limit below it; there it may use all of its energy.
    """

    def __init__(self, max_capacity, max_charging_power, max_discharging_power, history_length, energy_reserve=0.0):
        self.max_capacity = max_capacity
        self.max_charging_power = max_charging_power
        self.max_discharging_power = max_discharging_power
        self.history_length = history_length
        self.energy_reserve = energy_reserve
        # The limit of the mean grid draw of a quarter hour in W, None before the first complete quarter hour.
        self.limit = None
        self.limit_day = None
        # The capacity the battery simulation reported last in Wh, None until it has.
        self.capacity = None
        # The battery power set last in W, positive charges; it holds until the next one is set.
        self.battery_power = 0.0
        # (start, mean load) of the complete quarter hours.
        self.means = []
        self.interval_start = None
        # Of the current quarter hour: the hours with a value, the energy of the load and the energy from the grid.
        self.covered = 0.0
        self.load_energy = 0.0
        self.grid_energy = 0.0
        self.last_time = None
        self.last_load = None

    def learn(self, history):
        """Accounts for a history of the load, during which the battery was not used, and sets the limit from it."""
        for time, load in zip(history.index, history.to_numpy()):
            if self.last_time is None or time > self.last_time:
                self.add(time, float(load))
        if self.last_time is not None:
            self.update_limit(self.last_time)

    def add(self, time, load):
        """Accounts for the time since the value before, during which that value and the battery power held."""
        if self.last_time is None:
            self.interval_start = time.floor(INTERVAL)
        else:
            self.account(self.last_time, min(time, self.last_time + MAX_HOLD))
        self.close_interval(time)
        self.last_time = time
        self.last_load = load

    def account(self, start, end):
        # split at the quarter hours
        while start < end:
            self.close_interval(start)
            stop = min(end, self.interval_start + INTERVAL)
            hours = (stop - start) / HOUR
            self.covered += hours
            self.load_energy += self.last_load * hours
            self.grid_energy += (self.last_load + self.battery_power) * hours
            start = stop

    def close_interval(self, time):
        # completes the quarter hour when time lies beyond it
        if time < self.interval_start + INTERVAL:
            return
        if self.covered > 0:
            self.means.append((self.interval_start, self.load_energy / self.covered))
        self.interval_start = time.floor(INTERVAL)
        self.covered = self.load_energy = self.grid_energy = 0.0

    def update_limit(self, now):
        """Sets the limit from the quarter hours of the history length before now."""
        self.means = [(start, mean) for start, mean in self.means if start >= now - self.history_length]
        usable = (self.max_capacity - 2 * RESERVE) * (1 - self.energy_reserve)
        self.limit = lowest_limit([mean for _, mean in self.means], usable,
                                  self.max_charging_power, self.max_discharging_power)
        self.limit_day = now.date()

    def decide(self, time, load):
        """
        The battery power that keeps the mean grid draw of the current quarter hour below the limit if the load stays
        at `load`, within the powers of the battery: positive charges, negative discharges.
        """
        if self.limit is None or self.capacity is None:
            return 0.0
        remaining = (self.interval_start + INTERVAL - time) / HOUR
        if remaining < 1 / 60:
            # the power holds mostly in the next quarter hour
            allowed = self.limit
        else:
            allowed = min(self.limit, (self.limit * (INTERVAL / HOUR) - self.grid_energy) / remaining)
        power = allowed - load
        if power < 0:
            return max(power, -self.max_discharging_power) if self.capacity > RESERVE else 0.0
        return min(power, self.max_charging_power) if self.capacity < self.max_capacity - RESERVE else 0.0
