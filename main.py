"""
   Copyright 2022 InfAI (CC SES)

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

__all__ = ("Operator", )

from operator_lib.util import OperatorBase, Selector, logger, InitPhase, timestamp_to_str
from operator_lib.util.persistence import save, load
from operator_lib.util.config import MissingConfigValueError
import os
import pandas as pd
from shaving import Shaver
from history import read_input_history, NoHistoryError

import typing
import datetime

FIRST_DATA_FILENAME = "first_data_time.pickle"
# The battery is controlled at least once a minute, and at once when the power it needs changes by more than this
# share of its discharging power.
CONTROL_INTERVAL = pd.Timedelta(1, "min")
REACTION_SHARE = 0.1

from operator_lib.util import Config, Inputs, Outputs
class CustomConfig(Config):
    init_phase_length: float = 2
    init_phase_level: str = "d"
    # The battery, as in the battery simulation: capacity in Wh, powers in W.
    max_capacity: float = 500
    max_charging_power: float = 1000
    max_discharging_power: float = 1000
    # Percent of the battery's energy that the limit is set without, for a day that needs more than any before.
    energy_reserve: float = 10
    # A field of the library, declared again so that flows can set it.
    logger_level: str = "warning"

    def __init__(self, d, **kwargs):
        super().__init__(d, **kwargs)

        if self.init_phase_length != '':
            self.init_phase_length = float(self.init_phase_length)
        else:
            self.init_phase_length = 2

        if self.init_phase_level == '':
            self.init_phase_level = 'd'

        for name in ("max_capacity", "max_charging_power", "max_discharging_power"):
            value = getattr(self, name)
            value = float(value) if value != '' else float(getattr(CustomConfig, name))
            if value <= 0:
                raise ValueError(f"config value '{name}' must be greater than 0, not {value}")
            setattr(self, name, value)

        self.energy_reserve = float(self.energy_reserve) if self.energy_reserve != '' else float(CustomConfig.energy_reserve)
        if not 0 <= self.energy_reserve < 100:
            raise ValueError(f"config value 'energy_reserve' must be at least 0 and below 100, not {self.energy_reserve}")

# The power of the consumption device in W and the capacity of the battery simulation in Wh. The time of a value
# is the time the platform received it, so there is no input for it.
class PeakShavingInputs(Inputs):
    power: float
    capacity: float

class PeakShavingOutputs(Outputs):
    battery_power: float
    timestamp: str
    trigger_battery: str
    initial_phase: str

class Operator(OperatorBase):
    configType = CustomConfig
    inputs = PeakShavingInputs
    outputs = PeakShavingOutputs

    selectors = [
        Selector({"name": "battery", "args": ["capacity"]}),
        Selector({"name": "consumption_device", "args": ["power"]})
    ]
    # Where the state is kept, the volume of the deployment; not a config value, which a flow could change.
    data_path = "/opt/data"
    # This much history of the power is read at the start, and the limit is set from this much every day.
    history_length = pd.Timedelta(14, "d")

    def init(self,  *args, **kwargs):
        super().init(*args, **kwargs)
        if not os.path.exists(self.data_path):
            os.mkdir(self.data_path)

        self.shaver = Shaver(self.config.max_capacity, self.config.max_charging_power,
                             self.config.max_discharging_power, self.history_length, self.config.energy_reserve / 100)
        self.last_control = None

        history = self.read_history()
        self.shaver.learn(history)
        logger.debug(f"PEAK SHAVING:        {len(history)} values of history, limit {self.shaver.limit}")

        self.first_data_time = load(self.data_path, FIRST_DATA_FILENAME)
        if not history.empty and (not self.first_data_time or history.index[0] < self.first_data_time):
            # The initial phase counts from the first value of the history, so a history of its length ends it at once.
            self.first_data_time = history.index[0]
            save(self.data_path, FIRST_DATA_FILENAME, self.first_data_time)

        self.init_phase_duration = pd.Timedelta(self.config.init_phase_length, self.config.init_phase_level)
        # No message before the first value: the battery simulation reads every message of this operator.
        self.init_phase_handler = InitPhase(self.data_path, self.init_phase_duration, self.first_data_time, self.produce)

    def read_history(self):
        # Timescale holds the power all along, so the limit is set anew at every start and a restart or a new
        # volume loses nothing. Any other failure of the read fails the start, and the platform starts it again.
        no_history = pd.Series([], index=pd.DatetimeIndex([]), dtype=float)
        try:
            frame = read_input_history("power", self.history_length)
        except (NoHistoryError, MissingConfigValueError) as e:
            logger.warning(f'Starting without history: {e}')
            return no_history
        frame = frame.dropna(subset=['power'])
        # As live: UTC without a timezone.
        index = pd.DatetimeIndex(pd.to_datetime(frame['time'], utc=True)).tz_localize(None)
        return pd.Series(frame['power'].astype(float).to_numpy(), index=index).sort_index()

    def run(self, data: typing.Dict[str, typing.Any], selector: str, device_id, timestamp: datetime.datetime):
        # UTC without a timezone, as the history: the quarter hours of the grid draw are those of UTC as well.
        current_timestamp = pd.Timestamp(timestamp).tz_convert("UTC").tz_localize(None)
        if selector == "battery":
            self.shaver.capacity = float(data["capacity"])
            logger.debug(f"PEAK SHAVING:        Current Capacity: {self.shaver.capacity}; time: {current_timestamp}")
            return None

        # A value no later than the one before, as one that the history already holds, is left out.
        if self.shaver.last_time is not None and current_timestamp <= self.shaver.last_time:
            return None
        if not self.first_data_time:
            self.first_data_time = current_timestamp
            save(self.data_path, FIRST_DATA_FILENAME, self.first_data_time)
            self.init_phase_handler = InitPhase(self.data_path, self.init_phase_duration, self.first_data_time, self.produce)

        load = float(data['power'])
        self.shaver.add(current_timestamp, load)

        init_value = {
            "battery_power": 0,
            "timestamp": timestamp_to_str(current_timestamp),
            "trigger_battery": "no"
        }
        if self.init_phase_handler.operator_is_in_init_phase(current_timestamp):
            return self.init_phase_handler.generate_init_msg(current_timestamp, init_value)
        if self.init_phase_handler.init_phase_needs_to_be_reset():
            return self.init_phase_handler.reset_init_phase(init_value)

        if self.shaver.limit_day != current_timestamp.date():
            self.shaver.update_limit(current_timestamp)
            logger.debug(f"PEAK SHAVING:        Limit {self.shaver.limit} W from {len(self.shaver.means)} quarter hours")
        power = self.shaver.decide(current_timestamp, load)
        due = self.last_control is None or current_timestamp - self.last_control > CONTROL_INTERVAL
        if not due and abs(power - self.shaver.battery_power) <= REACTION_SHARE * self.shaver.max_discharging_power:
            return {"battery_power": self.shaver.battery_power, "timestamp": timestamp_to_str(current_timestamp), "trigger_battery": "no", "initial_phase": ""}

        self.shaver.battery_power = power
        self.last_control = current_timestamp
        logger.debug('PEAK SHAVING:        Power: '+str(load)+'  Battery Power: '+str(power)+'  Time: '+timestamp_to_str(current_timestamp))
        return {"battery_power": power, "timestamp": timestamp_to_str(current_timestamp), "trigger_battery": "yes", "initial_phase": ""}


from operator_lib.operator_lib import OperatorLib
if __name__ == "__main__":
    OperatorLib(Operator(), name="peak-shaving-operator", git_info_file='git_commit')
