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

from operator_lib.util import OperatorBase, Selector, logger, InitPhase, todatetime, timestamp_to_str
from operator_lib.util.persistence import save, load
import operator_lib.util as util
import os
import pandas as pd
import numpy as np
from KDEpy import FFTKDE
from scipy.signal import argrelextrema
from load import Load
from battery import Battery

FIRST_DATA_FILENAME = "first_data_time.pickle"
POWER_DATA_FILENAME = "power_data.pickle"
BATTERY_DATA_FILENAME = "battery_data.pickle"
TRAINING_STARTED_FILENAME = "training_started.pickle"
JOB_ID_FILENAME = "training_job_id.pickle"

from operator_lib.util import Config
class CustomConfig(Config):
    data_path = "/opt/data"
    init_phase_length: float = 2
    init_phase_level: str = "d"
    max_capacity: float = 500 # Wattstunden

    def __init__(self, d, **kwargs):
        super().__init__(d, **kwargs)

        if self.init_phase_length != '':
            self.init_phase_length = float(self.init_phase_length)
        else:
            self.init_phase_length = 2
        
        if self.init_phase_level == '':
            self.init_phase_level = 'd'

class Operator(OperatorBase):
    configType = CustomConfig

    selectors = [
        Selector({"name": "battery", "args": ["capacity", "capacity_time"]}),
        Selector({"name": "consumption_device", "args": ["power", "power_time"]})
    ]

    def init(self,  *args, **kwargs):
        super().init(*args, **kwargs)
        self.data_path = self.config.data_path
        self.max_capacity = self.config.max_capacity
        
        if not os.path.exists(self.data_path):
            os.mkdir(self.data_path)

        self.device_id = None

        self.historic_data_available = None

        self.initial_training_data = []

        self.load = Load()
        self.battery = None
        self.battery_power = 0

        self.first_data_time = load(self.config.data_path, FIRST_DATA_FILENAME)

        self.init_phase_duration = pd.Timedelta(self.config.init_phase_length, self.config.init_phase_level)        
        self.init_phase_handler = InitPhase(self.data_path, self.init_phase_duration, self.first_data_time, self.produce)
        value = {
            "battery_power": 0,
            "timestamp": timestamp_to_str(pd.Timestamp.now())
        }
        self.init_phase_handler.send_first_init_msg(value) 

        self.power_data = load(self.config.data_path, POWER_DATA_FILENAME, default=[])
        self.battery_data = load(self.config.data_path, BATTERY_DATA_FILENAME, default=[])
        self.training_happened = load(self.config.data_path, TRAINING_STARTED_FILENAME, default=None)

        self.one_min_data_window = []

    def training(self, initial_training_array):
        x, y = FFTKDE(kernel='gaussian', bw='silverman').fit(initial_training_array).evaluate()
        local_minima = list(x[argrelextrema(y, np.less)[0]])
        min_boundaries = [0]+local_minima
        max_boundaries = local_minima+[max(initial_training_array)]
        return min_boundaries, max_boundaries

    def stop(self):
        save(self.data_path, POWER_DATA_FILENAME, self.power_data)
        save(self.data_path, BATTERY_DATA_FILENAME, self.battery_data)
        save(self.data_path, FIRST_DATA_FILENAME, self.first_data_time)
        save(self.data_path, JOB_ID_FILENAME, self.job_id)
        save(self.data_path, TRAINING_STARTED_FILENAME, self.training_started)
        super().stop()
        
    def run(self, data, selector, device_id=None):
        if selector == "consumption_device":
            if not self.device_id:
                self.device_id = device_id
            current_timestamp = todatetime(data['power_time']).tz_localize(None)
            if not self.first_data_time:
                self.first_data_time = current_timestamp
                self.init_phase_handler = InitPhase(self.config.data_path, self.init_phase_duration, self.first_data_time, self.produce)

            if current_timestamp < pd.Timestamp.now() - pd.Timedelta(1,"hour") and self.historic_data_available == None:
                self.historic_data_available = True

            new_point = data['power']

            if self.historic_data_available and not self.training_happened:
                if current_timestamp - self.first_data_time <= pd.Timedelta(14,"day"):
                    self.initial_training_data.append({"time": current_timestamp, "data": new_point})
                else:
                    initial_training_array = np.array([sample["data"] for sample in self.initial_training_data])
                    min_boundaries, max_boundaries = self.training(initial_training_array)
                    self.training_happened = True
                    util.logger.debug(f"PEAK SHAVING:        Min boundaries: {min_boundaries}      Max boundaries: {max_boundaries}")

            if self.one_min_data_window == []:
                self.one_min_data_window.append({"power": new_point, "time": current_timestamp})
                self.one_min_window_ended = False
            else:
                if current_timestamp - self.one_min_data_window[0]["time"] <= pd.Timedelta(1,"min"):
                    self.one_min_data_window.append({"power": new_point, "time": current_timestamp})
                    self.one_min_window_ended = False
                else:
                    new_one_min_average_power = np.mean([entry["power"] for entry in self.one_min_data_window])
                    self.one_min_data_window = [{"power": new_point, "time": current_timestamp}]
                    self.one_min_window_ended = True
                    
                    self.load.track_high_seg(new_one_min_average_power, current_timestamp)
                    self.load.update_max(new_one_min_average_power)
                    self.load.update_segments()
                    
                    self.power_data.append({"power": new_one_min_average_power, "time": current_timestamp})
                    save(self.data_path, POWER_DATA_FILENAME, self.power_data)
                    util.logger.debug('PEAK SHAVING:        Power: '+str(new_one_min_average_power)+'  '+'Power Time: '+ timestamp_to_str(current_timestamp))

            init_value = {
                "battery_power": 0,
                "timestamp": timestamp_to_str(current_timestamp),
                "trigger_battery": "no"
            }
            operator_is_init = self.init_phase_handler.operator_is_in_init_phase(current_timestamp)
            if operator_is_init:
                self.battery_data.append(0)
                return self.init_phase_handler.generate_init_msg(current_timestamp, init_value)
            if self.init_phase_handler.init_phase_needs_to_be_reset():
                return self.init_phase_handler.reset_init_phase(init_value)
            
            if self.one_min_window_ended == False:
                return {"battery_power": self.battery_power, "timestamp": timestamp_to_str(current_timestamp), "trigger_battery": "no", "initial_phase": ""}

            if self.battery != None:
                discharge, dc_power = self.load.discharge_check(self.battery, new_one_min_average_power)
                charge, c_power = self.load.charge_check(new_one_min_average_power)
    
                if discharge:
                    self.battery_power = -dc_power
                elif charge:
                    self.battery_power = c_power
        
                self.load.update_corrected_max(self.battery_power, new_one_min_average_power)
                self.battery_data.append(self.battery_power)
            else:
                self.battery_power = 0
                self.load.update_corrected_max(self.battery_power, new_one_min_average_power)
                self.battery_data.append(self.battery_power)
        
            return {"battery_power": self.battery_power, "timestamp": timestamp_to_str(current_timestamp), "trigger_battery": "yes", "initial_phase": ""}
        elif selector == "battery":
            current_capacity = data["capacity"]
            capacity_time = todatetime(data["capacity_time"]).tz_localize(None)
            logger.debug(f"PEAK SHAVING:        Current Capacity: {current_capacity}; time: {capacity_time}")
            if self.battery == None:
                self.battery = Battery(current_capacity)
            else:
                self.battery.capacity = current_capacity
        


    
from operator_lib.operator_lib import OperatorLib
if __name__ == "__main__":
    OperatorLib(Operator(), name="user-profile-operator", git_info_file='git_commit')
