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

from operator_lib.util import OperatorBase, Selector, logger, InitPhase, todatetime, timestamp_to_str, get_ts_format_from_str
from operator_lib.util.persistence import save, load
import operator_lib.util as util
import os
import pandas as pd
from load import Load
from battery import Battery
import mlflow
import requests
import json

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
    ml_trainer_url: str = "http://ml-trainer-svc.trainer:5000"
    mlflow_url: str = "http://mlflow-svc.mlflow:5000"
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
        self.job_id = None
        self.model = None

        self.historic_data_available = None
        self.training_started = None

        self.ml_trainer_url = self.config.ml_trainer_url
        self.mlflow_url = self.config.mlflow_url

        self.load = Load()
        self.battery = None

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
        self.job_id = load(self.config.data_path, JOB_ID_FILENAME, default=None)
        self.training_started = load(self.config.data_path, TRAINING_STARTED_FILENAME, default=None)


    def start_training(self, timestamp, raw_timestamp):
        topic_name, path_to_time, path_to_value = self._get_input_topic()
        job_request = {
            "task": "peak_shaving",
            "data_source": "kafka",
            "data_settings": {
                "name": topic_name,
                "path_to_time": path_to_time,
                "path_to_value": path_to_value,
                "filterType": "device_id",
                "filterValue": self.device_id,
                "ksql_url": "http://ksql.kafka-sql:8088",
                "timestamp_format": get_ts_format_from_str(raw_timestamp), # "yyyy-MM-ddTHH:mm:ss", #yyyy-MM-ddTHH:mm:ss.SSSZ
                "time_range_value": "2",
                "time_range_level": "d"
            },
            "toolbox_version": "v2.2.86",
            "cluster": {
                "memory_worker_limit": "20G"
            },
            "ray_image": "ghcr.io/senergy-platform/ray:v0.0.8",
            "user_id": ""
        }
        util.logger.debug(f"PEAK SHAVING:        Start online training")
        res = requests.post(self.ml_trainer_url + "/job", json=job_request)
        util.logger.debug(f"PEAK SHAVING:        ML Trainer Response: {res.text}")
        if res.status_code != 200:
            util.logger.error(f"CPEAK SHAVING:        ant start training job {res.text}")
            return
        self.job_id = res.json()['task_id']
        util.logger.debug(f"PEAK SHAVING:        Created Training Job with ID: {self.job_id}")
        self.last_training_time = timestamp
        save(self.data_path, JOB_ID_FILENAME, self.job_id)

    def is_job_ready(self):
        res = requests.get(self.ml_trainer_url + "/job/"+self.job_id)
        res_data = res.json()
        job_status = res_data['success'] 
        util.logger.debug(f"PEAK SHAVING:        Training Job Status: {job_status}")
        if job_status == 'error':
            raise Exception(res_data['response'])

        return job_status == 'done'
    
    def load_model(self):
        mlflow.set_tracking_uri(self.mlflow_url)
        model_uri = f"models:/{self.job_id}@production"
        util.logger.debug(f"PEAK SHAVING:        Try to download model {self.job_id}")
        self.model = mlflow.pyfunc.load_model(model_uri)
        util.logger.debug(f"PEAK SHAVING:        Downloading model {self.job_id} was succesfull")
        unwrapped_model = self.model.unwrap_python_model()
        min_boundaries = unwrapped_model.get_cluster_min_boundaries()
        max_boundaries = unwrapped_model.get_cluster_max_boundaries()
        return min_boundaries, max_boundaries

    def _get_input_topic(self):
        dep_config = util.DeploymentConfig()
        config_json = json.loads(dep_config.config)
        opr_config = util.OperatorConfig(config_json)
        topic_name = None
        path_to_time = None 
        path_to_value = None
        for input_topic in opr_config.inputTopics:
            if self.device_id in input_topic.filterValue.split(','):
                topic_name = input_topic.name
                for mapping in input_topic.mappings:
                    if mapping.dest == "Power":
                        path_to_value = mapping.source
                    
                    if mapping.dest == "Power_Time":
                        path_to_time = mapping.source

        return topic_name, path_to_time, path_to_value

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
            current_timestamp = todatetime(data['Power_Time']).tz_localize(None)
            if not self.first_data_time:
                self.first_data_time = current_timestamp
                self.init_phase_handler = InitPhase(self.config.data_path, self.init_phase_duration, self.first_data_time, self.produce)

            if current_timestamp < pd.Timestamp.now():
                self.historic_data_available = True
            if self.historic_data_available and current_timestamp < pd.Timestamp.now() and not self.training_started:
                self.start_training(current_timestamp, data['Power_Time'])
                self.training_started = True
            if self.job_id and self.is_job_ready() and not self.model:
                min_boundaries, max_boundaries = self.load_model()
                util.logger.debug(f"PEAK SHAVING:        Min boundaries: {min_boundaries}      Max boundaries: {max_boundaries}")
            new_point = data['Power']
            self.power_data.append(new_point)
            util.logger.debug('PEAK SHAVING:        Power: '+str(new_point)+'  '+'Power Time: '+ timestamp_to_str(current_timestamp))

            self.load.track_high_seg(new_point)
            self.load.update_max(new_point)
            self.load.update_segments()

            init_value = {
                "battery_power": 0,
                "timestamp": timestamp_to_str(current_timestamp)
            }
            operator_is_init = self.init_phase_handler.operator_is_in_init_phase(current_timestamp)
            if operator_is_init:
                self.battery_data.append(0)
                return self.init_phase_handler.generate_init_msg(current_timestamp, init_value)
            if self.init_phase_handler.init_phase_needs_to_be_reset():
                return self.init_phase_handler.reset_init_phase(init_value)

            if self.battery != None:
                discharge, dc_power = self.load.discharge_check(self.battery, new_point)
                charge, c_power = self.load.charge_check(new_point)
    
                if discharge:
                    battery_power = -dc_power
                elif charge:
                    battery_power = c_power
        
                self.load.update_corrected_max(battery_power, new_point)
                self.battery_data.append(battery_power)
        
            return {"battery_power": battery_power, "timestamp": timestamp_to_str(current_timestamp), "initial_phase": ""}
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
