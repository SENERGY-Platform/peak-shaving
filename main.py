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

from operator_lib.util import OperatorBase, logger, InitPhase, todatetime, timestamp_to_str
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

JOB_ID_FILENAME = "training_job_id.pickle"

from operator_lib.util import Config
class CustomConfig(Config):
    data_path = "/opt/data"
    init_phase_length: float = 2
    init_phase_level: str = "d"
    ml_trainer_url: str = "http://ml-trainer-svc.trainer:5000"
    mlflow_url: str = "http://mlflow-svc.mlflow:5000"

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

    def init(self,  *args, **kwargs):
        super().init(*args, **kwargs)
        self.data_path = self.config.data_path
        
        if not os.path.exists(self.data_path):
            os.mkdir(self.data_path)

        self.device_id = None
        self.job_id = None

        self.historic_data_available = None
        self.training_started = None

        self.ml_trainer_url = self.config.ml_trainer_url
        self.mlflow_url = self.config.mlflow_url

        self.load = Load()
        self.battery = Battery()

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


    def start_training(self, timestamp):
        topic_name, path_to_time, path_to_value = self._get_input_topic()
        job_request = {
            "task": "ml_fit",
            "task_settings": {
                "use_case": "peak-shaving"
            },
            "experiment_name": "",
            "data_source": "kafka",
            "data_settings": {
                "name": topic_name,
                "path_to_time": path_to_time,
                "path_to_value": path_to_value,
                "filterType": "device_id",
                "filterValue": self.device_id,
                "ksql_url": "http://ksql.kafka-sql:8088",
                "timestamp_format": "unix", #yyyy-MM-ddTHH:mm:ss.SSSZ
                "time_range_value": "1",
                "time_range_level": "d"
            },
            "toolbox_version": "v2.2.52",
            "ray_image": "ghcr.io/senergy-platform/ray:v0.0.8"
        }
        util.logger.debug(f"Start online training")
        res = requests.post(self.ml_trainer_url + "/mlfit", json=job_request)
        util.logger.debug(f"ML Trainer Response: {res.text}")
        if res.status_code != 200:
            util.logger.error(f"Cant start training job {res.text}")
            return
        self.job_id = res.json()['task_id']
        util.logger.debug(f"Created Training Job with ID: {self.job_id}")
        self.last_training_time = timestamp
        save(self.data_path, JOB_ID_FILENAME, self.job_id)

    def is_job_ready(self):
        res = requests.get(self.ml_trainer_url + "/job/"+self.job_id)
        res_data = res.json()
        job_status = res_data['success'] 
        util.logger.debug(f"Training Job Status: {job_status}")
        if job_status == 'error':
            raise Exception(res_data['response'])

        return job_status == 'done'
    
    def load_model(self):
        mlflow.set_tracking_uri(self.mlflow_url)
        model_uri = f"models:/{self.job_id}@production"
        util.logger.debug(f"Try to download model {self.job_id}")
        self.model = mlflow.pyfunc.load_model(model_uri)
        util.logger.debug(f"Downloading model {self.job_id} was succesfull")
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
                    if mapping.dest == "value":
                        path_to_value = mapping.source
                    
                    if mapping.dest == "time":
                        path_to_time = mapping.source

        return topic_name, path_to_time, path_to_value

    def stop(self):
        super().stop()
        save(self.data_path, POWER_DATA_FILENAME, self.power_data)
        save(self.data_path, BATTERY_DATA_FILENAME, self.battery_data)
        save(self.data_path, FIRST_DATA_FILENAME, self.first_data_time)

    def run(self, data, selector = None, device_id=None):
        if not self.device_id:
            self.device_id = device_id
        current_timestamp = todatetime(data['Power_Time']).tz_localize(None)
        if not self.first_data_time:
            self.first_data_time = current_timestamp
            self.init_phase_handler = InitPhase(self.config.data_path, self.init_phase_duration, self.first_data_time, self.produce)

        if current_timestamp < pd.Timestamp.now():
            self.historic_data_available = True
        if self.historic_data_available and current_timestamp < pd.Timestamp.now() and not self.training_started:
            self.start_training(current_timestamp)
            self.training_started = True
        if self.job_id and self.is_job_ready():
            min_boundaries, max_boundaries = self.load_model()
            util.logger.debug(f"Min boundaries: {min_boundaries}      Max boundaries: {max_boundaries}")
        new_point = data['Power']
        self.power_data.append(new_point)
        util.logger.debug('Power: '+str(new_point)+'  '+'Power Time: '+ timestamp_to_str(current_timestamp))

        discharge, dc_power = self.load.discharge_check(self.battery, new_point)
        charge, c_power = self.load.charge_check(new_point)
    
        if discharge:
            real_dc_power = self.battery.discharge(dc_power)
            battery_power = -real_dc_power
        elif charge:
            real_c_power = self.battery.charge(c_power)
            battery_power = real_c_power
        self.load.track_high_seg(new_point)
        self.load.update_corrected_max(battery_power, new_point)
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
        
        self.battery_data.append(battery_power)

        if self.init_phase_handler.init_phase_needs_to_be_reset():
            return self.init_phase_handler.reset_init_phase(init_value)
        
        return {"battery_power": battery_power, "timestamp": timestamp_to_str(current_timestamp), "initial_phase": ""}
        


    
from operator_lib.operator_lib import OperatorLib
if __name__ == "__main__":
    OperatorLib(Operator(), name="user-profile-operator", git_info_file='git_commit')
