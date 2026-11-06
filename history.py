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

import copy

import operator_lib.util as util
from operator_lib.util import clock
from operator_lib.util.config import MissingConfigValueError
from operator_lib.util.helpers.timescale import read_timescale_frame
from operator_lib.util.helpers.ts_wrapper import read_history


class NoHistoryError(RuntimeError):
    """The input has no history in Timescale that the operator can read."""


def read_input_history(name, duration):
    """
    The history of the operator input `name` as a frame with the UTC time without a timezone in `time` and the
    values in a column `name`.

    provide_historic_frames_local reads every input and fails on one whose history only Kafka has, as the capacity
    from the battery simulation. So the one input is read here with the same readers: over the database connection
    where the deployment has one, through timescale-wrapper where it has a platform token instead.
    """
    dep_config = util.DeploymentConfig()
    opr_config = util.OperatorConfig(util.load_operator_config_json(dep_config))
    topics = [topic for topic in opr_config.inputTopics if any(m.dest == name for m in topic.mappings)]
    if len(topics) != 1:
        raise NoHistoryError(f"input {name} is mapped from {len(topics)} sources")
    topic = topics[0]
    if topic.filterType != "DeviceId" or not topic.name.startswith("urn_infai_ses_service"):
        raise NoHistoryError(f"input {name} does not come from a device")
    devices = [device.strip() for device in (topic.filterValue or "").split(",") if device.strip()]
    if len(devices) != 1:
        raise NoHistoryError(f"input {name} comes from {len(devices)} devices")
    topic = copy.deepcopy(topic)
    topic.filterValue = devices[0]
    # Only the input itself, not other values of the same service.
    topic.mappings = [m for m in topic.mappings if m.dest == name]

    ts_conn = dep_config.ts_conn or opr_config.config.ts_conn
    if ts_conn:
        return read_timescale_frame(ts_conn, topic, duration, end=clock.fixed())
    if opr_config.config.ts_wrapper_url and dep_config.senergy_token:
        return read_history(opr_config.config.ts_wrapper_url, dep_config.senergy_token, topic, duration,
                            end=clock.fixed())
    raise MissingConfigValueError(f"cannot read the history of input {name}: neither TS_CONN nor 'ts_conn' "
                                  f"nor 'ts_wrapper_url' with a SENERGY_TOKEN is set")
