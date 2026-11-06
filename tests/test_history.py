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

import json
import os
import unittest

from unittest import mock

import pandas as pd
from operator_lib.util.config import MissingConfigValueError

from history import NoHistoryError, read_input_history

DEVICE_TOPIC = "urn_infai_ses_service_5a7b"
BATTERY = {"name": "analytics-battery-simulation", "filterType": "OperatorId", "filterValue": "battery-node",
           "mappings": [{"dest": "capacity", "source": "Capacity"}]}
DAYS = pd.Timedelta(14, "d")


def device(devices, extra_mappings=()):
    return {"name": DEVICE_TOPIC, "filterType": "DeviceId", "filterValue": devices,
            "mappings": [{"dest": "power", "source": "value.power.value"}, *extra_mappings]}


def environment(topics, config=None, **variables):
    # the deployment as the flow engine starts it
    env = {"CONFIG": json.dumps({"config": config or {}, "inputTopics": topics})}
    env.update(variables)
    return mock.patch.dict(os.environ, env, clear=True)


class TestReadInputHistory(unittest.TestCase):
    def read(self, topics, config=None, **variables):
        frame = pd.DataFrame({"time": [pd.Timestamp("2026-06-01 10:00")], "power": [1000.0]})
        with environment(topics, config, **variables), \
                mock.patch("history.read_timescale_frame", return_value=frame) as self.timescale, \
                mock.patch("history.read_history", return_value=frame) as self.wrapper:
            return read_input_history("power", DAYS)

    def topic(self, reader):
        # the topic comes right before the duration: (ts_conn, topic, duration) or (url, token, topic, duration)
        args = reader.call_args.args
        self.assertIs(DAYS, args[-1])
        return args[-2]

    def test_reads_the_device_of_the_input_over_the_database_connection(self):
        self.read([BATTERY, device("device-1")], TS_CONN="postgres://timescale")
        self.assertEqual("postgres://timescale", self.timescale.call_args.args[0])
        topic = self.topic(self.timescale)
        self.assertEqual((DEVICE_TOPIC, "device-1"), (topic.name, topic.filterValue))
        self.assertEqual(["power"], [m.dest for m in topic.mappings])

    def test_reads_only_the_input_from_a_service_with_more_values(self):
        self.read([device("device-1", [{"dest": "other", "source": "value.energy.value"}])], TS_CONN="postgres://timescale")
        self.assertEqual(["power"], [m.dest for m in self.topic(self.timescale).mappings])

    def test_reads_one_device_written_with_a_trailing_comma(self):
        self.read([device("device-1, ")], TS_CONN="postgres://timescale")
        self.assertEqual("device-1", self.topic(self.timescale).filterValue)

    def test_reads_through_timescale_wrapper_with_a_platform_token(self):
        self.read([device("device-1")], {"ts_wrapper_url": "http://wrapper"}, SENERGY_TOKEN="token")
        self.assertEqual(("http://wrapper", "token"), tuple(self.wrapper.call_args.args[:2]))
        self.assertEqual("device-1", self.topic(self.wrapper).filterValue)

    def test_has_no_history_for_several_devices(self):
        with self.assertRaises(NoHistoryError):
            self.read([device("device-1,device-2")], TS_CONN="postgres://timescale")

    def test_has_no_history_for_the_output_of_another_operator(self):
        battery_as_power = dict(BATTERY, mappings=[{"dest": "power", "source": "battery_power"}])
        with self.assertRaises(NoHistoryError):
            self.read([battery_as_power], TS_CONN="postgres://timescale")

    def test_needs_a_database_connection_or_a_platform_token(self):
        with self.assertRaises(MissingConfigValueError):
            self.read([device("device-1")])


if __name__ == '__main__':
    unittest.main()
