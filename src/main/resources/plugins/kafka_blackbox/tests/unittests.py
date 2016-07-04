"""
Copyright (c) 2016 Cisco and/or its affiliates.

This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
The code, technical concepts, and all information contained herein, are the property of
Cisco Technology, Inc. and/or its affiliated entities, under various laws including copyright,
international treaties, patent, and/or contract. Any use of the material herein must be in
accordance with the terms of the License.
All rights not expressly granted by the License are reserved.

Unless required by applicable law or agreed to separately in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied.

Purpose:    Unit testing

"""

import time
import unittest

from mock import patch
from plugins.common.defcom import ZkPartitions, KkBrokers, KkBrokersHealth
from pnda_plugin import Event

class TestKafkaBlackbox(unittest.TestCase):

    @patch('plugins.common.zkclient.ZkClient')
    def test_normal_use(self, zk_mock):
        from plugins.kafka_blackbox.TestbotPlugin import KafkaBlackbox
        zk_mock.return_value.ping.return_value = True
        zk_mock.return_value.topics.return_value = [ZkPartitions('avro.internal.test', {'valid': True, 'list': [{0: {'leader': 1, 'isr': [1]}}]})]
        zk_mock.return_value.brokers.return_value = KkBrokersHealth("127.0.0.1:9092", "", 1, 0, [KkBrokers(1, "127.0.0.1", "9092", "9050", True)])
        plugin = KafkaBlackbox()
        values = plugin.runner(("--zconnect 127.0.0.1:2181"), True)

        self.assertEqual(6, len(values))
        i = 0
        test_ok = []
        test_ok.append(Event(0,'kafka','kafka.nodes',[],"127.0.0.1:9092"))
        test_ok.append(Event(0,'kafka','kafka.nodes.ok',[],1))
        test_ok.append(Event(0,'kafka','kafka.nodes.ko',[],0))
        test_ok.append(Event(0,'kafka','kafka.partitions.ok',[],1))
        test_ok.append(Event(0,'kafka','kafka.partitions.ko',[],0))
        test_ok.append(Event(0,'kafka','kafka.health',[],"OK"))
        for data in test_ok:
            self.assertEqual(values[i].source, data.source)
            self.assertEqual(values[i].metric, data.metric )
            self.assertEqual(values[i].causes, data.causes)
            self.assertEqual(values[i].value, data.value)
            i = i + 1

        zk_mock.return_value.topics.return_value = [ZkPartitions('avro.internal.test', {'valid': False, 'list': [{0: {'leader': 1, 'isr': [1]}}]})]
        del values[:]
        values = plugin.runner(("--zconnect 127.0.0.1:2181"), True)

        self.assertEqual(6, len(values))
        i = 0
        test_ok = []
        test_ok.append(Event(0,'kafka','kafka.nodes',[],"127.0.0.1:9092"))
        test_ok.append(Event(0,'kafka','kafka.nodes.ok',[],1))
        test_ok.append(Event(0,'kafka','kafka.nodes.ko',[],0))
        test_ok.append(Event(0,'kafka','kafka.partitions.ok',[],0))
        test_ok.append(Event(0,'kafka','kafka.partitions.ko',[],1))
        test_ok.append(Event(0,'kafka','kafka.health',['topic / partition inconsistency in zookeeper'],"WARN"))
        for data in test_ok:
            self.assertEqual(values[i].source, data.source)
            self.assertEqual(values[i].metric, data.metric )
            self.assertEqual(values[i].causes, data.causes)
            self.assertEqual(values[i].value, data.value)
            i = i + 1


if __name__ == '__main__':
    unittest.main()