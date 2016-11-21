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

Purpose:    Common definitions used by plugins

"""

from collections import namedtuple

ZkKafkaBroker = namedtuple('ZkKafkaBroker', ['id', 'host', 'port'])
ZkKafkaConsumers = namedtuple('ZkKafkaConsumers', ['id', 'partitions'])
ZkPartitions = namedtuple('ZkPartitions', ['id', 'partitions'])
ZkKafkaTopic = namedtuple('ZkKafkaTopic', ['topic', 'broker', 'num_partitions'])
KkBrokers = namedtuple('KkBrokers', ['id', 'host', 'port', 'jmx_port', 'alive'])
KkBroker = namedtuple('KkBroker', ['id', 'host', 'port', 'jmx_port', 'alive'])
KkBrokersHealth = namedtuple('KkBrokersHealth', ['connect', \
  'error', 'num_ok', 'num_ko', 'list'])
ZkNode = namedtuple('ZkNode', ['host', 'port', 'alive'])
ZkNodesHealth = namedtuple('ZkNodesHealth', ['connect', 'error', \
  'num_ok', 'num_ko', 'list'])

TestbotResult = namedtuple('TestbotResult',
                           [
                               'sent',
                               'received',
                               'notvalid',
                               'avg_ms'
                           ])

PartitionState = namedtuple('PartitionState',
                            [
                                'broker',           # Broker host
                                'port',             # Broker port
                                'topic',            # Topic on broker
                                'partId',           # Partition id
                                'alive'             # broker alive
                            ])
MonitorSummary = namedtuple('PartitionsSummary',
                            [
                                'num_partitions',   # Number of partitions.
                                'list_brokers',     # host:port list, comma separated
                                'list_brokers_ko',  # host:port list, comma separated
                                'num_brokers_ok',   # Number of Kafka Brokers alive
                                'num_brokers_ko',   # Number of Kafka Brokers unreachable
                                'num_zk_ok',        # Number of zk node alive
                                'list_zk',          # host:port list, comma separated
                                'list_zk_ko',       # host:port list, comma separated
                                'num_zk_ko',        # Number of zk node unreachable
                                'num_part_ok',      # Number of partition ok
                                'num_part_ko',      # Number of partition HS
                                'partitions'        # Tuple of PartitionStates
                            ])

ZkMonitorSummary = namedtuple('ZkSummary',
                              [
                                  'num_zk_ok',        # Number of zk node alive
                                  'list_zk',          # host:port list, comma separated
                                  'list_zk_ko',       # host:port list, comma separated
                                  'num_zk_ko'        # Number of zk node unreachable
                              ])
