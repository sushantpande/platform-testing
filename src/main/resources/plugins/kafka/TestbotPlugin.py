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

Purpose:    Kafka whitebox tests

"""

# check http://kafka.apache.org/documentation.html#monitoring for more
# information on JMX

# todo: clean up get functions in order to make them more generic

import time
import argparse
import sys
import os
import logging

import requests

from plugins.common.zkclient import ZkClient

from pnda_plugin import PndaPlugin
from pnda_plugin import Event

sys.path.insert(0, '../..')

TestbotPlugin = lambda: KafkaWhitebox()

TIMESTAMP_MILLIS = lambda: int(time.time() * 1000)
HERE = os.path.abspath(os.path.dirname(__file__))
LOGGER = logging.getLogger("TestbotPlugin")

class KafkaWhitebox(PndaPlugin):
    '''
    Whitebox test plugin for Kafka
    '''

    def __init__(self):
        self.broker_list = []
        self.zk_list = []
        self.postjson = False
        self.display = False
        self.results = []
        self.topic_list = []


    def read_args(self, args):
        '''
            This class argument parser.
            This shall come from main runner in the extra arg
        '''
        parser = argparse.ArgumentParser(
            prog=self.__class__.__name__,
            usage='%(prog)s [options]',
            description='Show state of Zk-Kafka cluster',
            add_help=False)
        parser.add_argument('--brokerlist', default='localhost:9092',
          help='comma separated host:port pairs, each corresponding to ' + \
          'a kafka broker (default: localhost:9092)')
        parser.add_argument('--zkconnect', default='localhost:2181',
          help='comma separated host:port pairs, each corresponding to a ' + \
          'zk host (default: localhost:2181)')
        return parser.parse_args(args)

    def get_brokertopicmetrics(self, host, topic, broker_id):
        '''
        Get brokertopicmetrics
        '''
        for jmx_path_name in ["BytesInPerSec", "BytesOutPerSec", \
                              "MessagesInPerSec"]:
            for jmx_data in ["RateUnit", "OneMinuteRate", \
                    "EventType", "Count", "FifteenMinuteRate",
                    "FiveMinuteRate", "MeanRate"]:
                url_jmxproxy = ("http://127.0.0.1:8000/jmxproxy/%s/"
                "kafka.server:type=BrokerTopicMetrics,"
                "name=%s,topic=%s/%s") % (host, jmx_path_name, topic, jmx_data)

                response = requests.get(url_jmxproxy)
                if response.status_code == 200:
                    LOGGER.debug("Getting %s - %s", response.text, url_jmxproxy)
                    self.results.append(Event(TIMESTAMP_MILLIS(),
                      'kafka',
                      'kafka.brokers.%d.topics.%s.%s.%s' %
                      (broker_id,
                       topic,
                       jmx_path_name,
                       jmx_data), [], response.text)
                     )
                elif response.status_code == 404:
                    self.results.append(Event(TIMESTAMP_MILLIS(),
                      'kafka',
                      'kafka.brokers.%d.topics.%s.%s.%s' %
                      (broker_id,
                       topic,
                       jmx_path_name,
                       jmx_data), [], '0')
                     )
                else:
                    LOGGER.error("ERROR for url_jmxproxy: %s", url_jmxproxy)

        return None

    def get_operatingsysteminfo(self, host, broker_id):
        '''
        Get operatingsysteminfo
        '''
        for jmx_data in ["OpenFileDescriptorCount",
                         "CommittedVirtualMemorySize",
                         "FreePhysicalMemorySize",
                         "SystemLoadAverage",
                         "Arch",
                         "ProcessCpuLoad",
                         "FreeSwapSpaceSize",
                         "TotalPhysicalMemorySize",
                         "Name",
                         "ObjectName",
                         "TotalSwapSpaceSize",
                         "ProcessCpuTime",
                         "MaxFileDescriptorCount",
                         "SystemCpuLoad",
                         "Version",
                         "AvailableProcessors"]:
            url_jmxproxy = "http://127.0.0.1:8000/jmxproxy/" + \
              "%s/java.lang:type=OperatingSystem/%s" % (host, jmx_data)

            response = requests.get(url_jmxproxy)
            if response.status_code == 200:
                LOGGER.debug("Getting %s fo %s", response.text, url_jmxproxy)
                self.results.append(Event(TIMESTAMP_MILLIS(),
                  'kafka',
                  'kafka.brokers.%d.system.%s' %
                  (broker_id, jmx_data), [], response.text))
            else:
                LOGGER.error("ERROR for url_jmxproxy: %s", url_jmxproxy)

        return None

    def get_underreplicatedpartitions(self, host, broker_id):
        '''
        Get underreplicatedpartitions
        '''
        url_jmxproxy = ("http://127.0.0.1:8000/jmxproxy/%s/"
                        "kafka.server:type=ReplicaManager,"
                        "name=UnderReplicatedPartitions/Value") % host

        response = requests.get(url_jmxproxy)
        if response.status_code == 200:
            LOGGER.debug("Getting %s fo %s", response.text, url_jmxproxy)
            self.results.append(Event(TIMESTAMP_MILLIS(),
              'kafka',
              'kafka.brokers.%d.UnderReplicatedPartitions' %
              broker_id, [], response.text))
        else:
            LOGGER.error("ERROR for url_jmxproxy: %s", url_jmxproxy)
        # todo: impact health as this need to be 0

        return None

    def get_activecontrollercount(self, host, broker_id):
        '''
        Get activecontrollercount
        '''
        url_jmxproxy = ("http://127.0.0.1:8000/jmxproxy/%s/"
                        "kafka.controller:type=KafkaController,"
                        "name=ActiveControllerCount/Value") % host

        response = requests.get(url_jmxproxy)
        if response.status_code == 200:
            LOGGER.debug("Getting %s fo %s", response.text, url_jmxproxy)
            self.results.append(Event(TIMESTAMP_MILLIS(),
                'kafka',
                'kafka.brokers.%d.ActiveControllerCount' %
                broker_id, [], response.text))
        else:
            LOGGER.error("ERROR for url_jmxproxy: %s", url_jmxproxy)
        # todo: impact health as only one broker in the cluster should have 1

        return None

    def get_leaderelectionrateandtimems(self, host, broker_id):
        '''
        Get leaderelectionrateandtimems
        '''
        for jmx_data in ["StdDev",
                         "75thPercentile",
                         "Mean",
                         "LatencyUnit",
                         "RateUnit",
                         "98thPercentile",
                         "95thPercentile",
                         "99thPercentile",
                         "EventType",
                         "Max",
                         "Count",
                         "FiveMinuteRate",
                         "MeanRate",
                         "50thPercentile",
                         "OneMinuteRate",
                         "Min",
                         "999thPercentile",
                         "FifteenMinuteRate"]:
            url_jmxproxy = ("http://127.0.0.1:8000/jmxproxy/%s/"
                "kafka.controller:type=ControllerStats,"
                "name=LeaderElectionRateAndTimeMs/%s") % (host, jmx_data)

            response = requests.get(url_jmxproxy)
            if response.status_code == 200:
                LOGGER.debug("Getting %s fo %s", response.text, url_jmxproxy)
                self.results.append(Event(TIMESTAMP_MILLIS(),
                    'kafka',
                    'kafka.brokers.%d.controllerstats.LeaderElection.%s' %
                    (broker_id, jmx_data), [], response.text))
            else:
                LOGGER.error("ERROR for url_jmxproxy: %s", url_jmxproxy)

        # todo: impact health as non-zero when there are broker failures
        return None

    def get_uncleanleaderelections(self, host, broker_id):
        '''
        Get UncleanLeaderElectionsPerSec
        '''
        for jmx_data in ["RateUnit",
                         "OneMinuteRate",
                         "EventType",
                         "Count",
                         "FifteenMinuteRate",
                         "FiveMinuteRate",
                         "MeanRate"]:
            url_jmxproxy = ("http://127.0.0.1:8000/jmxproxy/%s/"
                  "kafka.controller:type=ControllerStats,"
                  "name=UncleanLeaderElectionsPerSec/%s") % (host, jmx_data)

            response = requests.get(url_jmxproxy)
            if response.status_code == 200:
                LOGGER.debug("Getting %s fo %s", response.text, url_jmxproxy)
                self.results.append(Event(TIMESTAMP_MILLIS(),
                  'kafka',
                  ('kafka.brokers.%d.'
                   'controllerstats.UncleanLeaderElections.%s') %
                  (broker_id, jmx_data), [], response.text))
            else:
                LOGGER.error("ERROR for url_jmxproxy: %s", url_jmxproxy)

        # todo: impact health as non-zero when there are broker failures
        return None

    def process_brokers(self):
        '''
        Process the brokers
        '''
        # TODO see brokerID
        for broker_index in xrange(1, len(self.broker_list) + 1):
            broker = self.broker_list[broker_index - 1]
            for topic in self.topic_list:
                self.get_brokertopicmetrics(broker, topic, broker_index)
            self.get_operatingsysteminfo(broker, broker_index)
            self.get_underreplicatedpartitions(broker, broker_index)
            self.get_activecontrollercount(broker, broker_index)
            self.get_leaderelectionrateandtimems(broker, broker_index)
            self.get_uncleanleaderelections(broker, broker_index)
        return None

    def runner(self, args, display=True):
        '''
            Main section.
        '''
        LOGGER.debug("runner started")

        plugin_args = args.split() \
            if args is not None and (len(args.strip()) > 0) \
            else ""

        options = self.read_args(plugin_args)

        self.broker_list = options.brokerlist.split(",")
        self.zk_list = options.zkconnect.split(",")

        for zookeeper in self.zk_list:
            if ':' in zookeeper:
                host, port = zookeeper.split(':', 1)
                port = int(port)

                client = ZkClient(host, port)
                if client.ping():
                    topics = client.topics()
                    for topic in topics:
                        if not topic.id in self.topic_list:
                            self.topic_list.append(topic.id)
                            LOGGER.debug(
                                "adding %s to the topic list", topic.id)

        LOGGER.debug("Perform white box test on topics %s", \
          '-'.join(self.topic_list))
        self.process_brokers()

        LOGGER.debug("runner finished")

        if display:
            self._do_display(self.results)

        return self.results
