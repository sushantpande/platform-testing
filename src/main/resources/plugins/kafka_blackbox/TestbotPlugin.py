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

Purpose:    Plugin for blackbox Kafka tests

"""

import time
import argparse
import sys
import os
import logging

from prettytable import PrettyTable

from plugins.common.zkclient import ZkClient, ZkError
from plugins.kafka_blackbox.prod2cons import Prod2Cons
from plugins.common.defcom import MonitorSummary, PartitionState, TestbotResult
from plugins.common.defcom import ZkNodesHealth, ZkNode, KkBroker

from pnda_plugin import PndaPlugin
from pnda_plugin import Event
from pnda_plugin import MonitorStatus

sys.path.insert(0, '../..')
TIMESTAMP_MILLIS = lambda: int(time.time() * 1000)
HERE = os.path.abspath(os.path.dirname(__file__))
LOGGER = logging.getLogger("TestbotPlugin")

TestbotPlugin = lambda: KafkaBlackbox() # pylint: disable=invalid-name


def getzknodes(zconnect):
    '''
        Returns a list of zknodes tuples, where each tuple represents
        a zk node with host/port and alive status.
    '''

    LOGGER.debug("getzknodes started")
    zok = 0
    zko = 0
    node_list = []
    bconnect = ""
    berror = ""
    zconnectsplit = zconnect.split(",")
    for zpart in zconnectsplit:
        if ':' in zpart:
            host, port = zpart.split(':', 1)
            port = int(port)
            if bconnect != "":
                bconnect += ","
            bconnect += "%s:%d" % (host, port)
            try:
                client = ZkClient(host, port)
                if client.ping():
                    node_list.append(ZkNode(host, port, True))
                    zok += 1
                else:
                    if berror != "":
                        berror += ","
                    berror += "%s:%d" % (host, port)
                    node_list.append(ZkNode(host, port, False))
                    zko += 1
                    LOGGER.error(
                        "Zookeeper node unreachable (%s:%d)", host, port)
            except ZkError:
                LOGGER.error(
                    "Zookeeper node unreachable (%s:%d)", host, port)
                zko += 1
                node_list.append(ZkNode(host, port, False))
    LOGGER.debug("getzknodes finished")
    return ZkNodesHealth(bconnect, berror, zok, zko, node_list)

def get_broker_by_id(brokers, search):
    '''
    Get broker by id
    '''
    for broker in brokers.list:
        if broker.id == search:
            return KkBroker(
                search, broker.host, broker.port, broker.jmx_port, broker.alive)
    return None

class ProcessorError(Exception):
    '''
    Processor errors
    '''
    def __init__(self, msg):
        Exception.__init__(self, msg)
        self.msg = msg

    def __str__(self):
        return self.msg


class KafkaBlackbox(PndaPlugin):
    '''
    Main body of plugin
    '''

    def __init__(self):
        self.zconnect = ""
        self.prod2cons = False
        self.postjson = False
        self.display = False
        self.runtesttopic = "avro.internal.testbot"
        self.avro_schema = "%s/%s" % (HERE, "dataplatform-raw.avsc")
        self.runnbtest = 10
        self.consumer_timeout = 1  # max number of second to wait for
        self.results = []
        self.whitebox = False

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
        parser.add_argument('--zconnect', default='localhost:2181',
                            help=('comma separated host:port pairs, each corresponding to a zk host (default: localhost:2181)'))
        parser.add_argument('--prod2cons', action='store_const', const=True,
                            help='Run a producer/consumer test')

        return parser.parse_args(args)

    def do_display(self, results_summary, zk_data, test_result):
        '''
            Receive a summary tuples, and then build a friendly
            on the standard output as a result of the monitoring running.
            The second object is the test result from prod2cons.
        '''

        LOGGER.debug("do_display start")

        table = PrettyTable(['Broker', 'Port', 'Topic', 'PartId', 'Valid'])
        table.align['broker'] = 'l'

        if zk_data and len(zk_data.partitions) > 0:
            for part in zk_data.partitions:
                table.add_row(
                    [part.broker, part.port, part.topic, \
                    part.partId, part.alive])

        if zk_data:
            print table.get_string(sortby='Broker')
            print
            print 'List of brokers:            %s' % zk_data.list_brokers
            print 'List of brokers (ko):       %s' % zk_data.list_brokers_ko
            print 'Number of brokers (ok):     %d' % zk_data.num_brokers_ok
            print 'Number of brokers (ko):     %d' % zk_data.num_brokers_ko
            print 'List of zk:                 %s' % zk_data.list_zk
            print 'List of zk (ko):            %s' % zk_data.list_zk_ko
            print 'Number of zk nodes (ok):    %d' % zk_data.num_zk_ok
            print 'Number of zk nodes (ko):    %d' % zk_data.num_zk_ko
            print 'Number of partitions (ok):  %d' % zk_data.num_part_ok
            print 'Number of partitions (ko):  %d' % zk_data.num_part_ko
            print 'Number of partitions:       %d' % zk_data.num_partitions
            print 'Run (total):                %d' % self.runnbtest
            print 'Run (sent):                 %d' % test_result.sent
            print 'Run (rcv):                  %d' % test_result.received
            print 'Run (total):                %d' % test_result.notvalid
            print 'Run (avg ms):               %d' % test_result.avg_ms

        print '-' * 50
        print 'overall status: ',
        print "OK" if results_summary.value == MonitorStatus["green"] else \
              "WARN" if results_summary.value == MonitorStatus["amber"] else \
              "ERROR"
        if results_summary.value != MonitorStatus["green"]:
            print 'causes:'
            print results_summary.causes
        print '-' * 50
        LOGGER.debug("do_display finished")

    def analyse_results(self, zk_data, test_result):
        '''
        Analyse the partition summary and Prod2Cons
        Then set the the test result flag accordingly
        I the test flag is not green, put a reason explaining why
        Then return a json
        '''
        analyse_status = MonitorStatus["green"]
        analyse_causes = []
        analyse_metric = 'kafka.health'

        if zk_data and len(zk_data.list_zk_ko) > 0:
            LOGGER.error(
                "analyse_results : at least one zookeeper node failed")
            analyse_status = MonitorStatus["red"]
            analyse_causes.append(
                "zookeeper node(s) unreachable (%s)" % zk_data.list_zk_ko)

        if zk_data and len(zk_data.list_brokers_ko) > 0:
            LOGGER.error("analyse_results : at least one broker failed")
            analyse_status = MonitorStatus["red"]
            analyse_causes.append("broker(s) unreachable (%s)" %
                                  zk_data.list_brokers_ko)

        if zk_data and zk_data.num_part_ko > 0:
            LOGGER.error("analyse_results : at least one topic / partition inconsistency")
            if analyse_status != MonitorStatus["red"]:
                analyse_status = MonitorStatus["amber"]
            analyse_causes.append(
                "topic / partition inconsistency in zookeeper")

        if self.prod2cons:
            if test_result.sent == test_result.received \
             and test_result.notvalid == 0:
                LOGGER.debug("analyse_results - test for messages sent / received is valid")
            else:
                LOGGER.error("analyse_results - test for messages sent / received failed")
                analyse_status = MonitorStatus["red"]
                analyse_causes.append("producer / consumer failed " + \
                    "(sent %d, rcv_ok %d, rcv_ko %d)" %
                                      (test_result.sent,
                                       test_result.received,
                                       test_result.notvalid))

        return Event(
            TIMESTAMP_MILLIS(), 'kafka', \
            analyse_metric, analyse_causes, analyse_status)

    def process(self, zknodes, gbrokers, partitions):
        '''
        Returns a named tuple of type PartitionsSummary.
        '''
        LOGGER.debug("process started")
        topic_ok = 0
        topic_ko = 0
        process_results = []
        for obj in partitions:
            parts_object = obj.partitions["list"]
            if obj.partitions["valid"] is True:
                for parts in parts_object:
                    # Get the partition leader
                    for part, partinfo in parts.iteritems():
                        leader_read = partinfo['leader']
                        broker = get_broker_by_id(
                            gbrokers, '%d' % leader_read)

                        if broker is not None:
                            process_results.append(
                                PartitionState(
                                    broker.host,
                                    broker.port,
                                    obj.id,
                                    part,
                                    obj.partitions["valid"]))
                    topic_ok += 1
            else:
                topic_ko += 1
                LOGGER.error("Topic not in a good state (%s)", obj.id)
                process_results.append(PartitionState(None,
                                                      None,
                                                      obj.id,
                                                      None,
                                                      obj.partitions["valid"]))

        self.results.append(Event(TIMESTAMP_MILLIS(),
                                  'kafka',
                                  'kafka.nodes',
                                  [],
                                  gbrokers.connect))

        self.results.append(Event(TIMESTAMP_MILLIS(),
                                  'kafka',
                                  'kafka.nodes.ok',
                                  [],
                                  gbrokers.num_ok))

        self.results.append(Event(TIMESTAMP_MILLIS(),
                                  'kafka',
                                  'kafka.nodes.ko',
                                  [],
                                  gbrokers.num_ko))

        self.results.append(Event(TIMESTAMP_MILLIS(),
                                  'kafka',
                                  'kafka.partitions.ok',
                                  [],
                                  topic_ok))

        self.results.append(Event(TIMESTAMP_MILLIS(),
                                  'kafka',
                                  'kafka.partitions.ko',
                                  [],
                                  topic_ko))

        LOGGER.debug("process finished")
        return MonitorSummary(num_partitions=len(process_results),
                              list_brokers=gbrokers.connect,
                              list_brokers_ko=gbrokers.error,
                              num_brokers_ok=gbrokers.num_ok,
                              num_brokers_ko=gbrokers.num_ko,
                              list_zk=zknodes.connect,
                              list_zk_ko=zknodes.error,
                              num_zk_ok=zknodes.num_ok,
                              num_zk_ko=zknodes.num_ko,
                              num_part_ok=topic_ok,
                              num_part_ko=topic_ko,
                              partitions=tuple(process_results)
                             )

    def runner(self, pluginargs, display=True):
        '''
            Main section.
        '''
        LOGGER.debug("runner started")
        array_args = pluginargs.split(" ")
        options = self.read_args(array_args)
        self.zconnect = options.zconnect
        self.prod2cons = options.prod2cons
        self.display = display
        # split zonnect in pair of zhost, zport
        zknodes = getzknodes(self.zconnect)
        LOGGER.debug(zknodes)
        prev_zk_data = None
        zk_data = None
        brokers = None
        for zkn in zknodes.list:
            LOGGER.debug("processing %s:%d", zkn.host, zkn.port)
            if zkn.alive is True:
                try:
                    client = ZkClient(zkn.host, zkn.port)
                    brokers = client.brokers()
                    topics = client.topics()
                    zk_data = self.process(zknodes, brokers, topics)
                except ZkError, exc:
                    LOGGER.error('Failed to access Zookeeper: %s', str(exc))
                    break
                except ProcessorError, exc:
                    LOGGER.error('Failed to process: %s', str(exc))
                    break
                if prev_zk_data is not None:
                    if (
                            prev_zk_data.num_partitions != zk_data.num_partitions or
                            prev_zk_data.num_part_ok != zk_data.num_part_ok or
                            prev_zk_data.num_part_ko != zk_data.num_part_ko):
                        LOGGER.error("Inconsistency found in zk (%s,%d) tree comparison", zkn.host, zkn.port)
                    else:
                        LOGGER.debug("No inconsistency found in zk (%s,%d) tree comparison", \
                                     zkn.host, zkn.port)
                prev_zk_data = zk_data
        if not zk_data:
            zk_data = MonitorSummary(num_partitions=-1,
                                     list_brokers="",
                                     list_brokers_ko="",
                                     num_brokers_ok=-1,
                                     num_brokers_ko=-1,
                                     list_zk=self.zconnect,
                                     list_zk_ko=self.zconnect,
                                     num_zk_ok=0,
                                     num_zk_ko=len(zknodes.list),
                                     num_part_ok=-1,
                                     num_part_ko=-1,
                                     partitions=tuple()
                                    )
        test_result = TestbotResult(-1, -1, -1, -1)
        if self.prod2cons:
            LOGGER.debug("=> E2E producer / consumer test required")
            # Now, pick up a broker and run a prod2cons test run
            if brokers and len(brokers.connect) > 0:
                # beta1: use the first of the list
                pairbrokers = brokers.connect.split(',')
                shost, sport = pairbrokers[0].split(':')
                try:
                    test_runner = Prod2Cons(shost,
                                            int(sport),
                                            self.avro_schema,
                                            self.runtesttopic,
                                            self.runnbtest,
                                            self.consumer_timeout)
                    test_runner.consumer_reset()
                    test_runner.prod()
                    test_result = test_runner.cons()
                except ValueError as error:
                    LOGGER.error("Error on Prod2Cons " + str(error))
            else:
                LOGGER.error("No valid broker found for running prod2cons run")

        results_summary = self.analyse_results(zk_data, test_result)

        if self.display:
            self.do_display(results_summary, zk_data, test_result)

        self.results.append(results_summary)
        LOGGER.debug("runner finished")
        return self.results
