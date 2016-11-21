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

Purpose:      Blackbox test for the Deployment manager

"""

import time
import argparse
import eventlet
import requests

from pnda_plugin import PndaPlugin
from pnda_plugin import Event

TIMESTAMP_MILLIS = lambda: int(round(time.time() * 1000))

TestbotPlugin = lambda: DMBlackBox() # pylint: disable=invalid-name


class DMBlackBox(PndaPlugin):

    def __init__(self):
        pass

    def read_args(self, args):
        '''
        This class argument parser.
        This shall come from main runner in the extra arg
        '''
        parser = argparse.ArgumentParser(prog=self.__class__.__name__, \
         usage='%(prog)s [options]', description='Key metrics from CDH cluster')
        parser.add_argument('--dmendpoint', default='http://localhost:5000', \
         help='DM endpoint e.g. http://localhost:5000')

        return parser.parse_args(args)


    def runner(self, args, display=True):
        '''
        Main section.
        '''
        plugin_args = args.split() \
        if args is not None and (len(args.strip()) > 0) \
        else ""

        options = self.read_args(plugin_args)

        values = []

        try:
            start = TIMESTAMP_MILLIS()
            with eventlet.Timeout(100):
                req = requests.get("%s/repository/packages" % (options.dmendpoint), timeout=20)
            end = TIMESTAMP_MILLIS()
            packages_available_ok = True
            packages_available_count = len(req.json())
            packages_available_ms = end-start
            values.append(Event(TIMESTAMP_MILLIS(), "deployment-manager", \
                "deployment-manager.packages_available_time_ms", \
                [], packages_available_ms))
            values.append(Event(TIMESTAMP_MILLIS(), "deployment-manager", \
                "deployment-manager.packages_available_count", \
                [], packages_available_count))
        except Exception:
            packages_available_ok = False
        values.append(Event(TIMESTAMP_MILLIS(), "deployment-manager", \
            "deployment-manager.packages_available_succeeded", \
            [], packages_available_ok))

        try:
            start = TIMESTAMP_MILLIS()
            with eventlet.Timeout(100):
                req = requests.get("%s/packages" \
                    % (options.dmendpoint), timeout=20)
            end = TIMESTAMP_MILLIS()
            packages_deployed_ok = True
            packages_deployed_count = len(req.json())
            packages_deployed_ms = end-start
            values.append(Event(TIMESTAMP_MILLIS(), "deployment-manager", \
                "deployment-manager.packages_deployed_time_ms", \
                [], packages_deployed_ms))
            values.append(Event(TIMESTAMP_MILLIS(), 'deployment-manager', \
                "deployment-manager.packages_deployed_count", \
                [], packages_deployed_count))
        except Exception:
            packages_deployed_ok = False
        values.append(Event(TIMESTAMP_MILLIS(), 'deployment-manager', \
            "deployment-manager.packages_deployed_succeeded", \
            [], packages_deployed_ok))
        cause = ""
        health = "OK"
        if not packages_available_ok or not packages_deployed_ok:
            health = "ERROR"
            cause = "Deployment manager package APIs are not working"
        values.append(Event(TIMESTAMP_MILLIS(), 'deployment-manager',
                            'deployment-manager.health', [cause], health))
        if display:
            self._do_display(values)
        return values
