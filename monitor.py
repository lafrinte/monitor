#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import sys
import time
import yaml
import gevent
import pyinotify
from pykafka import KafkaClient
from multiprocessing import Queue as p_queue
from multiprocessing import Process
from daemons import daemonizer

PATH = '/tmp/2.logs'
LOG = '/tmp/monitor.logs'
KAFKA = '172.17.0.2:9092'
TOPIC = b'test'
SINDB = os.path.join(os.getenv('HOME'), '.sindb_{}'.format(os.path.basename(PATH)))





class FileInotify(object):

    def __init__(self, _queue):
        self.wm = None
        self.mask = None
        self.notifier = None
        self.queue = _queue
        self._init_option()

    def _init_option(self):
        self.wm = pyinotify.WatchManager()
        self.mask = pyinotify.IN_MODIFY

    def start(self, filename):
        handler = EventHandler(filepath=filename, queue=self.queue)
        self.notifier = pyinotify.Notifier(self.wm, handler)
        self.wm.add_watch(filename, self.mask, rec=True)
        self.notifier.loop()


def file_monitor(filename, _queue):
    _monitor = FileInotify(_queue)
    _monitor.start(filename)


def kafka_productor(kafka, topic, _queue):
    _p = KafkaProductor(kafka, topic, _queue)
    _p.start


def parse_yml(path):

    """
    :param path:
    :return: dict

    :yml mode:

    """
    return yaml.load(open(path))

@daemonizer.run(pidfile="/tmp/monitor.pid")
def main():
    queue = p_queue()
    monitor = Process(name="Monitor", target=file_monitor, args=(PATH, queue))
    productor = Process(name="Productor", target=kafka_productor, args=(KAFKA, TOPIC, queue))
    monitor.daemon = True
    productor.daemon = True

    monitor.start()
    productor.start()

    return 0

if __name__ == "__main__":
    cnf_path = sys.argv[1]
    main(conf_path)
