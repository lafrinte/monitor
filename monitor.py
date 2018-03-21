#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import time
import pyinotify
from pykafka import KafkaClient
from multiprocessing import Queue as p_queue
from multiprocessing import Process
from daemons import daemonizer

PATH = '2.logs'
LOG = '/tmp/monitor.logs'
KAFKA = '172.17.0.2:9092'
TOPIC = b'test'
SINDB = os.path.join(os.getenv('HOME'), '.sindb_{}'.format(PATH))


class Error(Exception):

    def __init__(self, msg=""):
        Exception.__init__(self, msg)
        self.msg = msg

    def __repr__(self):
        ret = '%s.%s.%s' % (self.__class__.__module__, self.__class__.__name__, self.msg)
        return ret.strip()

    __str__ = __repr__


class KafkaConnectErr(Error):

    def __init__(self, addr, msg=None):
        Error.__init__(self, msg)
        self.msg = msg
        self.kafka = addr
        if msg is None:
            self.msg = "Unable to connect to a kafka broker %s to fetch metadata" % repr(self.kafka)


class EventHandler(pyinotify.ProcessEvent):

    def __init__(self, *args, **kwargs):
        super(EventHandler, self).__init__(*args, **kwargs)
        self.path = kwargs.get('filepath')
        self.file = open(kwargs.get('filepath'))
        self.queue = kwargs.get('queue')
        self.f_info = {
            '_st_ino': os.stat(kwargs.get('filepath'))[1],
            '_st_dev': os.stat(kwargs.get('filepath'))[2]
        }
        self.offset = self.get_offset()
        self.msg_head = re.compile(r'^\[')
        self.msg = list()
        self.is_prompt = False
        self.service_tag = self.get_mark()
        self._init_file_seek()

    def _init_file_seek(self):
        if self.offset > 0:
            self.file.seek(self.offset)

    def get_offset(self):

        """
        :Describe: Compare the innode number and the device number between os.stat and the data saved in .sindb.
                  Only when the _st_ino and _st_dev are both equal, the offset will set equally in .sindb. otherwise
                  set the offset to 0(means read from the first line)
        :return: offset, int type
        """

        _offset = 0
        if os.path.isfile(SINDB) is True and os.stat(SINDB)[6] > 0:
            with open(SINDB, 'r') as f:
                _st_data = f.readline().split(',')
            _st_ino, _st_dev, _st_size = int(_st_data[0]), int(_st_data[1]), int(_st_data[2])
            if _st_ino == self.f_info['_st_ino'] and _st_dev == self.f_info['_st_dev']:
                _offset = _st_size
        return _offset

    def save_sindb(self):

        """
        :Describe: write the device num, innode num and the offset in sindb
        :return: no return
        """
        with open(SINDB, 'w') as f:
            f.writelines(','.join([str(self.f_info['_st_ino']), str(self.f_info['_st_dev']), str(self.offset)]))

    def process_IN_MODIFY(self, event):
        self.cut_line()
        self.save_sindb()

    def get_mark(self):
        return time.time().__str__().split('.')[1]

    def multline_parse(self, new_line):

        """
        :Describe: if the new line cannot match self.msg_head pattern, it will link to the end of the last line
        :param new_line: the new line read from target file, string type
        :return: msg, string type. Only when the multiline string have been linked, it will return the linked string
        """

        if self.msg_head.search(new_line) and self.is_prompt is False:
            self.msg.append(new_line)
            self.is_prompt = True
        elif self.msg_head.search(new_line) and self.is_prompt is True:
            msg, self.msg = ''.join(self.msg), [new_line]
            return msg
        else:
            self.msg.append(new_line)

    def add_service_tags(self, line):

        try:
            str(line).index('Service Begin')
            self.service_tag = self.get_mark()
        except ValueError:
            pass

        return '{0} ssc:{1}'.format(line, self.service_tag)

    def cut_line(self):

        last_n = os.stat(self.path)[6]
        while self.offset < last_n:
            line = self.file.readline()
            self.offset += len(line)
            self.file.seek(self.offset)
            msg = self.multline_parse(line)

            if msg:
                self.save_sindb()
                self.queue.put(self.add_service_tags(msg).replace('\n', ' '))


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


class KafkaProductor(object):

    def __init__(self, addr, topic, _queue):
        self.kafka = addr
        self.topic = topic
        self.session = None
        self.queue = _queue
        self._kafka_connect()

    def _kafka_connect(self):

        try:
            self.client = KafkaClient(hosts=self.kafka)
        except:
            raise KafkaConnectErr(self.kafka)

    @property
    def start(self):
        self.session = self.client.topics[str.encode(self.topic)]

        with self.session.get_producer(delivery_reports=True) as producer:
            count = 0

            while True:
                count += 1
                producer.produce(self.queue.get(), partition_key=str.encode(str(count)))
                if count % 10 == 0:
                    while True:
                        try:
                            msg, exc = producer.get_delivery_report(block=False)
                            if exc is not None:
                                print('Failed to deliver msg {}: {}'.format(msg.partition_key, repr(exc)))
                        except:
                            break


def file_monitor(filename, _queue):
    _monitor = FileInotify(_queue)
    _monitor.start(filename)


def kafka_productor(kafka, topic, _queue):
    _p = KafkaProductor(kafka, topic, _queue)
    _p.start


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
   main()
