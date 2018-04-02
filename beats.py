#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import gevent
import pyinotify
import gevent.queue
from gevent.pool import Group
from multiprocessing import Queue
from multiprocessing import Process

from .lib.logs import Logger
from .lib.yml import BeatsConfParse
from .lib.kafka import KafkaProductor
from .lib.EventHander import SimpleEventHandler, MultilineEventHandler, TagsEventHandler, TagAndMultilineEventHandler

root = os.path.dirname(os.path.abspath(__file__))
logger = Logger('{0}/logs/{1}.logs'.format(root, sys.argv[0].split('.')[0]), sys.argv[0])
CONF = '{0}/conf/filebeat.yml'.format(root)


def start_a_monitor(**kwargs):
    g_queue = gevent.queue.Queue()
    kwargs['queue'].put(dict(queue=g_queue, topic=kwargs['topics'], host=kwargs['bootstrap_server']))

    _wm = pyinotify.WatchManager()
    if kwargs.__contains__('multiline') and kwargs.__contains__('tags'):
        _handler = TagAndMultilineEventHandler(queue=g_queue,
                                              fields=kwargs['fields'],
                                              tag_head_pattern=kwargs['tags']['forward'],
                                              multi_head_pattern=kwargs['multiline']['patterns'])
    elif kwargs.__contains__('multiline'):
        _handler = MultilineEventHandler(queue=g_queue,
                                         fields=kwargs['fields'],
                                         multi_head_pattern=kwargs['multiline']['patterns'])
    elif kwargs.__contains__('tags'):
        _handler = TagsEventHandler(queue=g_queue,
                                    fields=kwargs['fields'],
                                    tag_head_pattern=kwargs['tags'['forward']])
    else:
        _handler = SimpleEventHandler(queue=g_queue,fields=kwargs['fields'])

    _notifier = pyinotify.Notifier(_wm, _handler)
    _wm.add_watch(path=kwargs['paths'], mask=pyinotify.IN_MODIFY, rec=True, do_glob=True, quiet=True)
    _notifier.loop()


def processing_productors(_data, _queue):
    group = Group()
    jobs = [gevent.spawn(start_a_monitor, queue=_queue, **x['prospectors'], **x['output']) for x in _data]

    for job in jobs:
        group.add(job)
    group.join()


def start_a_kafka(**kwargs):
    KafkaProductor(host=kwargs['host'], topic=kwargs['topic'], queue=kwargs['queue']).start


def processing_consumers(_queues):
    group = Group

    while _queues.empty() is not True:
        data = _queues.get()
        group.add(gevent.spawn(start_a_kafka, queue=data['queue'], host=data['host'], topic=data['topic']))
    group.join()


def main():
    conf_data = BeatsConfParse(CONF).run
    queue = Queue()
    productors = Process(name="productors", target=processing_productors, args=(conf_data, queue))
    consumers = Process(name="comsumers", target=processing_consumers, args=(queue))


if __name__ == '__main__':
    main()
