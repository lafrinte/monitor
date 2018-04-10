#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import pyinotify
from multiprocessing import Queue
from multiprocessing import Process

from lib.logs import Logger
from lib.daemon import daemon_init
from lib.yml import BeatsConfParse
from lib.kafka import KafkaProductor
from lib.EventHander import SimpleEventHandler, MultilineEventHandler, TagsEventHandler, TagAndMultilineEventHandler

root = os.path.dirname(os.path.abspath(__file__))
logger = Logger('{}/logs/{}.logs'.format(root, sys.argv[0].split('.')[0]), sys.argv[0])
CONF = '{0}/conf/filebeat.yml'.format(root)


def start_a_monitor(**kwargs):
    kwargs['queue'].put(dict(queue=kwargs['queue'], topic=kwargs['topics'], host=kwargs['bootstrap_server']))

    _wm = pyinotify.WatchManager()
    if kwargs.__contains__('multiline') and kwargs.__contains__('tags'):
        _handler = TagAndMultilineEventHandler(queue=kwargs['queue'],
                                              fields=kwargs['fields'],
                                              tag_head_pattern=kwargs['tags']['forward'],
                                              multi_head_pattern=kwargs['multiline']['patterns'])
    elif kwargs.__contains__('multiline'):
        _handler = MultilineEventHandler(queue=kwargs['queue'],
                                         fields=kwargs['fields'],
                                         multi_head_pattern=kwargs['multiline']['patterns'])
    elif kwargs.__contains__('tags'):
        _handler = TagsEventHandler(queue=kwargs['queue'],
                                    fields=kwargs['fields'],
                                    tag_head_pattern=kwargs['tags'['forward']])
    else:
        _handler = SimpleEventHandler(queue=kwargs['queue'],fields=kwargs['fields'])

    _notifier = pyinotify.ThreadedNotifier(_wm, _handler)
    _notifier.daemon = True
    _wm.add_watch(path=kwargs['paths'], mask=pyinotify.IN_MODIFY, rec=True, do_glob=True, quiet=True)
    return _notifier


def processing_productors(datas, queues):
    jobs = [start_a_monitor(queue=queue, **data['prospectors'], **data['output']) for data, queue in zip(datas, queues)]

    for job in jobs:
        logger.info('Get monitor thread info {}'.format(print(job)))
        job.start()

    while True:
        try:
            time.sleep(10)
        except:
            for job in jobs:
                job.stop()
                raise
 

def start_a_kafka(**kwargs):
    KafkaProductor(host=kwargs['host'], topic=kwargs['topic'], queue=kwargs['queue']).start


def processing_consumers(_queues):

    while _queues.empty() is not True:
        data = _queues.get()
        group.add(gevent.spawn(start_a_kafka, queue=data['queue'], host=data['host'], topic=data['topic']))
    group.join()


@daemon_init
def main():
    yaml = BeatsConfParse(CONF).run
    queues = [{'queue': Queue(), 'kafka': x['output']['bootstrap_server'], 'topic': x['output']['topic']} for x in yaml]

    productors = Process(name="productors", target=processing_productors, args=(yaml, queues))
    consumers = Process(name="comsumers", target=processing_consumers, args=(queues))

    productors.start()
    consumers.start()
    productors.join()
    consumers.join()


if __name__ == '__main__':
    main()
