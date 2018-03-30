#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import pyinotify
from gevent.pool import Group
from multiprocessing import Queue
from multiprocessing import Process

from .lib.logs import Logger
from .lib.yml import BeatsConfParse
from .lib.EventHander import SimpleEventHandler, MultilineEventHandler, TagsEventHandler, TagAndMultilineEventHandler

root = os.path.dirname(os.path.abspath(__file__))
logger = Logger('{0}/logs/{1}.logs'.format(root, sys.argv[0].split('.')[0]), sys.argv[0])

CONF = '{0}/conf/filebeat.yml'.format(root)


def start_a_monitor(**kwargs):
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

    _notifier = pyinotify.Notifier(_wm, _handler)
    _wm.add_watch(path=kwargs['paths'], mask=mask, rec=True, do_glob=True, quiet=True)
    _notifier.loop()


def processing_productors(_data, _queue):
    group = Group()
    group.map(start_a_monitor, )


def processing_consumers(_data, _queues):
    pass



def main():
    conf_data = BeatsConfParse(CONF).run
    queue = Queue()
    productors = Process(name="productors", target=processing_productors, args=(conf_data, queue))
    consumers = Process(name="comsumers", target=processing_consumers, args=(conf_data, queue))



if __name__ == '__main__':
    main()



wm = pyinotify.WatchManager()
mask = pyinotify.IN_MODIFY
handler = TagAndMultilineEventHandler(queue=Queue(), fields={'_soft': 'esb', '_product': 'dazhanggui', '@ip': '172.17.0.2'}, tag_head_pattern='ServiceBegin', multi_head_pattern='^\[')
notifier = pyinotify.Notifier(wm, handler)
wm.add_watch(path='/tmp/*.logs', mask=mask, rec=True, do_glob=True, quiet=True)
notifier.loop()