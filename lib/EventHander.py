#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import time
import pyinotify


class BaseEventHandler(pyinotify.ProcessEvent):

    def __init__(self, *args, **kwargs):
        super(BaseEventHandler, self).__init__(*args, **kwargs)
        self.sindb = os.path.join(os.getenv('HOME'), '.sindb')
        self.locale = os.getenv('LANG').lower().split('.')[-1]

        if os.path.isfile(self.sindb) is not True:
            f = open(self.sindb, 'w')
            f.write('')

    def decode(self, line):
        if isinstance(line, bytes) is True:
            msg = line.decode(self.locale)
        return msg

    def get_offset(self):
        lines = open(self.sindb).readlines()

        if lines.__len__() == 0:
            return 0

        match = [line.split(',')[-1] for line in lines if self.key_pattern.match(line)]
        if match.__len__() > 1 or match.__len__() == 0:
            return 0

        return int(match[0])

    def save_offset(self, offset):
        offset_info = '{0},{1}\n'.format(self.key_word, offset)
        lines = open(self.sindb).readlines()

        if lines.__len__() == 0:
            open(self.sindb, 'w+').write(offset_info)
        else:
            for line in enumerate(lines):
                if self.key_pattern.match(line[1]):
                    lines.pop(line[0])
            lines.append(offset_info)
            open(self.sindb, 'w+').writelines(lines)

    def get_mark(self):
        return time().__str__().split('.')[1]

    def process_IN_MODIFY(self, event):
        self.path = event.path
        self.key_word = '{0},{1}'.format(os.stat(event.path)[1], os.stat(event.path)[2])
        self.key_pattern = re.compile('^{0}'.format(self.key_word))
        self.offset = self.get_offset()


class SimpleEventHandler(BaseEventHandler):

    """
    :Describe: 1. For parsing without multiline and tags.
               2. Do not focus the order of the lines
    """

    def __init__(self, *args, **kwargs):
        super(SimpleEventHandler, self).__init__(*args, **kwargs)
        self.queue = kwargs.get('queue')

    def process_IN_MODIFY(self, event):
        BaseEventHandler.process_IN_MODIFY(self, event)
        self.cut_lines()

    def cut_lines(self):
        with open(self.path, 'rb') as f:
            f_size = os.stat(self.path)[6]

            while self.offset < f_size:
                f.seek(self.offset)
                line = f.readline()
                self.offset += len(line)
                self.queue.put_nowait(BaseEventHandler.decode(self, line))
            self.save_offset(self.offset)


class MultilineEventHandler(BaseEventHandler):

    """
    Describe: 1. For parsing multiline logs without tags.
    Attention: self.queue is a dict, the key is a string linked by inode and device no. with delimiter ','
               the value is a multiprocess.Queue()
    """

    def __init__(self, *args, **kwargs):
        super(MultilineEventHandler, self).__init__(*args, **kwargs)
        self.queue = kwargs.get('queue')
        self.msg_head = kwargs.get('multi_head_pattern')
        self.msg = dict()
        self.is_prompt = dict()

    def special_list(self, key_word):
        if self.msg.__contains__(key_word) is not True:
            self.msg[key_word] = list()
        if self.is_prompt.__contains__(key_word) is not True:
            self.is_prompt[key_word] = list()

    def process_IN_MODIFY(self, event):
        BaseEventHandler.process_IN_MODIFY(self, event)
        self.special_list(self.key_word)
        self.cut_lines()

    def multline_parse(self, new_line, key_word):

        new_line = BaseEventHandler.decode(self, new_line)
        if self.msg_head.search(new_line) and self.is_prompt[key_word] is False:
            self.msg[key_word].append(new_line)
            self.is_prompt[key_word] = True
        elif self.msg_head.search(new_line) and self.is_prompt[key_word] is True:
            msg, self.msg[key_word] = ''.join(self.msg[key_word]), [new_line]
            return msg
        else:
            self.msg[key_word].append(new_line)

    def cut_lines(self):
        with open(self.path, 'rb') as f:
            f_size = os.stat(self.path)[6]

            while self.offset < f_size:
                f.seek(self.offset)
                line = f.readline()
                self.offset += len(line)
                msg = self.multline_parse(line, self.key_word)

                if msg:
                    self.queue[self.key_word].put_nowait(msg)
            self.save_offset(self.offset)


class TagsEventHandler(BaseEventHandler):

    def __init__(self, *args, **kwargs):
        super(TagsEventHandler, self).__init__(*args, **kwargs)
        self.queue = kwargs.get('queue')
        self.tag_head = kwargs.get('multi_head_pattern')
        self.msg = dict()
        self.is_prompt = dict()

wm = pyinotify.WatchManager()
mask = pyinotify.IN_MODIFY
handler = EventHandler(queue=Queue())
notifier = pyinotify.Notifier(wm, handler)
wm.add_watch(path='/tmp/*.logs', mask=mask, rec=True, do_glob=True, quiet=True)
notifier.loop()






