#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import json
import pyinotify
from time import time


class BaseEventHandler(pyinotify.ProcessEvent):
    def __init__(self, *args, **kwargs):
        super(BaseEventHandler, self).__init__(*args, **kwargs)
        self.sindb = os.path.join(os.getenv('HOME'), '.sindb')
        self.locale = os.getenv('LANG').lower().split('.')[-1]
        self.fields = kwargs.get('fields') or dict()
        self.key_word = None
        self.key_pattern = None
        self.offset = None
        self.path = None

        if os.path.isfile(self.sindb) is not True:
            f = open(self.sindb, 'w')
            f.write('')

    def decode(self, line):
        if isinstance(line, bytes) is True:
            line = line.decode(self.locale) d
        return line

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
        self.fields['path'] = self.path
        self.cut_lines()

    def cut_lines(self):
        with open(self.path, 'rb') as f:
            f_size = os.stat(self.path)[6]

            while self.offset < f_size:
                f.seek(self.offset)
                line = f.readline()
                self.offset += len(line)
                self.queue.put_nowait(json.dumps(
                    dict(fields=self.fields, messages=BaseEventHandler.decode(self, line).replace('\n', ''))))
            self.save_offset(self.offset)


class MultilineEventHandler(BaseEventHandler):
    """
    Describe: 1. For parsing multiline logs without tags.
    """

    def __init__(self, *args, **kwargs):
        super(MultilineEventHandler, self).__init__(*args, **kwargs)
        self.queue = kwargs.get('queue')
        self.msg_head = re.compile(kwargs.get('multi_head_pattern'))
        self.msg = dict()
        self.is_prompt = dict()

    def special_list(self, key_word):
        if self.msg.__contains__(key_word) is not True:
            self.msg[key_word] = list()
        if self.is_prompt.__contains__(key_word) is not True:
            self.is_prompt[key_word] = False

    def process_IN_MODIFY(self, event):
        BaseEventHandler.process_IN_MODIFY(self, event)
        self.fields['path'] = self.path
        self.special_list(self.key_word)
        self.cut_lines()

    def multline_parse(self, new_line, key_word):

        new_line = BaseEventHandler.decode(self, new_line)
        if self.msg_head.search(new_line) and self.is_prompt[key_word] is False:
            self.msg[key_word].append(new_line)
            self.is_prompt[key_word] = True
        elif self.msg_head.search(new_line) and self.is_prompt[key_word] is True:
            msg, self.msg[key_word] = ''.join(self.msg[key_word]).replace('\n', ' ').strip(), [new_line]
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
                    self.queue.put_nowait(json.dumps(dict(fields=self.fields, messages=msg)))
            self.save_offset(self.offset)


class TagsEventHandler(BaseEventHandler):
    def __init__(self, *args, **kwargs):
        super(TagsEventHandler, self).__init__(*args, **kwargs)
        self.queue = kwargs.get('queue')
        self.tag_head = re.compile(kwargs.get('tag_head_pattern'))
        self.service_tag = dict()

    def special_list(self, key_word):
        if self.service_tag.__contains__(key_word) is not True:
            self.service_tag[key_word] = BaseEventHandler.get_mark(self)

    def process_IN_MODIFY(self, event):
        BaseEventHandler.process_IN_MODIFY(self, event)
        self.fields['path'] = self.path
        self.special_list(self.key_word)
        self.cut_lines()

    def add_service_tags(self, new_line, key_word):

        new_line = BaseEventHandler.decode(self, new_line)
        if self.tag_head.search(new_line):
            self.service_tag[key_word] = BaseEventHandler.get_mark(self)
        self.fields['ssc'] = self.service_tag[key_word]
        return new_line.replace('\n', '')

    def cut_lines(self):
        with open(self.path, 'rb') as f:
            f_size = os.stat(self.path)[6]

            while self.offset < f_size:
                f.seek(self.offset)
                line = f.readline()
                self.offset += len(line)
                self.queue.put_nowait(json.dumps(
                    dict(fields=self.fields, messages=self.add_service_tags(line, self.key_word))))
            self.save_offset(self.offset)


class TagAndMultilineEventHandler(MultilineEventHandler, TagsEventHandler):
    def __init__(self, *args, **kwargs):
        super(TagAndMultilineEventHandler, self).__init__(*args, **kwargs)

    def special_list(self, key_word):
        TagsEventHandler.special_list(self)
        MultilineEventHandler.special_list(self)

    def process_IN_MODIFY(self, event):
        BaseEventHandler.process_IN_MODIFY(self, event)
        self.special_list(self.key_word)
        self.cut_lines()

    def cut_lines(self):
        with open(self.path, 'rb') as f:
            f_size = os.stat(self.path)[6]

            while self.offset < f_size:
                f.seek(self.offset)
                line = f.readline()
                self.offset += len(line)
                msg = self.multline_parse(line, self.key_word)

                if msg:
                    self.queue.put_nowait(json.dumps(
                        dict(fields=self.fields, messages=TagsEventHandler.add_service_tags(self, msg, self.key_word))))
            self.save_offset(self.offset)
