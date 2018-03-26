#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import pyinotify
from hashlib import sha1
from .error import NecessaryParaNotExist
from .yml import BeatsConfParse

RANDOM_STR = os.getenv('MONITOR_SECURITY_KEY') or 'monitor security key'
KEY = sha1(RANDOM_STR.encode('utf-8')).hexdigest()
SINDB = os.path.join(os.getenv('HOME'), '.sindb{}'.format(os.path.basename(KEY)))

class EventHandler(pyinotify.ProcessEvent):

    def __init__(self, *args, **kwargs):
        super(EventHandler, self).__init__(*args, **kwargs)
        self.paths = kwargs.get('path')
        self.exclude = None or kwargs.get()
        self.queue = kwargs.get('queue')
        self.f_info = {
            '_st_ino': os.stat(kwargs.get('filepath'))[1],
            '_st_dev': os.stat(kwargs.get('filepath'))[2]
        }
        self.offset = self.get_offset()
        self.is_multiline = None or bool(kwargs.get('mutiline'))
        if self.is_multiline is True:
            self.msg_head_pattern = re.compile(r'{}'.format(kwargs.get('head')))

        self.msg = list()
        self.is_prompt = False
        self.service_tag = ''
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
        if isinstance(new_line, bytes) is True:
            new_line = new_line.decode('utf-8')

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

    def __init__(self, _queue, **kwargs):
        self.wm = None
        self.mask = None
        self.notifier = None
        self.all = kwargs
        self._init_option()

    def _init_option(self):
        files = [x.startswith() for x in os.listdir(os.getenv('HOME'))]
        if not files:
            self.key = sha1(RANDOM_STR.encode('utf-8')).hexdigest()
            self.sindb = os.path.join(os.getenv('HOME'), '.sindb{}'.format(self.key))
        else:
            self.sindb = files[0]
        self.wm = pyinotify.WatchManager()
        self.mask = pyinotify.IN_MODIFY

    def _init_para(self):


    def start(self, filename):
        handler = EventHandler(filepath=filename, queue=self.queue)
        self.notifier = pyinotify.Notifier(self.wm, handler)
        self.wm.add_watch(filename, self.mask, rec=True)
        self.notifier.loop()
