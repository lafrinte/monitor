#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
from yaml import load
from .error import YmlParseErr


class BeatsConfParse(object):

    def __init__(self, path):
        self.path = path
        self.data = dict()
        self.pattern = re.compile(r'%{\[.*\]}')
        self._init()

    def _init(self):

        try:
            self.data = load(open('{}'.format(self.path), 'r'))
        except FileNotFoundError:
            raise FileNotFoundError
        except:
            raise YmlParseErr(self.path)

    def replace(self, input, output):
        for key, value in output.items():
            if not self.pattern.search(str(value)):
                continue
            para = re.sub(r'[\%\{\}\[\]]', '', value).split('.')[-1]
            output[key] = input['fields'][para]
        return output

    def data_processing(self):
        return [{'prospectors': x, 'output': self.replace(x, self.data['output'][0])} for x in self.data['prospectors']]

    @property
    def run(self):
        return self.data_processing()
