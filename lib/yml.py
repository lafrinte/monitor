#!/usr/bin/env python
# -*- coding: utf-8 -*-

from yaml import load
from .error import YmlParseErr


class BeatsConfParse(object):

    def __init__(self, path):
        self.path = path
        self.data = dict()
        self._init()

    def _init(self):

        try:
            self.data = load(open('{}'.format(self.path), 'r'))
        except FileNotFoundError:
            raise FileNotFoundError
        except:
            raise YmlParseErr(self.path)

    def data_processing(self):
        return [{'prospectors': x, 'output': self.data['output'][0]} for x in self.data['filebeat.prospectors']]

    @property
    def run(self):
        return self.data_processing()
