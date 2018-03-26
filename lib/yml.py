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

    @property
    def run(self):
        return self.data
