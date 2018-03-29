#!/usr/bin/env python
# -*- coding: utf-8 -*-

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


class NecessaryParaNotExist(Error):

    def __init__(self, para, msg=None):
        Error.__init__(self, msg)
        self.msg = msg
        self.para = para
        if msg is None:
            self.msg = "Necessary parameter %s is not exist or null" % repr(self.para)


class YmlParseErr(Error):

    def __init__(self, path, msg=None):
        Error.__init__(self, msg)
        self.msg = msg
        self.path = path
        if msg is None:
            self.msg = "%s is not a legal yaml configuration" % repr(self.path)


class PathNotExist(Error):

    def __init__(self, path, msg=None):
        Error.__init__(self, msg)
        self.msg = msg
        self.path = path
        if msg is None:
            self.msg = "%s is not exist" % repr(self.path)