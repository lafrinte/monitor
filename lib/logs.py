#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging


class Logger(object):

    def __init__(self, _path, _logger):
        self.logger = logging.getLogger(_logger)
        self.logger.setLevel(logging.INFO)

        fh = logging.FileHandler(_path)
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('[%(astime)s] [%(name)s] [%(levelname)s]: %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

    def info(self,msg):
        self.logger.info(msg)

    def error(self, msg):
        self.logger.error(msg)
