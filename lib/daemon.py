#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import functools


def daemon_init(func):

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            pid = os.fork()
            if pid > 0:
                os._exit(0)
        except OSError:
            os._exit(1)

        os.setsid()
        os.chdir("/")
        os.umask(0)

        try:
            pid = os.fork()
            if pid > 0:
                os._exit(0)
        except OSError:
            os._exit(1)
        func(*args, **kwargs)
    return wrapper