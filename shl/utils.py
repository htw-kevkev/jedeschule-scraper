# -*- coding: utf-8 -*-
"""
Created on Sun Oct  4 11:05:19 2020

@author: kevin.kretzschmar
"""

import logging
import sys

loggers = {}

def set_logging(name="logger"):
    """
    To avoid repeating logging set-up
    :return: logger
    """
    global loggers

    if loggers.get(name):
        return loggers.get(name)
    else:
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        loggers[name] = logger
        return logger