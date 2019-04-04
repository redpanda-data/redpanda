#!/usr/bin/env python3
import sys
import os
import logging
import logging.handlers

sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')


def set_logger_for_main(level=logging.INFO):
    fmt_string = '%(levelname)s:%(asctime)s %(filename)s:%(lineno)d] %(message)s'
    logging.basicConfig(format=fmt_string)
    formatter = logging.Formatter(fmt_string)
    for h in logging.getLogger().handlers:
        h.setFormatter(formatter)
    logger.setLevel(level)


def is_debug():
    return logger.level == logging.DEBUG
