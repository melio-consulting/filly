# -*- coding: utf-8 -*-
import logging

def setup_custom_logger(name):
    formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

    logger = logging.getLogger(name)

    if (logger.hasHandlers()):
        logger.handlers.clear()

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    logger.propagate = False

    return logger
