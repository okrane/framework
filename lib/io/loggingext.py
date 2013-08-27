# -*- coding: utf-8 -*-
"""
Created on Wed Aug 14 12:17:28 2013

@author: njoseph
"""
import logging
import traceback

# Create and set a logger with needed params

def set_logging(name='',
                fmt=(" -----------------------------------------------"
                "\n %(asctime)s  %(levelname)-3s [%(process)s %(threadName)s %(filename)s@%(lineno)d in %(funcName)s] %(message)s"),
                datefmt="%Y-%m-%d %H:%M:%S",
                filename=None,
                lev=logging.INFO):
    # reset
    logger = logging.getLogger(name)
    logger.handlers=[]
    logger.setLevel(logging.INFO)
    # Format for our loglines
    formatter = logging.Formatter(fmt=fmt,datefmt=datefmt)
    # Setup console logging
    ch = logging.StreamHandler()
    ch.setLevel(lev)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    # Setup file logging as well
    if filename is not None:
        fh = logging.FileHandler(filename)
        fh.setLevel(lev)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

def errorwt(strin,name=''):
    logging.error(strin+'s\n Stack: %s' % (''.join(traceback.format_stack())), exc_info=True)
