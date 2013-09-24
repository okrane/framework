# -*- coding: utf-8 -*-
"""
Created on Mon Sep 09 09:51:34 2013

@author: njoseph
"""

from pymongo import *
import pandas as pd
import datetime as dt
import time as time
import numpy as np
# import lib.data.matlabutils as matlabutils
import logging
from lib.logger.custom_logger import *
from lib.dbtools.connections import Connections
import lib.dbtools.read_dataset as read_dataset


class MarketDataProcessor(object):
    
    ###########################################################################
    # INIT
    ###########################################################################
    def __init__(self,date = None, security_id = None):
        
        # MANDATORY INPUT
        if date is None or security_id is None:
            logging.error('bad mandatory colnames')
            raise ValueError('bad mandatory colnames')
            
        # INPUT
        """date"""
        self.date=date
        self.date_str=dt.datetime.strftime(date,'%d/%m/%Y')
        self.security_id=security_id
        
        # DATA
        self.data_tick=None
        self.data_daily=None
        
        # CONNECTION INFO
        
        
    ###########################################################################
    # METHOD GET DATA
    ###########################################################################
    def get_data_tick(self):
        
        if self.data_tick is not None:
            logging.info('get_data_tick is already loaded')
            return
            
        self.data_tick=read_dataset.ftickdb(security_id=self.security_id,date=self.date_str)
        
    def get_data_daily(self):
        
        if self.data_daily is not None:
            logging.info('get_data_daily is already loaded')
            return
        
        self.data_daily=read_dataset.trading_daily(start_date=self.date_str,end_date=self.date_str,security_id=self.security_id,include_agg=True)        
        
if __name__=='__main__':
    
    from lib.data.ui.Explorer import Explorer
    #-----  ENTRY OCCURENCE 
    test = MarketDataProcessor(date = dt.datetime(2013,8,30,0,0,0), security_id = 110)
    test.get_data_tick()
    test.get_data_daily()
    
    
        