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
import lib.dbtools.get_repository as get_repository

class MarketDataProcessor(object):
    
    ###########################################################################
    # INIT
    ###########################################################################
    def __init__(self,date = None, start_date = None , end_date = None , security_id = None):
        
        # MANDATORY INPUT
        if (date is None and (start_date is None or end_date is None)) or security_id is None:
            logging.error('bad mandatory colnames')
            raise ValueError('bad mandatory colnames')
            
        # INPUT
        """date"""
        if date is not None:
            self.start_date = dt.datetime.combine(date.date(),dt.datetime.strptime('00:00:00','%H:%M:%S').time())
            self.end_date = dt.datetime.combine(date.date(),dt.datetime.strptime('23:59:59','%H:%M:%S').time())
        else:
            self.start_date = start_date
            self.end_date = end_date
            
        self.security_id = security_id
        
        # DATA
        self.data_tick = None
        self.data_daily = None
        
        # referentiel liee au data
        self.data_tick_referential = None
        
        # CONNECTION INFO
        
    ###########################################################################
    # METHOD GET DATA
    ###########################################################################
    def get_data_tick(self):
        
        #--------------------------------
        #-- TESTS
        #--------------------------------
        if self.data_tick is not None:
            logging.info('get_data_tick is already loaded')
            return
        
        if (self.end_date-self.start_date).days >= 1:
            raise ValueError('works only on one date')
        
        #--------------------------------
        #-- get data tick
        #--------------------------------
        self.data_tick=read_dataset.ftickdb(security_id=self.security_id,date=dt.datetime.strftime(self.start_date,'%d/%m/%Y'))
        
        #--------------------------------
        #-- tick referential
        #--------------------------------
        self.data_tick_referential = pd.DataFrame()
        if self.data_tick.shape[0]:
            self.data_tick_referential = get_repository.exchangeinfo(exchange_id = np.unique(self.data_tick['exchange_id'].values).tolist())
        
        
    def get_data_daily(self, out_colnames = None):
        
        if self.data_daily is not None:
            logging.info('get_data_daily is already loaded')
            return
        
        self.data_daily=read_dataset.trading_daily(start_date=dt.datetime.strftime(self.start_date,'%d/%m/%Y'),
                                                   end_date=dt.datetime.strftime(self.end_date,'%d/%m/%Y'),
                                                   security_id=self.security_id,
                                                   include_agg=True, out_colnames = out_colnames)
        
    ###########################################################################
    # METHOD COMPUTE STATS ON TICKS
    ###########################################################################
    #def __index_data_tick(self, start_date = None, end_date = None, exclude_auction = [0,0,0,0]):
        

        
if __name__=='__main__':
    
    from lib.data.ui.Explorer import Explorer
    #-----  ENTRY TICK 
    test = MarketDataProcessor(date = dt.datetime(2013,8,30,0,0,0), security_id = 110)
    test.get_data_tick()
    test.get_data_daily()
    print test.data_tick_referential
    
#     #-----  ENTRY DAILY 
#     test = MarketDataProcessor(start_date = dt.datetime(2013,8,30,0,0,0),end_date = dt.datetime(2013,9,10,0,0,0), security_id = [2,110])
#     test.get_data_daily(out_colnames = ['open_prc'])  
#     print test.data_daily
        