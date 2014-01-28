# -*- coding: utf-8 -*-
"""
Created on Mon Sep 09 09:51:34 2013

@author: njoseph
"""

from pymongo import *
import pandas as pd
import datetime as dt
import pytz
import time as time
import numpy as np
# import lib.data.matlabutils as matlabutils
import logging
from lib.logger.custom_logger import *
from lib.dbtools.connections import Connections
import lib.dbtools.read_dataset as read_dataset
import lib.dbtools.get_repository as get_repository
from lib.io.toolkit import get_traceback

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
                                                   include_agg = True, out_colnames = out_colnames)
        
    ###########################################################################
    # METHOD COMPUTE STATS ON TICKS
    ###########################################################################
    def test(self):
        d = {'mode' : 'during', 'start_date' : pytz.UTC.localize(self.start_date), 'end_date' : pytz.UTC.localize(self.end_date), 'market_visibility' : 'lit' ,'exclude_auction' : [0,0,0,1]}
        return self.__find_index_tick(**d)
    
    def compute_agg_period_tick(self, filterd = {'mode' : 'during'}, slicerd = None , output_mode = 'dict'):
        
        #-----------
        # test inputs
        if not isinstance(slicerd, dict) or not isinstance(filterd, dict):
            raise ValueError('bad inputs')
            
        #-----------
        # filter tick kdata    
        filter_id = self.__find_index_tick(**filterd)
        
        #-----------
        # apply for each slicer
        analytics ={}
        
        if any(filter_id):
            for name in slicerd.keys():
                try:
                    analytics.update({name : slicerd[name]['slicer'](self.data_tick[filter_id])})
                except:
                    get_traceback()
                    raise ValueError('error computing slicer name <' + name + '>')
        else:
            for name in slicerd.keys():
                try:
                    analytics.update({name : slicerd[name]['default']})
                except:
                    get_traceback()
                    raise ValueError('error with default value of slicer name <' + name + '>')           
            
        #-----------
        # output     
        if output_mode == 'dict':
            return analytics
        else:
            return pd.DataFrame([analytics])
    
     
    def __find_index_tick(self, 
                          #-----  period_mode : 'during'
                          mode = None, 
                          #----- timeframe and time phase
                          start_date = None, end_date = None,
                          #----- time phase
                          exclude_auction = [0,0,0,0],
                          #----- market information
                          market = 'all', # possible 'all' or 'main'
                          market_visibility = 'all', # possible 'all' , 'lit' , 'dark'
                          #----- constraint
                          order_limit = None, #None or value
                          order_side = None #N one or -1/1
                          ):
        
        if self.data_tick.shape[0] == 0:
            raise ValueError('Tick data is empty')
        
        out = (self.data_tick['price'] > 0)
        
        #---------------------
        #- BY MODE : for timeframe
        
        if mode == 'during':
            
            #----- timeframe 
            if start_date is not None and end_date is not None:
                out = out & (map(lambda x : x >= start_date and x <= end_date,[x.to_datetime() for x in self.data_tick.index]))
                
            elif start_date is not None:
                out = out & (map(lambda x : x >= start_date , [x.to_datetime() for x in self.data_tick.index]))
                
            elif end_date is not None:
                out = out & (map(lambda x : x <= end_date , [x.to_datetime() for x in self.data_tick.index]))                
        
        elif mode == 'before':
            
            if start_date is not None:
                out = out & (map(lambda x : x < start_date,[x.to_datetime() for x in self.data_tick.index]))
                
            else:
                raise ValueError('start_date should be indicated')
            
        elif mode == 'before':
            
            if end_date is not None:
                out = out & (map(lambda x : x > start_date,[x.to_datetime() for x in self.data_tick.index]))
                
            else:
                raise ValueError('end_date should be indicated')
            
        else:
            logging.info('Unknown mode <' + mode +'>')
            return
        
        
        #----- timeframe and time phase
            
        if any(np.array(exclude_auction)==1):
            if exclude_auction[0]==1:
                out = out & (self.data_tick['opening_auction'] == 0 )
            if exclude_auction[1]==1:
                out = out & (self.data_tick['intraday_auction'] == 0 )
            if exclude_auction[2]==1:
                out = out & (self.data_tick['closing_auction'] == 0 )
            if exclude_auction[3]==1:
                out = out & ~( (self.data_tick['auction'] == 1) & 
                               (self.data_tick['opening_auction'] == 0) & 
                               (self.data_tick['intraday_auction'] == 0 ) & 
                               (self.data_tick['closing_auction'] == 0) )
                
        #----- market information
        if market == 'main':
            # find exchange_id main
            if ( self.data_tick_referential.shape[0] > 0 and np.any(self.data_tick_referential['EXCHANGETYPE'] == 'M') ):
                exchange_id_main = self.data_tick_referential[self.data_tick_referential['EXCHANGETYPE'] == 'M']['exchange_id'].values[0]
                
            else:
                get_traceback()
                logging.error('main exchange_id has to be known')
                raise ValueError('main exchange_id has to be known')
            
            out = out & (self.data_tick['exchange_id'] == exchange_id_main)
            
        elif not (market == 'all'):
            raise ValueError('Unknown market <' + market +'>')
            
        #----- market information
        if market_visibility == 'lit':
            out = out & (self.data_tick['dark'] == 0)
            
        elif market_visibility == 'dark':
            out = out & (self.data_tick['dark'] == 1)
            
        elif not (market_visibility == 'all'):
            raise ValueError('Unknown market_visibility <' + market +'>')
        
        #----- constraint
        if order_limit is not None and order_side is not None:
            if order_limit > 0:
                out = out & ( self.data_tick['price']*order_side <= order_limit*order_side )
                
        return out        




if __name__=='__main__':
    
    from lib.data.ui.Explorer import Explorer
    import lib.stats.slicer as slicer
    #-----  ENTRY TICK 
    test = MarketDataProcessor(date = dt.datetime(2014,1,8,0,0,0), security_id = 110)
    test.get_data_tick()
    test.get_data_daily()
    print test.test()
    print test.compute_agg_period_tick(slicerd = {'volume' : lambda df : np.sum(df.volume.values),
                                                  'last' : lambda df : slicer.last_finite(df.price.values),
                                                  'nb_trades' : lambda df : len(df.volume.values)})
    print test.compute_agg_period_tick(filterd = {'mode' : 'during' , 'market' : 'main' }, 
                                       slicerd = {'volume' : lambda df : np.sum(df.volume.values),
                                                  'last' : lambda df : slicer.last_finite(df.price.values),
                                                  'nb_trades' : lambda df : len(df.volume.values)})
    
#     #-----  ENTRY DAILY 
#     test = MarketDataProcessor(start_date = dt.datetime(2013,8,30,0,0,0),end_date = dt.datetime(2013,9,10,0,0,0), security_id = [2,110])
#     test.get_data_daily(out_colnames = ['open_prc'])  
#     print test.data_daily
        