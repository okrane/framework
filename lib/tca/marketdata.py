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
import lib.data.dataframe_tools as dftools
from lib.data.ui.Explorer import Explorer

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
    def compute_agg_period_tick(self, filterd = {'mode' : 'during'}, slicerd = None , output_mode = 'dict'):
        
        #-----------
        # test inputs
        if not isinstance(slicerd, dict) or not isinstance(filterd, dict):
            raise ValueError('bad inputs')
            
        #-----------
        # filter tick kdata    
        filter_id = self.find_index_tick(**filterd)
        
        #-----------
        # apply for each slicer
        analytics ={}
        
        if any(filter_id):
            for name in slicerd.keys():
                try:
                    #-- check if dict_data has to be added
                    if 'dict_data' in slicerd[name]:
                        
                        if not isinstance(slicerd[name]['dict_data'],dict):
                            raise ValueError('dict_data for slicer has to be a dict, slicer name <'++'>')
                            
                        analytics.update({name : slicerd[name]['slicer'](self.data_tick[filter_id] , slicerd[name]['dict_data'])})
                        
                    else:
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
    
     
    def find_index_tick(self, 
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
            
            out = out & (dftools.select_sorted_index_datetime(self.data_tick , start_date = start_date , end_date = end_date , start_strict = False , end_strict = True))
                      
        elif mode == 'before':
            
            if start_date is not None:
                out = out & (dftools.select_sorted_index_datetime(self.data_tick , end_date = start_date , end_strict = True))
                
            else:
                raise ValueError('start_date should be indicated')
            
        elif mode == 'during_after':
            
            if start_date is not None:
                out = out & (dftools.select_sorted_index_datetime(self.data_tick , start_date = start_date , start_strict = False ))
                
            else:
                raise ValueError('end_date should be indicated')
            
        elif mode == 'after':
            
            if end_date is not None:
                out = out & (dftools.select_sorted_index_datetime(self.data_tick , start_date = end_date , start_strict = False))
                
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
            
            exchange_id_main = -1000000
            # find exchange_id main
            if ( self.data_tick_referential.shape[0] > 0 and np.any(self.data_tick_referential['EXCHANGETYPE'] == 'M') ):
                exchange_id_main = self.data_tick_referential[self.data_tick_referential['EXCHANGETYPE'] == 'M']['exchange_id'].values[0]
            else:
                logging.info('no main exchange_id for security id <' + str(self.security_id) + '>')
            
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

    ###########################################################################
    # METHOD COMPUTE STATS ON DAILY
    ###########################################################################
    def compute_formula_daily(self, filterd = {}, cols = None , formulad = None , output_mode = 'dict'):
        
        #-----------
        # test inputs
        if (not isinstance(filterd, dict)) and (cols is None and formulad is None):
            raise ValueError('bad inputs')
        
        #-----------
        # filter tick kdata    
        filter_id = self.find_index_daily(**filterd)
        
        #-----------
        # apply for each slicer
        analytics = {}
        
        if any(filter_id):
            
            #------ cols
            if cols is not None:
                
                if not isinstance(cols,list):
                    raise ValueError('cols has to be a list')
                    
                if any([x not in self.data_daily.columns.tolist() for x in cols]):
                    raise ValueError('one of the cols is not in data_daily')
                    
                for name in cols:
                    #-- attention ne fonctionne uniquement pour une valeur !
                    if len(self.data_daily[filter_id][name].values) != 1:
                        raise ValueError('only works on')
                    analytics.update({name : self.data_daily[filter_id][name].values[0]})
                    
            #------ formulad
            if formulad is not None:
                
                if not isinstance(formulad,dict):
                    raise ValueError('formulad has to be a dict') 
                           
                for name in formulad.keys():
                    try:
                        analytics.update({name : formulad[name](self.data_daily[filter_id])[0]})
                    except:
                        raise ValueError('Error on formula named :<' + name +'>')
        
        else:
            logging.info('no data for this filter')
            # chose to return a None dict
            for name in formulad.keys():
                analytics.update({name : None})
            
        #-----------
        # output     
        if output_mode == 'dict':
            return analytics
        else:
            return pd.DataFrame([analytics])
    
    def find_index_daily(self, 
                          #----- market information
                          market = 'all', # possible 'all' or 'main'
                          ):
        
        if self.data_daily.shape[0] == 0:
            raise ValueError('Daily data is empty')
        
        out = (self.data_daily['open_prc'] > 0)
        
        #---------------------
        #- BY MODE : for timeframe
        
        if market == 'main':
            out = out & (self.data_daily['EXCHANGETYPE'] == 'M')
            
        elif market == 'all':
            out = out & (np.isnan(self.data_daily['trading_destination_id']))
            
        else:
            raise ValueError('Unknown market <' + market +'>')            
            
        return out
        
if __name__=='__main__':

    from lib.data.ui.Explorer import Explorer
    import lib.stats.slicer as slicer
    #tz=pytz.timezone('Europe/Paris') 
    tz = pytz.UTC
    
    #-----  test occurrence
    test = MarketDataProcessor(date = dt.datetime(2014,3,5,0,0,0), security_id = 363664)
    to = time.clock()
    test.get_data_tick()
    print time.clock() - to
    test.get_data_daily()    
    
    #Explorer(test.data_tick[map(lambda x : x >= tz.localize(dt.datetime(2014,3,5,15,42,25)) and x < tz.localize(dt.datetime(2014,3,5,15,57,50)),[x.to_datetime() for x in test.data_tick.index])])
    #test.data_tick[map(lambda x : x >= tz.localize(dt.datetime(2014,3,5,15,42,25)) and x < tz.localize(dt.datetime(2014,3,5,15,57,50)),[x.to_datetime() for x in test.data_tick.index])].plot()
    
    vals = test.compute_agg_period_tick(filterd = {'mode' : 'during',
                                                   'start_date' : tz.localize(dt.datetime(2014,3,5,15,42,25)),
                                                    'end_date' : tz.localize(dt.datetime(2014,3,5,15,57,50)),
                                                    'exclude_auction' : [1,1,1,1],
                                                    'market' : 'main',
                                                    'market_visibility' : 'lit',
                                                    'order_limit' : None,
                                                    'order_side' : None},
                                        slicerd = {'volume' : {'default' : 0 , 'slicer' : lambda df : np.sum(df.volume.values)}} , 
                                        output_mode = 'dict')
    
    print vals
    #-----  ENTRY TICK  + test compute_agg_period_tick
    test = MarketDataProcessor(date = dt.datetime(2014,1,16,0,0,0), security_id = 10481)
    test.get_data_tick()
    test.get_data_daily()
    
    # Explorer(test.data_tick)
    print test.compute_agg_period_tick(slicerd = {'volume' : {'default' : 0 , 'slicer' : lambda df : np.sum(df.volume.values)},
                                                  'last' : {'default' : 0 , 'slicer' : lambda df : slicer.last_finite(df.price.values)},
                                                  'nb_trades' : {'default' : 0 , 'slicer' : lambda df : len(df.volume.values)},
                                                  'plr' : {'default' : 0 , 'dict_data' : {'exec_qty' : 100}, 'slicer' : lambda df,dicdata : dicdata['exec_qty'] / np.sum(df.volume.values)}})
    print test.compute_agg_period_tick(filterd = {'mode' : 'during' , 'market' : 'main' }, 
                                       slicerd = {'volume' : {'default' : 0 , 'slicer' : lambda df : np.sum(df.volume.values)},
                                                  'last' : {'default' : 0 , 'slicer' : lambda df : slicer.last_finite(df.price.values)},
                                                  'nb_trades' : {'default' : 0 , 'slicer' : lambda df : len(df.volume.values)}})
    
    print test.compute_formula_daily(filterd = {'market' : 'main'}, 
                                       formulad = {'m_spread' : lambda df : df.average_spread_numer.values/df.average_spread_denom.values},
                                       cols = ['open_prc'])    
#     #-----  ENTRY DAILY 
#     test = MarketDataProcessor(start_date = dt.datetime(2013,8,30,0,0,0),end_date = dt.datetime(2013,9,10,0,0,0), security_id = [2,110])
#     test.get_data_daily(out_colnames = ['open_prc'])  
#     print test.data_daily
        