'''
Created on 27 avr. 2011

@author: elber
'''

from simep import __tvfo_mode__
from simep.core.baseagent import BaseAgent
from simep.sched import Order
from simep.tools import date2num
from simep.funcs.data.pyData import pyData
from simep.funcs.bus import *
from simep.funcs.stdio.utils import pyLog
import math
import numpy as num
from simep.agents.volatilite_zi import *




class MarketOrderFlowEstimator(BaseAgent):
    
    
    
   
    @staticmethod
    def public_parameters():
        parameters = {'time_step'   : {'label' : 'Time Step'  , 'value' : '00:10:00:000000' },
                      'result_filename'  : {'label' : 'Result Filename' , 'value' : 'C:/matlab_results/market_orders.m'}}
        return {'setup': {}, 'parameters': parameters}
    
    @staticmethod
    def indicators_list():
        return []
    
    
    
   
    
    def __init__(self, setup, context, params, trace):
        super(MarketOrderFlowEstimator, self).__init__(setup, context, params, trace)
        #self['time_step']           = date2num(self['time_step'])
        params['time_step'] = date2num(params['time_step'])
        self.venue_id       = self.ba['venue_ids'][0]
        # get feed
        self.feed           = self.ba['feed'][self.venue_id][0]
        self.start_time     = self.feed['start_time']          
        self.end_time       = self.feed['end_time']  
        self.time           = self.end_time - self.start_time
        self.n              = self.time/self['time_step']
        self.list_nbr_exec  = [0 for i in range(self.n)]
        self.list_size_exec = [0 for i in range(self.n)]
    def process(self, event):
        # check code
        code = self.update(event)
        if code <= 0:
            return False
        
       
        
        if self.feed['TRADE_EVENT']:
            t           = event.getTimeStamp()
            venue_id = event.getVenueId()
            delta_time   = t - self.start_time
            idx         = int((delta_time/ self['time_step']))
            trades = self.marketManager.getTrades(venue_id)
            self.list_nbr_exec[idx] +=  len(trades)
            for trade in trades :
                self.list_size_exec[idx]+=trade.size  
    def last_process(self) :
        dic_data = {}
        for i in range(self.n):
            dic_data['map_acnt%02d' %i] = self.list_nbr_exec[i]
        self.append_to_m_file(self['result_filename'], dic_data)
            
            
        #print float(sum(self.list_nbr_exec))
        