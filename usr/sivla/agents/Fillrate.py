from simep.core.baseagent import BaseAgent
from simep.sched import Order
from simep.funcs.data.pyData import pyData

import math
import scipy as sp
import numpy as np
import random

class Fillrate(BaseAgent):
    '''
    classdocs
    '''
    @staticmethod
    def public_parameters():
        setup      = {'name'         : {'label' : 'Name'             , 'value' : 'Fill001'}}
        parameters = {'side'         : {'label' : 'Side'             , 'value' : Order.Buy},
                      'd'         : {'label' : 'Distance'             , 'value' : 0},
                      'cycle'         : {'label' : 'Cycle'             , 'value' : 15}}
        return {'setup': setup, 'parameters': parameters}

    def __init__(self, setup, context, parameters, trace):
        BaseAgent.__init__(self, setup, context, parameters, trace)
        self.needExecReportEvt          = True
        self.order_list = []        
        self.trades = 0
        self.wake_up = 0
        
    def process(self, evt):
        self.update()       
        feed = self.marketManager.getFeedInfo(evt.getVenueId())
        self.trades += feed['NUM_MOVES']       
        ats = 1
        
        
        for i in feed['LAST_DEALS_IDXS']:
            self.append_indicator( {'price': feed['DEALS_PRICES'][i], 
                                    'qty'  : feed['DEALS_QTYS'][i]},
                                    date = feed['TIME_NUM'] 
                                 )
        
        
        if self.wake_up <= self.trades:
            for order in self.moneyManager.getPrivateBook().keys():
                self.cancelOrder(order.orderId)
                      
            if self['side'] == Order.Buy:
                best = feed['BEST_BID1']
                price = best - self['d'] * 0.005;
            elif self['side'] == Order.Sell:
                best = feed['BEST_ASK1']
                price = best + self['d'] * 0.005;                   
            
            order =  self.create_order(evt.getVenueId(), 'Placement', self['side'], price, ats, Order.Limit, Order.DAY)         
            self.wake_up = self.trades + self['cycle']
        
    def processReport(self, evt):  
        feed = self.marketManager.getFeedInfo(evt.getVenueId())       
        self.append_indicator({'size': evt.size, 
                               'price': evt.price}, 
                               date = feed['TIME_NUM'])
                                                   
        