from simep.core.baseagent import BaseAgent
from simep.sched import Order
from simep.funcs.data.pyData import pyData

import math
import scipy as sp
import numpy as np
import random

class MarketMaker(BaseAgent):
    '''
    classdocs
    '''
    @staticmethod
    def public_parameters():
        setup      = {'name'         : {'label' : 'Name'             , 'value' : 'MM001'}}
        parameters = {'d_b'         : {'label' : 'd buy'             , 'value' : 0},
                      'cycle_b'         : {'label' : 'cycle_buy'             , 'value' : 15},
                      'd_s'         : {'label' : 'd sell'             , 'value' : 0},
                      'cycle_s'         : {'label' : 'cycle sell'             , 'value' : 15}}
        return {'setup': setup, 'parameters': parameters}

    def __init__(self, setup, context, parameters, trace):
        BaseAgent.__init__(self, setup, context, parameters, trace)
        self.needExecReportEvt          = True
        self.order_list_b = []
        self.order_list_s = []        
        self.trades = 0
        self.wake_up_b = 0
        self.wake_up_s = 0
        
    def process(self, evt):
        self.date = evt.getEvtTime()
        self.trades += len(evt.getTrades())        
        ats = 1
        
        for tr in evt.getTrades():
            self.appendIndicator(pyData('init', 
                                        date = [self.date], 
                                        value = {'price': [tr.price],        
                                                 'qty': [tr.size]}))
        
        self._orderbook = self.market.findOrderBook(self['ric'])
        if self.wake_up_b <= self.trades:            
            if self.order_list_b:
                for order in self.order_list_b:
                    self._orderbook.processCancelOrder(order.orderId)
                    self.order_list_b.remove(order)
            
            self.lob = evt.getLob()
            if not self.lob:
                return
                      
            
            best = self.lob.getBid(0).price
            price = best - self['d_b'] * 0.005;
                           
            
            order =  self.create_order('B', Order.Buy, price, ats, Order.Limit, Order.DAY)
            order.needExecReport = True              
            self.order_list_b.append(order)  
            self._orderbook.processCreateOrder(order)            
            self.wake_up_b = self.trades + self['cycle_b']
            
            
        if self.wake_up_s <= self.trades:            
            if self.order_list_s:
                for order in self.order_list_s:
                    self._orderbook.processCancelOrder(order.orderId)
                    self.order_list_s.remove(order)
            
            self.lob = evt.getLob()
            if not self.lob:
                return


            best = self.lob.getAsk(0).price
            price = best + self['d_s'] * 0.005;                   
            
            order =  self.create_order('S', Order.Sell, price, ats, Order.Limit, Order.DAY)
            order.needExecReport = True              
            self.order_list_s.append(order)  
            self._orderbook.processCreateOrder(order)            
            self.wake_up_s = self.trades + self['cycle_s']
        
    def processReport(self, evt):
        prefix = evt.orderId[0]
        size = evt.size if prefix == 'B' else -evt.size 
        self.appendIndicator(pyData('init', 
                                    date = [self.date], 
                                    value = {'size': [size],        
                                             'price': [evt.price]}))
        