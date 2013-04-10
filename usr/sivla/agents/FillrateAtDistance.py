from simep.core.baseobserver import BaseAgent
from simep.sched import Order
from simep.funcs.data.pyData import pyData

import math
import numpy as np


class FillrateAtDistance(BaseAgent):
    @staticmethod
    def public_parameters():
        
        setup      = {'name'         : {'label' : 'Name'             , 'value' : 'FillrateAtDistance001'}}
        parameters = {'d'            : {'label' : 'Distance'         , 'value' : 1}, 
                      'cycle'        : {'label' : 'Cycle Time'       , 'value' : 15}, 
                      'side'         : {'label' : 'Side'             , 'value' : Order.Buy}, 
                      }
        return {'setup': setup, 'parameters': parameters}

    def __init__(self, setup, context, parameters, trace):
        BaseAgent.__init__(self, setup, context, parameters, trace);
        self.timer = 0;
        self.wake_up = 0
        
        self.order_list = []
        self.needExecReportEvt          = True
        self.trades = 0
    
    def process(self, evt):
        
        self.trades += len(evt.getTrades())        
        ats = 1000

        self._orderbook = self.market.findOrderBook(self['ric'])
        if self.wake_up <= self.trades:            
            if self.order_list:
                for order in self.order_list:
                    self._orderbook.processCancelOrder(order.orderId)
                      
            if self['side'] == Order.Buy:                
                lob = evt.getLob()
                if not lob:
                    return                   
                
                best = lob.getBid(0).price
                price_list = [best - i * 0.005 for i in range(5)]
                    
                self.order_list = []                            
                for i in range(len(price_list)):
                    
                    prefix = '%d - Distance' % i
                    order =  self.create_order(prefix, self['side'], price_list[i], 1000, Order.Limit, Order.DAY)
                    order.needExecReport = True
                    self.order_list.append(order)                    
                    self._orderbook.processCreateOrder(order)
            elif self['side'] == -1:
                pass
            
            self.wake_up = self.trades + self['cycle']
            
    def processReport(self, evt):
        size = evt.size
        d    = int(evt.orderId[0])
        self.appendIndicator({'size': size, 'd': d})
        
            
            
            

    
