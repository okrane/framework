from simep.core.baseagent import BaseAgent
from simep.sched import Order
from simep.funcs.data.pyData import pyData

import math
import scipy as sp
import numpy as np
import random

class EasyTrader(BaseAgent):
    @staticmethod
    def public_parameters():
        setup      = {'name'         : {'label' : 'Name'             , 'value' : 'EasyTrader001'}}
        parameters = {'side'         : {'label' : 'Side'             , 'value' : Order.Buy}}
        return {'setup': setup, 'parameters': parameters}
    
    
    def __init__(self, setup, context, parameters, trace):
        BaseAgent.__init__(self, setup, context, parameters, trace)
        self.needExecReportEvt          = True
        self.proba = 0.05
        self.avg = 100
        self.stdev = 5        
        
    def process(self, evt):
        self.date = evt.getEvtTime()
        self._orderbook = self.market.findOrderBook(self['ric'])
        if random.random() < self.proba:
            price = evt.getLob().getAsk(0).price if self['side'] == Order.Buy else evt.getLob().getBid(0).price
            self.quantity = int(random.gauss(self.avg, self.stdev**2))
            
            order = self.create_order('OrderPassif', self['side'], price, self.quantity, Order.Limit, Order.DAY)
            order.needExecReport = True                                
            self._orderbook.processCreateOrder(order)
        
       # Pour annuler un ordre:
       # self._orderbook.processCancelOrder(order.orderId)
    
    def processReport(self, evtReport):
        size = evtReport.size
        price = evtReport.price
        print "Execution: %d@%f | %d" % (size,price, self.quantity)
        
        self.appendIndicator(pyData('init', 
                                    date = [self.date], 
                                    value = {'size': [size],
                                             'quantity': [self.quantity]}))
        
        