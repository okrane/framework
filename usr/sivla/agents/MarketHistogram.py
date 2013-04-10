from simep.core.baseobserver import BaseObserver
from simep.sched import Order
from simep.funcs.data.pyData import pyData
import math
import numpy as np


class MarketHistogram(BaseObserver):
    
    @staticmethod
    def public_parameters():
        
        setup      = {'name'         : {'label' : 'Name'             , 'value' : 'MarketHistogram001'}}
        parameters = {'cycle'        : {'label' : 'Cycle Time'       , 'value' : 15}, 
                      'side'         : {'label' : 'Side'             , 'value' : Order.Buy},                      
                      }
        return {'setup': setup, 'parameters': parameters}
    
    @staticmethod
    def indicators_list():
        #return ['avg_trade_size_t_60', 'avg_spread_bp_t_60']
        return []  
    
    def __init__(self, setup, context, parameters, trace):        
        super(MarketHistogram, self).__init__(setup, context, parameters, trace)       
        
        self.trades = []
        self.histogram = {}
        self.next_deal = 0
        self.opposite = 0
        self.best = 0
        
    def process(self, evt):
        c = self.update(evt)
        if self.next_deal <= self._bus['business_time_t']:
            for tr in self.trades:
                if self.histogram.has_key(tr[0]):
                    self.histogram[tr[0]] += tr[1]
                else:
                    self.histogram[tr[0]] = tr[1]
            
            self.histogram['opposite'] = self.opposite
            self.histogram['best'] = self.best
            self.histogram['current_opposite'] = evt.getLob().getBid(0).price if self['side'] == Order.Sell else evt.getLob().getAsk(0).price
            self.histogram['current_best'] = evt.getLob().getAsk(0).price if self['side'] == Order.Sell else evt.getLob().getBid(0).price
            self.appendIndicator(self.histogram)
            
            self.next_deal = self._bus['business_time_t'] + self['cycle']
            self.opposite = evt.getLob().getBid(0).price if self['side'] == Order.Sell else evt.getLob().getAsk(0).price
            self.best = evt.getLob().getAsk(0).price if self['side'] == Order.Sell else evt.getLob().getBid(0).price
            self.trades = []
            self.histogram = {}
            
        
        for t in evt.getTrades():
            self.trades.append((t.price, t.size))
        
    
    def processReport(self, evt):
        pass
        
        
        