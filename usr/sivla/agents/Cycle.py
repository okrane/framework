from simep.core.baseobserver import BaseObserver
from simep.sched import Order
from simep.funcs.data.pyData import pyData
import math
import numpy as np


class CyclePlacement(BaseObserver):
    
    @staticmethod
    def public_parameters():
        
        setup      = {'name'         : {'label' : 'Name'             , 'value' : 'Cycle001'}}
        parameters = {'size'         : {'label' : 'Size'             , 'value' : 10000}, 
                      'reference'    : {'label' : 'Reference'        , 'value' : 'best'},
                      'd'            : {'label' : 'Distance'         , 'value' : 1}, 
                      'cycle'        : {'label' : 'Cycle Time'       , 'value' : 5}, 
                      'side'         : {'label' : 'Side'             , 'value' : Order.Buy}, 
                      'bussinessTime': {'label' : 'Use Business Time', 'value' : True}}
        return {'setup': setup, 'parameters': parameters}
    
    @staticmethod
    def indicators_list():
        return ['avg_trade_size_t_60', 'avg_spread_bp_t_60']  
    
    def __init__(self, setup, context, parameters, trace):
        super(CyclePlacement, self).__init__(setup, context, parameters, trace)
        
        self._parameters['bt'] = True      
        self.side = 1 if self._parameters['side']== Order.Buy else -1         
        self.timer = self['cycle']                
        self.needExecReportEvt = True        
        self.needAllEvts = True
        
        self.next_deal = 0
    
        self.distance_best    = 0
        self.distance_best_op = 2
        
    def process(self, evt):
        self.update(evt)
        tick_size = 0.005 # TODO
        ats = self._bus['avg_trade_size_t_60']
        
        ### Reference
        best_opposite = evt.getLob().getBid(0).price if self.side == -1 else evt.getLob().getAsk(0).price
        if self['reference'] == 'best':
            reference = evt.getLob().getBid(0).price if self.side == 1 else evt.getLob().getAsk(0).price
        elif self['reference'] == 'best_opposite':
            reference = evt.getLob().getBid(0).price if self.side == -1 else evt.getLob().getAsk(0).price
        elif self['reference'] == 'mid':
            reference = (evt.getLob().getBid(0).price + evt.getLob().getAsk(0).price) / 2
        elif self['reference'] == 'historic_spread':
            reference = best_opposite - self.side * int(self._bus['avg_spread_bp_t_60'] * best_opposite / (tick_size * 10000))
            
        
        if not ats:
            ats = 1
        if not self._bus['avg_spread_bp_t_60']:
            distance_bo = self.distance_best_op
        else:
            distance_bo = int(self._bus['avg_spread_bp_t_60'] * best_opposite / (tick_size * 10000))
                
        
        if self.next_deal <= self._bus['business_time_t']:
            best_op = evt.getLob().getBid(0).price if self.side == -1 else evt.getLob().getAsk(0).price
            best    = evt.getLob().getBid(0).price if self.side == 1 else evt.getLob().getAsk(0).price
            self.price = reference - self.side * self['d'] * tick_size
            if self.side == 1:
                volume = evt.getLob().getBid(0).size + evt.getLob().getBid(1).size / 2
                v = np.array([evt.getLob().getBid(i).size for i in range(5)])
                offset, = np.where(np.cumsum(v) < volume)
                if v[offset]:                                       
                    self.price_volume =  evt.getLob().getBid(int(offset[0])).price
                else:
                    self.price_volume =  evt.getLob().getBid(0).price + tick_size
            else : 
                volume = evt.getLob().getAsk(0).price + evt.getLob().getAsk(1).price / 2
                v = np.array([evt.getLob().getAsk(i).size for i in range(5)])                              
                offset = np.where(np.cumsum(v) < volume)
                if offset:                    
                    self.price_volume =  evt.getLob().getAsk(int(offset[0])).price
                else:
                    self.price_volume =  evt.getLob().getAsk(0).price - tick_size
                        
            self.pr_opp = best_op - self.side * distance_bo * tick_size            
            self.pr_best= best - self.side * self.distance_best * tick_size
            
            self.next_deal = self._bus['business_time_t'] + self.timer
        
        
        ### Availability Best Opposite
        v = np.array([0, 0, 0, 0, 0])
        for i in range(5):
            if self.side == 1 and evt.getLob().getBid(i).price > self.pr_opp:
                v[i] = evt.getLob().getBid(i).size
            elif self.side == -1 and evt.getLob().getAsk(i).price < self.pr_opp:
                v[i] = evt.getLob().getAsk(i).size
        if self.side == 1:
            price = np.array([evt.getLob().getBid(i).price for i in range(5)])
        elif self.side == -1:
            price = np.array([evt.getLob().getAsk(i).price for i in range(5)])
                
        availability_best_op = - self.side * (1 - (np.dot(v, price) + self.pr_opp * ats ) / (best_opposite * (v.sum() + ats)))
        
        
        ### Availability Best
        v = np.array([0, 0, 0, 0, 0])
        for i in range(5):
            if self.side == 1 and evt.getLob().getBid(i).price > self.pr_best:
                v[i] = evt.getLob().getBid(i).size
            elif self.side == -1 and evt.getLob().getAsk(i).price < self.pr_best:
                v[i] = evt.getLob().getAsk(i).size
                
        availability_best = - self.side * (1 - (np.dot(v, price) + self.pr_best * ats ) / (best_opposite * (v.sum() + ats)))
        
        ### generic
        v = np.array([0, 0, 0, 0, 0])
        for i in range(5):
            if self.side == 1 and evt.getLob().getBid(i).price > self.price:
                v[i] = evt.getLob().getBid(i).size
            elif self.side == -1 and evt.getLob().getAsk(i).price < self.price:
                v[i] = evt.getLob().getAsk(i).size
                
        availability = - self.side * (1 - (np.dot(v, price) + self.price * ats ) / (best_opposite * (v.sum() + ats)))
        
        
        v = np.array([0, 0, 0, 0, 0])
        for i in range(5):
            if self.side == 1 and evt.getLob().getBid(i).price > self.price_volume:
                v[i] = evt.getLob().getBid(i).size
            elif self.side == -1 and evt.getLob().getAsk(i).price < self.price_volume:
                v[i] = evt.getLob().getAsk(i).size
                
        availability_volume = - self.side * (1 - (np.dot(v, price) + self.price_volume * ats ) / (best_opposite * (v.sum() + ats)))
        
        self.appendIndicator({'availability_b': availability_best,
                              'availability_bo': availability_best_op,
                              'availability': availability,
                              'price'    :  self.price, 
                              'price_op': self.pr_opp, 
                              'price_b' : self.pr_best, 
                              'opposite': best_opposite, 
                              'price_volume': self.price_volume,
                              'availability_volume': availability_volume
                              })
        
        
    def processReport(self, evtReport):   
        pass
       
    

   