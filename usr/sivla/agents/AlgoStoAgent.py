from simep.core.tactic import Tactic
from simep.sched import Order
from simep.funcs.data.pyData import pyData

from usr.dev.sivla.funcs.MathTools.StochasticApproximation import approx_1 as sa

import math
import scipy as sp
import numpy as np
import random

class AlgoStoAgent(Tactic):
    '''
    classdocs
    '''
    @staticmethod
    def public_parameters():
        setup      = {'name'         : {'label' : 'Name'             , 'value' : 'AlgoStoAgent'}}
        parameters = {'side'         : {'label' : 'Side'             , 'value' : Order.Buy},
                      'delta'         : {'label' : 'Distance'             , 'value' : 0},
                      'cycle'         : {'label' : 'Cycle'             , 'value' : 15},
                      'q'          : {'label' : 'Ratio Quantity'    , 'value' : 0.05} 
                      }
        return {'setup': setup, 'parameters': parameters}

    def __init__(self, setup, context, parameters, trace):
        Tactic.__init__(self, setup, context, parameters, trace)
        self.needExecReportEvt          = True
        self.sa = sa(context['security_id'], context['date'])
        self.wake_up = 0        
        self.trades  = 0
        self.cycles  = 0
        self.context = {}
        self.context['S'] = list()
        self.side    = 1 if self['side'] == Order.Buy else -1
        self.delta   = self['delta']
        self.q_main  = 0
        self.q_secondary = 0
        
        
    def process(self, evt):        
        self.update()
        feed = self.marketManager.getFeedInfo(evt.getVenueId())
        best = feed['BEST_BID1'] if self.side == 1 else feed['BEST_ASK1']
        best_opposite = feed['BEST_ASK1'] if self.side == 1 else feed['BEST_BID1']              
        ats = 1
        
        self.context['S'].append(best)
        if self.wake_up <= feed['NUM_MOVES']:
            # Fin de cycle: anuler tous les ordres            
            self.context['S_END'] = best_opposite
            self.context['C_MAIN'] = self.penalty(feed, self.q_main)
            self.context['C_SECONDARY'] = self.penalty(feed, self.q_secondary)
            self.context['N_MAIN'] = self.q_main
            self.context['N_SECONDARY'] = self.q_secondary      
            
            for order_id in self.moneyManager.getPrivateBook().keys():
                self.cancelOrder(evt.getVenueId(), order_id)
            
            # Algo Sto
            H = self.sa.compute_H(self.context)            
            self.delta = self.delta - self.sa.gamma(self.cycles) * H            
            self.context['delta'] = self.delta
            self.append_indicator(self.context, date = evt.getEvtTime())
            self.context.clear()
            self.context['S'] = list()
            #======================= Fin du Cycle ====================================
            
            # placement initial\
            prix_main = best - self.side * self.delta * 0.005 # todo: ticks...
            prix_secodary = best - self.side * self.delta * 0.005 - self.side * 0.005      
            self.createLimitOrder(evt.getVenueId(), prix_main, ats, prefix = 'Main')
            self.createLimitOrder(evt.getVenueId(), prix_secodary, ats, prefix = 'Secondary')
            self.context['S_START'] = best
            self.wake_up = feed['NUM_MOVES'] + self['cycle']
        
        
    def processReport(self, evt):
        feed = self.marketManager.getFeedInfo(evt.getVenueId())       
        ats = 1
        
        if 'Main' in evt.getExecutedOrderId():
            self.q_main += evt.size
            if self['q'] >= self.q_main:
                self.createLimitOrder( evt.getVenueId(), evt.price, ats, prefix = 'Main')
        else:  
            self.q_secondary += evt.size
            if self['q'] >= self.q_secondary:
                self.createLimitOrder(evt.getVenueId(), evt.price, ats, prefix = 'Secondary')
        
    def penalty(self, feed, qty):                
        best_opposite = feed['BEST_ASK1'] if self.side == 1 else feed['BEST_BID1']
        result = 0
        q_r = self['q'] - qty
        d = 1
        while self['q'] - qty > 0:
            if self.side == 1:
                size         = min(self['q'] - qty, feed['BEST_ASIZ%d' % d])
                qty += size                    
                result += feed['BEST_ASK%d' % d] * size
            else:
                size         = min(self['q'] - qty, feed['BEST_ABID%d' % d])
                qty += size                    
                result += feed['BEST_BID%d' % d] * size
            d = d + 1
        return result / (q_r * best_opposite)
        
        