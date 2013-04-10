from simep.core.tactic import Tactic
from simep.sched import Order
from simep.funcs.data.pyData import pyData

import math
import scipy as sp
import numpy as np
import random

class PassivePVOLTracking(Tactic):
    '''
    classdocs
    '''
    @staticmethod
    def public_parameters():
        setup      = {}
        parameters = {'side'      : {'label' : 'Side'             , 'value' : Order.Buy},
                      'k'         : {'label' : 'Corrected Target Volume Percentage'   , 'value' : 0},
                      'c'         : {'label' : 'Surmass coefficient'   , 'value' : 15}}
        return {'setup': setup, 'parameters': parameters}

    def __init__(self, setup, context, parameters, trace):
        Tactic.__init__(self, setup, context, parameters, trace)
        self.needExecReportEvt          = True
        self.volume = 0
        self.side = 1 if self['side'] == Order.Buy else -1
    
    def process(self, evt):        
        market = self.marketManager.getFeedInfo(evt.getMarketId())
        ats = 100
        # Initialize Market Grid
        current_state = {}
        for i in range(5):
            limit_value   = market['BEST_BID%s' % (i+1)] if self.side == 1 else market['BEST_ASK%s' % (i+1)]
            current_state[limit_value] = 0
        
        # Fill Market Grid and Cancel Orders out of scope
        print self.moneyManager.getPrivateBook().keys()
        for id, order in self.moneyManager.getPrivateBook().iteritems():
            if order['Price'] in current_state.keys():
                current_state[order['Price']] += order['LeavesQty']
            elif self.side * order['Price'] < self.side * min(sorted(current_state.keys(), descend = self.side)):
                print "Cancel", id
                self.cancelOrder(evt.getMarketId(), id)
        #print current_state
        # Insert Orders to match market quantity        
        for i in range(5):
            limit_qty = market['BEST_BSIZ%s' % (i+1)] if self.side == 1 else market['BEST_ASIZ%s' % (i+1)]
            limit_value   = market['BEST_BID%s' % (i+1)] if self.side == 1 else market['BEST_ASK%s' % (i+1)]
            if self['k'] * limit_qty - current_state[limit_value] > ats:                 
                print self.createLimitOrder(evt.getMarketId(), limit_value, self['c'] * (self['k'] * limit_qty - current_state[limit_value]), prefix = 'limit')
                #print "Limit: %f@%f" % (self['c'] * (self['k'] * limit_qty - current_state[limit_value]), limit_value)
        
    def processReport(self, evt):
        print 'exec'
        self.volume += evt.size 
        self.append_indicator({'volume': self.volume, 
                               'market_volume': self.ba['feed']['VOLUME']})