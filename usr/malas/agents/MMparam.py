from usr.dev.sivla.agents.Tactic import Tactic
from simep.sched import Order, Trade
from usr.dev.sivla.funcs.data.pyData import pyData
import math
from datetime import datetime, time


class MMparam(Tactic):
    @staticmethod
    def public_parameters():
        return {'size' : {'type': 'Int', 'range': [500, 10000], 'comment': 'Placement Size'}, 
                'd': {'type': 'Int', 'range': [0, 5], 'comment': 'Placement Distance in respect to the best price'}, 
                'cycle': {'type': 'Int', 'range': [5, 30], 'comment': 'Number of events before repositioning'}, 
                'bussinessTime': {'type': 'boolean', 'default': True, 'comment' : 'Whether the agent uses bussiness time cycles to reposition'}}
    
    @staticmethod
    def check_parameters(parameters):
        pass        
    
    def __init__(self, name, security_id, distance, cycle, size, bussinessTime = 1):
        super(MMparam, self).__init__(name , security_id)
        # parameters that can be modified in the GUI
        self._parameters = {'Size': size, 'd': distance,
                            'Cycle': cycle, 'bt': bussinessTime,                            
                            'RIC': security_id,
                            'StartTime': datetime.strptime('15:20:00', "%H:%M:%S"),
                            'EndTime': datetime.strptime('15:40:00', "%H:%M:%S")    }
        
        # constants
        self.BID     = +1
        self.UNKNOWN =  0
        self.ASK     = -1
        self.orderSide = {self.BID: Order.Buy, self.ASK: Order.Sell}
        
        # initial state
        self.timer        = {self.BID: cycle, self.ASK: cycle} 
        self.exec_qty     = {self.BID:     0, self.ASK:     0}         
        self.exec_turn    = {self.BID:     0, self.ASK:     0}         
        self._activeOrder = {self.BID:  None, self.ASK:  None}   
        self.ats = 0
        
        
        self.vwap   = {'ALGO': {self.BID: 0, self.ASK: 0}, 'MARKET': {self.BID: 0, self.ASK: 0, self.UNKNOWN: 0}}
        self.volume = {'ALGO': {self.BID: 0, self.ASK: 0}, 'MARKET': {self.BID: 0, self.ASK: 0, self.UNKNOWN: 0}}  
            
        # demands to the market
        self.needExecReportEvt = True        
        self.needAllEvts = True
        
        
        self.window = 600
        self.trades = {'ALGO': [], 'MARKET': []}
        
        self.timestamp = ''
        # variables for reporting/memory
        self.phi = {'ALGO': {self.BID: 0, self.ASK: 0}, 'MARKET': {self.BID: 0, self.ASK: 0, self.UNKNOWN: 0}}
        self.c = 0
        
    def enabled(self, context = None):
        return True
    
    def process(self, evt):
        n    = len(evt.getTrades())
        self.timestamp = evt.getLob().evtTime
        
        for trade in evt.getTrades():      
            # bid or ask?      
            market_side = 0
            self.trades['MARKET'].append((self.timestamp, market_side, trade.price, trade.size))
        
        # ATS in Qty
        L = len(self.trades['MARKET'])
        if L > 60:   
            self.ats = (1/60) * sum([a[3] for a in self.trades['MARKET'][(L-60):]])            
        
        # flow computations
        index_market = max(0, len(self.trades['MARKET']) - self.window)
        index_algo   = 0
        if len(self.trades['ALGO']) > 0:
            
            while (self.trades['ALGO'][index_algo][0] < self.trades['MARKET'][index_market][0]):
                index_algo += 1
        
            # sliding window:
            indices   = {'ALGO': range(index_algo, len(self.trades['ALGO'])), 
                       'MARKET': range(index_market, len(self.trades['MARKET']))}
            self.vwap   = {'ALGO': {self.BID: 0, self.ASK: 0}, 'MARKET': {self.BID: 0, self.ASK: 0, self.UNKNOWN: 0}}
            self.volume = {'ALGO': {self.BID: 0, self.ASK: 0}, 'MARKET': {self.BID: 0, self.ASK: 0, self.UNKNOWN: 0}}  
            for k in indices.keys(): 
                for s in self.vwap[k].keys():
                    for i in indices[k]:
                        if s == self.trades[k][i][1]:
                            self.vwap[k][s]   += self.trades[k][i][2] * self.trades[k][i][3]
                            self.volume[k][s] += self.trades[k][i][3]
                        if self.volume[k][s] > 0:
                            self.vwap[k][s] /= self.volume[k][s]
                        else:
                            self.vwap[k][s]  = 0.0
                        
                    self.phi[k][s] = float(self.volume[k][s]) / float(self.window)
#                    print ( "%s/%d: phi= %f, vwap = %f, volume = %f" % 
#                            (k, s, self.phi[k][s], self.vwap[k][s], self.volume[k][s]) )
                               
        # get Lob status 
        lob = evt.getLob()
        prices = {self.BID: lob.getBid(self._parameters['d']).price, self.ASK: lob.getAsk(self._parameters['d']).price}                 
        for s in self.timer.keys():      
            if self._parameters['bt']:
                self.timer[s] -= n
        
            # if time expired: update the order
            if self.timer[s] <= 0: 
                # reset timer
                self.timer[s] = self._parameters['Cycle']
                
                # cancel current order and replace
                if self._activeOrder[s]:                
                    self._cancelOrder(self._activeOrder[s])                            
                
                self._activeOrder[s] = self._createLimitOrder(self.orderSide[s], self._parameters['Size'], 
                                                                              prices[s])            
                self._orderbook.processCreateOrder(self._activeOrder[s])            
                
    def processReport(self, evtReport):
        if datetime.strptime(self.timestamp[0:8], "%H:%M:%S").time()>time(15,29,00):
            return
        if (evtReport.orderId[0] == 'S'):
            my_side =  self.ASK
        else:
            my_side =  self.BID
        self.timer[my_side] = -1
            
        self.trades['ALGO'].append((self.timestamp, my_side, evtReport.price, evtReport.size))        
        self.exec_qty[my_side]  +=  evtReport.size        
        self.exec_turn[my_side] +=  evtReport.size * evtReport.price
        self.appendIndicator(pyData('init', date = [self.timestamp], 
                                    value = {'phi_algo_bid':   [self.phi['ALGO'][self.BID]], 
                                             'phi_algo_ask':   [self.phi['ALGO'][self.ASK]], 
                                             'phi_market_bid': [self.phi['MARKET'][self.BID]], 
                                             'phi_market_ask': [self.phi['MARKET'][self.ASK]], 
                                             'phi_market_unknown': [self.phi['MARKET'][self.UNKNOWN]],
                                             
                                             'vwap_algo_bid':   [self.vwap['ALGO'][self.BID]], 
                                             'vwap_algo_ask':   [self.vwap['ALGO'][self.ASK]], 
                                             'vwap_market_bid': [self.vwap['MARKET'][self.BID]], 
                                             'vwap_market_ask': [self.vwap['MARKET'][self.ASK]], 
                                             'vwap_market_unknown': [self.vwap['MARKET'][self.UNKNOWN]],
                                              
                                             'exec_qty_bid':    [self.exec_qty[self.BID]],
                                             'exec_qty_ask':    [self.exec_qty[self.ASK]],
                                             'exec_turn_bid':  [self.exec_turn[self.BID]],
                                             'exec_turn_ask':  [self.exec_turn[self.ASK]],
                                             
                                             'price': [evtReport.price]
                                             }))
        return None
    
    def _createLimitOrder(self, side, qty, price, prefix='MM'):
        if side == Order.Sell:
            side_str = 'S'
        else:
            side_str = 'B'
        order = self._createOrder(side_str + prefix + '_%s' % self['RIC'], side, price, Order.Limit, qty, Order.DAY)
        order.needExecReport = True        
        return order
    
    def _createMarketOrder(self, side, qty, prefix='MM'):
        order = self._createOrder(prefix + '_%s' % self['RIC'], side, 0, Order.Market, qty, Order.DAY)
        order.needExecReport = True
        return order

    def update(self, param):
        pass