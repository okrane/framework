'''
Created on 8 sept. 2010

@author: benca
'''

from simep.funcs.stdio.utils import pyLog
from simep.core.baseagent import BaseAgent
from simep.funcs.data.pyData import pyData
from simep.tools import date2num



class ReadOrdersObserver(BaseAgent):
    
    def __init__(self, setup, context, params, trace):
        super(ReadOrdersObserver, self).__init__(setup, context, params, trace)
        self._log_file = open(self['log_filename'], 'w')
        self.sliding_indicators = {'VWAP': 0, 'spread': 0}
        self.pending_indicators = {'VWAP': 0, 'spread': 0}
        self.sliding_memory     = {'TURNOVER': [], 'VOLUME': [], 'spread': []}
        self.pending_countdown  = list()
        self.trade_list = list()
        
    def process(self, event):
        ba['event']['TIME_STR'] = event.timestamp
        lob = event.getLob(self['ric'])
        trades = event.getTrades()
        nb_trades = len(trades)
        for trade in trades:
            # record trade info in a sliding buffer
            self.sliding_memory['TURNOVER'].append(trade.price * trade.size)
            self.sliding_memory['VOLUME'].append(trade.size)
            self.sliding_memory['spread'].append(lob.getAsk(0).price - lob.getBid(0).price)
            # maintain the buffer
            if len(self.sliding_memory['TURNOVER'])>60:
                self.sliding_memory['TURNOVER'].pop(0)
                self.sliding_memory['VOLUME'].pop(0)
                self.sliding_memory['spread'].pop(0)
                # compute the current sliding indicators
                self.sliding_indicators['VWAP'] = sum(self.sliding_memory['TURNOVER']) / sum(self.sliding_memory['VOLUME'])
                self.sliding_indicators['spread'] = sum(self.sliding_memory['spread'])/60.0
                
        for  i in range(len(self.pending_countdown)-1, -1 ,-1):          
        # do I need to store a pending info?
            self.pending_countdown[i] = self.pending_countdown[i] -nb_trades
            if self.pending_countdown[i]<=0:
                
                 # Get stored trade data
                  recorded_trade = self.trade_list[i]
                  
                # record the tradef or post processing
                  self.appendIndicator( pyData('init', 
                                         date = [date2num(recorded_trade['date'])/(24*3600*1000)], 
                                         value = { 'price'       : [recorded_trade['price']], 
                                                  'VOLUME'       : [recorded_trade['VOLUME']],
                                                  'last vwap'    : [recorded_trade['last vwap']], 
                                                  'last spread'  : [recorded_trade['last spread']],
                                                  'future vwap'  : [self.sliding_indicators['VWAP']], 
                                                  'future spread': [self.sliding_indicators['spread']]} ))
                  self.trade_list.pop(i)
                  self.pending_countdown.pop(i)
                  
        # to pretty print data in terminal             
        pob = event.getMoneyManager().getPrivateBook()
        if len(pob) != 0:
            self.record_private_orderbook(pob)
       
    def processReport(self, eventReport):
        execs = eventReport.getMoneyManager().getLastExecutions()
        if len(execs)>0:
                print "%d trade occured" % len(execs)
                for last_exec  in  execs:
                    # it is time to record
                    trade_data_to_record = {'date' : ba['event']['TIME_STR'],
                                    'price'     : last_exec['Price'], 
                                    'VOLUME'     : last_exec['Quantity'],
                                    'last vwap'  : self.sliding_indicators['VWAP'], 
                                    'last spread': self.sliding_indicators['spread']
                                    }
                    self.trade_list.append(trade_data_to_record)                       
                    self.pending_countdown.append(60);    
        
    def results(self):
        self._log_file.close()
        self.indicators.sort_by('date')
        for i in range(len(self.trade_list) - 1 , - 1 , -1):
                # we may have some pending trades to add
                # Get stored trade data
                  recorded_trade = self.trade_list[i]
                  
                # record the trade for post processing
                  self.appendIndicator( pyData('init', 
                                         date = [date2num(recorded_trade['date'])/(24*3600*1000)], 
                                         value = {'price'     :  [recorded_trade['price']], 
                                                  'VOLUME'     : [recorded_trade['VOLUME']],
                                                  'last vwap'  : [recorded_trade['last vwap']], 
                                                  'last spread': [recorded_trade['last spread']],
                                                  'back vwap'  : [self.sliding_indicators['VWAP']], 
                                                  'back spread': [self.sliding_indicators['spread']]} ))
                  self.trade_list.pop(i)
                  self.pending_countdown.pop(i)
        return self.indicators
    
    def record_private_orderbook(self, pob):
        for key,val in pob.iteritems():
            pob_str = '%s, %- 20s: OrderQty=%04d  LeavesQty=%04d  Price=%06.03f' %(val['EffectiveTime'], key, val['OrderQty'], val['LeavesQty'], val['Price'])
            if self['log_filename'] != None:
                self._log_file.write(pob_str + '\n')
            if self['print']:
                print pob_str
        if self['print']:
            print ''
        if self['log_filename'] != None:
            self._log_file.write('\n')
    