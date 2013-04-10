'''
Created on Jun 18, 2010

@author: sivla
'''

from simep.core.baseagent import BaseAgent
from simep.sched import Order, Trade
from simep.funcs.data.pyData import pyData, convertStr
from simep.sched import dateNum

from datetime import datetime

class LobRecorder(BaseAgent):
    @staticmethod
    def run_visualization(data, title = None, frequency = 1):
        print "Limit Orderbook Visualization. Total Steps: %d" % (len(data.date)) 
        from PyQt4 import QtGui
        import sys
        app = QtGui.QApplication(sys.argv)
        
        from usr.dev.sivla.funcs.plots.AnimatedPlots import Monitor
        w = Monitor(data, title, frequency, )
        w.setWindowTitle("Intraday Limit Orderbook")
        w.show()
        sys.exit(app.exec_())
    
    @staticmethod
    def public_parameters():
        setup      = {'name'          : {'label' : 'Name'             , 'value' : 'LobRecorder001'}}
        parameters = {'start'         : {'label' : 'Start'             , 'value' : Order.Buy},
                      'end'           : {'label' : 'End'               , 'value' : 0},
                      }
        return {'setup': setup, 'parameters': parameters}
    
    def __init__(self, setup, context, parameters, trace):        
        BaseAgent.__init__(self, setup, context, parameters, trace)
        
        self.date_str = context['date']
        self.record_only_trade_events = self['record_only_trade_events']
        
        self.indicators = pyDataLob('init', date = [], value = {}, info = {'name' : self['name']})
    
        self.indicators.info['context']    = context.copy()
        self.indicators.info['setup']      = setup.copy()
        self.indicators.info['parameters'] = parameters.copy()
        
    def last_process(self):
        #self.indicators.plot()        
        #LobRecorder.run_visualization(self.indicators, self['name'], 1) 
        pass
        #return self.indicators
       
    def process(self, evt):
        if self.update(evt) <= 0:
            return False
        self._time_t = dateNum(evt.getTimeStampStr())
        #self._bus = self.bus.get(self['ric'] + '_' + str(self['trading_destination_id']))
        
       
              
        #get Lob
        feed = self.marketManager.getFeedInfo(evt.getVenueId())      
        timestamp = evt.getTimeStampStr()  
        
        #get Trades
        volume = 0
        n    = len(feed['DEALS_QTYS'])        
        if n > 0:
            for tradesize in feed['DEALS_QTYS']:                                
                volume += tradesize
        elif self.record_only_trade_events: return 
            
        bid_value = []
        bid_size  = []
        ask_value = []
        ask_size  = []
        
        for i in range(5):
            bid_value.append(feed['BEST_BID%i' % (i+1)])
            ask_value.append(feed['BEST_ASK%i' % (i+1)])
            bid_size.append(feed['BEST_BSIZ%i' % (i+1)])
            ask_size.append(feed['BEST_ASIZ%i' % (i+1)])
        
        dt = datetime(year   = convertStr(self.date_str[0:4]),  
                      month  = convertStr(self.date_str[4:6]), 
                      day    = convertStr(self.date_str[6:8]), 
                      hour   = convertStr(timestamp[0:2]),
                      minute = convertStr(timestamp[3:5]),
                      second = convertStr(timestamp[6:8]),
                      microsecond = 1000 * convertStr(timestamp[9:12]))
        self.append_indicator(pyDataLob('init', date = [dt], value = {'bid_price': [bid_value],
                                                                  'ask_price': [ask_value], 
                                                                  'bid_size':[bid_size], 
                                                                  'ask_size':[ask_size], 
                                                                  'volume':[volume], 
                                                                  'vwap': [ feed['VWAP']]}))
        

class pyDataLob(pyData):
    def plot(self):
        import matplotlib.pyplot as plt
        plt.figure(1)
        
        plt.subplot(211)
        price = []        
        for i in range(len(self['bid_price'])):            
            price.append((self['bid_price'][i][0] + self['ask_price'][i][0]) / 2)
        
        plt.plot(self.date, price, 'bv')
        plt.plot(self.date, self.value['vwap'], 'r-')
        
        plt.subplot(212)
        markerline, stemlines, baseline = plt.stem(self.date, self.value['volume'], '--')
        plt.setp(markerline, 'markerfacecolor', 'g')
        plt.setp(stemlines, 'color', 'g')
        plt.setp(baseline, 'color','r', 'linewidth', 2)
        