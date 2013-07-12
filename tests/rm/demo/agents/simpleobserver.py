from simep.tools import date2num
from simep.agents.baseagent import BaseAgent



# ------------------------------------------------- #
# This is the observer that we define for the       #
# simulation. It simply computes the market vwap    #
# and record the (best_bid, best_ask, vwap) in a    #
# log file.                                         #
# ------------------------------------------------- #
class SimpleObserver(BaseAgent):
    
    # constructor
    def __init__(self, setup, context, params, trace):
        # initialize the base class (BaseAgent)
        BaseAgent.__init__(self, setup, context, params, trace)
        # indicate that you don't want execution reports,
        # since you don't send any orders
        self.needExecReportEvt = False        
        self.needAllEvts       = True
        # open file
        self._log_file             = params['file']
        # initialize the weighted_price and market volume
        # in order to be able to compute the vwap
        self._weighted_price   = 0.0
        self._market_volume    = 0
    
    # record orderbook
    def record_orderbook(self, lob):
        if date2num(self._timestamp) > self['start_time']:
            lob_str  = '%s, BID1: %04d @ %05.02f' %(self._timestamp, lob.getBid(0).size, lob.getBid(0).price)
            lob_str += '    '
            lob_str += 'ASK1: %04d @ %05.02f' %(lob.getAsk(0).size, lob.getAsk(0).price)
            self._log_file.write(lob_str + '\n')
            for i in range(1,5):
                lob_str  = '%s, BID%d: %04d @ %05.02f' %('            ', i+1, lob.getBid(i).size, lob.getBid(i).price)
                lob_str += '    '
                lob_str += 'ASK%d: %04d @ %05.02f' %(i+1, lob.getAsk(i).size, lob.getAsk(i).price)
                self._log_file.write(lob_str + '\n')
    
    # method to record events
    def record(self, event_str):
        print event_str
        if self['record_vwap_in_log']:
            self._log_file.write(event_str + '\n')
    
    # the process method is the one called 
    def process(self, event):
        
        # get latest market trades
        event_trades = event.getTrades()
        # update weighted_price and market_volume
        for trade in event_trades:
            price = trade.price
            size  = trade.size
            self._weighted_price += (price*size)
            self._market_volume  += size
        # compute vwap
        if self._market_volume != 0:
            vwap = self._weighted_price / self._market_volume
        else:
            vwap = -1.0
        
        # get best bid price and best ask price
        lob      = event.getLob()
        best_bid = lob.getBid(0).price
        best_ask = lob.getAsk(0).price
        
        # build the string line to record
        self._timestamp = event.getEvtTimeStr()
        event_str = '%s, best_bid=%06.03f, best_ask=%06.03f, vwap=%06.03f' %(self._timestamp,best_bid,best_ask,vwap)
        self.record(event_str)
        if self['record_orderbook_in_log']:
            self.record_orderbook(lob)
        
            