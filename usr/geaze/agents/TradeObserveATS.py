from simep.bin.simepcore import EventTypes, SideTypes, Order
from simep.core.basetactic import BaseTactic
import time
from simep.funcs.dbtools.connections import Connections

class TradeObserverATS(BaseTactic):
    
    @staticmethod
    def public_parameters():
        return {'parameters': {'dummy_parameter1' : {'label' : 'Dummy Parameter 1', 'value' : False},
                               'dummy_parameter2' : {'label' : 'Dummy Parameter 2', 'value' : 387},
                               'dummy_parameter3' : {'label' : 'Dummy Parameter 3', 'value' : 'my_string'}}}
    
    
    
    # constructor
    def __init__(self, setup, context, params, trace):        
        BaseTactic.__init__(self, setup, context, params, trace)
        self.ats = Connections.exec_sql('BSIRIUS', 'select indicator_value from quant_data..ci_security_indicator where security_id = 110 and trading_destination_id = 4 and indicator_id = 25 ')[0][0];
        self.trade_counter = 0
        self.nb_order_bids = 0
        self.nb_order_ask = 0
        
    
    def process(self, event):
        market = self.marketManager.getFeedInfo(event.venueId)
        trades = self.marketManager.getTrades(event.venueId)
         
        
        self.trade_counter += 1 if len(trades) > 0 else 0
        #print "Nb Trades update %d"%self.trade_counter
        
        
        td_id = event.venueId.split("#")[0]
        
        
        
        for t in trades:
            export = {'price' : t.price, 
                      'volume': t.size, 
                      'bid'   : market['BEST_BID1'],
                      'ask'   : market['BEST_ASK1'],
                      'bid_size': market['BEST_BSIZ1'],
                      'ask_size': market['BEST_ASIZ1'],
                      'td_id': int(td_id),
                      'limits_per_ATS' : .5*(market['BEST_BSIZ1']+market['BEST_ASIZ1'])/self.ats,
                      'volume_per_ATS' : (t.size)/self.ats,
                      'aver_limits_per_ATS' : .5*(market['BEST_BSIZ1']/market['NO_BIDMMKR']
                                                        +market['BEST_ASIZ1']/market['NO_ASKMMKR'])/self.ats
                      }
            
            self.append_indicator(export, timestamp = t.time)
    
    def processReport(self, event):
        pass
    
    