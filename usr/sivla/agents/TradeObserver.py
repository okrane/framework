from simep.bin.simepcore import EventTypes, SideTypes, Order
from simep.core.basetactic import BaseTactic
import time

class TradeObserver(BaseTactic):
    
    @staticmethod
    def public_parameters():
        return {'parameters': {'dummy_parameter1' : {'label' : 'Dummy Parameter 1', 'value' : False},
                               'dummy_parameter2' : {'label' : 'Dummy Parameter 2', 'value' : 387},
                               'dummy_parameter3' : {'label' : 'Dummy Parameter 3', 'value' : 'my_string'}}}
    
    
    
    # constructor
    def __init__(self, setup, context, params, trace):        
        BaseTactic.__init__(self, setup, context, params, trace)
         
    
    def process(self, event):
        market = self.marketManager.getFeedInfo(event.venueId)
        trades = self.marketManager.getTrades(event.venueId)
        
        for t in trades:
            export = {'price' : t.price, 
                      'volume': t.size, 
                      'bid'   : market['BEST_BID1'],
                      'ask'   : market['BEST_ASK1'],
                      'bid_size': market['BEST_BSIZ1'],
                      'ask_size': market['BEST_ASIZ1'],
                      'trading_destination_id': event.venueId                      
                      }
            
            self.append_indicator(export, timestamp = t.time)
    
    def processReport(self, event):
        pass
    
    