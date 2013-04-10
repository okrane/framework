from simep.bin.simepcore import EventTypes, SideTypes, Order
from simep.core.basetactic import BaseTactic
import time

class Trader(BaseTactic):
    
    @staticmethod
    def public_parameters():
        return {'parameters': {'d' : {'label' : 'distance', 'value' : 0},
                               'dt' : {'label' : 'distance', 'value' : 15},
                               }}
    
    # constructor
    def __init__(self, setup, context, params, trace):        
        BaseTactic.__init__(self, setup, context, params, trace)
        self.next = 0
    
    def process(self, event):
        market = self.marketManager.getFeedInfo(event.venueId)
        #trades = self.marketManager.getTrades(event.venueId)
        #print self.moneyManager.getPrivateBook()
        
        if market['NUM_MOVES'] > self.next:
            for order_id in self.moneyManager.getPrivateBook().keys():
                self.cancelOrder(event.venueId, order_id)
            
            order_prefix    = 'my_passive_order'
            side            = SideTypes.Buy
            price           = market['BEST_BID1'] - self['d']* 0.005
            qty             = 200
            order_type      = Order.Limit
            exec_type       = Order.DAY
            self.createOrder(event.venueId, order_prefix, side, price, qty, order_type, exec_type)
            
            self.next = market['NUM_MOVES'] + self['dt']
        
    
    def processReport(self, event):
        #print dir(event)
        print "Execution", event.price, event.quantity
    
    