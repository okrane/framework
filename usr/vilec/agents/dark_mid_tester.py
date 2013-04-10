from simep.core.basetactic import BaseTactic
from simep.bin.simepcore import Order
from collections import defaultdict

class DarkMidTester(BaseTactic):
    @staticmethod 
    def public_parameters():
       return {'parameters': {'DUMMY' : {'label' : 'PARAMETER FoR THE DARK MID AGENT', 'value' : 30}}}
   
    def __init__(self, setup, context, parameters, trace):
        super(DarkMidTester, self).__init__(setup, context, parameters, trace)
        self.enableMoneyManager= True
        self.ats = 100
        self.allData = defaultdict(list)
        self.marketManager.ae_venue_id = self.ba['venue_ids'][0]
        self.first_time = True
        self.venue_ids = context["venue_ids"]
        self.venue_types = context["trading_venue_types"]

        self.tv_type = {}
        self.mid_tv= list()
        
        for key in self.venue_ids:
            self.tv_type[self.venue_ids[key]] = self.venue_types[key]
            if self.venue_types[key] == 'M':
                self.mid_tv.append(self.venue_ids[key])

    def process(self, evt):
        # check IdleEvent
        if self.update(evt) <= 0:
            return False
        
        venue_id      = evt.venueId
        marketManager = self.marketManager
            
        # check market is fine
        if marketManager.getFeedInfo(venue_id, 'SPREAD') == None:
            return False
        
        marketManager.getVenue(venue_id)
        marketManager.isPrimaryVenue(venue_id)
        
        bid    = marketManager.getFeedInfo(venue_id, 'BEST_BID1')
        ask    = marketManager.getFeedInfo(venue_id, 'BEST_ASK1')
        self.marketManager.getFeedInfo(venue_id)
      
        if self.first_time :
            # find the mid tradinbg venue
            
            self.createOrder(self.mid_tv[0], "Mid_Deal", 1, 0.5, self.ats, Order.PegOnMidPrice, Order.DAY)
            self.first_time = False
        else:
            orders = self.moneyManager.getPrivateBook()
            for o in orders :
                print o
            
        # creates and places an order on the market "market_id"
#        if marketManager.getFeedInfo(venue_id, 'TRADE_EVENT'):
#            self.createOrder(venue_id, "SampleTrader", Order.Buy, bid, self.ats, Order.Limit, Order.DAY)

    def processReport(self, evt):
        print "Order has been fullfiled !!"
        pass
