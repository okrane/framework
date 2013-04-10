from simep.core.subtactic import Tactic
from random import *
import math



class VOPTracking(Tactic):
    
    def __init__(self, setup, context, parameters):
        
        Tactic.__init__(self, setup, context, parameters)
        self.price_grid = {}
        self.last_trade_time = '00:00:00'
        self.total_qty_so_far = 0
        
        self.sub_tactic_indicators = {}
    
    def toMs(self, time_str):
        # conversion to MS
        hour = int(time_str[0:2])
        minute = int(time_str[3:5])
        second = int(time_str[6:8])
        
        return second + 60 * minute + 3600 * hour
        
    def process(self, evt):
        if not self.marketManager.getFeedInfo('AE'):  
            self.log('No Analitical Engine image found. Skipping this event...')          
            return
        market = self.marketManager.getFeedInfo(evt.getMarketId())
        ae     = self.marketManager.getFeedInfo('AE')
                
        print market['DEAL_TIME'], self.last_trade_time
        ### Analytic
        for p, v in self.price_grid.iteritems():  
            alpha = 0.005
            self.price_grid[p] = math.exp(-alpha * (self.toMs(market['DEAL_TIME']) - self.toMs(self.last_trade_time))) * v
        
        
                
        # new trades
        for trade in self.marketManager.getTrades(evt.getMarketId()):
            print trade.hour 
            if trade.hour > self.last_trade_time:
                if trade.price in self.price_grid.keys():
                    self.price_grid[trade.price] += trade.size
                else:
                    self.price_grid[trade.price] = trade.size
                    
                if self.last_trade_time < trade.hour:
                    self.last_trade_time = trade.hour
                
        
        ### Placement
        # compute price with regard to the grid:
        ats = int(ae['A_QTY_3'] / ae['A_NPLRS_3'] * (1 + random() * 0.2 - 0.1))
        price = self.computePrice(ats, self.cumsum(self.price_grid))
        print 'Price:', price
        if not price:
            price = market['BEST_OPPOSITE1']
        quantity = ats
        
      
        if not self.moneyManager.getPrivateBook():
            self.log('Insert Order: %d@%f. BidAsk: [%d|%f - %d|%f].' % (quantity, price, self.marketManager.getBid(0).size, self.marketManager.getBid(0).price, self.marketManager.getAsk(0).size, self.marketManager.getAsk(0).price))
            self.createLimitOrder(evt.getMarketId(), price, quantity, prefix = 'PassiveTracking' )
        else:
            order_id = self.moneyManager.getPrivateBook('PassiveTracking').keys()[0]
            if self.moneyManager.getPrivateBook('PassiveTracking')[order_id]['Price'] != price:
                self.log('Modify Order: %d@%f. BidAsk: [%d|%f - %d|%f].' % (quantity, price, self.marketManager.getBid(0).size, self.marketManager.getBid(0).price, self.marketManager.getAsk(0).size, self.marketManager.getAsk(0).price))
                self.modifyOrder(evt.getMarketId(), order_id, price, quantity)
            #modify the order if the price is different
        
    def computePrice(self, volume, grid):
        print grid
        price = None
        for p in sorted(grid.keys(), reverse=(self.side==1)):
            if grid[p] >= volume:
                price = p
                
        return price
            
    def cumsum(self, grid):
        a = {}
        cum_volume = 0
        for p in sorted(grid.keys(), reverse = self.side == -1):
            a[p] = grid[p] + cum_volume
            cum_volume += grid[p]
        return a
         
    
    def processReport(self, evt):
        for x in self.moneyManager.getLastExecutions():
            self.log("Order Filled: Qty=%d Price=%f" % (x['Quantity'], x['Price']))
            self.total_qty_so_far += x['Quantity']
        
    def acceptSubscription(self, sub_id, sub):        
        return sub_id == self.primary_venue_id

    def acceptVenue(self, venue_id, venue):
                
        if venue.info["type"] != "Lit":
            return False        
        if str(venue.info["destinationId"]) == str(self['destinationId']):
            self.primary_venue_id = venue_id
            self.trading_destination  = venue.info['destinationId']
            return True
        return False

    def extraSubscriptions(self):
        
        ''' Returns any extra subscriptions needed. 
            @return
                Dictionary: {name: (Source, Ric, ReqdProcess)}
        '''
        return {'AE':(self['vwapSource'], 'S=%d@%s;P=bEHRC;Z=UTC;H=4;F=%s;T=%s;EX=top;EX=cross;EX=auc' % (self['InstrumentID'], self.trading_destination, self['EffectiveTime'][9:], self['ExpireTime'][9:]) )}