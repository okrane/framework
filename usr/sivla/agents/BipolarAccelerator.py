from simep.core.subtactic import Tactic
from random import *
from scipy.stats import norm
from math import sqrt

class BipolarAccelerator(Tactic):
    
    def __init__(self, setup, context, parameters):
        Tactic.__init__(self, setup, context, parameters)
        self.next_deal = 0
        
        self.quantile = 0.3
        self.distance = None
        self.k = 10
    
    def process(self, evt):        
        if not evt.getFeedInfo('AE'):            
            return
        self.log ("[%d] ExecQty = %d. RemainingQty = %d. AvailableQty = %d. LeavesQty = %d" % 
            (evt.getFeedInfo('AE', 'C1_CONVNUM'),
             evt.getExecQty(), 
             evt.getTotalQty() - evt.getExecQty(),
             evt.getTotalQty() - evt.getExecQty() - evt.getLeavesQty(),
             evt.getLeavesQty()))

        tick_size = self.getReferenceData('PriceStep', venue_id = evt.getMarketId(), price = evt.getFeedInfo(evt.getMarketId(), 'BEST_OPPOSITE1'))        
        # Compute Price Zone
        time_remaining = 1 # TODO
        vol_gk = evt.getFeedInfo('AE', 'GEN_VAL15')
        if not vol_gk:
            vol_gk = 10
        low_limit  = norm.ppf(self.quantile) * vol_gk * sqrt(time_remaining) + self['ParentRefPrice']
        high_limit = norm.ppf(1 - self.quantile) * vol_gk * sqrt(time_remaining) + self['ParentRefPrice']
        best_op    = evt.getFeedInfo(evt.getMarketId(), 'BEST_OPPOSITE1')        
        self.tmp = int(evt.getFeedInfo('AE', 'C1_CONVNUM'))
        middle_zone = (low_limit <= best_op and best_op <= high_limit)
        
        if middle_zone:
            # Initialize distance placement
            if self.distance == None:                
                spread = evt.getFeedInfo(evt.getMarketId(), 'BEST_OPPOSITE1') - evt.getFeedInfo(evt.getMarketId(), 'BEST1')                        
                self.distance = int(spread / tick_size)
                
            # compute placement price
            price = best_op - self.side * self.distance * tick_size
            if self['Price']:
                price = min(price, self['Price']) if self.side == 1 else max(price, self['Price']) 
            
            # compute placement Quantity
            quantity = (self['OptimalQty']) / self.k
            ats = evt.getFeedInfo('AE', 'A_QTY_3') / evt.getFeedInfo('AE', 'A_NPLRS_3')
            quantity = min(min(quantity, 8 * ats), self['OptimalQty'] - evt.getExecQty())
            
            if not evt.getPrivateBook(): 
                price = price - self.side * tick_size
                self.distance = self.distance + 1
                
                self.log('Insert Order: %d@%f. BidAsk: [%f - %f]' % (quantity, price, evt.getBid(0).price, evt.getAsk(0).price))
                self.createLimitOrder(evt.getMarketId(), price, quantity, prefix = 'PassiveTracking' )
                self.next_deal = int(evt.getFeedInfo('AE', 'C1_CONVNUM')) + evt.getFeedInfo('AE', 'PARCL_SIZE') * evt.getFeedInfo('AE', 'CONV_FAC')
                self.appendIndicator({'event': 'new', 'middle': middle_zone, 'low_limit': low_limit, 'high_limit': high_limit, 'distance': self.distance, 'price': price, 'quantity': quantity, 'aggressive_quantity': 0, 'best_bid': evt.getBid(0).price, 'best_ask': evt.getAsk(0).price})
                
                
            elif self.next_deal <= int(evt.getFeedInfo('AE', 'C1_CONVNUM')):
                order_id = evt.getPrivateBook().keys()[0] # always only 1 single order on the market
                
                price = price + self.side * tick_size
                self.distance = self.distance - 1
                
                if evt.getPrivateBook()[order_id]['Price'] != price or evt.getPrivateBook()[order_id]['LeavesQty'] != quantity:
                    self.log('Modify Order: %d@%f. BidAsk: [%f - %f]' % (quantity, price, evt.getBid(0).price, evt.getAsk(0).price))            
                    self.modifyOrder(evt.getMarketId(), order_id, price, quantity)
                
                self.next_deal = int(evt.getFeedInfo('AE', 'C1_CONVNUM')) + evt.getFeedInfo('AE', 'PARCL_SIZE') * evt.getFeedInfo('AE', 'CONV_FAC')
                self.appendIndicator({'event': 'modify', 'middle': middle_zone, 'low_limit': low_limit, 'high_limit': high_limit, 'distance': self.distance, 'price': price, 'quantity': quantity, 'aggressive_quantity': 0, 'best_bid': evt.getBid(0).price, 'best_ask': evt.getAsk(0).price})
            
                        
        else:
            if self.distance == None:                
                spread = evt.getFeedInfo(evt.getMarketId(), 'BEST_OPPOSITE1') - evt.getFeedInfo(evt.getMarketId(), 'BEST1')                        
                self.distance = int(spread / tick_size)
            #if self.distance == None:                
            #   spread = evt.getFeedInfo('AE', 'SPREAD1')                
            #   if not spread:
            #       spread = evt.getFeedInfo(evt.getMarketId(), 'BEST_OPPOSITE1') - evt.getFeedInfo(evt.getMarketId(), 'BEST1')                    
            #    self.distance = int(spread / tick_size)
            
            # compute placement price
            price = best_op - self.side * self.distance * tick_size
            if self['Price']:
                price = max(price, self['Price']) if self.side == 1 else min(price, self['Price']) 
            
            # compute placement Quantity
            quantity = evt.getTotalQty() / self.k
            ats = evt.getFeedInfo('AE', 'A_QTY_3') / evt.getFeedInfo('AE', 'A_NPLRS_3')
            quantity = min(quantity, evt.getTotalQty() - evt.getExecQty())
            q_aggr   = (evt.getTotalQty() - evt.getExecQty() - quantity) / self.k
            
            if not evt.getPrivateBook(): 
                price = price - self.side * tick_size
                self.distance = self.distance + 1
                
                self.log('Insert Order: %d@%f. BidAsk: [%f - %f]' % (quantity, price, evt.getBid(0).price, evt.getAsk(0).price))
                self.createLimitOrder(evt.getMarketId(), price, quantity, prefix = 'PassiveTracking' )
                self.createMarketOrder(evt.getMarketId(), q_aggr, prefix = 'AggressiveTracking')
                self.next_deal = int(evt.getFeedInfo('AE', 'C1_CONVNUM')) + evt.getFeedInfo('AE', 'PARCL_SIZE') * evt.getFeedInfo('AE', 'CONV_FAC')
                self.appendIndicator({'event': 'new', 'middle': middle_zone, 'low_limit': low_limit, 'high_limit': high_limit, 'distance': self.distance, 'price': price, 'quantity': quantity, 'aggressive_quantity': q_aggr, 'best_bid': evt.getBid(0).price, 'best_ask': evt.getAsk(0).price})       
            
            elif self.next_deal <= int(evt.getFeedInfo('AE', 'C1_CONVNUM')):
                order_id = evt.getPrivateBook().keys()[0] # always only 1 single order on the market
                
                price = price + self.side * tick_size
                self.distance = self.distance - 1
                
                if evt.getPrivateBook()[order_id]['Price'] != price or evt.getPrivateBook()[order_id]['LeavesQty'] != quantity:
                    self.log('Modify Order: %d@%f. BidAsk: [%f - %f]' % (quantity, price, evt.getBid(0).price, evt.getAsk(0).price))            
                    self.modifyOrder(evt.getMarketId(), order_id, price, quantity)
                    self.createMarketOrder(evt.getMarketId(), q_aggr, prefix = 'AggressiveTracking')
                
                self.next_deal = int(evt.getFeedInfo('AE', 'C1_CONVNUM')) + evt.getFeedInfo('AE', 'PARCL_SIZE') * evt.getFeedInfo('AE', 'CONV_FAC')
                self.appendIndicator({'event': 'modify', 'middle': middle_zone, 'low_limit': low_limit, 'high_limit': high_limit, 'distance': self.distance, 'price': price, 'quantity': quantity, 'aggressive_quantity': q_aggr, 'best_bid': evt.getBid(0).price, 'best_ask': evt.getAsk(0).price})       
    
    def processReport(self, evt):
        total_qty_so_far = self.bus.get('%s_TotalQuantity' % self['ParentAutomatonOrderID'])
        if not total_qty_so_far:
            total_qty_so_far = 0
        for x in evt.getLastExecutions():
            self.log("Order Filled: Qty=%d Price=%f" % (x['Quantity'], x['Price']))
            total_qty_so_far += x['Quantity']
        
        self.log('Total Exec Quantity by Simep Agent: %d' % total_qty_so_far)
        self.bus.put('%s_TotalQuantity' % self['ParentAutomatonOrderID'], total_qty_so_far)
    
    
    def acceptSubscription(self, sub_id, sub):
        return sub_id == self.primary_venue_id

    def acceptVenue(self, venue_id, venue):
        if venue.info["type"] != "Lit":
            return False        
        if venue.info["primary"] == '1':
            self.primary_venue_id = venue_id
            return True
        return False

    def extraSubscriptions(self):
        ''' Returns any extra subscriptions needed. 
            @return
                Dictionary: {name: (Source, Ric, ReqdProcess)}
        '''
        return {'AE':('VWAP_P', 'S=%d@%s;P=bEHRC;PL=RI;PL=MW;Z=UTC;H=4;F=%s;T=%s;EX=top;EX=cross;EX=auc' % (self['InstrumentID'], self.primary_venue_id, self['EffectiveTime'][9:], self['ExpireTime'][9:]) )}
    