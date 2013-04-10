from simep.core.subtactic import Tactic
from random import *
from simep.funcs.data.pyData import pyData

class BinaryAccelerator(Tactic):
    
    def __init__(self, setup, context, parameters):
        Tactic.__init__(self, setup, context, parameters)
        self.next_deal = 0
        self.aggressive_opposite_quantity_ratio = 0.5
        self.aggressive_probability_coef = 0.5
        
        ##passive indicators
        self.passive_counter = 0
        self.nb_passive_cycle = 0
        self.passive_life_at_best=0
        self.availability = 0
        
        ##Aggressive indicators
        
        
    
    def process(self, evt):                
        #print self['MinMultAts']
        #print self['ParentRefPrice']
        if not self.marketManager.getFeedInfo('AE'):            
            return
        self.log ("[%d] ExecQty = %d. RemainingQty = %d. AvailableQty = %d. LeavesQty = %d. OptimalQty = %d" % 
            (self.marketManager.getFeedInfo('AE', 'C1_CONVNUM'),
             self.moneyManager.getExecQty(), 
             self.moneyManager.getTotalQty() - self.moneyManager.getExecQty(),
             self.moneyManager.getTotalQty() - self.moneyManager.getExecQty() - self.moneyManager.getLeavesQty(),
             self.moneyManager.getLeavesQty(), 
             self['OptimalQty']))
        
        availableQuantity = self.moneyManager.getTotalQty() - self.moneyManager.getExecQty() - self.moneyManager.getLeavesQty()
        
        
        ats = int(self.marketManager.getFeedInfo('AE', 'A_QTY_3') / self.marketManager.getFeedInfo('AE', 'A_NPLRS_3') * (1 + random() * 0.2 - 0.1))
        
        ####################################################################################################################
        ######################################  Aggressive Tracking  ##########################################################
        ####################################################################################################################       # Aggressive Part
        reserved_quantity = 0
        qty_aggressive    = 0
        callbacks = self.bus.get('%s_NoCallbacks' % self['ParentAutomatonOrderID'])
        if not callbacks:
            callbacks = 1
        
        #print 'Callbacks:', callbacks, 'Trades:', self.marketManager.getFeedInfo('AE', 'C1_CONVNUM')
        
        # Check if in the money
        if self.side * self.marketManager.getFeedInfo(evt.getMarketId(), 'BEST_OPPOSITE1') < self.side * self['ReferencePrice']:                 
            qty_aggressive = self.aggressive_opposite_quantity_ratio * self.marketManager.getFeedInfo(evt.getMarketId(), 'BEST_OPPOSITE_SIZE1')
            qty_aggressive = min(max(1 * ats, min(qty_aggressive, 8 * ats)), availableQuantity)
            if (availableQuantity - qty_aggressive) <= ats:               
                qty_aggressive = availableQuantity
            
            probability = self.aggressive_probability_coef * self.marketManager.getFeedInfo('AE', 'C1_CONVNUM') / callbacks
            
            if random() < probability:              
                self.createLimitOrder(evt.getMarketId(), 
                                      self.marketManager.getFeedInfo(evt.getMarketId(), 'BEST_OPPOSITE1'), 
                                      qty_aggressive, 
                                      execType = 'Fak', 
                                      prefix = 'AggressiveTracking')
                reserved_quantity = qty_aggressive
                self.log('Aggressive Order Fak: %d@%f. BidAsk: [%d|%f - %d|%f]. Probability: %f.' % (qty_aggressive, self.marketManager.getFeedInfo(evt.getMarketId(), 'BEST_OPPOSITE1'), self.marketManager.getBid(0).size, self.marketManager.getBid(0).price, self.marketManager.getAsk(0).size, self.marketManager.getAsk(0).price, probability))
            
        self.bus.put('%s_NoCallbacks' % self['ParentAutomatonOrderID'], callbacks + 1)
        
        
        ####################################################################################################################
        ######################################  Passive Tracking  ##########################################################
        ####################################################################################################################
        price = self.marketManager.getFeedInfo(evt.getMarketId(), 'BEST1') - self.side * self.marketManager.getFeedInfo('AE', 'KO_DIST') * self.getReferenceData('PriceStep', venue_id = evt.getMarketId(), price = self.marketManager.getFeedInfo(evt.getMarketId(), 'BEST1'))
        if self['Price']:
            price = min(price, self['Price']) if self.side == 1 else max(price, self['Price'])            
        
        #Compute Quantity
        quantity = int(self['OptimalQty'] / self['MultDt'] * (1 + random() * 0.2 - 0.1)) # TODO: splitting of orders
        quantity = min(max(1 * ats, min(quantity, 8 * ats)), self.moneyManager.getTotalQty() - self.moneyManager.getExecQty() - reserved_quantity)
        if (self.moneyManager.getTotalQty() - self.moneyManager.getExecQty() - reserved_quantity - quantity) <= ats:            
            quantity = self.moneyManager.getTotalQty() - self.moneyManager.getExecQty() - reserved_quantity
        
        if not self.moneyManager.getPrivateBook():      
            self.createLimitOrder(evt.getMarketId(), price, quantity, prefix = 'PassiveTracking' )
            self.next_deal = int(self.marketManager.getFeedInfo('AE', 'C1_CONVNUM')) + self.marketManager.getFeedInfo('AE', 'PARCL_SIZE') * self.marketManager.getFeedInfo('AE', 'CONV_FAC')
            self.order_status = True
            self.log('Insert Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity, price, self.marketManager.getBid(0).size, self.marketManager.getBid(0).price, self.marketManager.getAsk(0).size, self.marketManager.getAsk(0).price, self.next_deal))
        
        elif self.next_deal <= int(self.marketManager.getFeedInfo('AE', 'C1_CONVNUM')) or not self.order_status:
            order_id = self.moneyManager.getPrivateBook().keys()[0] # always only 1 single order on the market            
            
            if (self.order_status and (self.moneyManager.getPrivateBook()[order_id]['Price'] != price or self.moneyManager.getPrivateBook()[order_id]['LeavesQty'] < quantity)) \
               or (not self.order_status and self.side * self.moneyManager.getPrivateBook()[order_id]['Price'] < self.side * price) :
                
                self.modifyOrder(evt.getMarketId(), order_id, price, quantity)
                self.next_deal = int(self.marketManager.getFeedInfo('AE', 'C1_CONVNUM')) + self.marketManager.getFeedInfo('AE', 'PARCL_SIZE') * self.marketManager.getFeedInfo('AE', 'CONV_FAC')
                self.order_status = False
                
                self.log('Modify Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity, price, self.marketManager.getBid(0).size, self.marketManager.getBid(0).price, self.marketManager.getAsk(0).size, self.marketManager.getAsk(0).price, self.next_deal))            
            
            elif not self.order_status and self.side * self.moneyManager.getPrivateBook()[order_id]['Price'] >= self.side * price:
                self.order_status = True
                
        
        self.log('ATS: %d, QAggressive: %d, QPassive: %d' % (ats, qty_aggressive, quantity))
    
    def processReport(self, evt):
        total_qty_so_far = self.bus.get('%s_TotalQuantity' % self['ParentAutomatonOrderID'])
        if not total_qty_so_far:
            total_qty_so_far = 0
        for x in self.moneyManager.getLastExecutions():
            self.log("Order Filled: Qty=%d Price=%f" % (x['Quantity'], x['Price']))
            total_qty_so_far += x['Quantity']
        
        if 'AggressiveTracking' in self.moneyManager.getLastExecutions():
            order_type = 'A'
        elif 'PassiveTracking' in evt.orderId:
            order_type = 'P'
            
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
        return {'AE':('VWAP_P', 'S=%d@%s;P=bEHRC;Z=UTC;H=4;F=%s;T=%s;EX=top;EX=cross;EX=auc' % (self['InstrumentID'], self.primary_venue_id, self['EffectiveTime'][9:], self['ExpireTime'][9:]) )}
    
    

    
    