'''BinaryAccelerator Tactic For Implementation Shortfall
CACVersion $File: BinaryAccelerator.py$ Version 1.1.15 $Branch: QuantitativeResearch$ $Date: 06/28/2012 01:00:04 PM$
Release Notes 
1.1.15
 - Fixed the ATS computation to match all other tactics

1.1.8 
 - Added a default value for the cycle size   

1.1.4 
 - Fixed a bug regarding the reassessment being sooner than the entry in case of delayed posting

1.1.3
 - Now using the Aggressiveness Levels in order to wait a number of trades before reposting
   after a fully filled execution of the passive placement 

1.1.2
 - MaxShow Quantity can no longer be superior to the Iceberg Order Quantity

1.1.1
 - Now using IcebergMono to submit the Passive Tracking 

1.1.0
 - Passive Tracking occurrences are now handled via the Iceberg Automaton

1.0.3
 - Added a custom indicator for the aggressive participation probability
 
1.0.2  
 -The algo will now subscribe to the parent Analytical Engine 
   (thus allowing easy switching between sources)
 -The aggressive placement parameters: 
  OppositeQuantityRatio and AggressiveProbabilityCoef are now recovered from a config file
 -The Aggressive Placement now takes into account the limit price of the parent algo
 -The default test for the cycle parameter has been corrected to test also "float" values  

1.0.1

 -The algo will now subscribe to the trading_destination of the parent algo instead of the default main destination
 -Added default values for some Analytical Engine Parameters 
 -Added an error message if critical Analytical Engine Parameters are missing  
  
 1.0.0
 -First Release
''' 

 
__version__ = "1.1.15"
__author__  = 'svlasceanu@cheuvreux.com'
__reviewer__= 'clehalle@cheuvreux.com'
__review_date__ = '11/02/2011'


from random import *
from simep.core.subtactic import Tactic

class BinaryAccelerator(Tactic):
    
    
    def __init__(self, setup, context, parameters):
        
        Tactic.__init__(self, setup, context, parameters)
        self.next_deal = 0
        self.delay     = 0     
    
    def process(self, evt):
        venueId = evt.venueId
        if not self.marketManager.getFeedInfo('AE'):  
            self.log('No Analitical Engine image found. Skipping this event...')          
            return
        else:
            # No Bussiness Time count available: cannot compute
            if not isinstance(self.marketManager.getFeedInfo('AE', 'C1_CONVNUM'), int):
                self.error('Unable to retrieve Bussiness Time data <C1_CONVNUM> from the AE.')
            
            # No ATS computation available: cannot compute
            if not (isinstance(self.marketManager.getFeedInfo('AE', 'A_QTY_3'), int) and 
                    isinstance(self.marketManager.getFeedInfo('AE', 'A_NPLRS_3'), int)):
                self.error('Unable to retrieve Historic Volume data <A_QTY_3, A_NPLRS_3> from the AE.')
            
            # No Cycle placement data available: use default instead
            if ((isinstance(self.marketManager.getFeedInfo('AE', 'PARCL_SIZE'), int) or isinstance(self.marketManager.getFeedInfo('AE', 'PARCL_SIZE'), float)) and
               (isinstance(self.marketManager.getFeedInfo('AE', 'CONV_FAC'), int) or isinstance(self.marketManager.getFeedInfo('AE', 'CONV_FAC'), float))):               
                self.cycle_size = self.marketManager.getFeedInfo('AE', 'PARCL_SIZE') * self.marketManager.getFeedInfo('AE', 'CONV_FAC')                
            else:                
                self.cycle_size = int(self['AggregatedDealsDefault'])
        
        self.log ("[%d] ExecQty = %d. RemainingQty = %d. AvailableQty = %d. LeavesQty = %d. OptimalQty = %d. PrivateBook: %s" % 
            (self.marketManager.getFeedInfo('AE', 'C1_CONVNUM'),
             self.moneyManager.getExecQty(), 
             self.moneyManager.getTotalQty() - self.moneyManager.getExecQty(),
             self.moneyManager.getTotalQty() - self.moneyManager.getExecQty() - self.moneyManager.getLeavesQty(),
             self.moneyManager.getLeavesQty(), 
             self['OptimalQty'],
             self.moneyManager))
        
        availableQuantity = self.moneyManager.getTotalQty() - self.moneyManager.getExecQty() - self.moneyManager.getLeavesQty()                
        ats = int(self.marketManager.getVenue(venueId)['ats'] * (1 + random() * 0.2 - 0.1))
        
        ####################################################################################################################
        ######################################  Aggressive Tracking  ##########################################################
        ####################################################################################################################       # Aggressive Part
        reserved_quantity = 0
        qty_aggressive    = 0 
        callbacks = self.bus.get('%s_NoCallbacks' % self['ParentAutomatonOrderID'])
        if not callbacks:
            callbacks = 10
        #print 'Callbacks:', callbacks, 'Trades:', self.marketManager.getFeedInfo('AE', 'C1_CONVNUM')
        
        # Check if in the money
        probability = 1.0 *  self['AggressiveProbabilityCoef'] * self.marketManager.getFeedInfo('AE', 'C1_CONVNUM') / callbacks
        if self.side * self.marketManager.getFeedInfo(venueId, 'BEST_OPPOSITE1') < self.side * self['ReferencePrice']:                 
            qty_aggressive = self['OppositeQuantityRatio'] * self.marketManager.getFeedInfo(venueId, 'BEST_OPPOSITE_SIZE1')
            qty_aggressive = min(max(self['MinMultATS'] * ats, min(qty_aggressive, self['MaxMultATS'] * ats)), availableQuantity)
            if (availableQuantity - qty_aggressive) <= ats:               
                qty_aggressive = availableQuantity
            
            if random() < probability:
                # Compute price and add limit price constraint.
                price_aggressive = self.marketManager.getFeedInfo(venueId, 'BEST_OPPOSITE1')
                if self['Price']:
                    price_aggressive = min(price_aggressive, self['Price']) if self.side == 1 else max(price_aggressive, self['Price'])              
                if price_aggressive == 'MX':
                    self.error('Best Opposite Market Price Equals MX: stopping tactic')
                self.log('Aggressive Order Fak: %d@%f. BidAsk: [%d|%f - %d|%f]. Probability: %f.' % (qty_aggressive, price_aggressive, self.marketManager.getFeedInfo(venueId, 'BEST_BSIZ1'), self.marketManager.getFeedInfo(venueId, 'BEST_BID1'), self.marketManager.getFeedInfo(venueId, 'BEST_ASIZ1'), self.marketManager.getFeedInfo(venueId, 'BEST_ASK1'), probability))               
                
                self.createOccurrence('MarketOrder',
                                      venue_id = venueId, 
                                      prefix = 'AggressiveTracking',
                                      tactic_params = {'OrderQty': qty_aggressive, 
                                                       'Price': price_aggressive, 
                                                       'OrderType': 'limit', 
                                                       'MarketTimeInForce': 'Fak'} 
                                    )
                reserved_quantity = qty_aggressive
                
            
        self.bus.put('%s_NoCallbacks' % self['ParentAutomatonOrderID'], callbacks + 1)
        self.appendIndicator({'AggressiveProbability': probability})
        
        ####################################################################################################################
        ######################################  Passive Tracking  ##########################################################
        ####################################################################################################################

        price = self.marketManager.getFeedInfo(venueId, 'BEST1')
        if self['Price']:
            price = min(price, self['Price']) if self.side == 1 else max(price, self['Price'])            
        if price == 'MX':
            self.error('Best Market Price Equals MX: stopping tactic')
        
        #Compute Quantity
        quantity = int(self['OptimalQty'] / self['MultDt'] * (1 + random() * 0.2 - 0.1)) # TODO: splitting of orders
        quantity = min(max(self['MinMultATS'] * ats, min(quantity, self['MaxMultATS'] * ats)), self.moneyManager.getTotalQty() - self.moneyManager.getExecQty() - reserved_quantity)
        if (self.moneyManager.getTotalQty() - self.moneyManager.getExecQty() - reserved_quantity - quantity) <= ats:            
            quantity = self.moneyManager.getTotalQty() - self.moneyManager.getExecQty() - reserved_quantity
        
        if not self.moneyManager.getPrivateBook('PassiveTracking') and self.delay <= int(self.marketManager.getFeedInfo('AE', 'C1_CONVNUM')):      
            #self.createLimitOrder(venueId, price, quantity, prefix = 'PassiveTracking' )            
            self.createOccurrence('IcebergMono', 
                                 venue_id = venueId, 
                                 prefix   = 'PassiveTracking', 
                                 tactic_params = {'Price': price, 
                                                  'OrderQty': quantity,
                                                  'MaxShow': min(ats,quantity)})
            
            self.next_deal = int(self.marketManager.getFeedInfo('AE', 'C1_CONVNUM')) + self.cycle_size
            self.order_status = True
            self.log('Insert Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity, price, self.marketManager.getFeedInfo(venueId, 'BEST_BSIZ1'), self.marketManager.getFeedInfo(venueId, 'BEST_BID1'), self.marketManager.getFeedInfo(venueId, 'BEST_ASIZ1'), self.marketManager.getFeedInfo(venueId, 'BEST_ASK1'), self.next_deal))
        
        elif self.next_deal <= int(self.marketManager.getFeedInfo('AE', 'C1_CONVNUM')) or not self.order_status:
            order_id = self.moneyManager.getPrivateBook('PassiveTracking').keys()[0] # always only 1 single passive order on the market            
            
            if (self.order_status and (self.moneyManager.getPrivateBook()[order_id]['Price'] != price or self.moneyManager.getPrivateBook()[order_id]['LeavesQty'] < quantity)) \
               or (not self.order_status and self.side * self.moneyManager.getPrivateBook()[order_id]['Price'] < self.side * price) :

                self.modifyOccurrence('IcebergMono', 
                                      venueId, 
                                      order_id, 
                                      tactic_params = {'Price': price, 
                                                       'OrderQty': quantity, 
                                                       'MaxShow': min(ats, quantity)})
                
                self.next_deal = int(self.marketManager.getFeedInfo('AE', 'C1_CONVNUM')) + self.cycle_size
                self.order_status = False
                
                self.log('Modify Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity, price, self.marketManager.getFeedInfo(venueId, 'BEST_BSIZ1'), self.marketManager.getFeedInfo(venueId, 'BEST_BID1'), self.marketManager.getFeedInfo(venueId, 'BEST_ASIZ1'), self.marketManager.getFeedInfo(venueId, 'BEST_ASK1'), self.next_deal))            
            
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
        
        # if fully filled set delay to a number of trades in function of the aggressiveness level
        if not self.moneyManager.getPrivateBook('PassiveTracking'):
            nb_trades = int(self['DelayParticipation'].split(',')[int(self['ExecutionStyle'] / 5)])
            self.delay = self.marketManager.getFeedInfo('AE', 'C1_CONVNUM') + nb_trades
            self.next_deal = self.delay
            self.log('Fully Filled Passive Placement: waiting %d trades before reposting (until %d).' % (nb_trades, self.delay))
    
        self.log('Total Exec Quantity by Simep Agent: %d' % total_qty_so_far)
        self.bus.put('%s_TotalQuantity' % self['ParentAutomatonOrderID'], total_qty_so_far)
    
    def reset(self, client_params, reject_msg):
        '''  Function Called whenever a modification is demanded
             @param client_params: the new algo parameters
             @param reject_msg: error message in case of rejection of parameters.
                                proper syntax: reject_msg.message = "error message"
             @return: one of [Tactic.Modify.ACCEPT : accept the new parameters 
                             Tactic.Modify.REINITIALIZE: accept the parameters with reintialization of the TVFO startup
                             Tactic.Modify.REJECT: dismiss the new parameters] 
        '''
        
        if client_params.has_key('Price') and self.moneyManager.getPrivateBook():
            order_id = self.moneyManager.getPrivateBook('PassiveTracking').keys()[0]
            order    = self.moneyManager.getPrivateBook('PassiveTracking')[order_id]
            
            # if the existing order has its price outside the limit price cancel it.
            if order['Price'] * self.side > client_params['Price'] * self.side:
                self.cancelOccurrence(order['VenueId'], order_id)
                return Tactic.Modify.REINITIALIZE
    
        if client_params['OrderQty'] < self.moneyManager.getExecQty() + self.moneyManager.getLeavesQty() and self.moneyManager.getPrivateBook():
            order_id = self.moneyManager.getPrivateBook('PassiveTracking').keys()[0]
            order    = self.moneyManager.getPrivateBook('PassiveTracking')[order_id]
            
            self.modifyOccurrence('IcebergMono',
                                  order['VenueId'], 
                                  order_id,
                                  tactic_params = {'OrderQty': client_params['OrderQty'],
                                                   'Price': order['Price'],
                                                   })
            return Tactic.Modify.REINITIALIZE
        
        if client_params.has_key('destination_id') and self['destinationId'] != client_params['destinationId']:
            return Tactic.Modify.REINITIALIZE
        
        return Tactic.Modify.ACCEPT
    
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
    
    

    
    