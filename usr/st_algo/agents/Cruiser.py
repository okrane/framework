'''Cruiser Tactic For Vwap

'''

# Two main variables are used to decide the actual behaviour:
# * reserve price
# * best price
#


__version__ = "1.0.0"
__author__  = 'svlasceanu@cheuvreux.com'


from random import *
from simep.core.subtactic import Tactic

class Cruiser(Tactic):
    class State:
        CRUISE = 'CRUISE'
        ACCELERATE = 'ACCELERATE'
        AGGRESSIVE = 'AGGRESSIVE'
    
    def __init__(self, setup, context, parameters):        
        Tactic.__init__(self, setup, context, parameters)
        self.next_deal      = 0        
        self.delay          = 0     
        self.delta_distance = 0
        self.delta          = 1
        self.mult_spread    = 3
        self.state          = Cruiser.State.CRUISE
        self.enable_delta   = True        
        self.not_executed   = True
        self.ref_price      = 0
        self.fillrate       = dict()
        self.valid          = False
    
    def process(self, evt):                
        if not self.marketManager.getFeedInfo('AE'):  
            self.log('No Analytical Engine image found. Skipping this event...')          
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
            if (isinstance(self.marketManager.getFeedInfo('AE', 'PARCL_SIZE'), int) or isinstance(self.marketManager.getFeedInfo('AE', 'PARCL_SIZE'), float) and
               isinstance(self.marketManager.getFeedInfo('AE', 'CONV_FAC'), int) or isinstance(self.marketManager.getFeedInfo('AE', 'CONV_FAC'), float)):
                self.cycle_size = self.marketManager.getFeedInfo('AE', 'PARCL_SIZE') * self.marketManager.getFeedInfo('AE', 'CONV_FAC')                
            else:
                self.cycle_size = self['AggregatedDealsDefault']
                
        market = self.marketManager.getFeedInfo(evt.getVenueId())
        ae = self.marketManager.getFeedInfo('AE')
          
        # TODO: modify
        self.cycle_size = min(5, ae['PARCL_SIZE'] * ae['CONV_FAC'])
        #self.ats = int(self.marketManager.getVenue(evt.getVenueId())['ats'] * (1 + random() * 0.2 - 0.1))
        
        self.ats = ae['A_QTY_3'] / ae['A_NPLRS_3'] * (1 + random() * 0.2 - 0.1)
        self.log ("[%d] ExecQty = %d. AvailableQty = %d. LeavesQty = %d. OptimalQty = %d. PrivateBook: %s. BBO: [%d@%.3f - %d@%.3f]" % 
            (self.marketManager.getFeedInfo('AE', 'C1_CONVNUM'),
             self.moneyManager.getExecQty(),
             self.moneyManager.getTotalQty() - self.moneyManager.getExecQty() - self.moneyManager.getLeavesQty(),
             self.moneyManager.getLeavesQty(), 
             self['OptimalQty'],
             self.moneyManager,
             market['BEST_BSIZ1'], market['BEST_BID1'], market['BEST_ASIZ1'], market['BEST_ASK1']))
        
        self.updateFillRateGrid(evt)        
        self.state = self.evaluateState(market, ae)
        
        if self.moneyManager.getPrivateBook('PassiveTracking'):
            self.valid = True
        
        if self.trackingReassessmentCondition(ae): 
            
            self.delta = self.reassessment('Volumic', self.computeQuantity(ae), self.ref_price, evt, ae, market)
            
            price = self.computePrice(evt, market, ae)
            quantity = self.computeQuantity(ae)                
            self.ref_price = market['BEST_OPPOSITE1']
            # if I can free up some qty from the opportunity:
            available_qty = self.moneyManager.getTotalQty() - self.moneyManager.getExecQty() - self.moneyManager.getLeavesQty()
            
            if quantity > 0:
                self.next_deal = ae['C1_CONVNUM'] + self.cycle_size
                self.log('Update Order [%d@%.3f] Next Deal: %d. HistoSpread: %f. Delta: %d. ReferencePrice: %f' % (quantity, price,self.next_deal, ae['SPREAD1'] * market['BEST_OPPOSITE1'] / 10000, self.delta, self.ref_price ))
                self.updateOccurrence('PassiveTracking', price, quantity, evt)
            self.valid = False
            self.not_executed = True        
        
    def trackingReassessmentCondition(self, ae):
        # Tracking Reassessment If:
        # Cruise Mode:
        #    * Bussiness Time Elapsed
        #    * No Valid Occurrence this bussiness interval
        #    * Exec Quantity inferior to OptimalQty
        # AccelerateMode:
        #    * Fully Filled Order
        #    * Not validated Order
        #    * Bussiness Time Elapsed
        
        cruise_condition = self.state == Cruiser.State.CRUISE and \
                           self.moneyManager.getExecQty() < self['OptimalQty'] and \
                           (self.next_deal <= ae['C1_CONVNUM'] or not self.valid)
        accelerate_condition = self.state == Cruiser.State.ACCELERATE and \
                               (not self.moneyManager.getPrivateBook('PassiveTracking') or \
                               (self.next_deal < ae['C1_CONVNUM'] or not self.valid))
        
        return cruise_condition or accelerate_condition
    
    
    
    def reassessment(self, type, quantity, ref_price, evt, ae, market):
        if type == 'Volumic':            
            tick_size = self.getReferenceData('PriceStep', venue_id=evt.getVenueId(), price=market['BEST_OPPOSITE1'])
            spread    = ae['SPREAD1'] * market['BEST_OPPOSITE1'] / 10000
                
            self.log('Volumic Reassessment of Cruiser Tactic:')
            self.log('Hist Spread: \t%.3f' % spread)
            self.log('Tick Size  : \t%.3f' % tick_size)
            self.log('Ref Price  : \t%.3f' % ref_price)
            self.log('TargetQty  : \t%d'   % quantity)
            self.log('I| Price | Volume@Price | Cumulated Value')            
            # Compute Volume Over Price
            fillrate_list = []
            price_list    = sorted(self.fillrate.keys(), reverse = (self.side == -1)) 
            for price in price_list:                
                self.log('%d| %.3f | %5d | %5d' % (int((ref_price - self.side *(spread + price))/tick_size), price, self.fillrate[price], sum(fillrate_list) + self.fillrate[price]))
                fillrate_list.append(sum(fillrate_list) + self.fillrate[price])
            self.log('-------------------------------------')    
            self.fillrate = {}
            
            # First Time Reassessment Condition: use default value for delta
            if ref_price == 0:                
                self.log('New Target Distance: %d. (First Time Evaluating)' % self.delta)
                return self.delta
            
            # If not enough volume go back to 1 spread distance            
            if not fillrate_list or quantity > max(fillrate_list):                
                self.log('New Target Distance: 0. (Target Quantity out of Fill Rate Range)')
                return 0
            else:
                index = 0
                while index < len(fillrate_list) and quantity > fillrate_list[index]:
                    index += 1                
                self.log ('New Target Distance: %d' % (int((ref_price - self.side * (spread + price_list[index])) / tick_size)))
                return (ref_price - self.side * (spread + price_list[index])) / tick_size          
            #self.delta = self.side * (ref_price - sorted(self.fillrate.keys(), reverse = (self.side == -1))[index])
        if type == 'Gradient':
            if self.not_executed:
                return self.delta - 1
            elif not self.moneyManager.getPrivateBook('PassiveTracking'):
                return self.delta + 1        
            return self.delta
    
    def updateFillRateGrid(self, evt):
        # if new trades:
        #if 'DEAL_TIME' in self.marketManager.getUpdateList(evt.getVenueId()):
        market = self.marketManager.getFeedInfo(evt.getVenueId())
        if market['TRADE_EVENT']:
            tr = self.marketManager.getTrades()            
            self.fillrate.setdefault(tr[0].price, 0)
            self.fillrate[tr[0].price] += tr[0].size
                    
    
    def evaluateState(self, market, ae):
        if self.side == 1: # buying
            if self['SIGNAL_FW'] < 0.4 or self['SIGNAL_BW'] < 0.25: 
                return Cruiser.State.ACCELERATE
            return Cruiser.State.CRUISE
        if self.side == -1: # selling
            if self['SIGNAL_FW'] > 0.6 or self['SIGNAL_BW'] > 0.75: 
                return Cruiser.State.ACCELERATE
            return Cruiser.State.CRUISE
    
                 
          
    def updateOccurrence(self, prefix ,price, quantity, evt):
        if self.moneyManager.getPrivateBook(prefix):
            order_id = self.moneyManager.getPrivateBook(prefix).keys()[0]
            self.modifyOccurrence('IcebergMono',
                                  evt.getVenueId(),
                                  order_id,
                                  tactic_params = {'OrderQty': quantity, 
                                                   'Price': price, 
                                                   'OrderType': 'limit',
                                                   'MaxShow': self.ats})
        else:
            self.createOccurrence('IcebergMono',
                                  evt.getVenueId(),
                                  prefix = prefix,
                                  tactic_params = {'OrderQty': quantity, 
                                                   'Price': price, 
                                                   'OrderType': 'limit',
                                                   'MaxShow': self.ats})
    def processReport(self, evt):      
        self.not_executed = False
    
    def computePrice(self, evt, market, ae):
        self.tick_size = self.getReferenceData('PriceStep', venue_id=evt.getVenueId(), price=market['BEST_OPPOSITE1'])
        spread = ae['SPREAD1'] * market['BEST_OPPOSITE1'] / 10000        
       
        self.log('Algo State: %s' % self.state)        
        if self.state == Cruiser.State.CRUISE:            
            self.log('Best Opposite: \t %.3f' % market['BEST_OPPOSITE1'])            
            self.log('Limit Price  : \t ' + 'None' if not self['Price'] else '%.3f' % self['Price'])
            self.log('Target Price = BestOpposite - HistoSpread - Distance x Tick Size')
            self.log('Target Price = %.3f - %.3f - %d x %.3f' % (market['BEST_OPPOSITE1'], spread, self.delta, self.tick_size))
            self.log('Target Price = %.3f' % (market['BEST_OPPOSITE1'] - self.side * (spread + self.delta * self.tick_size)))
            self.log('Max (Non-Aggressive) Price: %.3f' % (market['BEST_OPPOSITE1'] - self.side * self.tick_size))
            price = self.__bestBetween(market['BEST_OPPOSITE1'] - self.side * (spread + self.delta * self.tick_size), 
                                       market['BEST_OPPOSITE1'] - self.side * self.tick_size)

        if self.state == Cruiser.State.ACCELERATE:            
            price = self.__worstBetween(market['BEST1'], market['BEST_OPPOSITE1'] - self.side * spread)
        
        if price == 'MX':
            self.error('Best Market Price Equals MX: stopping tactic')
        if self['Price']:
            price = self.__bestBetween(self['Price'], price)            
        
        self.log('New Limit Price: %.3f' % price)
        self.log('----------------------------------------')
        return price
    
    def __worstBetween(self, *args):
        return max(args) if self.side == 1 else min(args)
    
    def __bestBetween(self, *args):
        return min(args) if self.side == 1 else max(args)

    def computeQuantity(self, ae):        
        if self.state == Cruiser.State.CRUISE:
            quantity = min((self['OptimalQty'] - self.moneyManager.getExecQty()), self['OptimalQty'] / self['MultDt'])
            if quantity > 0:
                quantity = min(max(self['MinMultATS'] * self.ats, min(quantity, self['MaxMultATS'] * self.ats)), self.moneyManager.getTotalQty() - self.moneyManager.getExecQty())
            else:
                quantity = 0
                
        if self.state == Cruiser.State.ACCELERATE:
            quantity = self['OptimalQty'] / self['MultDt']
            quantity = min(max(self['MinMultATS'] * self.ats, min(quantity, self['MaxMultATS'] * self.ats)), self.moneyManager.getTotalQty() - self.moneyManager.getExecQty())
            
        return quantity
    
    
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
        return {'AE':(self['vwapSource'], 'S=%d@%s;P=bEHRC;PL=MW;Z=UTC;H=4;F=%s;T=%s;EX=top;EX=cross;EX=auc' % (self['InstrumentID'], self.trading_destination, self['EffectiveTime'][9:], self['ExpireTime'][9:]) )}
    