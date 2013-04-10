'''Scripted PVOL Algorithm

'''
__version__ = "1.0"
__author__  = 'midan@cheuvreux.com'
__reviewer__= 'clehalle@cheuvreux.com'
__review_date__ = '11/02/2011'

# import under AlgoBox
#from simep.tactic import Tactic

# import under SIMEP
from simep.core.subtactic import Tactic
from random import *

class ScriptedPVOL(Tactic):
    def __init__(self, setup, context, parameters):
        
        Tactic.__init__(self, setup, context, parameters)
        self.current_state = {}
        self.current_taps  = {} 
        self.deltaVolumes = [0.0, 0.0, 0.0]
        self.curZone = -1
        self.ats = 0.0
        self.qmin = 0.0
        self.qmax = 0.0
        self.qmid = 0.0
        self.delta_modify_qty =0
        # self.coeffRate = (self['benchmark_rate_min'] - self['benchmark_rate_max']) / ()            
    
    
    def validateModifyParameters(self,client_params, rejection_message):
        
        
        if client_params['ParticipationRate'] < client_params['MinPctVolume']:
            
            return False
        
        best_opposite = self.marketManager.getFeedInfo(self.primary_venue_id, 'BEST_OPPOSITE1')
        spread = self.marketManager.getFeedInfo(self.primary_venue_id, 'BEST_ASK1') -self.marketManager.getFeedInfo(self.primary_venue_id, 'BEST_BID1')   
        
        if client_params['WouldLevel'] * self.side >=  best_opposite + self.side * spread: 
            # reject, use  another algo !
            rejection_message.message = "validateModifyParameters: ORde rejected because of WouldLevel"
            return  False 
        
        if client_params['ParticipationRate'] < client_params['MinPctVolume']:
            rejection_message.message = "validateModifyParameters: Order  rejected because of inconsistant Min % and Max %"
            return False
        
        return  True
        
               
    

    def reasses_on_modify(self, OrderPrefix, qty_to_free):
        
        tracking_orders = list()
        
        for occ in self.moneyManager.getPrivateBook(OrderPrefix).keys():
            tracking_orders.append((occ, self.moneyManager.getPrivateBook()[occ]['LeavesQty'], self.moneyManager.getPrivateBook()[occ]['Price']))
        
        # TODO : Test this
        #sort the orders (far from the best) first
        
        tracking_orders.sort(key=lambda order:order[2], reverse=self.side == 1)
        for order in tracking_orders:
            self.modifyOccurrence(tactic_type='IcebergMono', venue_id=self.moneyManager.getPrivateBook()[order[0]]["VenueId"], oc_id=order[0], tactic_params={'Price':order[2], 'OrderQty':max(0, order[1] - qty_to_free)})
            qty_to_free = qty_to_free - order[1]
            if qty_to_free <= 0:
                # no need ton cancel more
                return 0
        return qty_to_free

    def reset(self, client_params, reject_msg):
       
        # check validity of the new parameters and reject if necessary 
        if not self.validateParameters(client_params, reject_msg):
            return Tactic.Modify.REJECT
        
        # other rejection case 
        if client_params['OrderQty'] < self.moneyManager.getExecQty():
            # we need to reject 
            # display reason for rejection
            reject_msg.message = 'Modifcation rejected : Asked quantity is less than quantity already executed'
            return Tactic.Modify.REJECT
        
        
        
        # Cancel and Replace
        
        base_volume_computation_changed = (client_params['ParticipationRate'] != self['ParticipationRate']) or (
        client_params['MinPctVolume'] != self['MinPctVolume']) or (
        client_params['Price'] != self['Price'])
               
        
        if base_volume_computation_changed :
            # Cancel & Replace
            # we restart completely the tactic, in any cases,  ? 
            # what we should do with the occurences ? Cancel  all of them   
            for occ in self.moneyManager.getPrivateBook().keys():
                venue_id  = self.moneyManager.getPrivateBook()[occ]["VenueId"]
                self.cancelOccurrence(venue_id, occ)
            #record the modification 
            self.delta_modify_qty += self.moneyManager.getExecQty()
            #reset the tactic
            return Tactic.Modify.REINITIALIZE
        
        # manage new quantity by respect to executed quantity
            
        if client_params['OrderQty'] > self.moneyManager.getLeavesQty():
            # we can continue, with the new asked quantity
            # update the available Quantity with the new Amount
            self.moneyManager.setTotalQuantity(client_params['OrderQty'])
            return Tactic.Modify.ACCEPT
            
        # manage new quantity by respect to leaves quantity
        
        if client_params['OrderQty'] < self.moneyManager.getLeavesQty():
            # we will need to free some quantity using the following order :
            # first cancel passive occurence, order by price , best price (far from the best) first

            # Initialize the Quantity to free 
            qty_to_free =  self.moneyManager.getLeavesQty() - client_params['OrderQty'] 
            
            # Passive (Iceberg + TAP)
            qty_to_free = self.reasses_on_modify("PassiveTracking", qty_to_free)
            
            # Aggressive : if qty_to_free == 0 , the following function will do nothing
            qty_to_free = self.reasses_on_modify("AggressiveTracking", qty_to_free)
            
            return Tactic.Modify.REINITIALIZE
            
        # otherwise accept the modifications
        
        return Tactic.Modify.ACCEPT
    
    def process(self, evt):
        ''' Function to be called for each update on the market 
            @return: 
                - compute thresholds for delta volume --> the current zone
                - depending on current zone --> decide to launch (tracking and/or slippage) tactic
        '''
        # update ats
        self.ats = int(self.marketManager.getFeedInfo('AE', 'A_QTY_3') / self.marketManager.getFeedInfo('AE', 'A_NPLRS_3') * (1 + random() * 0.2 - 0.1))
        # adjust rate        
        self.rateAdjustment(evt)
        # update current_state       
        self.updateCurrentState(evt)
        # compute different delta volume
        self.deltaVolumes = self.computeVolumeThreshold(evt)
        # determine the current zone
        self.curZone = self.currentZone()
        # compute the imblance
        self.computeImbalance(evt)
        
        self.log ("[%d] ExecQty = %d. RemainingQty = %d. AvailableQty = %d. LeavesQty = %d. PrivateBook: %s" % 
            (self.marketManager.getFeedInfo('AE', 'C1_CONVNUM'),
             self.moneyManager.getExecQty(), 
             self.moneyManager.getTotalQty() - self.moneyManager.getExecQty(),
             self.moneyManager.getTotalQty() - self.moneyManager.getExecQty() - self.moneyManager.getLeavesQty(),
             self.moneyManager.getLeavesQty(),             
             self.moneyManager))
        
        # depending on the current zone, switch to the suitable tactics        
        self.tracking(evt)    
        self.slippage(evt)
        self.opportunity(evt)
    
    def processReport(self, evt):
        total_qty_so_far = self.bus.get('%s_TotalQuantity' % self['ParentAutomatonOrderID'])
        if not total_qty_so_far:
            total_qty_so_far = 0
        for x in self.moneyManager.getLastExecutions():
            self.log("Order Filled: Qty=%d Price=%f" % (int(x['Quantity']), float(x['Price'])))
            total_qty_so_far += int(x['Quantity'])
        
        self.log('Total Exec Quantity by Simep Agent: %d' % total_qty_so_far)
        self.bus.put('%s_TotalQuantity' % self['ParentAutomatonOrderID'], total_qty_so_far)
        self.updateIndicators()
        
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
    
    def computeVolumeThreshold(self, evt):
        
        ''' Compute the 3 thresholds for the delta volume
            @return:
                List: (Q_min(t), Q_max(t), Q_opt(t))
        '''
        # get the current market volume up to time t
        mktVolume = self.marketManager.getFeedInfo('AE', 'QTE_CNT1')
        # adjust executed qty by respect to modify received
        excVolume = self.moneyManager.getExecQty() - self.delta_modify_qty
        
        minDeltaVolume = (self.qmin * mktVolume - excVolume + self.qmin * self.estimateVolume())/(1.0-self.qmin)
        maxDeltaVolume = (self.qmax * mktVolume - excVolume + self.qmax * self.estimateVolume())/(1.0-self.qmax)
        
        # using predicted volume for the next period           
        optDeltaVolume = (self.qmid * mktVolume - excVolume + self.qmid * self.estimateVolume())/(1.0-self.qmid)
        
        return [minDeltaVolume, optDeltaVolume, maxDeltaVolume]
            
    def rateAdjustmentWrtFixing(self):
        ''' Adjust the participation rate w.r.t. error due to fixing volume   
            @return:
                adjusted rate             
        '''
        
    def rateAdjustment(self,evt):
        ''' Adjust the participation rate w.r.t. benchmark (EVP, Strict, PVOL)   
            @return:
                adjusted rate            
        '''
                   
        if ((self['benchmark'] == 'SVT') or (self['benchmark'] == 'PVOL')):
            self.qmin = self['MinPctVolume'] * 0.01
            self.qmax = self['ParticipationRate'] * 0.01
        elif (self['benchmark'] == 'EVP'):
            qmid = 0.01* (self['ParticipationRate'] + (self.marketManager.getFeedInfo(evt.getMarketId(), 'TIME_NUM') - self.beginTime)*self.coeffRate)
            self.qmin = qmid - 0.01 
            self.qmax = qmid + 0.01
        
        self.qmid = 0.5 * (self.qmin + self.qmax)        
    
    def estimateVolume(self):
        ''' Compute an estimation for the next period's volume
            @return: 
                Volume of market solely for the next period
            TODO: using a Moving Average with respect to the volume curve
        '''
        return 0.0
    
    def currentZone(self):
        ''' Determine the current zone for the algorithm
            @return:
                the current zone (1, 2 or 3) 
        '''
        curZone = sum([d <= 0 for d in self.deltaVolumes])
        return curZone
    
    def tracking(self, evt):
        ''' Tactic "tracking" 
            @precondition: in each zone
            @postcondition: 2 limit orders are placed if necessary
        '''
        
        checkXFire = False
        volumetoadd1 = 0
        volumetoadd2 = 0 
        
        curXFire = self.moneyManager.getPrivateBook('PassiveTrackingTAP')
        if curXFire:
            existing_leave_quantity = curXFire[curXFire.keys()[0]]['LeavesQty']
        else:
            existing_leave_quantity = 0
        
         
            
            
                
        # verify living volume in the order book        
        # determine the max volume place-able = min(Q_max - Q_t, Q_max - Q_opt)
        if (self.curZone == 3): # AVANCE
            # cancel all orders            
            for key_price in self.current_state.keys():
                # cancel this order
                self.cancelOrder(evt.getMarketId(), self.current_state[key_price][1])
                # and delete it from the current_state
                del self.current_state[key_price] 
        else:
            deltaVolumes = self.deltaVolumes
            
            availableQty = min(deltaVolumes[2], deltaVolumes[2] - deltaVolumes[1])
            
            market = self.marketManager.getFeedInfo(evt.getMarketId())
            
            # FIRST ORDER
            iFirst = 1 
            # verify the current zone
            cum_vol = 0
            find_price = False
            if (self.curZone <= 1): #if LATE -> always place an order at the first limit
                best_price = market['BEST1']
                cum_vol  = market['BEST_SIZE1']
                first_quantity_to_be_placed = self['TrackingLimitOrder1'] * cum_vol * self.qmid
                find_price = True
            else: # otherwise
                '''
                    From the current evt, compute the price and volume of the tracking orders to be placed
                    - the first order will be made at the price such that : cum. vol. up to price (from the best) cum_vol >= 1/q ATS, qty = max(ATS, q * cumVol)
                '''            
                while ((iFirst < 5) and not (find_price)): 
                    best_price = market['BEST%d' % iFirst]
                    best_size  = market['BEST_SIZE%d' % iFirst]
                    cum_vol += best_size
                    find_price = (self['TrackingLimitOrder1'] * cum_vol * self.qmid >= self.ats)
                    iFirst+=1
                # if there is not enough volume at the first 5 limits 
                if not find_price:
                    pass
                # then determine the quantity to be placed
                '''
                TODO: estimate constants TrackingLimitOrder1,2 for i=1,2
                '''
                first_quantity_to_be_placed = self['TrackingLimitOrder1'] * cum_vol * self.qmid
                
            # SECOND ORDER
            '''
                From the current evt, compute the price and volume of the tracking orders to be placed, for the second order         
                - distance Delta w.r.t. to the first price
                - such that there is 1/q * ATS between the first order and the second order   
            '''        
            # verify the current zone
            iSecond = iFirst 
            cum_vol = 0
            find_price = False
            if (self.curZone <= 1): #if LATE -> always place an order at the first limit + Delta
                second_price = market['BEST%d' % min(iFirst+self['TrackingDelta'],5)]            
                second_quantity_to_be_placed = self.ats
                find_price = True
            else: # otherwise
                '''
                    From the current evt, compute the price and volume of the tracking orders to be placed
                    - the first order will be made at the price such that : cum. vol. up to price (from the best) cum_vol >= 1/q ATS, qty = max(ATS, q * cumVol)
                '''            
                while ((iSecond < 5) and not (find_price)): 
                    second_price = market['BEST%d' % iSecond]
                    best_size  = market['BEST_SIZE%d' % iSecond]
                    cum_vol += best_size
                    find_price = (self['TrackingLimitOrder2'] * cum_vol * self.qmid >= self.ats)                
                    iSecond+=1
                    
                if not find_price:
                    # if there is not enough volume at the first 5 limits
                    # then place an order at the distant Delta w.r.t. the first order, but must be within the 5th limit
                    second_price = market['BEST%d' % min(iFirst+self['TrackingDelta'],5)]
                    cum_vol  = sum([market['BEST_SIZE%d' % j] for j in range(iFirst, min(iFirst+self['TrackingDelta'], 5))])
                    second_quantity_to_be_placed = self['TrackingLimitOrder2'] * cum_vol * self.qmid
                    find_price = True
                else:
                    # if there is enough volume
                    # then 
                    second_price = min(second_price, market['BEST%d' % min(iFirst+self['TrackingDelta'],5)]) if (self.side== 1) else max(second_price, market['BEST%d' % min(iFirst+self['TrackingDelta'],5)])
                    cum_vol = sum([market['BEST_SIZE%d' % j] for j in range(iFirst, iFirst+self['TrackingDelta'], iSecond)])
                    second_quantity_to_be_placed = self['TrackingLimitOrder2'] * cum_vol * self.qmid
            
            # VOLUME COLLECTOR
            sorted_prices = sorted(self.current_state.keys(), reverse = (self.side == 1))        
            # modify the first order to the best_price
            leaveQty = 0
            if (len(sorted_prices) >= 1):
                # place first order by modifying the first price
                # for small quantity, round up to 1 ats
                quantity = max(self.ats, first_quantity_to_be_placed)
                # modify only the quantity if necessary
                # condition on price 
                if (sorted_prices[0] != best_price):
                    # condition on quantity
                    volumetoadd1 = quantity - self.current_state[sorted_prices[0]][0] 
                    if ( volumetoadd1 > self.ats):
                        
                        self.log("Tracking - Place the first IcebergMono - Modify existing order")                   
                        
                        # manage qty in the xfire here ? 
                        # if we have already some qty in the xfire, we will always first use this quantity
                        
                        volume_to_remove1 = min(quantity,existing_leave_quantity)
                        checkXFire = True
                        
                        # TODO : we have to decide if we want create a new ordre (in order not to loose the priority), or if we modify the whole order 
                        
                        if  :
                            # we don't care loosing priority, and we areally modify the order.
                            # in this case, if there is already an order at the same price, we have to merge the order to create a single one
                            
                        self.modifyOccurrence(tactic_type = 'IcebergMono',
                                         venue_id = evt.getMarketId(),                             
                                         oc_id    = self.current_state[sorted_prices[0]][1],
                                         tactic_params = {'Price': best_price, 
                                                          'OrderQty': quantity})
                        else:
                            # we  care loosing priority so we send a new order on the same limit
                            self.createOccurrence(tactic_type = 'IcebergMono',
                                     venue_id = evt.getMarketId(),
                                     prefix   = 'PassiveTracking',                                 
                                     tactic_params = {'Price': best_price, 
                                                      'OrderQty': volumetoadd1,
                                                      'MaxShow': min(self.ats, quantity, self.imbalance)})
                        # TODO : we have to decide if we want create a new order (in order not to loose the priority), or if we modify the whole order
                        leaveQty = quantity
                    else:
                        # if not enough qty, add to XFire
                        checkXFire = True

                else:
                    leaveQty = self.current_state[sorted_prices[0]][0]
            
                if (len(sorted_prices) >= 2):            
                    # place second order by modifying the second price
                    # for small quantity, round up to 1 ats
                    
                    quantity = max(self.ats, min(second_quantity_to_be_placed, availableQty - leaveQty))
                    # modify only the quantity if necessary
                    # condition on price
                    if (sorted_prices[1] != second_price):
                        # condition on quantiy
                        volumetoadd2 = quantity - self.current_state[sorted_prices[1]][0] 
                        if (volumetoadd2 > self.ats):
                            
                        # manage qty in the xfire here  
                        # if we still have some qty in the xfire, we will first use this quantity
                        
                            volume_to_remove2 = min(quantity, max(0,existing_leave_quantity - volume_to_remove1))
                            checkXFire = True
                            self.log("Tracking - Place the second IcebergMono - Modify existing order")
                            self.modifyOccurrence(tactic_type = 'IcebergMono',
                                             venue_id = evt.getMarketId(),                                 
                                             oc_id    = self.current_state[sorted_prices[1]][1],
                                             tactic_params = {'Price': second_price, 
                                                              'OrderQty': quantity})
                        else:
                            # if not enough qty, add to XFire
                            checkXFire = True
                    
                    # and cancel all others
                    for j in range(2,len(sorted_prices)):
                        self.log("Tracking - Cancel all existing order")
                        # cancel this order
                        self.cancelOrder(evt.getMarketId(), self.current_state[sorted_prices[j]][1])
                        # and delete it from the current_state
                        del self.current_state[sorted_prices[j]]
                    
                    
                else:
                    # place second order by inserting at new price
                    self.log("Tracking - Place the second IcebergMono - Insert new order")
                    # for small quantity, round up to 1 ats
                    quantity = max(self.ats, min(second_quantity_to_be_placed, availableQty - leaveQty))
                    # insert new order
                    self.createOccurrence(tactic_type = 'IcebergMono',
                                     venue_id = evt.getMarketId(),
                                     prefix   = 'PassiveTracking',                                 
                                     tactic_params = {'Price': second_price, 
                                                      'OrderQty': quantity,
                                                      'MaxShow': min(self.ats, quantity, self.imbalance)})
                    
            else:
                # insert 2 new orders
                self.log('Tracking - Insert two IcebergMono')
                quantity = max(self.ats, first_quantity_to_be_placed)
                self.createOccurrence(tactic_type = 'IcebergMono',
                                 venue_id = evt.getMarketId(),
                                 prefix   = 'PassiveTracking',                                 
                                 tactic_params = {'Price': best_price, 
                                                  'OrderQty': quantity,
                                                  'MaxShow': min(self.ats, quantity, self.imbalance)})
                leaveQty = quantity
                
                if (availableQty > leaveQty):
                    quantity = max(self.ats, min(second_quantity_to_be_placed, availableQty - leaveQty))
                    self.createOccurrence(tactic_type = 'IcebergMono',
                                     venue_id = evt.getMarketId(),
                                     prefix   = 'PassiveTracking',                                 
                                     tactic_params = {'Price': second_price, 
                                                      'OrderQty': quantity,
                                                      'MaxShow': min(self.ats, quantity, self.imbalance)})
            # check if there is additional volume
            volume_to_remove = volume_to_remove1 + volume_to_remove2
            volumetoadd = volumetoadd1 + volumetoadd2 - volume_to_remove
            new_xfire_qty = existing_leave_quantity + volumetoadd
            
            if checkXFire: 
                if curXFire:
                    if  new_xfire_qty > 0:
                    # depending on the quantity in this xfire, we may move the quantity to a Limit  
                    
                        self.log('Tracking - Modify TAP')
                        self.modifyOccurrence(tactic_type = 'TAP',
                                              venue_id = evt.getMarketId(), 
                                              oc_id = curXFire.keys()[0],
                                              tactic_params = {'OrderQty': new_xfire_qty,
                                                               'Price': best_price}
                                              )
                    else :
                        # we can cancel everything, no more available quantity
                        self.log('Tracking - Cancel  TAP')
                        
                        self.cancelOccurrence(venue_id = evt.getMarketId(), 
                                              oc_id = curXFire.keys()[0])
                else:
                    if  new_xfire_qty > 0:
                        # create new specific one
                        self.log('Tracking - Place TAP')                    
                        self.createOccurrence(tactic_type = 'TAP',
                                          venue_id = evt.getMarketId(), 
                                          prefix = 'PassiveTrackingTAP',
                                          tactic_params = {'OrderQty': new_xfire_qty, 
                                                           'Price': best_price}
                                          ) 
                    
    def mergeXfireParamsWithConfig(self, xfire_params):
        
        occurence_params= xfire_params
        
        # check presence of venue type for aggressive
        if 'LIT' in self['ExcludeAggressiveType'] or 'LIT' in xfire_params['ExcludeAggressiveType']:
            occurence_params['ExcludeAggressiveType'] = 'LIT '
             
        if 'MID' in self['ExcludeAggressiveType'] or 'MID' in xfire_params['ExcludeAggressiveType']:
            occurence_params['ExcludeAggressiveType'] += 'MID '
            
        if 'DARK' in self['ExcludeAggressiveType'] or 'DARK' in xfire_params['ExcludeAggressiveType']:
            occurence_params['ExcludeAggressiveType'] += 'DARK'
            
        # check presence of venue type for passive
        if  'LIT' in self['ExcludePassiveType'] or 'LIT' in xfire_params['ExcludePassiveType']:
            occurence_params['ExcludePassiveType'] = 'LIT '
        if  'MID' in self['ExcludePassiveType'] or 'MID' in xfire_params['ExcludePassiveType']:
            occurence_params['ExcludePassiveType'] += 'MID '
        if  'DARK' in self['ExcludePassiveType'] or 'DARK' in xfire_params['ExcludePassiveType']:
            occurence_params['ExcludePassiveType'] += 'DARK'
        
        # manage exclusion of primary venue
        
        if  'Y' in self['ExcludePrimary'] or 'Y' in xfire_params['ExcludePrimary']:
            occurence_params['ExcludePrimary'] ='Y'
        else:
            occurence_params['ExcludePrimary'] ='N'
        
        return occurence_params    
                                                        
    
    def slippage(self, evt):
        ''' Tactic "slippage"
            @precondition: Q(t) < Q_opt(t)            
        '''        
        deltaVolumes = self.deltaVolumes
        market = self.marketManager.getFeedInfo(evt.getMarketId())
        
        # if Q(t) < Q_opt(t)
        
        if deltaVolumes[1] > 0:
            
            quantity = max(self.ats, deltaVolumes[1])
            
            if deltaVolumes[0] > 0: # if Q(t) <= Q_min(t)
                
                # place an TAP  order up to Q_opt(t)                
                curXFire = self.moneyManager.getPrivateBook('ModerateTracking')
                price = market['BEST_OPPOSITE1']
                
                # Manage Venues here : we need also Primary + MTF + Dark + Mid , so we do not excluse everything
                xfire_params = self.mergeXfireParamsWithConfig({'OrderQty': quantity, 
                                                                'Price': price,
                                                                'ExcludeAggressiveType' : '',
                                                                'ExcludePassiveType' : ''})
                
                if not curXFire:
                
                    self.createOccurrence(tactic_type = 'TAP',
                                      venue_id = evt.getMarketId(), 
                                      prefix = 'ModerateTracking',
                                      tactic_params = xfire_params)
                else:
                    self.modifyOccurrence(tactic_type = 'TAP',
                                      venue_id = evt.getMarketId(), 
                                      oc_id = curXFire.keys()[0],
                                      tactic_params = xfire_params)
            else: # if Q_min(t) < Q(t) < Q_opt(t)
                
                
                # get the current XFire if exist
                curXFire = self.moneyManager.getPrivateBook('ModerateTracking')
                
                mid_price = 0.5 * (market['BEST1'] + market['BEST_OPPOSITE1'] )
                
                # we may exclude everything in passive, maybe something to check
                xfire_params = self.mergeXfireParamsWithConfig({'OrderQty': quantity, 
                                                                'Price': mid_price,
                                                                'ExcludeAggressiveType' : 'L',
                                                                'ExcludePassiveType' : 'L'})
                
                # if there is no XFire
                if not curXFire:
                    # create a Cross Fire
                    self.log('Slippage - Place TAP') 
                    self.createOccurrence(tactic_type = 'TAP',
                                          venue_id = evt.getMarketId(), 
                                          prefix = 'ModerateTracking',
                                          tactic_params = xfire_params)
                elif (quantity - curXFire[curXFire.keys()[0]]['LeavesQty']) > self.ats:
                    # modify only the quantity
                    
                    self.log('Slippage - Modify TAP')
                    self.modifyOccurrence(tactic_type = 'TAP',
                                          venue_id = evt.getMarketId(), 
                                          oc_id = curXFire.keys()[0],
                                          tactic_params = xfire_params)
    def opportunity(self, evt):
        '''
            Tactic "opportunity"
        '''
        # - price : when Deep In the Money
        
        
        
    def updateCurrentState(self, evt):
        ''' 
            From the current evt, update the current state
        '''
        # initialize the current_state
        self.current_state = {}

        # and update the current_state with live orders
        for order_id, order in self.moneyManager.getPrivateBook('PassiveTracking').iteritems():
            self.current_state[float(order['Price'])] = [int(order['LeavesQty']), order_id]
        
            
    
    def updateIndicators(self):
        
        time = self.indicators.date[len(self.indicators.date)-1]
        tactic_id = self.indicators.value['tactic_id'][-1]
        self.append_indicator({'event': 'UPDATE',
                               'tactic_id': tactic_id,
                               'total_quantity': self.moneyManager.getExecQty(),
                               'min_delta': self.deltaVolumes[0],
                               'opt_delta': self.deltaVolumes[1],
                               'max_delta': self.deltaVolumes[2], 
                               'current_zone': self.curZone,
                               'mkt_volume': self.marketManager.getFeedInfo('AE', 'QTE_CNT1')}, time)   
    
    def computeImbalance(self, evt):
        
        market = self.marketManager.getFeedInfo(evt.getMarketId())
        self.imbalance = (market['BEST_OPPOSITE_SIZE1'] + market['BEST_OPPOSITE_SIZE2'] - market['BEST_SIZE1'] - market['BEST_SIZE2'])
        
        