'''Scripted PVOL Algorithm'''
'__version__ = "1.0"'
'__author__ = ''midan@cheuvreux.com'''
'__reviewer__ = ''clehalle@cheuvreux.com'''
'__review_date__ = ''11/02/2011'''

# import under AlgoBox
#from simep.tactic import Tactic

# import under SIMEP
from simep.core.subtactic import Tactic
from random import *
import  math 


#from scipy.stats.mstats import mquantiles

class ScriptedPVOL(Tactic):
    class MarketState:
        REGULAR  = 1
        INVALID = 2
        WAIT    = 3
     

               
        
    def __init__(self, setup, context, parameters):
        Tactic.__init__(self, setup, context, parameters)
        self.next_slippage_deal = 0
        self.current_state = {}
        self.current_taps = {} 
        self.deltaVolumes = [0.0, 0.0, 0.0]
        self.curZone = -1
        self.ats = 0.0
        self.qmin = 0.0
        self.qmax = 0.0
        self.qmid = 0.0
        self.algo_starting = True
        self.slippage_volume = 0
        self.slippage_amount = 0
        self.tracking_volume = 0
        self.delta_modify_qty = 0
        self.tracking_amount = 0
        self.opportunity_volume = 0 
        self.opportunity_amount = 0
        self.in_the_money = False
        self.fwd_signal = 0.5
        self.bwd_signal = 0.5
        self.is_in_auction = False
        self.would_qty = 0
        self.total_would_qty = 0
        self.would_amount    = 0
        #test 
        self.mkt_qty = 0
        self.last_mkt_qty = 0
        # opportuniy signal
        self.total_opportunity_volume = 0
        self.best_fwd_opp_signal = 0
        #  
        self.all_fwd_signal = list()
        self.all_bwd_signal = list()
        
        
        self.min_fwd = 0
        self.max_fwd = 0
        
        self.min_bwd = 0
        self.max_bwd = 0
        
    def __worstBetween(self, *args):
        return max(args) if self.side == 1 else min(args)
    
    def __bestBetween(self, *args):
        return min(args) if self.side == 1 else max(args)


    def checkParameters(self, parameters, error):
        self._parameters = dict(parameters)
        # Buy = 1, Sell = 2
        
        if self['MinPctVolume'] == None and self['ParticipationRate'] == None :
            error.message = "Invalid Participation Rates : At least one percentage must be known"
            return False
        
        
        if self['MinPctVolume'] == None and self['ParticipationRate'] != None :
            #value by default Min % = Max % - 0.1  * Max % 
            self._parameters['MinPctVolume'] = max(0,0.9 * self['ParticipationRate'])
            
            
        if self['MinPctVolume'] != None and self['ParticipationRate'] == None :
           #value by default Max % = Min % + 0.1  * Min % 
            self._parameters['ParticipationRate'] = min(100,1.1 * self['MinPctVolume'])
        
      

        
        if  self['MinPctVolume']> self['ParticipationRate']:
            error.message = "Invalid Participation Rates : MinPctVolume (%s) must be < to ParticipationRate(%s)"%(self['MinPctVolume'],self['ParticipationRate'])
            return False
        
        if self["FwdQuantile"]:
            self.fwd_quantile = float(self["FwdQuantile"])
            
        if self["BwdQuantile"]:
            self.bwd_quantile = float(self["BwdQuantile"])
            
            
        if self['Side'] == '1' or self['Side'] == 1:
            self.side = 1
        else:
            self.side = -1
            
        if self["Price"]:
            self.limit_price = float(self["Price"])
        elif self.side == 1:
            self.limit_price = 1e99    
        else:
            self.limit_price = -1e99
    
        if self["AdjustedPriceInd"]:
            self.would_level  = float(self["AdjustedPriceInd"])
        elif self.side == 1:
            self.would_level = 0
        elif self.side == -1:
            self.would_level  = 1e99

        if self['OpeningAuctionPolyCoeff']:
            self.openingAuctionPolyCoeff = [float(self['OpeningAuctionPolyCoeff'].split(',')[i]) for i in range(5)]
        if self['ClosingAuctionPolyCoeff']:
            self.closingAuctionPolyCoeff = [float(self['ClosingAuctionPolyCoeff'].split(',')[i]) for i in range(5)]
        
        #delta_fwdthreshold=0.05
        #delta_bwdthreshold=25

        
        self.fwdThreshold = 0.5 - self.side *  self['delta_fwdthreshold']/100
        self.bwdThreshold = 0.5 - self.side *  self['delta_bwdthreshold']/100
        
        self.next_opportunity_deal = 0
        
        return True           
    
    
    def validateModifyParameters(self, client_params, rejection_message):
       
        if client_params['ParticipationRate'] < client_params['MinPctVolume']:
            return False
        
        best_opposite = self.marketManager.getFeedInfo(self.primary_venue_id, 'BEST_OPPOSITE1')
        spread = self.marketManager.getFeedInfo(self.primary_venue_id, 'BEST_ASK1') - self.marketManager.getFeedInfo(self.primary_venue_id, 'BEST_BID1')   
        
        if client_params.has_key('WouldLevel') and client_params['WouldLevel'] * self.side >= best_opposite + self.side * spread: 
            # reject, use  another algo !
            rejection_message.message = "validateModifyParameters: Order rejected because of WouldLevel"
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

    def validateParameters(self):
                # retrieve trading hours 
        return True
    
    
    
    def reset(self, client_params, reject_msg):
       
       
        self.log("[RESET] Processing  modification")
        # check validity of the new parameters and reject if necessary 
        if not self.validateModifyParameters(client_params, reject_msg):
            reject_msg.message = "Rejecting parameters"
            return Tactic.Modify.REJECT
        
        # other rejection case 
        if client_params['OrderQty'] < self.moneyManager.getExecQty():
            # we need to reject 
            # display reason for rejection
            reject_msg.message = 'Modification rejected : Asked quantity is less than quantity already executed'
            return Tactic.Modify.REJECT
        
        
        
        # Cancel and Replace
        
        
        limit_price_has_not_changed =  (client_params.has_key('Price') and self['Price'] and  self['Price'] == client_params['Price'] ) or  (not client_params.has_key('Price') and self['Price'] == None)
        would_price_has_not_changed =  (client_params.has_key("AdjustedPriceInd") and self['AdjustedPriceInd'] and  self['AdjustedPriceInd'] == client_params['AdjustedPriceInd'] ) or  (not client_params.has_key('AdjustedPriceInd') and self['AdjustedPriceInd'] == None)
         
        base_volume_computation_changed = (client_params['ParticipationRate'] != self['ParticipationRate']) or (
        client_params['MinPctVolume'] != self['MinPctVolume']) or not (limit_price_has_not_changed and would_price_has_not_changed)
               
        
        if base_volume_computation_changed :
            self.log("[RESET] Base Volume changed , reinitializing tactic")
            # Cancel & Replace
            # we restart completely the tactic, in any cases,  ? 
            # what we should do with the occurences ? Cancel  all of them   
            for occ in self.moneyManager.getPrivateBook().keys():
                venue_id = self.moneyManager.getPrivateBook()[occ]["VenueId"]
                self.cancelOccurrence(venue_id, occ)
            #record the modification 
            self.delta_modify_qty += self.moneyManager.getExecQty()
            #reset the tactic
            return Tactic.Modify.REINITIALIZE
        
        # manage new quantity by respect to executed quantity
            
        if client_params['OrderQty'] > self.moneyManager.getLeavesQty():
            # we can continue, with the new asked quantity
            # update the available Quantity with the new Amount
            self.moneyManager.setTotalQty(client_params['OrderQty'])
            self.log("Updating Total Qty")
            return Tactic.Modify.ACCEPT
            
        # manage new quantity by respect to leaves quantity
        
        if client_params['OrderQty'] < self.moneyManager.getLeavesQty():
            # we will need to free some quantity using the following order :
            # first cancel passive occurence, order by price , best price (far from the best) first

            # Initialize the Quantity to free 
            qty_to_free = self.moneyManager.getLeavesQty() - client_params['OrderQty'] 
            
            # Passive (Iceberg + TAP)
            qty_to_free = self.reasses_on_modify("PassiveTracking", qty_to_free)
            
            # Aggressive : if qty_to_free == 0 , the following function will do nothing
            qty_to_free = self.reasses_on_modify("AggressiveTracking", qty_to_free)
            return Tactic.Modify.REINITIALIZE
            
        # otherwise accept the modifications
        self.log("Accept Modifications")
        return Tactic.Modify.ACCEPT
    
    def compute_auction_order(self, trading_state, last_price):
        # compute (price, volume) of the order in the auction
        if trading_state == 'O':
            if self['OpeningOccurrencePosition'] == '':
                price_auction = last_price*(1.0 - self.side*0.2)
            else:
                price_auction = self['OpeningOccurrencePosition']
        elif trading_state == 'C':
            if self['ClosingOccurrencePosition'] == '':
                price_auction = last_price*(1.0 - self.side*0.2) 
            else:
                price_auction = self['ClosingOccurrencePosition'] 
        return self.__bestBetween( float(price_auction), self.limit_price)
         
    def process_timer(self):
        self.log("Process Timer called")
            
        
    def manage_auction_order(self, evt, price_auction, quantity_auction):
        # get all exist orders, already in self.current_state        
        
        auction_leaves_quantity = sum([ order[0] for order in self.current_state.values()])       
        delta_volume_to_adjust = quantity_auction - auction_leaves_quantity 

        
        if delta_volume_to_adjust > self.ats:
            # we add a new order
            self.createOccurrence(tactic_type='LimitOrder',
                             venue_id=evt.getMarketId(),
                             prefix='Auction',
                             tactic_params={'Price': price_auction,
                                            'OrderQty': int(delta_volume_to_adjust)})
            self.printOrder(price_auction, delta_volume_to_adjust, 'LimitOrder', 'Insert')
        elif delta_volume_to_adjust < -self.ats:
            # sort all exist order                        
            sorted_prices = sorted(self.current_state.keys(), reverse = (self.side == -1))
            freed_qty = 0
            cur_idx = 0
            while freed_qty > delta_volume_to_adjust:                
                if freed_qty - self.current_state[sorted_prices[cur_idx]][0] > delta_volume_to_adjust:
                    # cancel the last available order
                    self.cancelOccurrence(evt.getMarketId(), self.current_state[sorted_prices[cur_idx]][1]) 
                    freed_qty -= self.current_state[sorted_prices[cur_idx]][0]
                    cur_idx += 1
                else:
                    # modify only
                    new_quantity = self.current_state[sorted_prices[cur_idx]][0] + delta_volume_to_adjust - freed_qty                    
                    self.modifyOccurrence(tactic_type='LimitOrder',
                                         venue_id=evt.getMarketId(),
                                         oc_id=self.current_state[sorted_prices[cur_idx]][1],
                                         tactic_params={'Price': price_auction,
                                                        'OrderQty': int(new_quantity)})
                            
    def getCorrectedTheoVolCoeff(self, time_before_auction_end, trading_state):
        if time_before_auction_end is None:
            return 1.0
        elif time_before_auction_end < 0.0:
            # if we have passed the theoretical end
            return 1.0
        elif time_before_auction_end > 0.25: 
            # TODO time_before_auction_end in second?
            if trading_state == 'O':
                return self.openingAuctionPolyCoeff[4]
            elif trading_state == 'C':
                return self.closingAuctionPolyCoeff[4]
        else:
            if trading_state == 'O':
                return self.openingAuctionPolyCoeff[0] + self.openingAuctionPolyCoeff * time_before_auction_end + self.openingAuctionPolyCoeff[2]* time_before_auction_end^2 + self.openingAuctionPolyCoeff[3]* time_before_auction_end^3
            elif trading_state == 'C':                
                return self.closingAuctionPolyCoeff[0] + self.closingAuctionPolyCoeff[1]* time_before_auction_end + self.closingAuctionPolyCoeff[2]* time_before_auction_end^2 + self.closingAuctionPolyCoeff[3]* time_before_auction_end^3
    
    def validateMarketConditions(self):
        # if we have no best opposute, the raise an eeror and stop
        
        if not self.event_market['BEST_OPPOSITE1'] :
            return self.MarketState.INVALID
            
        
        if not self.event_market['BEST1']:
            return self.MarketState.INVALID
            
            
        trading_state = self.marketManager.getFeedInfo('AE', 'CONDCODE_2')
        
        # If the market is in AR state, the stock is "freezed" :
        if 'AR' in trading_state:
            return self.MarketState.WAIT
        
        
        return self.MarketState.REGULAR
        
    def process(self, evt):
        ''' Function to be called for each update on the market 
            @return: 
                - compute thresholds for delta volume --> the current zone
                - depending on current zone --> decide to launch (tracking and/or slippage) tactic
        '''
        self.event_market = self.marketManager.getFeedInfo(evt.getMarketId())
        # TODO : REmove for algobox
        LeavesQty = self.moneyManager.getLeavesQty()
        
        if LeavesQty > 0 :
            current_private_book = self.moneyManager.getPrivateBook();
            TotalLeaveQty = 0.0
            for o in current_private_book :  
                TotalLeaveQty +=    int(self.moneyManager.getPrivateBook()[o]["LeavesQty"])
                
            if TotalLeaveQty != LeavesQty: 
                print "Soemthing Wrong"
        # End TODO : Remove for algobox
        
        MarketState = self.validateMarketConditions()
        
        if MarketState == self.MarketState.INVALID:
            self.error("Stopping algo because of incorrect Market Conditions")
        elif MarketState == self.MarketState.WAIT:
            #we have to cancel all our orders, and 
            # do nothing in process until the market is not in regular conditions
            if len(current_private_book) > 0:
                    # Cancel all living orders which are not auciton orders
                        for order in current_private_book:
                                venue_id = self.moneyManager.getPrivateBook()[order]["VenueId"]
                                self.cancelOccurrence(venue_id, order) 
            return
        
        trading_state = self.marketManager.getFeedInfo('AE', 'CONDCODE_2')
        
        if trading_state == '' :
            trading_state = 'TR'
        
        
        self.last_mkt_qty = self.mkt_qty
        self.mkt_qty = self.marketManager.getFeedInfo('AE', 'QTE_CNT1')
                
        self.log("trading state is : %s "%(trading_state))
        self.log("Process -----> Last mkt_qty = %d "%(self.mkt_qty) )
        if trading_state[0] == 'A':
            # we are in the auction phase
            qty_freed = 0
            self.updateCurrentState(evt, 'Auction')

            if not self.is_in_auction: 
                # first time             
                current_private_book = self.moneyManager.getPrivateBook('Auction')
                self.log("TRADING PHASE : ENTERING AUCTION PHASE")
                
                if len(current_private_book) > 0:
                # Cancel all living orders which are not auciton orders
                    for order in current_private_book:
                        if "Auction" not in order:
                            qty_freed += self.moneyManager.getPrivateBook()[order]["LeavesQty"]
                            venue_id = self.moneyManager.getPrivateBook()[order]["VenueId"]
                            self.cancelOccurrence(venue_id, order)                
                self.is_in_auction = True
            
            #TODO
            # Trading HOURS :::: {'post_closing': '00:00:00 UTC', 'intraday_stop': '00:00:00 UTC', 'post_opening': '00:00:00 UTC', 'opening_auction': '05:10:00 UTC', 'gmt_offset_hours': '2', 'trading_destination_id': '4', 'opening_fixing': '05:14:00 UTC', 'intraday_resumption_fixing': '00:00:00 UTC', 'opening_fixing_margin': '0', 'quotation_group': '01', 'closing_fixing': '22:44:00 UTC', 'intraday_resumption_auction': '00:00:00 UTC', 'intraday_stop_fixing': '00:00:00 UTC', 'end_date': '00:00:00 UTC', 'opening': '05:15:00 UTC', 'intraday_stop_auction': '00:00:00 UTC', 'opening_margin': '0', 'closing_auction': '22:45:00 UTC', 'intraday_resumption': '00:00:00 UTC', 'closing_auction_margin': '0', 'opening_auction_margin': '0', 'closing': '22:45:00 UTC', 'closing_margin': '0', 'closing_fixing_margin': '0'}

            time_now = self.convertDateToSecond(evt.timestamp)
            
            # which auction ?? 
            if trading_state[1] == 'O':
                time_before_auction_end = self.convertDateStrToSecond(self.sec_id_trading_hours['opening_fixing']) - time_now
                  
            elif trading_state[1] == 'C':
                time_before_auction_end = self.convertDateStrToSecond(self.sec_id_trading_hours['closing_fixing']) - time_now
                
            elif trading_state[1] == 'I':
                time_before_auction_end = None
                #TODO : manage volatility auction or DAX intraday auction
                # we can't use the end time as it is not known
                #time_ending_fixing = self.convertDateStrToSecond(self.sec_id_trading_hours['intraday_stop_fixing'])
            
             
            corrected_theo_vol = self.event_market['THEORETICAL_VOLUME'] * self.getCorrectedTheoVolCoeff(float(time_before_auction_end/60.0), trading_state[1])
            # send an order at the current volume
            # compute the theoretical volume
            quantity_auction = corrected_theo_vol * self.qmid/(1.0 - self.qmid)
            # and cap it with usable volume
            quantity_to_place = min(quantity_auction, 
                                    qty_freed + self.moneyManager.getTotalQty() - self.moneyManager.getExecQty() - self.moneyManager.getLeavesQty())
            
            if quantity_to_place > 0 :                                
                
                # get the last price in the two cases: opeing and closing
                if trading_state[1] == 'O':
                    last_price = self.marketManager.getFeedInfo('AE', 'HST_CLOSE')
                else:
                    last_price = self.event_market['DEAL_PRICE']
                
                price_to_place = self.compute_auction_order(trading_state[1], last_price)    
                self.manage_auction_order(price_to_place, quantity_to_place)
        
            return
        
        
        self.log("TRADING PHASE : TRADING REGULAR")           
        # update ats
        print 'Volume so far %d, nb_deal = %d' % (self.marketManager.getFeedInfo('AE', 'A_QTY_3'), self.marketManager.getFeedInfo('AE', 'A_NPLRS_3'))
        nb_deal_so_far = self.marketManager.getFeedInfo('AE', 'A_NPLRS_3')
        if nb_deal_so_far > 0:
            self.ats = int(self.marketManager.getFeedInfo('AE', 'A_QTY_3') / nb_deal_so_far * (1 + random() * 0.2 - 0.1))
        else:
            self.ats = 100
        
        # adjust rate        
        self.rateAdjustment(evt)
        # update current_state       
        self.updateCurrentState(evt, 'PassiveTracking')
        
        #Would Level 
            
        self.wouldLevelProcessing(evt)
        
        if self.would_qty  == 0:
            
            # compute different delta volume
           
            # TODO / roll back to the norlma version
            self.deltaVolumes = self.computeVolumeThreshold(evt)
            # this is needed because our volume is not taken into account by the AE ????? 
            
            # self.deltaVolumes = self.computeVolumeThreshold__FORTEST___(evt)
            # determine the current zone
            self.curZone = self.currentZone()
            # compute the imblance
            self.computeImbalance(evt)
            
            mkt_qty = self.marketManager.getFeedInfo('AE', 'QTE_CNT1')
            exec_qty = self.moneyManager.getExecQty()
            plr = 0 if mkt_qty == 0 else  100.0 * exec_qty / mkt_qty
            
            #manage qty used by the slippage / opportunity / tracking
            available_qty  =  self.moneyManager.getTotalQty() - self.moneyManager.getExecQty() - self.moneyManager.getLeavesQty()
            if available_qty <  0:
                print("available_qty")
                current_private_book = self.moneyManager.getPrivateBook();
                for o in current_private_book :  
                     print ("order : P = %.2f, Q = %d,"%(float(self.moneyManager.getPrivateBook()[o]["Price"]),int(self.moneyManager.getPrivateBook()[o]["OrderQty"])))
                
            
            self.reserved_qty = 0             
            self.log ("[%d] ExecQty = %d , Market Qty = %d , PLR = %.2f,  RemainingQty = %d. AvailableQty = %d. LeavesQty = %d. PrivateBook: %s, BBO: [%d@%f - %d@%f]" % 
                (self.marketManager.getFeedInfo('AE', 'C1_CONVNUM'),
                 exec_qty,
                 mkt_qty,
                 plr,
                 self.moneyManager.getTotalQty() - self.moneyManager.getExecQty(),
                 available_qty,
                 self.moneyManager.getLeavesQty(),
                 self.moneyManager,
                 self.event_market['BEST_BSIZ1'], self.event_market['BEST_BID1'], self.event_market['BEST_ASIZ1'], self.event_market['BEST_ASK1']))
            
            
            #remaining quantity
            
            self.qty_usable = self.moneyManager.getTotalQty() - self.moneyManager.getExecQty()
            
            # depending on the current zone, switch to the suitable tactics        
            # call first slippage : if we are late, take some liquidity
            # whatr do we have already in slippage occurence ? 
            
            curXFire = self.moneyManager.getPrivateBook('ModerateTrackingTAP')
            if curXFire:
                current_slippage_qty = curXFire[curXFire.keys()[0]]['LeavesQty'] 
            else:
                current_slippage_qty = 0 
             
            
    
            self.opportunity(evt)
                    
            self.slippage(evt)
            
            qty_2_free = max(0 ,self.reserved_qty - (self.moneyManager.getTotalQty() - self.moneyManager.getExecQty()
                           - self.moneyManager.getLeavesQty() + current_slippage_qty) )
            #self.log("Slippage reserved Quantity is %d, so qty_to_free = %d" %(self.reserved_qty, qty_2_free))
            
            self.tracking(evt, qty_2_free )    
            self.log("Current Zone : %d, delta_volumes(minDeltaVolume, optDeltaVolume, maxDeltaVolume) = %s" % (self.curZone, self.deltaVolumes)) 
            self.updateIndicators()
        
    def processReport(self, evt):
        
        #update qty for each zone
        for x in self.moneyManager.getLastExecutions():
            self.log("Order Filled: ID : %s  Qty=%d Price=%f" % (x["OrderId"] , int(x['Quantity']), float(x['Price'])))
            if 'PassiveTracking' in x["OrderId"] :
                self.tracking_volume += int(x["Quantity"])
                self.tracking_amount += int(x["Quantity"]) * float(x["Price"])
            elif  'ModerateTrackingTAP' in x["OrderId"] : 
                self.slippage_volume += int(x["Quantity"])
                self.slippage_amount += int(x["Quantity"]) * float(x["Price"])
            elif "WouldOpportunity" in x["OrderId"] :
                self.total_would_qty += int(x["Quantity"])
                self.would_amount   += int(x["Quantity"])* float(x["Price"])
            elif "PriceOpportunity" in x["OrderId"] :
                self.total_opportunity_volume += int(x["Quantity"])
                self.opportunity_amount   += int(x["Quantity"])* float(x["Price"])

        exec_qty = self.moneyManager.getExecQty() - self.total_would_qty
        
        
        
        mkt_qty = self.marketManager.getFeedInfo('AE', 'QTE_CNT1')
        self.log(" ProcessReport -----> Current  mkt_qty = %d "%(self.mkt_qty) )
        
            
        self.last_mkt_qty = self.mkt_qty
        self.mkt_qty = mkt_qty
        

        plr_no_would = 0 if mkt_qty == 0 else  100.0 * exec_qty / mkt_qty
        plr_with_would = 0 if mkt_qty == 0 else  100.0 * (exec_qty + self.total_would_qty)/ mkt_qty
        
        self.log ("[Process Report] ExecQty = %d , Market Qty = %d , PLR (without would qty) = %.2f, PLR (with would qty) = %.2f"%( exec_qty,mkt_qty,plr_no_would,plr_with_would))
        self.computeVolumeThreshold__FORTEST___(evt)
        self.updateIndicators()
        
    def acceptSubscription(self, sub_id, sub):        
        return sub_id == self.primary_venue_id

    def acceptVenue(self, venue_id, venue):
        print "ACCEPTVENUE : Processing venue_id %s, venue info type  %s , venue.info -> : " % (venue_id, venue.info["type"])
        if venue.info["type"] != "Lit":
            return False        
        if str(venue.info["destinationId"]) == str(self['destinationId']):
            self.primary_venue_id = venue_id
            self.trading_destination = venue.info['destinationId']
            return True
        return False

    def extraSubscriptions(self):

        ''' Returns any extra subscriptions needed. 
            @return
                Dictionary: {name: (Source, Ric, ReqdProcess)}
        '''
        
        price_string = ''
        
        # Gestion du Limit Prices
        
        if self["Price"]:
            price_string = ";GE=%s"%(self["Price"])  if self.side == -1 else ";LE=%s"%(self["Price"])
        
        if self["AdjustedPriceInd"]:
            price_string +=  ";LE=%s"%(self["AdjustedPriceInd"])  if self.side == -1 else ";GE=%s"%(self["AdjustedPriceInd"])
            print "Using Would Level : %s"%(self["AdjustedPriceInd"])
        
        print 'Suscribing to AE, S=%d@%s;P=bEHRC;Z=UTC;H=4;F=%s;T=%s;EX=top;EX=cross;EX=auc%s;PL=os;' % (self['InstrumentID'], self.trading_destination, self['EffectiveTime'][9:], self['ExpireTime'][9:], price_string) 
        
        #TODO : See if we need any extra subscription , like TOPAGGR for instance 
        return {'AE':(self['vwapSource'], 'S=%d@%s;P=bEHRC;Z=UTC;H=4;F=%s;T=%s;EX=top;EX=cross;EX=auc%s;PL=os;' % (self['InstrumentID'], self.trading_destination, self['EffectiveTime'][9:], self['ExpireTime'][9:], price_string))}
    
    
    
    
    def computeVolumeThreshold(self, evt):
        
        ''' Compute the 3 thresholds for the delta volume
            @return:
                List: (Q_min(t), Q_max(t), Q_opt(t))
        '''
        # adjust executed qty by respect to modify received
        # TODO : is auction volume in getExecQty
        excVolume = self.moneyManager.getExecQty() - self.delta_modify_qty
        # get the current market volume up to time t
        mktVolume = self.marketManager.getFeedInfo('AE', 'QTE_CNT1')
        
        minDeltaVolume = (self.qmin * mktVolume - excVolume + self.qmin * self.estimateVolume()) / (1.0 - self.qmin)
        maxDeltaVolume = (self.qmax * mktVolume - excVolume + self.qmax * self.estimateVolume()) / (1.0 - self.qmax)
        
        # using predicted volume for the next period           
        optDeltaVolume = (self.qmid * mktVolume - excVolume + self.qmid * self.estimateVolume()) / (1.0 - self.qmid)
        
        return [minDeltaVolume, optDeltaVolume, maxDeltaVolume]
            
    
    def computeVolumeThreshold__FORTEST___(self, evt):
        
        ''' Compute the 3 thresholds for the delta volume
            @return:
                List: (Q_min(t), Q_max(t), Q_opt(t))
        '''
        # adjust executed qty by respect to modify received
        excVolume = self.moneyManager.getExecQty() - self.delta_modify_qty - self.total_would_qty
        # get the current market volume up to time t
        mktVolume = self.marketManager.getFeedInfo('AE', 'QTE_CNT1') + excVolume
        
        minDeltaVolume = (self.qmin * mktVolume - excVolume + self.qmin * self.estimateVolume()) / (1.0 - self.qmin)
        maxDeltaVolume = (self.qmax * mktVolume - excVolume + self.qmax * self.estimateVolume()) / (1.0 - self.qmax)
        
        # using predicted volume for the next period           
        optDeltaVolume = (self.qmid * mktVolume - excVolume + self.qmid * self.estimateVolume()) / (1.0 - self.qmid)
        
        return [minDeltaVolume, optDeltaVolume, maxDeltaVolume]
            
    
    
    
    def rateAdjustmentWrtFixing(self):
        ''' Adjust the participation rate w.r.t. error due to fixing volume   
            @return:
                adjusted rate             
        '''
        
    def rateAdjustment(self, evt):
        ''' Adjust the participation rate w.r.t. benchmark (EVP, Strict, PVOL)   
            @return:
                adjusted rate            
        '''
                   
        if ((self['benchmark'] == 'SVT') or (self['benchmark'] == 'PVOL')):
            self.qmin = self['MinPctVolume'] * 0.01
            self.qmax = self['ParticipationRate'] * 0.01
        elif (self['benchmark'] == 'EVP'):
            qmid = 0.01 * (self['ParticipationRate'] + (self.marketManager.getFeedInfo(evt.getMarketId(), 'TIME_NUM') - self.beginTime) * self.coeffRate)
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
        curZone = sum([d < 0 for d in self.deltaVolumes])
        return curZone



    def printOrder(self, order_price, order_qty, order_type_str, action):        
        self.log('Action :  %s  Order of type %s, P1 = %.4f,Q1 = %.2f' % (action, order_type_str, order_price , order_qty))
       
       
    def constructTrackingOrders(self, targetRate):
        # compute qty and price to fullfill the % vol without any constraints
        #Output
        new_orders = {}
        # FIRST ORDER
        iFirst = 1 
        # verify the current zone
        cum_vol = 0
        
        
        find_price = False
        
        
        #is our limit price in the LOB ? 
        limit_price_not_found = True
        level_in_lob = 1
        
        
        
        
        if self.side *self.limit_price > self.side *  self.event_market['BEST1']:
            limit_price_not_found = False
            iFirst = 1 
            
        
        while limit_price_not_found and level_in_lob <= 5:
            if self.event_market['BEST%d' % level_in_lob] == self.limit_price:
                limit_price_not_found = False
                iFirst = level_in_lob
            level_in_lob += 1    
            
        
        
        if  limit_price_not_found :
            # we are out of the market
            # just place one ats at the limit price and continue
            new_orders[self.limit_price] =self.ats
            new_orders[0] =0
            return new_orders
        
        
        if (self.curZone <= 1): #if LATE -> always place an order at the first limit
            best_price = self.event_market['BEST%d'%iFirst]
            cum_vol = self.event_market['BEST_SIZE%d'%iFirst]
            first_quantity_to_be_placed = self['TrackingLimitOrder1'] * cum_vol * targetRate
            find_price = True
        else: # otherwise
            '''
                From the current evt, compute the price and volume of the tracking orders to be placed
                - the first order will be made at the price such that : cum. vol. up to price (from the best) cum_vol >= 1/q ATS, qty = max(ATS, q * cumVol)
            '''            
            while ((iFirst < 5) and not (find_price)): 
                best_price = self.event_market['BEST%d' % iFirst]
                if best_price * self.side <= self.side * self.limit_price: 
                    best_size = self.event_market['BEST_SIZE%d' % iFirst]
                    cum_vol += best_size
                    find_price = (self['TrackingLimitOrder1'] * cum_vol * targetRate >= self.ats)
                    iFirst += 1
            # if there is not enough volume at the first 5 limits 
            if not find_price:
                pass
            # then determine the quantity to be placed
            '''
            TODO: estimate constants TrackingLimitOrder1,2 for i=1,2
            '''
            first_quantity_to_be_placed = self['TrackingLimitOrder1'] * cum_vol * targetRate
        
        #first_quantity_to_be_placed = min(first_quantity_to_be_placed, available_tracking_qty,self.qty_usable)
        #available_tracking_qty -= first_quantity_to_be_placed
        #self.qty_usable -= first_quantity_to_be_placed
        
        
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
            second_price = self.event_market['BEST%d' % min(iFirst + self['TrackingDelta'], 5)]            
            second_quantity_to_be_placed = self.ats
            find_price = True
        else: # otherwise
            '''
                From the current evt, compute the price and volume of the tracking orders to be placed
                - the first order will be made at the price such that : cum. vol. up to price (from the best) cum_vol >= 1/q ATS, qty = max(ATS, q * cumVol)
            '''            
            while ((iSecond < 5) and not (find_price)): 
                second_price = self.event_market['BEST%d' % iSecond]
                best_size = self.event_market['BEST_SIZE%d' % iSecond]
                cum_vol += best_size
                find_price = (self['TrackingLimitOrder2'] * cum_vol * targetRate >= self.ats)                
                iSecond += 1
                
            if not find_price:
                # if there is not enough volume at the first 5 limits
                # then place an order at the distant Delta w.r.t. the first order, but must be within the 5th limit
                second_price = self.event_market['BEST%d' % min(iFirst + self['TrackingDelta'], 5)]
                cum_vol = sum([self.event_market['BEST_SIZE%d' % j] for j in range(iFirst, min(iFirst + self['TrackingDelta'], 5))])
                second_quantity_to_be_placed = self['TrackingLimitOrder2'] * cum_vol * targetRate
                find_price = True
            else:
                # if there is enough volume
                # then 
                second_price = min(second_price, self.event_market['BEST%d' % min(iFirst + self['TrackingDelta'], 5)]) if (self.side == 1) else max(second_price, self.event_market['BEST%d' % min(iFirst + self['TrackingDelta'], 5)])
                cum_vol = sum([self.event_market['BEST_SIZE%d' % j] for j in range(iFirst, iFirst + self['TrackingDelta'], iSecond)])
                second_quantity_to_be_placed = self['TrackingLimitOrder2'] * cum_vol * targetRate
            
       
        
        if best_price * self.side > self.side * self.limit_price or  second_price * self.side > self.side * self.limit_price:
            self.error("Invalid Price Computation, price are inconsistant with the Limit Price, best_price = %.3f, second_price =%.3f"%(best_price, second_price)) 
       
        # second_quantity_to_be_placed = min(second_quantity_to_be_placed, available_tracking_qty,self.qty_usable) 
        new_orders[best_price] =first_quantity_to_be_placed
        new_orders[second_price] =second_quantity_to_be_placed
        
        return new_orders
    
    
        
    def prepare_new_actions(self, new_orders, existing_state, evt, available_tracking_qty, qty_to_free):
        # prepare modifications or insertions to do
        # dealing with common prices   
        # Compute intersection     
        
        if qty_to_free > 0: 
            self.log("REASSEMENT needed, Qty = %d" %qty_to_free)
            
        common_prices = [price for price in new_orders.keys() if price in existing_state.keys()]        
        action_transitions = {}
        
        # for each common price, modify this order (up to a condition on volume)
        #first_quantity_to_be_placed = min(first_quantity_to_be_placed, available_tracking_qty,self.qty_usable)
        #available_tracking_qty -= first_quantity_to_be_placed
        #self.qty_usable -= first_quantity_to_be_placed
        
        for new_order_price in sorted(common_prices,reverse = self.side == -1):
            new_qty =  new_orders[new_order_price]
            old_qty =  existing_state[new_order_price][0]
            #delta_qty = min(new_qty -old_qty , available_tracking_qty ) 
            delta_qty = new_qty - old_qty
            if  delta_qty > self.ats :
                # update this order
                capped_qty = min(available_tracking_qty, new_qty)
                action_transitions[new_order_price] =['U', new_order_price, capped_qty]
                available_tracking_qty -= capped_qty  
            else:
                # do nothing
                action_transitions[new_order_price] =['N', new_order_price, old_qty]
                available_tracking_qty -= old_qty
        # dealing with uncommon prices
        # take the remaining orders in new_orders
        # if there is still an order in existing_state then MODIFY
        # otherwise INSERT
        # Compute set difference
        uncommon_new_prices = [price for price in new_orders.keys() if price not in common_prices]
        uncommon_existing_prices = [price for price in existing_state.keys() if price not in common_prices]
        
        #more qty to the best price
        for new_order_price in sorted(uncommon_new_prices,reverse = self.side == -1) :
            new_qty =  new_orders[new_order_price]
            if len(uncommon_existing_prices) > 0:
                old_qty =  existing_state[uncommon_existing_prices[0]][0]
                # new_qty = old_qty + min(new_qty - old_qty,available_tracking_qty)     
                # update this order
                capped_qty = min(available_tracking_qty, new_qty)
                action_transitions[uncommon_existing_prices[0]] =['U', new_order_price, capped_qty]                 
                uncommon_existing_prices.remove(uncommon_existing_prices[0])
                available_tracking_qty -=  capped_qty  
            else:
                capped_qty = min( available_tracking_qty, new_qty)
                if capped_qty > 0: 
                    action_transitions[new_order_price] =['I', new_order_price, capped_qty]
                    available_tracking_qty -= capped_qty
        
        # defensive code
        for remaining_price in uncommon_existing_prices:
            # strange case, means we have some remaining orders w<othj no actions planned
            # so CANCEL them             
            action_transitions[remaining_price] =['C', 0, 0]
            available_tracking_qty += existing_state[remaining_price][0]
        
        print action_transitions
        
        if qty_to_free >  0:
            # adjust  Actions to free some qty for slippage
            
            print self.current_state
            previous_state_qty = sum([a[0] for a in self.current_state.values()])
            new_state_qty = sum([a[2] for a in action_transitions.values()]) 
            new_to_free =  math.ceil(qty_to_free - new_state_qty + previous_state_qty)
            
            print new_to_free
            if new_to_free > 0 :
                for price in sorted(action_transitions.keys(), reverse = self.side == 1):
                    if action_transitions[price][0] in ('U','I','N'):
                        if action_transitions[price][0] == 'N' : 
                            action_transitions[price][0] = 'U'
                        planned_qty = action_transitions[price][2]                  
                        action_transitions[price][2] = max ( 0 , planned_qty - new_to_free)
                        new_to_free = max(new_to_free - planned_qty, 0)
                                                       
        return action_transitions

    def apply_new_actions(self, action_transitions,evt):
        # apply actions, and manage orders
        
        for price in action_transitions.keys():
            new_price    =  action_transitions[price][1]
            new_quantity =  action_transitions[price][2]
            
            if action_transitions[price][0] == 'U':
                #modify an existing order
                self.log( "Modify , Q = %d"%(int(new_quantity)))
                self.modifyOccurrence(tactic_type='IcebergMono',
                                         venue_id=evt.getMarketId(),
                                         oc_id=self.current_state[price][1],
                                         tactic_params={'Price': new_price,
                                                         'OrderQty': int(new_quantity)})  
                self.printOrder(new_price, int(new_quantity), 'IcebergMono', 'Modify')
                
            elif action_transitions[price][0] == 'I':
                #insert a new order
                self.createOccurrence(tactic_type='IcebergMono',
                                         venue_id=evt.getMarketId(),
                                         prefix='PassiveTracking',
                                         tactic_params={'Price': new_price,
                                                          'OrderQty': int(new_quantity)})
                self.printOrder(new_price, int(new_quantity), 'IcebergMono', 'Insert')
            
            elif action_transitions[price][0] == 'C':
                # cancel these orders
                self.cancelOccurrence(evt.getMarketId(), self.current_state[price][1])
                self.printOrder(0, 0, 'IcebergMono', 'Cancel')         
        

    
    def tracking(self, evt, qty_to_free):
        ''' Tactic "tracking" 
            @precondition: in each zone
            @postcondition: 2 limit orders are placed if necessary
        '''
  
         
                
        # verify living volume in the order book        
        # determine the max volume place-able = min(Q_max - Q_t, Q_max - Q_opt)
        if (self.curZone == 3): # AVANCE
            # cancel all orders            
            for key_price in self.current_state.keys():
                # cancel this order
                self.log("Tracking - Cancel all existing order because zone == 3")
                # cancel this order
                self.cancelOccurrence(evt.getMarketId(), self.current_state[key_price][1])
                # and delete it from the current_state
                del self.current_state[key_price] 
        else:
            deltaVolumes = self.deltaVolumes
            
            
            if self.algo_starting:
                # at the beginning, special treatment to place orders
                self.sec_id_trading_hours = self.getReferenceData("SecurityTradingHours",  venue_id = evt.getMarketId())
                availableTrackingQty = min(self.qty_usable, 2 * self.event_market['BEST_SIZE1'])
                self.algo_starting = False
            else:
                #availableTrackingQty = min(deltaVolumes[2], deltaVolumes[2] - deltaVolumes[1])
                # we have to remove the slippage qty of what we want to place in tracking if late
                
                self.qty_usable      -= self.reserved_qty
                availableTrackingQty  = self.qty_usable 
                 
            
            
            #Refactoring start
            
            ordersToPlace = self.constructTrackingOrders(self.qmid)
            print ordersToPlace
            action_transitions = self.prepare_new_actions(ordersToPlace, self.current_state, evt, availableTrackingQty, qty_to_free)
            
            self.apply_new_actions(action_transitions,evt)
            
            #Refactoring end 
            #self.log("TRACKING : First Price  %f, Second Price = %f TRACKING : Sorted prices %s" % (best_price, second_price, sorted_prices))
            
           
                    
    def mergeXfireParamsWithConfig(self, xfire_params):
        
        occurence_params = xfire_params
        
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
            occurence_params['ExcludePassiveType'] += 'popo'
        
        # manage exclusion of primary venue
        
        if  self['ExcludePrimary'] == 1  or xfire_params['ExcludePrimary'] == 1 :
            occurence_params['ExcludePrimary'] = 1 
        else:
            occurence_params['ExcludePrimary'] = 0 
        
        
        
        if 'MID' not in occurence_params['ExcludePassiveType'] or  'MID' not in occurence_params['ExcludeAggressiveType']:
            self.error('Invalid configuration for a xfire')
            
        
        return occurence_params    
                  
    def modifySlippageTAPOccurence(self, market_id, new_xfire_params):
        curXFire = self.moneyManager.getPrivateBook('ModerateTrackingTAP')
        if not curXFire:
            self.createOccurrence(tactic_type='TAP',
                                          venue_id=market_id,
                                          prefix='ModerateTrackingTAP',
                                          tactic_params=new_xfire_params)
            self.printOrder(new_xfire_params["Price"], new_xfire_params["OrderQty"], 'SlippageTAP', 'Insert')
        elif curXFire[curXFire.keys()[0]]['ExcludePrimary'] != new_xfire_params['ExcludePrimary'] or curXFire[curXFire.keys()[0]].has_key('ParticipationRate') :
            #check if it is a "mid" occurence or "aggressive" one
            # we need a cancel & replace
            self.cancelOccurrence(market_id, curXFire.keys()[0])
            self.createOccurrence(tactic_type='TAP',
                                          venue_id=market_id,
                                          prefix='ModerateTrackingTAP',
                                          tactic_params=new_xfire_params)
            self.printOrder(new_xfire_params["Price"], new_xfire_params["OrderQty"], 'SlippageTAP', 'Cancel&Replace')
        else:
            # we don't need a cancel & replace
            self.modifyOccurrence(tactic_type='TAP',
                                              venue_id=market_id,
                                              oc_id=curXFire.keys()[0],
                                              tactic_params=new_xfire_params)
            self.printOrder(new_xfire_params["Price"], new_xfire_params["OrderQty"], 'SlippageTAP', 'Modify')
            
    
    def wouldLevelProcessing(self, evt):
        ''' Tactic "would"
            @precondition: V(t) < total_volume            
        '''
        best_opposite = self.event_market['BEST_OPPOSITE1']
        if self.__bestBetween(self.would_level,best_opposite) == best_opposite:
            self.log("Entering Would opportunity !!! , best opposite = %.2f , would level = %.2f"%(best_opposite, self.would_level) )
            would_price = self.would_level
            would_qty =  self.moneyManager.getTotalQty() - self.moneyManager.getExecQty()
            
            
            # send a FAK with the remaining qty, but cancel all orders first
            
            # cancellation 
            for occ in self.moneyManager.getPrivateBook().keys():
                venue_id = self.moneyManager.getPrivateBook()[occ]["VenueId"]
                self.cancelOccurrence(venue_id, occ)
                
            # FAK as a XFire
            self.printOrder(would_price, int(would_qty), 'WouldLevel ', 'Insert')
            
            self.createOccurrence('MarketOrder',
                      venue_id = evt.getMarketId(), 
                      prefix = 'WouldOpportunity',
                      tactic_params = {'OrderQty': int(would_qty), 
                                       'Price': would_price, 
                                       'OrderType': 'limit', 
                                       'MarketTimeInForce': 'Fak'} )
             
            self.would_qty = int(would_qty)
        else:
            self.would_qty = 0
            
        
            
    def slippage(self, evt):
        ''' Tactic "slippage"
            @precondition: Q(t) < Q_opt(t)            
        '''        
        deltaVolumes = self.deltaVolumes
          
        if deltaVolumes[1] > 0:
            # MANAGE
            # if Q(t) <= Q_min(t)
                # Manage Aggressive TAP Occurence to go back to qmin
                
                if (deltaVolumes[0] > 0 and deltaVolumes[1] >= self.ats and self.next_slippage_deal <= self.marketManager.getFeedInfo('AE', 'C1_CONVNUM')) or self.qty_usable <= self.ats:    
                    # last condition is for end of life management
                    #update tempo for slippage
                    if self.qty_usable <= self.ats:
                        print  "SLIPPAGE END ? : self.qty_usable (%d) < self.ats (%d)"%(self.qty_usable ,self.ats)
                    else:
                        self.log("entering Slippage, delta_volumes %s: " % (deltaVolumes))
                        
                    self.next_slippage_deal = self.marketManager.getFeedInfo('AE', 'C1_CONVNUM') + self['SlippageNbDeals']
                    quantity = min(max(self.ats, deltaVolumes[1]), self.moneyManager.getTotalQty() - self.moneyManager.getExecQty(), self["MaxMultATS"] * self.ats)
                    
                    # place an TAP  order up to Q_opt(t)                
                    
                    price = self.__bestBetween(self.event_market['BEST_OPPOSITE1'], self.limit_price)
                    
                    # Manage Venues here : we need also Primary + MTF + Dark + Mid , so we do not excluse everything
                    xfire_params = self.mergeXfireParamsWithConfig({'OrderQty': int(quantity),
                                                                    'Price': price,
                                                                    'ExcludeAggressiveType' : '',
                                                                    'ExcludePassiveType' : '',
                                                                    'ExcludePrimary' : 'N',
                                                                    'ParticipationRate' : 99,
                                                                    'AmountValidation' : 0})
                    curXFire = self.moneyManager.getPrivateBook('ModerateTrackingTAP')
                    if curXFire:
                        current_xfire_qty = curXFire[curXFire.keys()[0]]['LeavesQty'] 
                    else:
                        current_xfire_qty = 0  
                    
                    self.modifySlippageTAPOccurence( evt.getMarketId(), xfire_params)
                    self.reserved_qty += int(quantity) 
                elif deltaVolumes[1] >= 0:# if Q(t) <= Q_mid(t):
                    # get the current XFire if exist
                    mid_price = 0.5 * (self.event_market['BEST1'] + self.event_market['BEST_OPPOSITE1'])
                    mid_price = self.__bestBetween(self.event_market['BEST_OPPOSITE1'], self.limit_price)
                    quantity = min(max(self.ats, deltaVolumes[1]), self.moneyManager.getTotalQty() - self.moneyManager.getExecQty())
                
                    # we may exclude everything in passive, maybe something to check
                    # TODO / Rollback to the follwongcode for Lit exclusion
                    '''
                    xfire_params = self.mergeXfireParamsWithConfig({'OrderQty': int(quantity), 
                                                                    'Price': mid_price,
                                                                    'ExcludeAggressiveType' : 'LIT',
                                                                    'ExcludePassiveType' : 'LIT',
                                                                    'ExcludePrimary' : 'Y'})
                    '''
                    xfire_params = self.mergeXfireParamsWithConfig({'OrderQty': int(quantity),
                                                                    'Price': mid_price,
                                                                    'ExcludeAggressiveType' : 'LIT',
                                                                    'ExcludePassiveType' : 'LIT',
                                                                    'ExcludePrimary' : 'N',
                                                                    'ParticipationRate' : 99,
                                                                    'AmountValidation' : 0})
                    curXFire = self.moneyManager.getPrivateBook('ModerateTrackingTAP')
                    #TODO / defensive code ? what happen if we do not succeed to cancel ?
                    self.log("We have %d living Slippage orders"%(len(curXFire.keys())))
                    self.modifySlippageTAPOccurence(evt.getMarketId(), xfire_params)
                    self.reserved_qty +=int(quantity)
                else:
                    
                    curXFire = self.moneyManager.getPrivateBook('ModerateTrackingTAP')
                    if curXFire:
                        self.cancelOccurrence(evt.getMarketId(),curXFire.keys()[0] )
                        self.printOrder(0, 0, 'WouldLevel ', 'CANCEL ')
                
    def opportunity(self, evt):
        '''
            Tactic "opportunity"
        ''' 
        pass
           
        deltaVolumes = self.deltaVolumes
        
        # if Q(t) < Q_opt(t)
        
        # Opportunity signal
        # get the opportunity signal
        
        
        fwd_signal = self.marketManager.getFeedInfo('AE', 'FWD1_PRICE')
        self.fwd_signal = 0.5 if (fwd_signal == '') else fwd_signal
            
        bwd_signal = self.marketManager.getFeedInfo('AE', 'BALN_PRICE') 
        self.bwd_signal = 0.5 if (bwd_signal == '') else bwd_signal
        
        
        # adjusted_bwd_signal = 1 - self.bwd_signal if self.side == 1 else self.bwd_signal
        # adjusted_fwd_signal = 1 - self.fwd_signal if self.side == 1 else self.fwd_signal
        
        
        #self.all_fwd_signal.append(adjusted_bwd_signal)
        #self.all_bwd_signal.append(adjusted_fwd_signal)
        
        
       
        
        
        #self.log("Forward Threshold: [%s] , BackWard Threshold: [%s], signals ----> fwd : %.2f, bwd : %.2f "%(current_fwd_thresold, current_bwd_thresold, adjusted_fwd_signal, adjusted_bwd_signal) )
        # adjust enveloppe
        # IN THE MONEY ? 
        if self.qty_usable <= self.ats:
            #no opportunity at the end of the algo life
            self.log("no opportunity at the end of the algo life")
            pass
        else:
            if  (self.fwd_signal *self.side  < self.side * self.fwdThreshold or self.bwd_signal * self.side  < self.fwdThreshold*self.side ) and  self.next_opportunity_deal <= self.marketManager.getFeedInfo('AE', 'C1_CONVNUM'):
                self.log("============ITM=================OPPORTUNITY")
                best_opposite_qty = self.event_market['BEST_OPPOSITE_SIZE1']
                bo = self.event_market['BEST_OPPOSITE1']
                opp_qty = max (0,min(self.deltaVolumes[2],best_opposite_qty - self.ats))
                opp_qty = min(opp_qty,self.qty_usable)
                # we need to check with the limit price too , to avoid sending an order  which 
                # is not consistent witht the limit price
                if opp_qty >  0 and self.__bestBetween(bo, self.limit_price) == self.limit_price :
                    self.next_opportunity_deal = self.marketManager.getFeedInfo('AE', 'C1_CONVNUM') + self['OpportunityNbDeals']
                    self.printOrder(bo, int(opp_qty), 'Opportunity', 'Insert FAK ')
                    self.log("ITM,  qty available : %d"%(opp_qty))     
                    # FAK Opportunistic order
                    
                    
                    self.createOccurrence('MarketOrder',
                          venue_id = evt.getMarketId(), 
                          prefix = 'PriceOpportunity',
                          tactic_params = {'OrderQty': int(opp_qty), 
                                           'Price': bo, 
                                           'OrderType': 'limit', 
                                           'MarketTimeInForce': 'Fak'} )
                    self.opportunity_volume = opp_qty   
#                    self.total_opportunity_volume +=  self.opportunity_volume
                else:
                    self.log("ITM, but no qty available : Max Curve : %d ,best_opposite_qty -ats : %d ,qty_usable : %d  !!!"%(self.deltaVolumes[2],best_opposite_qty - self.ats,self.qty_usable))
            else:
                self.log("no opportunity ...")
        
        
    def updateCurrentState(self, evt, type_order):
        ''' 
            From the current evt, update the current state
        '''
        # initialize the current_state
        self.current_state = {}        
        # and update the current_state with live orders
        for order_id, order in self.moneyManager.getPrivateBook(type_order).iteritems():            
            self.current_state[float(order['Price'])] = [int(order['LeavesQty']), order_id]
        
        
            
    
    def updateIndicators(self):    
        #time = self.indicators.date[len(self.indicators.date) - 1]
        
#        tactic_id = self.indicators.value['tactic_id'][-1]
#        self.append_indicator({'event': 'UPDATE',
#                               'tactic_id': tactic_id,
#                               'total_quantity': self.moneyManager.getExecQty(),
#                               'min_delta': self.deltaVolumes[0],
#                               'opt_delta': self.deltaVolumes[1],
#                               'max_delta': self.deltaVolumes[2],
#                               'current_zone': self.curZone,
#                               'mkt_volume': self.marketManager.getFeedInfo('AE', 'QTE_CNT1')}, time)   

        
        #TODO : self.moneyManager.getStatistics() should return a dictionnary, not a float
        # bug declared in Mantis
        # Roll back when fixed 
        #Perf_VWAP = self.moneyManager.getStatistics()["vwap"]
        
        Perf_VWAP = self.moneyManager.getStatistics()  if isinstance(self.moneyManager.getStatistics(), float) else  self.moneyManager.getStatistics()["vwap"]
        
        Market_VWAP = self.marketManager.getFeedInfo('AE','WTD_AVE1') if isinstance(self.marketManager.getFeedInfo('AE','WTD_AVE1'), float) else 0
        exec_qty = self.moneyManager.getExecQty()
        total_qty  = self.moneyManager.getTotalQty()
        mkt_qty = self.marketManager.getFeedInfo('AE', 'QTE_CNT1')
        
        # Slippage and Tracking Qty
        
        second_tracking_price = 0
        second_tracking_qty   = 0

        if len(self.current_state.keys()) >  0:
            tracking_price = sorted(self.current_state.keys(), reverse = self.side == 1)[0]
            tracking_qty   = self.current_state[tracking_price][0]
            if len(self.current_state.keys()) >  1:
                second_tracking_price = sorted(self.current_state.keys(), reverse = self.side == 1)[1]
                second_tracking_qty   = self.current_state[second_tracking_price][0]
        else:
            tracking_price = 0
            tracking_qty   = 0
            
        
        Slippage_occ = self.moneyManager.getPrivateBook('ModerateTrackingTAP') 
        if Slippage_occ:
            slippage_price = float(Slippage_occ.values()[0]["Price"])
            slippage_qty   = int(Slippage_occ.values()[0]["LeavesQty"])
        else:
            slippage_price = 0 
            slippage_qty   = 0
            
        if slippage_qty <  0 :
            self.error("Invalid Quantity !!!!!")
            
         #TODO : Remove    
        # self.log(" MARKET TIME STAMP : %s"%self.event_market['TIMACT'])
        
        self.appendIndicator({ 'MktTimeStamp' :self.event_market['TIMACT'] if self.event_market.has_key('TIMACT') else '' ,
                               'EffectiveAskedQty' : total_qty,
                               'EffectiveExecQty' : exec_qty,
                               'ZoneInter' : self.currentZone() + 1, # +1 is for bacward compatiobility with previous version 
                               'VolSlippage': self.deltaVolumes[1], #positive when late
                               'CurVolPct': 0, #?
                               'ExecPct':  100.0 * exec_qty / total_qty,
                               'ExecSpread': 0, # ??? TBD
                               'VwapPrice' :    Perf_VWAP, # a verifier ???
                               'PLR'       :    100.0 * exec_qty / mkt_qty if mkt_qty > 0 else 0,
                               'WouldPLR'  : 0 , #TBD
                               'StdPLR'  : 0 , #TBD
                               'PnL'  : self.side *(Market_VWAP - Perf_VWAP) * exec_qty ,
                               'PnLBp'  : self.side * 10000.0 *(Market_VWAP - Perf_VWAP) /Market_VWAP if  Market_VWAP  *Perf_VWAP  != 0 else 0,
                               'MaxPrice' : 0, #TBD
                               'SlippageAvgPx' :  0 , #TODO
                               'TrackingQty' :  tracking_qty , #TODO
                               'TrackingPrice'    :  tracking_price , #TODO
                               'SecondTrackingQty' :  second_tracking_qty , #added
                               'SecondTrackingPrice'    :  second_tracking_price , #added
                               'SlippageQty' :  slippage_qty , #TODO
                               'SlippagePrice'    :  slippage_price , #TODO
                               'OpportunityVolume' : self.opportunity_volume,
                               'ForwardOppSignal' : 100.0* self.fwd_signal, 
                               'BackwardOppSignal' : 100.0*self.bwd_signal,
                               'WouldQty'    :  self.total_would_qty , #TODO
                               'WouldPrice'  :   self.would_amount / self.total_would_qty  if  self.total_would_qty > 0 else 0, #TODO
                               'OpeningQty' : 0 ,#TBD
                               'OpeningOccurrencePosition' : 0 ,#TBD
                               'IntradayQty' : 0,#TBD ??? 
                               'IntradayOccurrencePosition' : 0,#TBD ???
                               'ClosingQty' : 0 ,#TBD
                               'ClosingOccurrencePosition' : 0 ,#TBD
                               'OpeningExecPercentage' : 0 ,#TBD
                               'ClosingExecPercentage' : 0 ,#TBD
                               'TrackingExecPercentage' : 100.0 * self.tracking_volume / exec_qty if exec_qty > 0 else 0,#TOCHECK
                               'SlippageExecPercentage' : 100.0 * self.slippage_volume / exec_qty if exec_qty > 0 else 0,#TOCHECK
                               'WouldExecPercentage'  :  100.0 * self.total_would_qty  / exec_qty if exec_qty > 0 else 0, #TBD                               'BusinessTimeDealCount' :  self.marketManager.getFeedInfo('AE', 'C1_CONVNUM'), 
#                               'OpeningTheoricVolume' :  0, #TBD
#                               'ClosingTheoricVolume' :  0, #TBD
#                               'IntradayTheoricVolume' :  0, #TBD
#                               'MedAdjustedPercentage' :  0, #TBD
#                               'VolUpperBound'  : self.deltaVolumes[2],
                               'VolLowerBound' : self.deltaVolumes[0],
                               'MinExecQty' : mkt_qty * self.qmin,
                               'MedExecQty' : mkt_qty * self.qmid,
                               'MaxExecQty' : mkt_qty * self.qmax,
                               })
        
    def computeImbalance(self, evt):
        self.imbalance = (self.event_market['BEST_OPPOSITE_SIZE1'] + self.event_market['BEST_OPPOSITE_SIZE2'] - self.event_market['BEST_SIZE1'] - self.event_market['BEST_SIZE2'])
    
    def convertDateToSecond(self, datevalue):
        #print datestr, ':::', datestr[9:11], datestr[12:14], datestr[15:16]        
        return 24.0*3600*(datevalue.toordinal() + datevalue.hour / 24.0 + datevalue.minute / (24.0 * 60)+ datevalue.second / (24.0 * 60 * 60) + datevalue.microsecond / (24.0 * 60 * 60 * 1000000))
        
    def convertDateStrToSecond(self, datestr):        
        return int(datestr[0:1]) *24.0*3600.0 + int(datestr[3:4])* 3600.0 + int(datestr[6:7])   

