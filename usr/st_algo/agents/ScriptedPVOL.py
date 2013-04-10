'''Scripted PVOL Algorithm (% Volume) 

  CACVersion $File: ScriptedPVOL.py$ Version 1.1.0 $Branch: QuantitativeResearch$ $Date: 09/12/2012 04:11:25 PM$

'''
surround_branch = '$Branch: QuantitativeResearch$'
surround_header = '$Header: ScriptedPVOL.py: Revision: 3: Author: midan: Date: 09/12/2012 04:11:25 PM$'
tok_head = surround_header.split(': ') 
tok_branch = surround_branch.split(': ') 

local_version = 'r4'

__version__ = "CACVersion %s Version %s.%s %s" % (tok_head[1][:-3], tok_branch[1][:-1], local_version, tok_head[7])
__author__ = 'midan@cheuvreux.com'
__reviewer__ = 'svlasceanu@cheuvreux.com'
__review_date__ = '2011/09/26'


# import under AlgoBox
#from simep.tactic import Tactic
#from sm.MarketStateManager import MarketStateManager

# import under SIMEP
from simep.core.subtactic import Tactic

from collections import defaultdict
from collections import deque
from random import random
import types

class ScriptedPVOL(Tactic):
    class MarketState:
        REGULAR = 1
        INVALID = 2
        WAIT = 3
        CLOSED = 4
        
    class AlgoZone:
        FAR_AHEAD = 3
        AHEAD = 2
        LATE = 1
        VERY_LATE = 0
        
    class ZonePriority:
        Priority = {}
        # zone is 0, very late        
        Priority[0] = {'WouldFAKOrder':-1,
                            'TrackingOrder1': 2,
                            'TrackingOrder2': 3,
                            'SlippageOrder': 1,
                            'WouldOrder': 5 ,
                            'OpportunityOrder': 4 }
        
        # zone is 1,  late
        Priority[1] = {'WouldFAKOrder':-1,
                            'TrackingOrder1':  1,
                            'TrackingOrder2': 2,
                            'SlippageOrder': 3,
                            'WouldOrder': 5 ,
                            'OpportunityOrder': 4}
        
        # zone is 2, advance
        Priority[2] = {'WouldFAKOrder':-1,
                       'TrackingOrder1':  1,
                       'TrackingOrder2': 2,
                       'SlippageOrder': 7,
                       'WouldOrder': 5,
                       'OpportunityOrder': 4}
        
        # zone is 3, very advanced
        Priority[3] = {'WouldFAKOrder':-1,
                            'TrackingOrder1':  7,
                            'TrackingOrder2': 7,
                            'SlippageOrder': 7,
                            'WouldOrder': 7,
                            'OpportunityOrder': 7}
        
    class Tools:
        def __init__(self, side):
            self.side = side
    
        def worstBetween(self, *args):
            return max(args) if self.side == 1 else min(args)
    
        
        def invert_nested_dict(self, my_dict):
            flipped = defaultdict(dict)
            for key, val in sorted(my_dict.items(), reverse=self.side == 1):
                for subkey, subval in val.items():
                    flipped[subkey][key] = subval
            return flipped
            
        def count_substring_in_list(self, my_list, sub_str):
            counter = 0
            keys = list()
            lower_sub_str = sub_str.lower()
            for s in my_list:
                if s.lower().find(lower_sub_str) > -1 :
                    counter += 1
                    keys.append(s)
                    #print "tools:count_substring_in_list: %s is in %s" % (lower_sub_str, s.lower())
                #else :
                    #print "tools:count_substring_in_list: %s is not in %s" % (lower_sub_str, s.lower())
            return (counter, keys)   
        
        
        def bestBetween(self, *args):
            return min(args) if self.side == 1 else max(args)
                 
        def convertDateToSecond(self, datevalue):
            #print datestr, ':::', datestr[9:11], datestr[12:14], datestr[15:16]        
            return 24.0 * 3600 * (datevalue.toordinal() + datevalue.hour / 24.0 + datevalue.minute / (24.0 * 60) + datevalue.second / (24.0 * 60 * 60) + datevalue.microsecond / (24.0 * 60 * 60 * 1000000))
        
        def convertDateStrToSecond(self, datestr):        
            return int(datestr[0:1]) * 24.0 * 3600.0 + int(datestr[3:4]) * 3600.0 + int(datestr[6:7])            
        
    class DictDiffer(object):
        '''
        Calculate the difference between two dictionaries as:
        (1) items added
        (2) items removed
        (3) keys same in both but changed values
        (4) keys same in both and unchanged values
        '''
        def __init__(self, current_dict, past_dict):
            self.current_dict, self.past_dict = current_dict, past_dict
            self.set_current, self.set_past = set(current_dict.keys()), set(past_dict.keys())
            self.intersect = self.set_current.intersection(self.set_past)
        def added(self):
            return self.set_current - self.intersect 
        def removed(self):
            return self.set_past - self.intersect 
        def changed(self):
            return set([o for o in self.intersect if self.past_dict[o] != self.current_dict[o]])
        def unchanged(self):
            return set([o for o in self.intersect if self.past_dict[o] == self.current_dict[o]])
               
        
    def __init__(self, setup, context, parameters):
        
        Tactic.__init__(self, setup, context, parameters)
        
        # ATS        
        self.ats = 0.0
        
        #=======================================================================
        #                      Execution Rate: min, med, max
        #=======================================================================
        self.qmin = 0.0
        self.qmax = 0.0
        self.qmid = 0.0        
        self.qslipp = 0.0
        
        self.first_process = True
        self.total_amount = 0.0
        
        self.slippage_volume = 0
        self.slippage_amount = 0
        self.tracking_volume = 0        
        self.tracking_amount = 0
        self.opportunity_volume = 0 
        self.opportunity_amount = 0
        self.total_would_qty = 0
        self.would_amount = 0
        
        # counter for opportunity and slippage occurrences
        self.next_opportunity_deal = 0
        self.next_slippage_deal = 0
        
         
        self.mkt_qty = 0        
        self.spread_bid_ask_bp = 0        
        self.exec_qty_before_modify = 0
        
        # aggressiveness management
        self.aggressiveness_factor = 0
        self.width_enveloppe = 0
        
        # would level
        self.would_occ_qty = 0
        self.would_level_is_used = False
        
        # Mono/Multi Destination        
        self.is_mono_destination = True
        # limit price
        self.is_limited = False
        
        self.latest_submitted_parameters = None
        
        
        #=======================================================================
        #                     Auction Related Variables
        #=======================================================================
        self.is_in_auction = False
        self.first_time_in_auction = False
        self.first_time_in_trading_regular = False
        self.last_auction_code = None
        self.executed_in_auction = 0
        self.delta_auction_qty = 0
        self.market_auction_qty = 0
        self.total_auction_qty = 0                
        self.corrected_auction_qty = 0
        self.corrected_auction_qty_coeff = 0
        
        # wait images 
        self.wait_images = 0
        
        self.last_mkt_qty = 0
        
        self.rate_is_adjusted = False
        
        
        self.total_continuous_market_qty = 0
        self.delta_volumes = [0.0, 0.0, 0.0]
        self.current_zone = -1
        
        #=======================================================================
        #                         For Estimate Volume
        #=======================================================================
        self.last_trade_qty = deque()
        self.estimate_volume = 0
        
        #=======================================================================
        #                      For Tracking Occurrences
        #=======================================================================
        self.market_qty_between_tracking_exec = 0        
        self.last_mkt_qty_placing_tracking = 0
        self.last_exec_qty_placing_tracking = 0        
        self.coeff_first_tracking_occurrence = 0
        self.tracking_modification_coeff = 0
        self.last_mkt_qty_modification_tracking = 0
        
        self.begin_time = 0
        #=======================================================================
        #                            Market State
        #=======================================================================
        self.market_state = None        
        
        
        #=======================================================================
        #               Forward/Backward Signal Variables
        #=======================================================================        
        self.mean_fwd_signal = 0.0
        self.mean_bwd_signal = 0.0
        self.fwd_signal = 0.0
        self.bwd_signal = 0.0
        self.last_fwd_signal = deque()
        self.last_bwd_signal = deque()
        
        self.quantileMeanFwdLow = list()
        self.quantileMeanBwdLow = list()
        self.quantileMeanFwdHigh = list()
        self.quantileMeanBwdHigh = list()
        
        # signal condition
        self.signal_condition = 0.0
        
        try:
            from usr.dev.st_algo.funcs.OpportunitySignal import OpportunitySignal
            self.opportunity_signal = OpportunitySignal(window_size = self['LengthWindowSignals'])            
        except ImportError:
            self.opportunity_signal = None
        
        #=======================================================================
        #                          For Backward Signal
        #=======================================================================
        self.exp_exec_time  = 0        
        self.exp_end_of_period_volume = 0        
        
        #=======================================================================
        #                             Optimal Rate
        #=======================================================================
        self.optimal_rate = [0.0, 0.0]
        
        #=======================================================================
        #                         Execution Percentage
        #=======================================================================
        self.exec_pct = 0.0
        
    def checkParameters(self, parameters, error):
        '''
            @summary: check submitted parameters
            @todo:
                Execution Style
                Aggressiveness
        '''
        
        self.log("checkParameters: checking parameters")
        self.log("checkParameters: client_params: %s" % dict(self.client_params))
        self.log("checkParameters: current_params: %s" % self._parameters)
        if self['SimulatedMode']:
            self.log("checkParameters: Using Simulated Mode")
        else:
            self.log("checkParameters: NOT Using Simulated Mode")
            
        # Buy = 1, Sell = 2
        self.latest_submitted_parameters = self._parameters
        
        if (self['PrimaryVolume'] == 1) or (self['PrimaryVolume'] == None):
            self.is_mono_destination = True
            self.log("checkParameters: Mono Destination")
        else:
            self.is_mono_destination = False
            self.log("checkParameters: Multi Destination")
        
        if float(self['OrderQty']) <= 0:
            error.message = "Invalid Asked Quantity, please check"
        
        
        #=======================================================================
        #                               Min % and Max %
        #=======================================================================
        if (self['MinPctVolume'] == None) and (self['ParticipationRate'] == None):
            error.message = "Invalid Participation Rates: at least one percentage must be known"
            return False
        
        if not self['SpreadBidAskBp']: 
            if self._parameters.has_key('SpreadBidAskBpLowerBound') and self._parameters.has_key('SpreadBidAskBpUpperBound'):
                self.spread_bid_ask_bp = 0.5 * (float(self['SpreadBidAskBpUpperBound']) + float(self['SpreadBidAskBpLowerBound']))
        else:
            self.spread_bid_ask_bp = max(float(self['SpreadBidAskBp']), float(self['SpreadBidAskBpLowerBound']))
            self.spread_bid_ask_bp = min(float(self['SpreadBidAskBp']), float(self['SpreadBidAskBpLowerBound']))
        
        if (self['MinPctVolume'] == None) and (self['ParticipationRate'] != None):
            # value by default Min % = Max % - 0.1  * Max % 
            self._parameters['MinPctVolume'] = max(0, 0.9 * self['ParticipationRate'])
            
        if (self['MinPctVolume'] != None) and (self['ParticipationRate'] == None):
            # value by default Max % = Min % + 0.1  * Min % 
            error.message = "You must specify a Max %"
            return False
        
        if  self['MinPctVolume'] > self['ParticipationRate']:
            error.message = "Invalid Participation Rates : MinPctVolume (%s) must be < to ParticipationRate(%s)" % (self['MinPctVolume'], self['ParticipationRate'])
            return False
        
        # now check with respect to the Maximum PLR  
        if self['MaximumPLR']:
            if int(self['ParticipationRate']) > int(self['MaximumPLR']) :
                self._parameters['ParticipationRate'] = self['ParticipationRate']
        
        #=======================================================================
        #                            ExecutionStyle
        #=======================================================================
        if (self['ExecutionStyle'] == None): 
            # value by default 0, from 0 to 20 
            self._parameters['ExecutionStyle'] = 0
            
        self.aggressiveness_factor = 0.01 * float(self['ExecutionStyle'])/20.0
                    
        if self['Side'] == '1' or self['Side'] == 1:
            self.side = 1
        else:
            self.side = -1
            
        #=======================================================================
        # Tool class
        #=======================================================================
        self.tools = self.Tools(self.side)
        
        #=======================================================================
        #                    Limit and Would Price
        #=======================================================================
        if self["Price"]:
            self.is_limited = True
            self.limit_price = float(self["Price"])
        elif self.side == 1:
            self.is_limited = False
            self.limit_price = 1e99    
        else:
            self.is_limited = False
            self.limit_price = -1e99
    
        if self["AdjustedPriceInd"]:
            self.would_level_is_used = True
            self.would_level = float(self["AdjustedPriceInd"])
            if self.side * self.would_level >= self.side * self.limit_price:
                error.message = "The would level is incorrect by respect to the limit price."
                return False            
        elif self.side == 1:
            self.would_level = 0
        elif self.side == -1:
            self.would_level = 1e99
        
        #=======================================================================
        #                    Auction Related Variables
        #=======================================================================
        if self['OpeningAuctionPolyCoeff']:
            self.openingAuctionPolyCoeff = [float(self['OpeningAuctionPolyCoeff'].split(',')[i]) for i in range(5)]
        if self['ClosingAuctionPolyCoeff']:
            self.closingAuctionPolyCoeff = [float(self['ClosingAuctionPolyCoeff'].split(',')[i]) for i in range(5)]
                
        if self['ExcludeAuction']:            
            
            self.log('Auction Exclusions: %s' % self['ExcludeAuction'])
            excluded_auction_list = self['ExcludeAuction'].split(' ')
            
            self.participate_in_opening = ('1' not in excluded_auction_list)
            
            if self.participate_in_opening:
                self.log('checkParameters: Opening included')
            
            self.participate_in_closing = ('4' not in excluded_auction_list)
            
            if self.participate_in_closing:
                self.log('checkParameters: Closing included')
            
            self.participate_in_intraday = ('3' not in excluded_auction_list)
            
            if self.participate_in_intraday:
                self.log('checkParameters: Intraday included')
            
            self.participate_in_mid_auction = ('8' not in excluded_auction_list)
            
            if self.participate_in_mid_auction :
                self.log('checkParameters: Mid day included')  
            else:
                self.participate_in_mid_auction = False
                self.participate_in_intraday = False
                self.participate_in_closing = False
                self.participate_in_opening = False
             
        #=======================================================================
        #                          Max Wait Images
        #=======================================================================
        if self['MaxWaitImages']:
            self.max_wait_images = int(self['MaxWaitImages'])
        else:
            self.max_wait_images = 20
            
        #=======================================================================
        #                 Tracking Modification Condition
        #=======================================================================
        if self['TrackingModificationCoeff']:
            self.tracking_modification_coeff = float(self['TrackingModificationCoeff'])
        else:
            self.tracking_modification_coeff = 0.0
        
        #=======================================================================
        #                            Estimate Volume
        #=======================================================================
        if self['LengthWindowVolumes']:
            self.lengthWindowVolumes = int(self['LengthWindowVolumes'])
        else:
            self.lengthWindowVolumes = 10
        
        #=======================================================================
        #                      Forward / Backward Signal    
        #=======================================================================
        if self['QuantileMeanFwdLow']:
            quantileMeanFwdLow = self['QuantileMeanFwdLow'].split(';')
            self.quantileMeanFwdLow = sorted([float(x) for x in quantileMeanFwdLow], reverse = (self.side==1))
        
        if self['QuantileMeanFwdHigh']:
            quantileMeanFwdHigh = self['QuantileMeanFwdHigh'].split(';')
            self.quantileMeanFwdHigh = sorted([float(x) for x in quantileMeanFwdHigh], reverse = (self.side==1))
            
        if self['QuantileMeanBwdLow']:
            quantileMeanBwdLow = self['QuantileMeanBwdLow'].split(';')
            self.quantileMeanBwdLow = sorted([float(x) for x in quantileMeanBwdLow], reverse = (self.side==1))
        
        if self['QuantileMeanBwdHigh']:
            quantileMeanBwdHigh = self['QuantileMeanBwdHigh'].split(';')
            self.quantileMeanBwdHigh = sorted([float(x) for x in quantileMeanBwdHigh], reverse = (self.side==1))
        
        if self['LengthWindowSignals']:
            self.lengthWindowSignals = float(self['LengthWindowSignals'])
        else:
            self.lengthWindowSignals = 60.0
            
        return True           
    
    
    def validateModifyParameters(self, client_params, rejection_message):
        '''
            @summary: validate new submission parameters
            @param client_params: new submission parameters
            @param rejection_message: contains the reason for rejection, if any
            @return: True for validated, False otherwise  
        '''
       
        if not self.checkParameters(client_params, rejection_message):
            return False
        else:
            if not client_params.has_key('ParticipationRate'):
                rejection_message.message = "validateModifyParameters: order rejected because no max %"
                return False
            else :  
                if not client_params.has_key('MinPctVolume') :
                    client_params['MinPctVolume'] = max(0.0, 0.9 * client_params['ParticipationRate'])
                elif  client_params['ParticipationRate'] < client_params['MinPctVolume']:
                    return False
            
            best_opposite = self.marketManager.getFeedInfo(self.primary_venue_id, 'BEST_OPPOSITE1')
            spread = self.marketManager.getFeedInfo(self.primary_venue_id, 'BEST_ASK1') - self.marketManager.getFeedInfo(self.primary_venue_id, 'BEST_BID1')   
            
            if client_params.has_key('WouldLevel') and client_params['WouldLevel'] * self.side >= best_opposite + self.side * spread:
                
                # reject, use another algo
                rejection_message.message = "validateModifyParameters: order rejected because of WouldLevel"
                return False 
            
            if client_params['ParticipationRate'] < client_params['MinPctVolume']:
                rejection_message.message = "validateModifyParameters: order rejected because of inconsistent Min % and Max %"
                return False
            
            return True


    def reassesOnModify(self, order_prefix, qty_to_free):
        '''
            @summary: modify current order qty to satisfy the new OrderQty
            @param order_prefix: prefix of order to modify
            @param qty_to_free: quantity to be freed
            @return: new quantity to be freed
            @see: reset
        '''
        tracking_orders = list()
        
        for occ in self.moneyManager.getPrivateBook(order_prefix).keys():
            tracking_orders.append((occ, self.moneyManager.getPrivateBook()[occ]['LeavesQty'], self.moneyManager.getPrivateBook()[occ]['Price']))
       
        
        #sort the orders (far from the best) first        
        tracking_orders.sort(key = lambda order: order[2], reverse = (self.side == 1))
        for order in tracking_orders:
            self.modifyOccurrence(tactic_type = 'IcebergMono',
                                  venue_id = self.moneyManager.getPrivateBook()[order[0]]["VenueId"], 
                                  oc_id = order[0], 
                                  tactic_params = {'Price':order[2], 'OrderQty': max(0, order[1] - qty_to_free)})
            qty_to_free = qty_to_free - order[1]
            if qty_to_free <= 0:
                # no need to cancel more
                return 0
        return qty_to_free

    def validateParameters(self):
        # retrieve trading hours 
        return True    
    
    
    def reset(self, client_params, reject_msg):
        parameters_to_check = set([ 'MinPctVolume', 'ParticipationRate', 'Price', 'AdjustedPriceInd', 'ExecutionStyle', 'PrimaryVolumeOnly', 'OrderQty' ]) 
        new_params = self.convertToNumericValues(client_params) 
        dict_comparison = self.DictDiffer(new_params, self.latest_submitted_parameters)
        self.log('reset: dict_comparison = %s'%dict_comparison)

        self.log("reset: CURRENT_PARAMS: %s" % (self._parameters))
        self.log("reset: CLIENT_PARAMS: %s" % (client_params))
        self.log("reset: SELF(destinationId, PrimaryVolume): (%s,%s)" % (self["destinationId"], self['PrimaryVolume']))
        
        if len(dict_comparison.changed() & parameters_to_check) == 0 and len(dict_comparison.added() & parameters_to_check) == 0 and len(dict_comparison.removed() & parameters_to_check) == 0 : 
            self.log("reset: processing modification, but same parameters submitted")
            self.log("reset: differences between parameters: ")
            self.log("%s" % dict_comparison.changed() & parameters_to_check)
            self.log("Added:")
            self.log("%s" % dict_comparison.added() & parameters_to_check)
            self.log("Removed:")
            self.log("%s" % dict_comparison.removed() & parameters_to_check) 
            self.log("Original parameters:")
            self.log("%s" % self.latest_submitted_parameters) 
            self.log("Resubmitted  parameters:")
            self.log("%s" % new_params)
            self.latest_submitted_parameters = new_params
            self.rate_is_adjusted = False
            return Tactic.Modify.ACCEPT
        
        else :
            self.log("reset: processing  modification, parameters have been changed")
#            self.log("reset: differences between parameters:")
#            self.log("%s" %  dict_comparison.changed() & parameters_to_check)
#            self.log("Added:")
#            self.log("%s" %  dict_comparison.added() & parameters_to_check)
#            self.log("Removed:")
#            self.log("%s" %  dict_comparison.removed() & parameters_to_check) 
#            self.log("Original parameters:")
#            self.log("%s" %  self.latest_submitted_parameters) 
#            self.log("Resubmitted  parameters:")
#            self.log("%s" %  new_params)
            
        # check validity of the new parameters and reject if necessary 
        if client_params['OrderQty'] < self.moneyManager.getExecQty():
            # we need to reject 
            # display reason for rejection
            reject_msg.message = 'Modification Rejected: asked quantity is less than quantity already executed'
            return Tactic.Modify.REJECT        
        
        # Cancel and Replace
        limit_price_has_not_changed = (client_params.has_key('Price') and self['Price'] and self['Price'] == client_params['Price']) or (not client_params.has_key('Price') and self['Price'] == None)
        would_price_has_not_changed = (client_params.has_key("AdjustedPriceInd") and self['AdjustedPriceInd'] and self['AdjustedPriceInd'] == client_params['AdjustedPriceInd']) or (not client_params.has_key('AdjustedPriceInd') and self['AdjustedPriceInd'] == None)
        
        
        # Primary Volume Management on modify(TOTEST)
        if self._parameters.has_key('PrimaryVolume'):
            if self['PrimaryVolume'] != None:
                # we have a value: compare to existing one
                primary_only_has_not_changed = (client_params['PrimaryVolume'] == self['PrimaryVolume']) if client_params.has_key('PrimaryVolume') else True
        else :
            # raise an error
            self.error("reset: PrimaryVolume is not defined, something wrong here")
        
        # TVenue Management on modify (TOTEST)        
        if self._parameters.has_key('destinationId'):
            if  self["destinationId"] != None:
                # we have a value: compare to existing one
                if client_params.has_key('destinationId'):
                    td_has_not_changed = (client_params['destinationId'] == self['destinationId']) if  client_params.has_key('destinationId') else True
                    self.log("reset: destinationId : Old :%d, New : %d !" % (self['destinationId'], client_params['destinationId']))
                else:
                    td_has_not_changed = True
            else:
                self.error("reset: destinationId is None, something wrong here")
        else :
            self.error("reset: destinationId is undefined, something wrong here")
        
        if client_params.has_key("AdjustedPriceInd") and self['AdjustedPriceInd'] == None :
            self.would_level_is_used = False
        elif (not client_params.has_key("AdjustedPriceInd")) and  self['AdjustedPriceInd'] != None :
            self.would_level_is_used = True
        
        if not client_params.has_key('ParticipationRate'):  
            self.error("reset: please specify at least a max %")
        elif not client_params.has_key('MinPctVolume'):
            client_params['MinPctVolume'] = max(0, 0.9 * self['ParticipationRate'])
        
        vol_percent_changed = (client_params['ParticipationRate'] != self['ParticipationRate']) or (client_params['MinPctVolume'] != self['MinPctVolume'])          
    
        base_volume_computation_changed = vol_percent_changed or not (limit_price_has_not_changed and would_price_has_not_changed and td_has_not_changed and primary_only_has_not_changed)
        
        if base_volume_computation_changed :
            self.log("reset: Base Volume changed, reinitializing tactic")
            # Cancel & Replace             
            # what we should do with the occurrences ? Cancel all of them   
            self.cancelAllLivingOrders()
                        
            # market volume from new submission
            self.total_continuous_market_qty += self.marketManager.getFeedInfo('AE', 'QTE_CNT1')
            self.exec_qty_before_modify = self.moneyManager.getExecQty() 
                         
            self.log("reset: vol_percent_changed :%d" % vol_percent_changed)
            self.log("reset: limit_price_has_not_changed :%d" % limit_price_has_not_changed)
            self.log("reset: would_price_has_not_changed :%d" % would_price_has_not_changed)
            self.log("reset: td_has_not_changed :%d" % td_has_not_changed)
            self.log("reset: primary_only_has_not_changed :%d" % primary_only_has_not_changed)
                     
            self.log("reset: updating exec_qty_before_modify = %d" % (self.exec_qty_before_modify))
            
            # reset the tactic
            return Tactic.Modify.REINITIALIZE
        
        # manage new quantity with respect to executed quantity
        self.log("reset: OrderQty = %d, ExecQty = %d" % (client_params['OrderQty'], self.moneyManager.getExecQty()))    
        if client_params['OrderQty'] > self.moneyManager.getLeavesQty():
            # we can continue, with the new asked quantity
            # update the available Quantity with the new Amount
            self.moneyManager.setTotalQty(client_params['OrderQty'])
            self.log("reset: Updating Total Qty")
            return Tactic.Modify.ACCEPT
        
        else:
            # we will need to free some quantity using the following order: 
            # first cancel passive order, order by price, best price (far from the best) first
            # Initialize the Quantity to free 
            qty_to_free = self.moneyManager.getLeavesQty() - client_params['OrderQty'] 
            
            # Passive (Opportunity, Would, Tracking)
            qty_to_free = self.reassesOnModify("WouldOrder", qty_to_free)
            qty_to_free = self.reassesOnModify("OpportunityOrder", qty_to_free)
            qty_to_free = self.reassesOnModify("TrackingOrder", qty_to_free)            
            
            return Tactic.Modify.REINITIALIZE
            
        # otherwise accept the modifications
        self.log("reset: Accept Modifications")
        return Tactic.Modify.ACCEPT
   
    def computeAuctionPositionPrice(self, occurence_position, last_price):
        '''
            @summary: compute the price during a given auction phase (opening, intraday, closing)
            @see: computeAuctionPrice
        '''
        # get the offset
        if '+' in occurence_position:
            parts = occurence_position.split('+')
            base_position = parts[0]
            look_for_offset = True
        elif '-' in occurence_position:
            parts = occurence_position.split('-')
            base_position = parts[0]
            look_for_offset = True
        else:
            look_for_offset = False
            base_position = occurence_position
        
        self.log("computeAuctionPositionPrice: %s %d, base position is: %s " % (occurence_position, last_price, base_position))
        auction_price = None
        if look_for_offset:
            # we have an offset
            if len(parts) > 1 : 
                if 'bp' in parts[1]:
                    if '+' in occurence_position:
                        offset_bp = float(parts[1].split('bp')[0])
                    elif '-' in occurence_position:
                        offset_bp = -1 * float(parts[1].split('bp')[0])       
                    else :
                        self.error("computeAuctionPositionPrice: Not able to determine the price offset for %s" % occurence_position)
                else:
                    self.log("computeAuctionPositionPrice: Not able to determine the price offset for %s, using 0" % occurence_position)
        else :
            self.log("computeAuctionPositionPrice: No price offset for %s found, using 0" % occurence_position)
            offset_bp = 0
            # look for the right reference
        if base_position.lower() == 'last' :
            last = float(self.event_market['DEAL_PRICE'])
            self.log("computeAuctionPositionPrice: Using last: , %f" % last) 
            auction_price = float(last) + self.side * last_price * offset_bp / 10000.0
        elif base_position.lower() == 'market': 
            self.log("computeAuctionPositionPrice: using market price")
            auction_price = 'Market'
        elif base_position.lower() == 'close':
            close = float(self.marketManager.getFeedInfo('AE', 'HST_CLOSE'))
            self.log("computeAuctionPositionPrice: using close price: %f" % close)
            auction_price = float(close) + self.side * last_price * offset_bp / 10000.0
        elif base_position.lower() == 'open' or base_position.lower() == 'marketopen':
            open = float(self.marketManager.getFeedInfo('AE', 'OPEN1'))
            self.log("computeAuctionPositionPrice: using open  price: %f" % open)
            auction_price = float(open) + self.side * last_price * offset_bp / 10000.0
        else:
            self.error("computeAuctionPositionPrice: stopping, using unknown order position for auction: %s" % base_position)
        
        return auction_price
         
    
    def computeAuctionPrice(self, market_state, last_price):
        '''
            @summary: compute the price for the auction order, using Auction Position Price
            @see: computeAuctionPositionPrice
        '''
        # compute (price, volume) of the order in the auction
        self.log("computeAuctionPrice: trading state is %s" % market_state)
        
        if market_state == 'O':
            if self['OpeningOccurrencePosition'] != None or self['OpeningOccurrencePosition'] != '':
                auction_price = self.computeAuctionPositionPrice(self['OpeningOccurrencePosition'], last_price)
            else:
                self.log(" %s seems invalid, please check" % self['OpeningOccurrencePosition'])
        elif market_state == 'C':
            if self['ClosingOccurrencePosition'] != None or self['ClosingOccurrencePosition'] != '':
                auction_price = self.computeAuctionPositionPrice(self['ClosingOccurrencePosition'], last_price)
            else:
                self.log(" %s seems invalid, please check" % self['ClosingOccurrencePosition'])
        elif market_state == 'I':
            if self['IntradayOccurrencePosition'] != None or self['IntradayOccurrencePosition'] != '':
                auction_price = self.computeAuctionPositionPrice(self['IntradayOccurrencePosition'], last_price)
            else:
                self.log(" %s seems invalid, please check" % self['IntradayOccurrencePosition'])
            
        else:
            self.log('Trading state is %s, not recognized' % market_state)
  
        if isinstance(auction_price, float):            
            self.log("computeAuctionPrice: OccurrencePosition %s, auction_price = %f  " % (self['ClosingOccurrencePosition'], auction_price))
        elif isinstance(auction_price, basestring):
            self.log("computeAuctionPrice: OccurrencePosition %s ,auction_price = %s  " % (self['ClosingOccurrencePosition'], auction_price))
        
        if self.is_limited and isinstance(auction_price, basestring):
            auction_price = self.limit_price
        elif self.is_limited and isinstance(auction_price, float):
            auction_price = self.tools.bestBetween(self.limit_price, auction_price)
            
        return auction_price
            
        
    def prepareAuctionOccurrence(self, evt, auction_price, auction_quantity, auction_leaves_quantity):
        '''
            @summary: prepare the auction occurrence
            @param auction_price: price
            @param auction_quantity: quantity
            @param auction_leaves_quantity: leaves quantity
            @todo: 
                always possible to modify an Auction Order?
        '''
        
        delta_volume_to_adjust = auction_quantity - auction_leaves_quantity 
        self.log ("prepareAuctionOccurrence: delta_volume_to_adjust = %d" % int(delta_volume_to_adjust))
        auction_occurence = self.moneyManager.getPrivateBook("ClassicAuction")
        
        if len(auction_occurence) > 0:
            # we have already an occurrence, do a modify
            if len(auction_occurence) > 1:
                self.error("prepareAuctionOccurrence: we should have a single auction order")
            else:
                self.log("prepareAuctionOccurrence: we have already only one order, do a modify")                
                auction_occurence = self.moneyManager.getPrivateBook("ClassicAuction").keys()[0]
                self.log("prepareAuctionOccurrence: existing order is %s" % auction_occurence)
                auction_order_exist = True
        else:  
            auction_order_exist = False

        # we add a new order in all cases if no auction living order
        if delta_volume_to_adjust > self.ats or not auction_order_exist:
            # we add a new order            
            if isinstance(auction_price, basestring):
                # Market Order
                self.log("prepareAuctionOccurrence: price auction is a string %s"%auction_price)
                if not self.is_limited:
                    if 'Market' in auction_price:
                        # do we have already an auction order ? 
                        if auction_order_exist:
                            self.modifyOccurrence(tactic_type = 'MarketOrder',
                                                  venue_id = evt.getVenueId(),
                                                  oc_id = auction_occurence,
                                                  tactic_params = {'OrderQty': int(auction_quantity),
                                                                   'OrderType': 'market'})
                            self.printOrder(1000000, auction_quantity, 'Auction Market Order', 'Modify')   
                        else:
                            self.createOccurrence(tactic_type = 'MarketOrder',
                                                  venue_id = evt.getVenueId(),
                                                  prefix = 'ClassicAuction',
                                                  tactic_params = {'OrderQty': int(auction_quantity),
                                                                   'OrderType': 'market'})
                        self.log("prepareAuctionOccurrence: Market Auction Order Qty = %d" % int(auction_quantity))
                    else:
                        self.log("prepareAuctionOccurrence: something wrong with the auction price")
                else:
                    # do we have already an auction order ? 
                    if auction_order_exist:                         
                        self.modifyOccurrence(tactic_type = 'MarketOrder',
                                              venue_id = evt.getVenueId(),
                                              oc_id = auction_occurence,
                                              tactic_params = {'Price': self.limit_price,
                                                               'OrderQty': int(auction_quantity),
                                                               'OrderType': 'market'})
                        self.printOrder(self.limit_price, auction_quantity, 'Auction Limit Order', 'Modify', 'prepareAuctionOccurrence')    
                    else:
                        # no order, apply the limit price
                        self.createOccurrence(tactic_type = 'MarketOrder',
                                              venue_id = evt.getVenueId(),
                                              prefix = 'ClassicAuction',
                                              tactic_params={'Price': self.limit_price,
                                                             'OrderQty': int(auction_quantity),
                                                             'OrderType': 'limit'})
                        self.printOrder(self.limit_price, delta_volume_to_adjust, 'Auction Limit Order', 'Insert', 'prepareAuctionOccurrence')                    
            
            elif isinstance(auction_price, float):
                self.log("prepareAuctionOccurrence: price auction = %.2f"%auction_price)
                if auction_order_exist:
                    self.modifyOccurrence(tactic_type = 'MarketOrder',
                                          venue_id = evt.getVenueId(),
                                          oc_id = auction_occurence,
                                          tactic_params = {'Price': self.tools.bestBetween(auction_price, self.limit_price),
                                                           'OrderQty': int(auction_quantity),
                                                           'OrderType': 'limit'})
                    self.printOrder(auction_price, int(auction_quantity), 'Auction Limit Order', 'Modify', 'prepareAuctionOccurrence')
                
                else :
                    self.createOccurrence(tactic_type = 'MarketOrder',
                                          venue_id = evt.getVenueId(),
                                          prefix = 'ClassicAuction',
                                          tactic_params = {'Price': self.tools.bestBetween(auction_price, self.limit_price),
                                                           'OrderQty': int(auction_quantity),
                                                           'OrderType': 'limit'})                
                    self.printOrder(auction_price, int(delta_volume_to_adjust), 'Auction Limit Order', 'Insert', 'prepareAuctionOccurrence')
                
        elif delta_volume_to_adjust < -self.ats:
            
            if auction_order_exist:
                if isinstance(auction_price, float):
                    self.modifyOccurrence(tactic_type = 'MarketOrder',
                                          venue_id = evt.getVenueId(),
                                          oc_id = auction_occurence,
                                          tactic_params = {'Price': auction_price,
                                                           'OrderQty': int(auction_quantity),
                                                           'OrderType': 'limit'})
                    self.printOrder(auction_price, int(auction_quantity), 'Auction Limit Order', 'Modify', 'prepareAuctionOccurrence')
                elif isinstance(auction_price, basestring):
                    self.modifyOccurrence(tactic_type = 'MarketOrder',
                                          venue_id = evt.getVenueId(),
                                          oc_id = auction_occurence,
                                          tactic_params = {'OrderQty': int(auction_quantity),
                                                           'OrderType': 'market'})
                    self.printOrder(1e99, int(auction_quantity), 'Auction Market Order', 'Modify', 'prepareAuctionOccurrence')
                else:
                    self.log("prepareAuctionOccurrence: type of the auction price seems incorrect, please check")
                    
            else:
                self.error("prepareAuctionOccurrence: you want to lower the qty, but no existing order")
        else:
            self.log("prepareAuctionOccurrence: no order modified or inserted because of delta_volume_to_adjust")
       
        if self.would_level_is_used:
            curWouldAuction = self.moneyManager.getPrivateBook("WouldAcution").keys()[0]
            would_auction_qty = self.moneyManager.getTotalQty() - auction_quantity
            if would_auction_qty > 0:
                if curWouldAuction :
                    self.modifyOccurrence(tactic_type = 'MarketOrder',
                                          venue_id = evt.getVenueId(),
                                          oc_id = curWouldAuction,
                                          tactic_params = {'Price': self.would_level,
                                                           'OrderQty': int(would_auction_qty),
                                                           'OrderType': 'limit'})
                    self.printOrder(self.would_level, int(would_auction_qty), 'Auction Would Order', 'Modify', 'prepareAuctionOccurrence')
                else:
                    self.createOccurrence(tactic_type = 'MarketOrder',
                                          venue_id = evt.getVenueId(),
                                          prefix = 'WouldAcution',
                                          tactic_params = {'Price': self.would_level,
                                                           'OrderQty': int(would_auction_qty),
                                                           'OrderType': 'limit'})
                    self.printOrder(self.would_level, int(would_auction_qty), 'Auction Would Order', 'Insert', 'prepareAuctionOccurrence')
                            
    def getCorrectedTheoVolCoeff(self, time_before_auction_end, market_state):
        '''
            @summary: compute the coefficient to be multiplied with the THEO_VOL diffused by AE to get the corrected THEO_VOL
            @param time_before_auction_end: time before the theoretical end, as specified in trading hours
            @param market_state: one character code, specifies the current auction: (O)pening, (C)losing, (I)ntraday, (M)id-day  
        '''
        if time_before_auction_end is None:
            return 1.0
        elif time_before_auction_end < 0.0:
            # if we have passed the theoretical end
            return 1.0
        elif time_before_auction_end > 10.0: 
            # TODO time_before_auction_end in second?
            if market_state == 'O':
                return self.openingAuctionPolyCoeff[4]
            elif market_state == 'C':
                return self.closingAuctionPolyCoeff[4]
        else:
            if market_state == 'O':
                return self.openingAuctionPolyCoeff[0] + self.openingAuctionPolyCoeff * time_before_auction_end + self.openingAuctionPolyCoeff[2] * time_before_auction_end ^ 2 + self.openingAuctionPolyCoeff[3] * time_before_auction_end ^ 3
            elif market_state == 'C':                
                return self.closingAuctionPolyCoeff[0] + self.closingAuctionPolyCoeff[1] * time_before_auction_end + self.closingAuctionPolyCoeff[2] * time_before_auction_end ^ 2 + self.closingAuctionPolyCoeff[3] * time_before_auction_end ^ 3
    
    
    def validateMarketCondition(self, market_state):
        ''' 
            @summary: Function to validate a market condition
            @param market_state: trading state
            @return: state from the MarketState class
            @todo: check Would Level condition 
        '''
        
        in_auction = False
        
        if market_state == "WAIT":
            return self.MarketState.WAIT
        
        if len(market_state) > 0:
            if market_state[0] == 'A':
                in_auction = True
                self.auction_volume_is_adjusted = False
                if self.is_in_auction == False:
                    self.first_time_in_auction = True
                    self.first_time_in_trading_regular = False
                    
                self.is_in_auction = True
                self.log("validateMarketCondition: in auction")
            else:
                self.is_in_auction = False
                self.log("validateMarketCondition: not in auction %s" % market_state)
        else:
            self.log("validateMarketCondition: not able to determine market state")
            self.is_in_auction = False
            self.wait_images += 1
            if self.wait_images < self.max_wait_images:
                return self.MarketState.WAIT
            else:
                return self.MarketState.INVALID
            
        if not in_auction:
            if self.is_in_auction == True:
                self.first_time_in_trading_regular = True
                self.first_time_in_auction = False
                
            self.is_in_auction = False
            
            if not self.event_market['BEST1'] :
                self.log("validateMarketCondition: No Best1")
                self.wait_images += 1
                if self.wait_images < self.max_wait_images:
                    return self.MarketState.WAIT
                else:
                    return self.MarketState.INVALID
                        
            if not self.event_market['BEST2'] :
                self.log("validateMarketCondition: No Best2")
                
                if self.wait_images < self.max_wait_images :
                    self.wait_images += 1
                    return self.MarketState.WAIT
                else:
                    return self.MarketState.INVALID
        
            best_opposite = self.event_market['BEST_OPPOSITE1']
        
            if not best_opposite :
                self.log("validateMarketCondition: No BestOpposite")
                if self.wait_images < self.max_wait_images :
                        self.wait_images += 1
                        return self.MarketState.WAIT
                else:
                        return self.MarketState.INVALID
                    
            
            elif not (self.side * self.would_level < self.side * best_opposite * (1.0 + self.side * (2.0 * self.spread_bid_ask_bp + 10.0) / 10000.0)):
                #validate would level as we could not do this in the check_parameters
                self.log("validateMarketCondition: Would Level not validated")
                
                return self.MarketState.INVALID
            else:
                self.log("validateMarketCondition: Would Level validated")
                
        else:
            self.is_in_auction = True
            # we are in auction
            if market_state[1] == 'O' :
                return self.MarketState.WAIT if not self.participate_in_opening else self.MarketState.REGULAR
            elif market_state[1] == 'I' :
                return self.MarketState.WAIT if not self.participate_in_intraday else self.MarketState.REGULAR
            elif market_state[1] == 'C' :
                return self.MarketState.WAIT if not self.participate_in_closing else self.MarketState.REGULAR
            elif market_state[1] == 'M' :  
                return self.MarketState.WAIT if not self.participate_in_mid_auction else self.MarketState.REGULAR
            else:
                self.log("validateMarketCondition: in auction, but with an invalid state %s" % market_state)
                return self.MarketState.INVALID
            
        if ('TL' in market_state) or ('TA' in market_state) or ('TH' in market_state) or (market_state[0] == 'C' and market_state[1] != 'F'):
            print('validateMarketCondition: Market Closed, the trading state is :%s' % market_state)
            return self.MarketState.CLOSED    
        
        return self.MarketState.REGULAR
    
    def cancelAllLivingOrders(self, order_prefix = None, exclude_prefix = None):
        '''
            @summary: cancel all living orders with/without some prefix
            @param order_prefix: orders containing this prefix will be canceled
            @param exclude_prefix: order containing this prefix will be not canceled
            @return: freed_qty total qty freed from the canceled orders    
        '''
        PB = self.moneyManager.getPrivateBook(order_prefix)
        freed_qty = 0
        for occ in PB.keys():
            if exclude_prefix:
                if exclude_prefix not in occ :
                    venue_id = PB[occ]["VenueId"]
                    freed_qty += PB[occ]["LeavesQty"]
                    price_occ_to_cancel = PB[occ]['Price']
                    qty_occ_to_cancel = PB[occ]['LeavesQty']
                    self.cancelOccurrence(venue_id, occ)
                    self.printOrder(price_occ_to_cancel, qty_occ_to_cancel, order_prefix, "Cancel", function_name="cancelAllLivingOrders")
                else :
                    self.log("cancelAllLivingOrders: Not Canceling Order with prefix : %s" % occ)
            else:  
                venue_id = PB[occ]["VenueId"]
                freed_qty += PB[occ]["LeavesQty"]
                price_occ_to_cancel = PB[occ]['Price']
                qty_occ_to_cancel = PB[occ]['LeavesQty']
                self.cancelOccurrence(venue_id, occ)
                self.printOrder(price_occ_to_cancel, qty_occ_to_cancel, order_prefix, "Cancel", function_name="cancelAllLivingOrders")               
                
        return freed_qty
    
    def getLimitPriceLevelInLOB(self):        
        '''
            @summary: verify if the limit price is in LOB
            @return: 
              - positive value in [1,5] = first best order <= limit price
              - 6 : not in LOB              
        ''' 
        
        limit_price_not_found = True
        level_in_lob = 1
        
        if self.side * self.limit_price > self.side * self.event_market['BEST1']:
            limit_price_not_found = False
            level_in_lob = 6            
        
        while limit_price_not_found and level_in_lob <= 5:
            if isinstance(self.event_market['BEST%d' % level_in_lob], types.StringType):
                # not exist BEST at this level
                level_in_lob += 1
            elif isinstance(self.event_market['BEST%d' % level_in_lob], types.FloatType):
                if (self.event_market['BEST%d' % level_in_lob] - self.limit_price)*self.side >=0:
                    limit_price_not_found = False
                level_in_lob += 1
        
        return level_in_lob
    
    def getLastLevelInLOB(self):
        last_level_found = False
        level_in_lob = 1
        while (not last_level_found) and (level_in_lob <= 5):
            if self.event_market.has_key('BEST%d' % level_in_lob):
                if isinstance(self.event_market['BEST%d' % level_in_lob], types.FloatType):
                    level_in_lob += 1
                else:
                    last_level_found = True
            else:
                last_level_found = True
        level_in_lob = level_in_lob - 1
        return level_in_lob
     
    def mergeTrackingOccurrences(self, evt, orders):
        '''
            @summary: merge old and new tracking orders
            @see: prepareTrackingOccurrences
        '''
               
        dict_by_prices = self.tools.invert_nested_dict(orders)        
        
        for price in dict_by_prices.keys() :
            order_names = dict_by_prices[price].keys()            

            news = self.tools.count_substring_in_list(order_names, 'new')
            olds = self.tools.count_substring_in_list(order_names, 'old')
            
            nb_news = news[0]
            order_news = news[1]
            
            nb_olds = olds[0]
            order_olds = olds[1]
                        
            if nb_news == 0 and nb_olds == 0:                
                self.log("mergeTrackingOccurrences: news: %s, olds: %s" % (news, olds))
                
            if nb_news == 1 and nb_olds == 0:                
                # we need an order at a new price -> Insert
                volume_order = min(dict_by_prices[price][order_news[0]][0], self.moneyManager.getRemainingQty())
                self.createOccurrence(tactic_type = 'IcebergMono',
                                      venue_id = evt.getVenueId(),
                                      prefix = 'TrackingOrder%.3f' % price,
                                      tactic_params = {'Price': price,
                                                       'OrderQty': volume_order},
                                      priority = -2)
                
                self.printOrder(price, volume_order, "Tracking", "Insert", "mergeTrackingOccurrences")
                # we need to define the priority after ! bets price must have the best priority 
                
            if nb_news == 0 and nb_olds == 1:
                #retrieve the order name
                the_occ = dict_by_prices[price][order_olds[0]][1]
                #TOTEST : do we cancel the good one ? 
                tracking_occurence = self.moneyManager.getpvOrder(the_occ)
                if tracking_occurence:
                    venue_id = tracking_occurence["VenueId"]
                    price_occ_to_cancel = tracking_occurence['Price']
                    qty_to_cancel = tracking_occurence['OrderQty']                     
                    self.cancelOccurrence(venue_id, the_occ)
                    self.printOrder(price_occ_to_cancel, qty_to_cancel, "Tracking", "Cancel", "mergeTrackingOccurrences")
                else :
                    self.log("mergeTrackingOccurrences: Not able to find  %s in %s" % (the_occ, self.moneyManager.getPrivateBook("Tracking")))  
                
            if nb_news == 1 and nb_olds == 1:
                
                # do a modify if its necessary
                # if Q_new > Q_old + 1 ATS, then do a modify
                #if Q_new < Q_old, do a modify
                # else do nothing, doesn't worth a loss of priority
                
                new_volume = dict_by_prices[price][order_news[0]][0]
                old_volume = dict_by_prices[price][order_olds[0]][0]
                #TOTEST

                if new_volume > old_volume + self.ats :
                    # worth a modify, even if we lose the priority
                    self.log("prepareTrackingOccurrences:worth a modify, even if we lose the priority")
                    the_old_order_id = dict_by_prices[price][order_olds[0]][1]
                    the_old_occurence = self.moneyManager.getpvOrder(the_old_order_id)
                    volume_order = min(dict_by_prices[price][order_news[0]][0], self.moneyManager.getRemainingQty())
                    
                    self.modifyOccurrence(tactic_type = 'IcebergMono',
                                          venue_id = the_old_occurence["VenueId"],
                                          oc_id = the_old_order_id,
                                          tactic_params = {'Price': price, 'OrderQty': volume_order})
                    self.printOrder(price, volume_order, "Tracking", "Modify", "mergeTrackingOccurrences")
                    
                elif new_volume < old_volume :
                    self.log("prepareTrackingOccurrences: worth a modify, we do not lose the priority")
                    # worth a modify,  we do not lose the priority
                    the_old_order_id = dict_by_prices[price][order_olds[0]][1]
                    the_old_occurence = self.moneyManager.getpvOrder(the_old_order_id)
                    venue_id = the_old_occurence["VenueId"]
                    volume_order = min(dict_by_prices[price][order_news[0]][0], self.moneyManager.getRemainingQty())
                    
                    self.modifyOccurrence(tactic_type = 'IcebergMono',
                                          venue_id = venue_id,
                                          oc_id = the_old_order_id,
                                          tactic_params = {'Price': price, 'OrderQty': volume_order})
                    self.printOrder(price, volume_order, "Tracking", "Modify", "mergeTrackingOccurrences")
                else :
                    self.log('mergeTrackingOccurrences: a modify was requested but not executed, because volume condition.')
                
                
            if nb_news == 2 and nb_olds == 0:
                self.log("mergeTrackingOccurrences:  2 orders at new price")
                volume_order1 = min(dict_by_prices[price][order_news[0]][0], self.moneyManager.getRemainingQty())
                volume_order2 = min(dict_by_prices[price][order_news[1]][0], self.moneyManager.getRemainingQty() - volume_order1)
                
                # Send Order 1
                if volume_order1 > 0 :
                    self.createOccurrence(tactic_type='IcebergMono',
                                     venue_id=evt.getVenueId(),
                                     prefix='TrackingOrder',
                                     tactic_params={'Price': price,
                                                   'OrderQty': volume_order1},
                                     priority= -2)
                # Send Order 2
                else :
                    self.log('mergeTrackingOccurrences: negative volume order requested')
                if volume_order2 > 0 :
                    self.createOccurrence(tactic_type='IcebergMono',
                                     venue_id=evt.getVenueId(),
                                     prefix='TrackingOrder',
                                     tactic_params={'Price': price,
                                                   'OrderQty': volume_order2},
                                     priority= -2)  
                else :
                    self.log('mergeTrackingOccurrences: negative volume order requested')
                    
            if nb_news == 0 and nb_olds == 2:
                
                #TOTEST : cancel the 1st one  
                the_occ = dict_by_prices[price][order_olds[0]][1]
                self.log("mergeTrackingOccurrences:  2 orders to cancel")
                tracking_occurence = self.moneyManager.getpvOrderId(the_occ)
                venue_id = self.moneyManager.getpvVenueId(the_occ)
                self.cancelOccurrence(venue_id, tracking_occurence)
                
                #TOTEST : cancel the 2nd  one
                the_occ = dict_by_prices[price][order_olds[1]][1]
                tracking_occurence = self.moneyManager.getpvOrderId(the_occ)
                venue_id = self.moneyManager.getpvVenueId(the_occ)
                self.cancelOccurrence(venue_id, tracking_occurence)
                
        # now, set the correct priority and name in the Pending books depending on the price and the side        
        pending_tracking_orders = self.moneyManager.getPendingBook('TrackingOrder')
        if len(pending_tracking_orders) == 2:
            first_order_priority = "TrackingOrder1" if (self.side * float(pending_tracking_orders.values()[0].tactic_params['Price']) > self.side * float(pending_tracking_orders.values()[1].tactic_params['Price'])) else "TrackingOrder2"
            second_order_priority = "TrackingOrder2" if first_order_priority == "TrackingOrder1" else "TrackingOrder1"
        elif len(pending_tracking_orders) == 1:
            first_order_priority = "TrackingOrder1"
            second_order_priority = None
        else:
            #Bad case 
            first_order_priority = None
            second_order_priority = None
            
        # Set the priority        
        if first_order_priority != None :
            self.moneyManager.setPriority(pending_tracking_orders.keys()[0] , self.ZonePriority.Priority[self.current_zone][first_order_priority])
            self.log("mergeTrackingOccurrences: Setting Priority %d to order with Price : %.2f" % (self.ZonePriority.Priority[self.current_zone][first_order_priority],
                                                                                                float(pending_tracking_orders[pending_tracking_orders.keys()[0]].tactic_params["Price"])))
        if second_order_priority != None :
            self.moneyManager.setPriority(pending_tracking_orders.keys()[1] , self.ZonePriority.Priority[self.current_zone][second_order_priority])
            self.log("mergeTrackingOccurrences: Setting Priority %d to order with Price : %.2f" % (self.ZonePriority.Priority[self.current_zone][second_order_priority],
                                                                                             float(pending_tracking_orders[pending_tracking_orders.keys()[1]].tactic_params["Price"])))
        
        
    
    def updateOrdersPriority(self):
        '''
            @summary: update orders priority
        '''
        new_prios = {}
        PB = self.moneyManager.getPrivateBook()
        TrackingPB = dict(self.moneyManager.getPrivateBook("Tracking"))
        
        # Tracking
        TrackingPendingPrices =  dict((  k, x.tactic_params["Price"] )  for k,x in self.moneyManager.getPendingBook("Tracking").iteritems())
        TrackingDictPrices =  dict((  k, x["Price"] )  for k,x in TrackingPB.iteritems())
        
        if TrackingDictPrices:
            TrackingDictPrices.update(TrackingPendingPrices)
            update_tracking_priority = True
        else:
            update_tracking_priority = False
                    
        if update_tracking_priority : 
            oc_ids = TrackingDictPrices.keys()
            nb_tracking = len(oc_ids)
            
            if nb_tracking == 2 :
                if TrackingDictPrices[oc_ids[0]] * self.side > TrackingDictPrices[oc_ids[1]] * self.side :
                    new_prios[oc_ids[0]] = self.ZonePriority.Priority[self.current_zone]["TrackingOrder1"]
                    new_prios[oc_ids[1]] = self.ZonePriority.Priority[self.current_zone]["TrackingOrder2"]
                else: 
                    new_prios[oc_ids[0]] = self.ZonePriority.Priority[self.current_zone]["TrackingOrder2"]
                    new_prios[oc_ids[1]] = self.ZonePriority.Priority[self.current_zone]["TrackingOrder1"]
            elif  nb_tracking == 1 :
                new_prios[oc_ids[0]] = self.ZonePriority.Priority[self.current_zone]["TrackingOrder1"]
            elif nb_tracking > 2 :
                self.log("updateOrdersPriority: more than 2 tracking occurrences")
        
        # Slippage
        if  "Slippage" in PB.keys() :
            oc_slippage = self.moneyManager.getPrivateBook("Slippage").keys()
            new_prios[oc_slippage] = self.ZonePriority.Priority[self.current_zone]["SlippageOrder"]
        
        
        # Opportunity
        if  "Opportunity" in PB.keys() :
            oc_opp = self.moneyManager.getPrivateBook("Opportunity").keys()
            new_prios[oc_opp] = self.ZonePriority.Priority[self.current_zone]["OpportunityOrder"]
            
        if  "WouldOrder" in PB.keys() :
            oc_would = self.moneyManager.getPrivateBook("WouldOrder").keys()
            new_prios[oc_would] = self.ZonePriority.Priority[self.current_zone]["WouldOrder"]
            
        old_prios = self.moneyManager.getPriorityBook()         
        
        for occ_id, priority in new_prios.iteritems():            
            if old_prios[occ_id] != priority:
                self.log("updateOrdersPriority: change priority of order %s from %d to %d"%(occ_id,old_prios[occ_id],priority))
                self.moneyManager.setPriority(occ_id, priority)        
        
    def computeOptimalRate(self, signal_condition):
        ''' 
            @summary: compute the optimal rate from a signal condition as follows: max(min) pct for positive(negative) signal condition, otherwise mid pct  
            @param signal_condition: takes values in {0,1,-1}
            @return: optimal rate
            @see: function computeSignalCondition             
        '''        
        if (not self['ForwardSignalEnabled']) and (not self['BackwardSignalEnabled']):
            optimal_rate = [self.qslipp, self.qmax]
        else:  
            if signal_condition > 0:
                optimal_rate = self.qmid + (self.qmax - self.qmin)*0.25  
            elif signal_condition < 0:
                optimal_rate = self.qmid - (self.qmax - self.qmin)*0.25
            else:
                optimal_rate = self.qmid
            optimal_rate = [optimal_rate, optimal_rate]
            
        return optimal_rate
            
    def prepareTrackingOccurrences(self, evt):
        '''
            @summary: prepare tracking occurrences: check tracking placement condition and finally, merge tracking occurrences  
            @see: cancelAllLivingOrders, mergeTrackingOccurrences, checkTrackingPlacementCondition, checkTrackingModificationCondition
        '''        
        
        if (self.current_zone == self.AlgoZone.FAR_AHEAD):
            # cancel all orders, except would occurrences
            self.log("prepareTrackingOccurrences: algo is far ahead, cancel all existing tracking orders")
            self.cancelAllLivingOrders(exclude_prefix = "Would")
            return
        
        tracking_pb = self.moneyManager.getPrivateBook("Tracking")
        
        if len(tracking_pb.keys()) > 2 :
            self.log("prepareTrackingOccurrences: more than 2 tracking occurrences, PB: %s" % tracking_pb)            
        else :
            self.log("prepareTrackingOccurrences: we have %d occurrences, correct" % (len(tracking_pb.keys())))
        
        # determine level of limit price
        limit_price_level_in_lob = self.getLimitPriceLevelInLOB()        
        self.log("prepareTrackingOccurrences: limit_price_level_in_lob = %d" % (limit_price_level_in_lob))
        
        if limit_price_level_in_lob <= 5:
            self.log("prepareTrackingOccurrences: limit price is in LOB limit_price_level_in_lob = %d" % (limit_price_level_in_lob))            
            # limit price is out of LOB
            self.createOccurrence(tactic_type = 'IcebergMono',
                                 venue_id = evt.getVenueId(),
                                 prefix = 'TrackingOrder',
                                 tactic_params = {'Price': self.limit_price,
                                                  'OrderQty': self.ats},
                                 priority = self.ZonePriority.Priority[self.current_zone]['TrackingOrder1'])
            self.printOrder(self.limit_price, self.ats, 'TrackingOrder1', 'Insert', function_name="prepareTrackingOccurrences")
            
        else:
            # determine level of tracking occurrences
            self.log("prepareTrackingOccurrences: limit price is not in LOB limit_price_level_in_lob = %d" % (limit_price_level_in_lob))
            
            tracking_orders = {} 
            
            old_orders = self.moneyManager.getPrivateBook("Tracking")
            order_index = 1                
            for o in old_orders.keys() :                    
                tracking_orders['OldTrackingOrder%d' % order_index] = {old_orders[o]["Price"]:(old_orders[o]["OrderQty"], o)}
                self.log("prepareTrackingOccurrences: Old Order %s" % tracking_orders['OldTrackingOrder%d' % order_index])
                order_index += 1
            
            coeff_first_tracking_occurrence = min(self.computeVolumeCoeffTrackingOccurrences(self.optimal_rate[1]), 1.0)
            self.log('prepareTrackingOccurrences: coeff_first_tracking_occurrence = %.2f'%coeff_first_tracking_occurrence)
            self.coeff_first_tracking_occurrence = coeff_first_tracking_occurrence            
            volume_tracking_order_1 = int((1.0 + coeff_first_tracking_occurrence)*self.ats)
            volume_tracking_order_2 = self.ats
            
            last_level_in_lob = self.getLastLevelInLOB()
            if (last_level_in_lob == 0):
                self.log('prepareTrackingOccurrences: no BEST1')
            else:
                self.log('prepareTrackingOccurrences: last_level_in_lob = %d'%last_level_in_lob)                        
                tracking_orders['NewTrackingOrder1'] = { self.event_market['BEST1']: (volume_tracking_order_1, "TrackingOrder1") }
                if last_level_in_lob > 1:
                    tracking_orders['NewTrackingOrder2'] = { self.event_market['BEST%d'%last_level_in_lob]: (volume_tracking_order_2, "TrackingOrder2") }
                    
                if (len(old_orders.keys()) <= 1):                
                    if self.checkTrackingPlacementCondition(self.optimal_rate[1], 1):
                        self.log('prepareTrackingOccurrences: condition to place tracking orders is verified')
                        # condition is verified, can place orders
                        self.mergeTrackingOccurrences(evt, tracking_orders)
                        # and restart the volume timer
                        self.market_qty_between_tracking_exec = 0
                                      
                        self.last_mkt_qty_placing_tracking = self.marketManager.getFeedInfo('AE', 'QTE_CNT1')
                        self.last_exec_qty_placing_tracking = self.moneyManager.getExecQty()
                        self.last_mkt_qty_modification_tracking = self.marketManager.getFeedInfo('AE', 'QTE_CNT1')
                    else:
                        self.log('prepareTrackingOccurrences: condition to place tracking orders is not verified')
                else:
                    # if there exists order, just do a modify
                    if self.checkTrackingModificationCondition(self.optimal_rate[1]):
                        self.log('prepareTrackingOccurrences: condition to modify tracking orders is verified')
                        self.last_mkt_qty_modification_tracking = self.marketManager.getFeedInfo('AE', 'QTE_CNT1')
                        self.mergeTrackingOccurrences(evt, tracking_orders)
                    else:
                        self.log('prepareTrackingOccurrences: condition to modify tracking orders is not verified')                      
               
    
    def checkTrackingPlacementCondition(self, rate, level):
        '''
            @summary: check the tracking placement condition
            @param rate: the reference rate to track
            @param level: the level of LOB to check
            @note: following the market, taking into account the real volume of algo and necessary time before execution
            @see: prepareTrackingOccurrences  
        '''        
        if (rate > 0):            
            exec_qty_between_tracking_update = self.moneyManager.getExecQty() - self.last_exec_qty_placing_tracking
            real_market_qty_between_tracking_update = self.market_qty_between_tracking_exec - exec_qty_between_tracking_update
            threshold_volume = (1.0 - self.coeff_first_tracking_occurrence)*(1.0 - rate)/rate * self.ats - sum(filter(lambda d: self.event_market['BEST_SIZE%d'%d], [1,level]))                
            tracking_condition = real_market_qty_between_tracking_update >= threshold_volume
        else:            
            tracking_condition = False
                
        return tracking_condition        
    
    def checkTrackingModificationCondition(self, rate):
        '''
            @summary: check the tracking modification  condition
            @param rate: the reference rate to track
            @see: prepareTrackingOccurrences  
        '''        
        if (rate > 0):
                market_qty_between_tracking_modification = self.marketManager.getFeedInfo('AE', 'QTE_CNT1') - self.last_mkt_qty_modification_tracking
                threshold_volume = (1.0 - rate)/rate * self.ats
                tracking_condition = market_qty_between_tracking_modification >= threshold_volume*self.tracking_modification_coeff
        else:            
            tracking_condition = False
                
        return tracking_condition
    
    def computeVolumeCoeffTrackingOccurrences(self, rate):
        '''
            @summary: compute the corrected coefficient for tracking occurrence's volume
            @see: prepareTrackingOccurrences
        '''
        # rule 1: the longer that we must wait until execution, the bigger tracking order size should be
        if rate > 0: 
            volume_coeff = self.market_qty_between_tracking_exec/((1.0 - rate)/rate * self.ats)
        else:
            volume_coeff = 0
        # rule 2: if delta_auction_qty > 0 (volume missing during auction) then add an extra ATS
        if self.delta_auction_qty > self.ats:         
            volume_coeff += self.corrected_auction_qty_coeff
            
        return volume_coeff
    
    def computeAuctionVolumeCorrectionCoeffcient(self):
        '''
            @summary: compute the corrected auction volume coefficient
            @return: 3 possible values
                - 0 if nothing to correct
                - 1 if: late/very late in regular, late in auction 
                - -1 if: ahead/far ahead in regular, ahead in auction
            @see: computeVolumeThreshold
        '''
        
        corrected_auction_qty_coeff = 0
        if self.delta_auction_qty < -self.ats:
            # AHEAD during auction phase 
            if (self.current_zone == self.AlgoZone.VERY_LATE) or (self.current_zone == self.AlgoZone.LATE):
                # if LATE or VERY_LATE: increase volume
                self.corrected_auction_qty = self.ats
                self.delta_auction_qty += self.ats
            elif (self.current_zone == self.AlgoZone.AHEAD) or (self.current_zone == self.AlgoZone.FAR_AHEAD):
                # if AHEAD or FAR_AHEAD: try to delay 1 ATS
                corrected_auction_qty_coeff = -1
        elif self.delta_auction_qty > self.ats:
            # LATE during auction phase
            if (self.current_zone == self.AlgoZone.AHEAD) or (self.current_zone == self.AlgoZone.FAR_AHEAD):
                # if AHEAD or FAR_AHEAD: decrease volume
                self.corrected_auction_qty = -self.ats
                self.delta_auction_qty -= self.ats
            elif (self.current_zone == self.AlgoZone.LATE) or (self.current_zone == self.AlgoZone.VERY_LATE):
                # if LATE or VERY_LATE: try to get an extra ATS
                corrected_auction_qty_coeff = 1        
        
        return corrected_auction_qty_coeff
            
    def prepareSlippageOccurrences(self, evt):
        '''
            @summary: prepare slippage occurrences
            @see: cancelAllLivingOrders, modifySlippageOccurence
            @todo: 
                check if the number of SlippageOrder <= 1
                check if no slippage if delta_auction_qty < 0        
        '''            
        # get current XFire order
        curXFire = self.moneyManager.getPrivateBook("SlippageOrder")
        
        if self.current_zone == self.AlgoZone.VERY_LATE: 
            # under Qmin
            # depending on deltaVolume and tempo
            # - below Qmid more than 1 ATS => aggressive order at best opposite
            # - below Qmid less than 1 ATS => do nothing
            
            # check deltaVolume condition
            
            if (self.delta_volumes[0] > 0 and self.delta_volumes[1] >= self.ats):                
                if (self.next_slippage_deal <= self.marketManager.getFeedInfo('AE', 'C1_CONVNUM')):
                    # tempo is ok 
                    # update tempo
                    self.next_slippage_deal = self.marketManager.getFeedInfo('AE', 'C1_CONVNUM') + self['SlippageNbDeals']
                    
                    # send order
                    self.log('prepareSlippageOccurrences: delta_volumes %s' % self.delta_volumes)
                    self.log('prepareSlippageOccurrences: getRemainingQty %s' % self.moneyManager.getRemainingQty())
                    self.log('prepareSlippageOccurrences: ats %s' % self.ats)
                    
                    # quantity                    
                    adjusted_slippage_qty = self.corrected_auction_qty_coeff*self.ats + int(0.5 * (self.delta_volumes[0] + self.delta_volumes[1])) 
                    quantity = min(max(self.ats,  adjusted_slippage_qty), self.moneyManager.getRemainingQty(), self["MaxMultATS"] * self.ats)
                    
                    # price
                    price = self.tools.bestBetween(self.event_market['BEST_OPPOSITE1'], self.limit_price)
                    # manage config XFire Venues here : we need also Primary + MTF + Dark + Mid , so we do not exclude everything
                    xfire_params = self.mergeXfireParamsWithConfig({'OrderQty': int(quantity),
                                                                    'Price': price,
                                                                    'ExcludeAggressiveType' : '',
                                                                    'ExcludePassiveType' : '',
                                                                    'ExcludePrimary' : 'N',
                                                                    'ParticipationRate' : 99,
                                                                    'AmountValidation' : 0})
                    self.log("prepareSlippageOccurrences: algo is very late, sending aggressive XFire order")
                    self.modifySlippageOccurence(evt.getVenueId(), xfire_params, curXFire)
                else:
                    # tempo is KO
                    self.log("prepareSlippageOccurrences: algo is very late, waiting to send aggressive XFire order")
            else:
                # deltaVolume condition is KO
                self.log("prepareSlippageOccurrences: algo is very late, but not yet enough to send aggressive XFire order")
                    
        elif self.current_zone == self.AlgoZone.LATE:
            # in [Qmin, Qmid]
            # send passive XFire order to DARK only (Mid and Dark) at the mid price to go back to Qmid
            mid_price = 0.5 * (self.event_market['BEST1'] + self.event_market['BEST_OPPOSITE1'])
            
            adjusted_slippage_qty = self.corrected_auction_qty_coeff*self.ats + self.delta_volumes[1]             
            quantity = min(max(self.ats, adjusted_slippage_qty), self.moneyManager.getRemainingQty())
            
            xfire_params = self.mergeXfireParamsWithConfig({'OrderQty': int(quantity),
                                                            'Price': mid_price,
                                                            'ExcludeAggressiveType': 'LIT',
                                                            'ExcludePassiveType': 'LIT',
                                                            'ExcludePrimary': 'Y',
                                                            'ParticipationRate': 99,
                                                            'AmountValidation': 0})
            self.log("prepareSlippageOccurrences: algo is late (between Qmid and Qmin), sending TAP on Mid order")
            self.modifySlippageOccurence(evt.getVenueId(), xfire_params, curXFire)
            
        elif self.current_zone == self.AlgoZone.AHEAD:
            # in [Qmid, Qmax]
            # cancel all order
            self.log("prepareSlippageOccurrences: algo is ahead (between Qmid and Qmax), cancel all slippage order")
            self.cancelAllLivingOrders(order_prefix = 'SlippageOrder')
            
        elif self.current_zone == self.AlgoZone.FAR_AHEAD:
            # above Qmax
            self.log("prepareSlippageOccurrences: algo is far ahead (above Qmax), cancel all slippage order")
            self.cancelAllLivingOrders(order_prefix = 'SlippageOrder')
        
    def updateOpportunitySignal(self, evt):
        '''
            @summary: 
                - update opportunity signal (for simulation in SIMEP only)
                - compute mean signal
            @see OpportunitySignal class  
        '''
        
        if self.opportunity_signal != None:
            # Update expected execution time and end of period volume
            self.exp_exec_time = self.estimateEndOfPeriodTime(evt)
            self.exp_end_of_period_volume = self.estimateEndOfPeriodVolume(evt)
            # Update opportunity signal
            self.opportunity_signal.updateSignals(current_vol_gk_10mn_bp = self.marketManager.getFeedInfo('AE','garman_klass_bp_s_600'),
                                                  last_trades = self.marketManager.getTrades(), 
                                                  market_vwap = self.marketManager.getFeedInfo('AE', 'WTD_AVE1'),  
                                                  end_of_period_volume = self.exp_end_of_period_volume,
                                                  end_of_period_remaining_time = self.exp_exec_time)
            self.fwd_signal = self.opportunity_signal.getFwdOpportunitySignal()
            self.bwd_signal = self.opportunity_signal.getBwdOpportunitySignal()
        else:
            if self.event_market.has_key('FWD1_PRICE'):
                fwd_signal = self.marketManager.getFeedInfo('AE', 'FWD1_PRICE')
                self.fwd_signal = 0.5 if (fwd_signal == '') else fwd_signal
            
            if self.event_market.has_key('BALN_PRICE'):
                bwd_signal = self.marketManager.getFeedInfo('AE', 'BALN_PRICE') 
                self.bwd_signal = 0.5 if (bwd_signal == '') else bwd_signal
            
        if len(self.last_fwd_signal) >= self.lengthWindowSignals:
            self.last_fwd_signal.popleft()
            self.last_bwd_signal.popleft()
            
        self.last_fwd_signal.append(self.fwd_signal)
        self.last_bwd_signal.append(self.bwd_signal)
        
        if self.opportunity_signal != None:
            self.mean_fwd_signal = self.opportunity_signal.getMeanFwd()
            self.mean_bwd_signal = self.opportunity_signal.getMeanBwd()
            self.std_fwd_signal = self.opportunity_signal.getStdFwd()
            self.std_bwd_signal = self.opportunity_signal.getStdBwd()
        else:
            self.mean_fwd_signal = sum(self.last_fwd_signal)/self.lengthWindowSignals
            self.mean_bwd_signal = sum(self.last_bwd_signal)/self.lengthWindowSignals
        
    def computeSignalCondition(self):
        '''
            @summary: compute the signal condition
            @return: signal_condition
            @see: updateOpportunitySignal
        '''
        signal_condition = 0
        signal_condition_fwd = self.side*(int(self.mean_fwd_signal < self.quantileMeanFwdLow[self.current_zone]) - int(self.mean_fwd_signal > self.quantileMeanFwdHigh[self.current_zone]))
        signal_condition_bwd = self.side*(int(self.mean_bwd_signal < self.quantileMeanBwdLow[self.current_zone]) - int(self.mean_bwd_signal > self.quantileMeanBwdHigh[self.current_zone]))
        if self['ForwardSignalEnabled'] and not self['BackwardSignalEnabled']:                         
            signal_condition = signal_condition_fwd
        elif self['BackwardSignalEnabled'] and not self['ForwardSignalEnabled']:
            signal_condition = signal_condition_bwd            
        elif self['BackwardSignalEnabled'] and self['ForwardSignalEnabled']:                         
            signal_condition = 0.5*(signal_condition_fwd + signal_condition_bwd)
        return signal_condition 
        
    def prepareOpportunityOccurrences(self, evt):
        '''
            @summary: prepare opportunity occurrences
        '''
        
        if self['ForwardSignalEnabled'] or self['BackwardSignalEnabled']:
            
            opportunity_cnt_ok = (self.next_opportunity_deal <= self.marketManager.getFeedInfo('AE', 'C1_CONVNUM')) 
            if  opportunity_cnt_ok:
                best_opposite_qty = self.event_market['BEST_OPPOSITE_SIZE1']
                bo = self.event_market['BEST_OPPOSITE1']
                max_opp_qty = min(self.delta_volumes[2], best_opposite_qty, 2*self.ats)
                min_opp_qty = self.ats
                
                opportunity_qty_ok = (max_opp_qty > min_opp_qty)
                
                if opportunity_qty_ok:
                    # we need to check with the limit price too , to avoid sending an order which 
                    # is not consistent with the limit price
                    if self.signal_condition > 0 and self.tools.worstBetween(bo, self.limit_price) == self.limit_price:
                        self.next_opportunity_deal = self.marketManager.getFeedInfo('AE', 'C1_CONVNUM') + self['OpportunityNbDeals']                
        
                        # FAK Opportunistic order
                        opp_price = self.tools.bestBetween(bo, self.limit_price)
                        opp_qty = min_opp_qty
                         
                        self.log("prepareOpportunityOccurrences: sending opportunity order, P = %.2f, Q = %d" % (opp_price, int(opp_qty)))            
                        
                        self.createOccurrence('MarketOrder',
                            venue_id = evt.getVenueId(),
                            prefix = 'OpportunityOrder',
                            tactic_params={'OrderQty': int(opp_qty),
                                           'Price': opp_price,
                                           'OrderType': 'limit',
                                           'MarketTimeInForce': 'Fak'},
                                           priority=self.ZonePriority.Priority[self.current_zone]['OpportunityOrder'])
                    else:
                        self.log("prepareOpportunityOccurrences: ITM, but no qty available : Max Curve : %d ,best_opposite_qty - ats : %d" % (self.delta_volumes[2], best_opposite_qty - self.ats))
                        
            else:
                self.log("prepareOpportunityOccurrences: no opportunity")
    
    
    def manageAuctionOcurrences(self, evt):
        '''
            @summary: manage auction occurrences
            @see: prepareAuctionOccurrence
            @todo: 
                Auction Resumption
                Correct THEO_VOL for intraday auction
        '''
        if self.first_time_in_auction:
            qty_freed = self.cancelAllLivingOrders(order_prefix = None, exclude_prefix = 'Auction')
        else:
            qty_freed = 0
        
        time_now = self.tools.convertDateToSecond(evt.timestamp)
         
        if self.market_state[1] == 'O':            
            time_before_auction_end = self.convertDateStrToSecond(self.getReferenceData('SecurityTradingHours', venue_id=evt.getVenueId())['opening_fixing']) - time_now
            self.last_auction_code = 'O'              
        elif self.market_state[1] == 'C':            
            time_before_auction_end = self.convertDateStrToSecond(self.getReferenceData('SecurityTradingHours', venue_id=evt.getVenueId())['closing_fixing']) - time_now
            self.last_auction_code = 'C' 
        elif self.market_state[1] == 'I':
            # TODO / How do we manage intraday?            
            time_before_auction_end = None
            self.last_auction_code = 'I'
        elif self.market_state[1] == 'M':
            # TODO / How do we manage intraday?
            time_before_auction_end = None
            self.last_auction_code = 'M'
        elif self.market_state[1] == 'R': 
            self.log("manageAuctionOcurrences: during Auction Resumption: FEED %s" % self.event_market)
            return             
        
        if self.event_market.has_key('THEO_VOL'):
            theo_vol = self.event_market['THEO_VOL']
            if isinstance(theo_vol, basestring):
                theo_vol = int(theo_vol)
            self.log("manageAuctionOcurrences: auction THEO_VOL = %d" % theo_vol)
            # correct the THEO_VOL
            corrected_theo_vol = theo_vol * self.getCorrectedTheoVolCoeff(time_before_auction_end, self.market_state[1])
            
        elif self.market_state[1] == 'I' :
            venue_dict = self.marketManager.getVenue(evt.getVenueId())
            self.log("manageAuctionOcurrences: using Venue Info: %s" % venue_dict)
            corrected_theo_vol = int(venue_dict['usual_midopening_volume'])
        elif self.market_state[1] == 'O' :
            venue_dict = self.marketManager.getVenue(evt.getVenueId())
            self.log("manageAuctionOcurrences:: using Venue Info: %s" % venue_dict)
            corrected_theo_vol = int(venue_dict['med_daily_open_volume'])
        elif self.market_state[1] == 'C' :
            venue_dict = self.marketManager.getVenue(evt.getVenueId())
            self.log("manageAuctionOcurrences: using Venue Info: %s" % venue_dict)
            corrected_theo_vol = int(venue_dict['med_daily_close_volume'])
        
        self.log("manageAuctionOcurrences: using Auction Volume: %d" % int(corrected_theo_vol))
                                                        
        auction_private_book = self.moneyManager.getPrivateBook("Auction")
        q_placed_in_auction = 0   
        for order in auction_private_book:
            q_placed_in_auction += self.moneyManager.getPrivateBook()[order]["LeavesQty"]
        
        # compute quantity auction 
        auction_quantity = (corrected_theo_vol - q_placed_in_auction) * self.qmid / (1.0 - self.qmid)        
        self.log("manageAuctionOcurrences: auction_quantity = %d" % auction_quantity)   
        # and cap it with usable volume
        auction_quantity_to_place = min(auction_quantity, qty_freed + self.moneyManager.getTotalQty() - self.moneyManager.getExecQty() - self.moneyManager.getLeavesQty())        
        self.log("manageAuctionOcurrences: auction_quantity_to_place = %d" % auction_quantity_to_place)
        
        if auction_quantity_to_place > 0:
            # get the last price in the two cases: opening and closing
            if self.market_state[1] == 'O':
                last_price = self.marketManager.getFeedInfo('AE', 'HST_CLOSE')
            else:
                last_price = self.event_market['DEAL_PRICE']
            
            auction_price_to_place = self.computeAuctionPrice(self.market_state[1], last_price)    
            self.prepareAuctionOccurrence(evt, auction_price_to_place, auction_quantity_to_place, q_placed_in_auction)
    
        return
    
    
    def postAuctionAction(self, evt):
        
        time_now = self.tools.convertDateToSecond(evt.timestamp)  
        
        # the auction volume can still grow, so we need to do this 
        # for sometimes before being sure we have all the auction trades
        if self.last_auction_code == 'O':
            # opening_fixing
            AE_key_for_auction_volume = 'SESS1_VOL'
            key_for_trading_hours = 'opening_fixing'
        elif self.last_auction_code == 'I':
            # intraday_stop_fixing
            AE_key_for_auction_volume = 'SESS3_VOL'
            key_for_trading_hours = 'intraday_stop_fixing'
        elif self.last_auction_code == 'C':
            # closing_fixing 
            AE_key_for_auction_volume = 'SESS2_VOL'
            key_for_trading_hours = 'closing_fixing'
        elif self.last_auction_code == 'M':
            # intraday_resumption_fixing
            AE_key_for_auction_volume = 'SESS4_VOL'
            key_for_trading_hours = 'intraday_resumption_fixing'
        else:
            AE_key_for_auction_volume = None
            key_for_trading_hours = None
        
        if (AE_key_for_auction_volume != None) and (key_for_trading_hours != None):
            self.market_auction_qty = self.marketManager.getFeedInfo('AE', AE_key_for_auction_volume)
            if isinstance(self.market_auction_qty, basestring):
                if 'NA' in self.market_auction_qty:
                    self.market_auction_qty = 0
            time_after_last_auction = time_now - self.convertDateStrToSecond(self.sec_id_trading_hours[key_for_trading_hours])
        else:
            time_after_last_auction = 0
            
        # wait 10' after auction ending
        if (time_after_last_auction > 600):            
            self.total_auction_qty += self.executed_in_auction
            self.executed_in_auction = 0
            # compute the delta_auction_qty
            self.delta_auction_qty += self.qmin * self.market_auction_qty - self.total_auction_qty
            
            self.log("postAuctionAction: computing delta auction: self.market_auction_qty: %d, self.executed_in_auction : %d"%(self.market_auction_qty,self.executed_in_auction))
            self.log("postAuctionAction: self.total_auction_qty: %d"%self.total_auction_qty)
            
        
        
    def prepareWouldOccurrences(self, evt):
        '''
            @summary: prepare a would occurrence
            @todo: 
                - check if we have an order of would level
                - determine the would quantity             
        '''
        
        if self.would_level_is_used:
            curWouldOcc = self.moneyManager.getPrivateBook('WouldOrder')
            best_opposite = self.event_market['BEST_OPPOSITE1']
            is_in_would_area = (self.tools.bestBetween(self.would_level, best_opposite) == best_opposite)
            
            if not curWouldOcc :
                # Would level should be used, but no orders alive
                would_price = self.would_level                                
                if not is_in_would_area:
                    #Place an order with half of the remaining quantity to the market, and a low priority                  
                    
                    self.would_occ_qty = 0.5 * (self.moneyManager.getRemainingQty() - self.moneyManager.getLeavesQty())
                     
                    self.createOccurrence(tactic_type = 'IcebergMono',
                                          venue_id = evt.getVenueId(),
                                          prefix = 'WouldOrder',
                                          tactic_params = {'Price': self.would_level,
                                                           'OrderQty': int(self.would_occ_qty)},
                                          priority = self.ZonePriority.Priority[self.current_zone]['WouldOrder'])
                    self.printOrder(self.would_level, int(self.would_occ_qty), 'WouldLevelIcbgORder ', 'Insert', function_name = "prepareWouldOccurrences")
                      
                          
                else :  
                    #Send a FAK ORder with the remaining qty
                    would_qty = self.moneyManager.getRemainingQty()
                    self.createOccurrence(tactic_type = 'MarketOrder',
                                          venue_id = evt.getVenueId(),
                                          prefix = 'WouldOrder',
                                          tactic_params = {'OrderQty': int(would_qty),
                                                           'Price': would_price,
                                                           'OrderType': 'limit',
                                                           'MarketTimeInForce': 'Fak'},
                                          priority = self.ZonePriority.Priority[self.current_zone]['WouldFAKOrder'])
                    self.printOrder(would_price, int(would_qty), 'WouldLevelFAKORder ', 'Insert', function_name="prepareWouldOccurrences")
            
        else:
            # would is not used, we have to cancel any existing would order
            self.cancelAllLivingOrders(order_prefix = 'WouldOrder')
            
    
    def prepareEndOfLifeOccurrences(self, evt):
        '''
            @summary: prepare the end of life occurrence
            @return: create an occurrence of remaining qty
        '''
        remaining = self.moneyManager.getRemainingQty()
        quantity = min(self.ats, remaining)
        
        # price
        price = self.tools.bestBetween(self.event_market['BEST_OPPOSITE1'], self.limit_price)
        xfire_params = self.mergeXfireParamsWithConfig({'OrderQty': int(quantity),
                                                        'Price': price,
                                                        'ExcludeAggressiveType' : '',
                                                        'ExcludePassiveType' : '',
                                                        'ExcludePrimary' : 'N',
                                                        'ParticipationRate' : 99,
                                                        'AmountValidation' : 0})
        self.createOccurrence(tactic_type = 'TAP',
                                  venue_id = evt.getVenueId(),
                                  prefix = 'EnfOfLifeOrder',
                                  tactic_params = xfire_params,
                                  priority = -10)
                    
        self.log("prepareEndOfLifeOccurrences: entering end of life")
        
    
    def manageOccurrences(self, evt):
        '''
            @summary: manage occurrences during continuous trading
            @see: prepareEndOfLifeOccurrences, prepareTrackingOccurrences, prepareSlippageOccurrences, prepareWouldOccurrences, updateOrdersPriority
        '''
        
        # process end of life order 
        if self.moneyManager.getRemainingQty() <= self.ats:
            self.cancelAllLivingOrders(exclude_prefix = "EnfOfLifeOrder")
            current_enf_of_life_occ = self.moneyManager.getPrivateBook("EnfOfLifeOrder")
            if len(current_enf_of_life_occ) == 0:
                self.prepareEndOfLifeOccurrences(evt)
            else:
                self.log('manageOccurrences: EndOfLifeOrder exists')
        else:
            # process fist time in trading regular
            if self.first_time_in_trading_regular:
                self.cancelAllLivingOrders(order_prefix = 'Auction')
                
            # process tracking orders 
            self.prepareTrackingOccurrences(evt)
            
            # process slippage order
            self.prepareSlippageOccurrences(evt)
            
            # process opportunity order
            # self.prepareOpportunityOccurrences(evt)
            
            # process would order
            self.prepareWouldOccurrences(evt)
        
        # update once for all orders' priority
        self.updateOrdersPriority()
        
        
    def getMarketState(self, evt):
        '''
            @summary: get the market state using MarketStateManager
            @return: market state
        '''
        # get the MarketStateManager
        
        
        marketStateManager = self.moneyManager.getSM('MarketStateManager')
        if not marketStateManager:            
            trading_hours = self.getReferenceData('SecurityTradingHours', venue_id = evt.getVenueId())
            self.moneyManager.registerSM('MarketStateManager', MarketStateManager(self.event_market, trading_hours, evt.timestamp, evt.getVenueId()))
            marketStateManager = self.moneyManager.getSM('MarketStateManager')            
         
        market_state = marketStateManager.getMarketState()
        
        return market_state 
            
    def getAverageTradingSize(self, venue_id):
        ''' 
            @summary: compute the ATS (if it is not available from AE            
            @return: ATS 
        '''
        
        static_ats = self.marketManager.getVenue(venue_id)['ats']
        
        if static_ats > 0:            
            ats = max(1, int(static_ats * (1.0 + random() * 0.2 - 0.1)))
        else :  
            self.ats = int(self.marketManager.getFeedInfo('AE', 'A_QTY_3') / self.marketManager.getFeedInfo('AE', 'A_NPLRS_3'))
        
        return ats
        
         
    def process(self, evt):
        ''' 
            @summary: function to be called for each update on the market 
            @return: 
                - compute thresholds for delta volume --> the current zone
                - depending on current zone --> decide to launch (tracking/slippage/opportunity) occurrences
        '''

        self.event_market = self.marketManager.getFeedInfo(evt.getVenueId())        
        
        if self.first_process:
            self.begin_time = evt.timestamp
            # at the beginning, get the SecurityTradingHours for auction
            self.sec_id_trading_hours = self.getReferenceData("SecurityTradingHours", venue_id = evt.getVenueId())
        
        # what is the current state
        if self['SimulatedMode']:
            self.market_state = 'TR'
        else:
            self.market_state = self.getMarketState(evt)
         
        market_condition = self.validateMarketCondition(self.market_state)       
        
        if market_condition == self.MarketState.INVALID:
            self.error("process: stopping because of incorrect Market Condition")
            
        elif market_condition == self.MarketState.WAIT:
            self.log("process: waiting for correct Market Condition")
            pass
        
        elif market_condition == self.MarketState.CLOSED:
            self.error("process: stopping, Market is closed")
            
        # if we are here in this case, we are in the correct market conditions
        
        # adjust the rate
        self.adjustRate(evt)
        
        if self.is_in_auction:
            # manage Auction Occurrences            
            self.manageAuctionOcurrences(evt)
        else:            
            # post auction action
            if not self['SimulatedMode']:
                self.postAuctionAction(evt)
            
            # compute the average trading size
            self.ats = self.getAverageTradingSize(evt.getVenueId())
            self.log("process:getAverageTradingSize: ATS = %d" % self.ats)
            
            # compute the corrected auction qty coefficient
            self.corrected_auction_qty_coeff = self.computeAuctionVolumeCorrectionCoeffcient()
            self.log('process:computeAuctionVolumeCorrectionCoeffcient: corrected_auction_qty_coeff = %d'% self.corrected_auction_qty_coeff)
            
            # compute estimate volume
            self.estimate_volume = self.estimateVolume()
            self.log('process:computeVolumeThreshold: estimate_volume = %d'% self.estimate_volume)
            
            # compute volume threshold
            self.delta_volumes = self.computeVolumeThreshold()
            self.log('process:computeVolumeThreshold: delta_volumes = %s'% self.delta_volumes)
            
            # compute the current zone
            self.current_zone = self.computeCurrentZone()
            self.log('process:computeCurrentZone: current_zone = %d'% self.current_zone)
            
            # specific for tracking quantity update
            self.market_qty_between_tracking_exec = self.marketManager.getFeedInfo('AE', 'QTE_CNT1') - self.last_mkt_qty_placing_tracking
            self.log('process: market_qty_between_tracking_exec = %d' % self.market_qty_between_tracking_exec)
            
            # update opportunity signal
            self.updateOpportunitySignal(evt)
            
            # compute the signal condition
            self.signal_condition = self.computeSignalCondition()
            self.log('process:computeSignalCondition: signal_condition = %1.1f'% self.signal_condition)
            
            # compute the optimal rate
            self.optimal_rate = self.computeOptimalRate(self.signal_condition)
            self.log('process:computeOptimalRate: optimal_rate = [%2.2f, %2.2f]'% (self.optimal_rate[0], self.optimal_rate[1]))
            
            # manage all occurrences in trading regular phase
            self.manageOccurrences(evt)            
        
        if self.first_process:
            self.first_process = False
        
    def processReport(self, evt):
        '''
            @summary: action to be done when receiving report, called by tvfo layes
        '''
        
        #update qty for each zone
        for x in self.moneyManager.getLastExecutions():
            self.total_amount += int(x["Quantity"]) * float(x["Price"])
            self.log("processReport: Order Filled: ID: %s Qty=%d Price=%4.3f" % (x["OrderId"] , int(x['Quantity']), float(x['Price'])))
            if 'TrackingOrder' in x["OrderId"] :
                self.tracking_volume += int(x["Quantity"])
                self.tracking_amount += int(x["Quantity"]) * float(x["Price"])
                
            elif 'SlippageOrder' in x["OrderId"] : 
                self.slippage_volume += int(x["Quantity"])
                self.slippage_amount += int(x["Quantity"]) * float(x["Price"])
                
            elif "Opportunity" in x["OrderId"] :
                self.opportunity_volume += int(x["Quantity"])
                self.opportunity_amount += int(x["Quantity"]) * float(x["Price"])
                                
            elif "Would" in x["OrderId"] :
                self.total_would_qty += int(x["Quantity"])
                self.would_amount += int(x["Quantity"]) * float(x["Price"])
                            
            elif "Auction" in x["OrderId"] :
                self.rate_is_adjusted = False
                self.executed_in_auction += int(x["Quantity"])                
                self.log("Auction execution: %d" % int(x["Quantity"]))                
        
        # unique for simulation under SIMEP: to compute VWAP 
        exec_qty = self.moneyManager.getExecQty()
        mkt_qty = self.marketManager.getFeedInfo('AE', 'QTE_CNT1')
        
        self.last_mkt_qty = self.mkt_qty
        self.mkt_qty = mkt_qty

        plr_no_would = 0 if mkt_qty == 0 else  100.0 * (exec_qty - self.total_would_qty) / mkt_qty
        plr_with_would = 0 if mkt_qty == 0 else  100.0 * (exec_qty) / mkt_qty
        plr_since_last_modify = 0 if mkt_qty == 0 else  100.0 * (exec_qty - self.exec_qty_before_modify) / mkt_qty
        
        self.log ("processReport: ExecQty = %d , Market Qty = %d , PLR (without would qty) = %.2f, PLR (with would qty) = %.2f, PLR since last Modify = %.2f" % (exec_qty, mkt_qty, plr_no_would, plr_with_would, plr_since_last_modify))

        # compute execution percentage
        self.exec_pct = float(self.moneyManager.getExecQty()) / float(self.moneyManager.getTotalQty())
                        
        self.updateIndicators(evt)
        
    def acceptSubscription(self, sub_id, sub):
        '''
            @summary: function called by tvfo layer
        '''        
        return (sub_id == self.primary_venue_id)

    def acceptVenue(self, venue_id, venue):
        '''
            @summary: function called by tvfo layer
        '''
        print "ACCEPTVENUE : Processing venue_id %s, venue info type  %s , venue.info -> : " % (venue_id, venue.info["type"])
        print "destinationId -> %s" % self['destinationId']
        print "venue info"
        print  venue.info
        print "venue destinationId -> %s" % venue.info["destinationId"]
     
        if venue.info["type"] != "Lit":
            return False      
        if str(venue.info["destinationId"]) == str(self['destinationId']):
            self.primary_venue_id = venue_id
            self.trading_destination = venue.info['destinationId']
            return True
        return False
    
    
    def extraSubscriptions(self):

        ''' 
            @summary: returns any extra subscriptions needed 
            @return:
                Dictionary: {name: (Source, Ric, ReqdProcess)}
        '''
        # limit price management
        price_string = ''
        
        if self["Price"]:
            self.log("extraSubscriptions: Limit Price = %s" % self["Price"])
            price_string = ";GE=%s" % (self["Price"]) if self.side == -1 else ";LE=%s" % (self["Price"])
        else:
            self.log("extraSubscriptions: no limit price")
             
        
        if self["AdjustedPriceInd"]:
            price_string += ";LE=%s" % (self["AdjustedPriceInd"])  if self.side == -1 else ";GE=%s" % (self["AdjustedPriceInd"])
            self.log("extraSubscriptions: Would Level = %s" % (self["AdjustedPriceInd"]))
        else:
            self.log("extraSubscriptions: no would level")            
            
        # mono/multi management
        if self.is_mono_destination: 
            td_id_string = self.trading_destination
            self.log("extraSubscriptions: mono destination")
        else :
            td_id_string = 'all'
            self.log("extraSubscriptions: multi destination")            
        
        
        self.log('extraSubscriptions: suscribing to AE, S=%d@%s;P=bEHRC;Z=UTC;H=4;F=%s;T=%s;EX=top;EX=cross;EX=auc;%s;PL=os;' % (self['InstrumentID'], td_id_string, self['EffectiveTime'][9:], self['ExpireTime'][9:], price_string)) 
        self.log('extraSubscriptions: VWAP_SOURCE is : %s' % (self['vwapSource']))
        
        return {'AE':(self['vwapSource'], 'S=%d@%s;P=bEHRC;Z=UTC;H=4;F=%s;T=%s;EX=top;EX=cross;EX=auc;%s;PL=os' % (self['InstrumentID'], self.trading_destination, self['EffectiveTime'][9:], self['ExpireTime'][9:], price_string))}    
    
    def computeVolumeThreshold(self):
        ''' 
            @summary: compute the 3 thresholds for the delta volume
            @return:
                List: [Q_min(t) - Q(t), Q_opt(t) - Q(t), Q_max(t) - Q(t)]            
        '''
        
        # adjust executed qty by respect to modify received
        # exclude would and auction qty
        # include corrected auction qty 
        
        # correct effect of auction qty on regular qty
        self.corrected_auction_qty -= self.corrected_auction_qty_coeff * self.ats
        self.delta_auction_qty -= self.corrected_auction_qty_coeff * self.ats
        
        exec_volume = self.moneyManager.getExecQty() - self.total_auction_qty - self.total_would_qty + self.corrected_auction_qty
        
        # get the current market volume up to time t
        mkt_volume = self.marketManager.getFeedInfo('AE', 'QTE_CNT1') + self.total_continuous_market_qty
        
        min_delta_volume = (self.qmin * mkt_volume - exec_volume) / (1.0 - self.qmin)
        max_delta_volume = (self.qmax * mkt_volume - exec_volume + self.qmax * self.estimate_volume) / (1.0 - self.qmax)
        
        # using predicted volume for the next period
        opt_delta_volume = (self.optimal_rate[0] * mkt_volume - exec_volume) / (1.0 - self.optimal_rate[0])
        self.width_enveloppe = (max_delta_volume - min_delta_volume) / self.ats if self.ats > 0 else 0
         
        return [min_delta_volume, opt_delta_volume, max_delta_volume]
    
        
    def adjustRate(self, evt):
        ''' 
            @summary: adjust the participation rate w.r.t. Benchmark (EVP, Strict, PVOL)
            @return: adjusted rate            
        '''
        
        if not self.rate_is_adjusted:
            if ((self['Benchmark'] == 'SVT') or (self['Benchmark'] == 'PVOL')):
                self.qmax = self['ParticipationRate'] * 0.01
                
                if self['ParticipationRate'] == self['MinPctVolume']:
                    self.log("adjustRate: ParticipationRate = MinPctVolume, systematically adjust MinPctVolume = 0.9 ParticipationRate")
                    self.qmin = self.qmax * 0.9
                else:
                    self.qmin = self['MinPctVolume'] * 0.01
                
            elif (self['Benchmark'] == 'EVP'):
                qmid = 0.01 * (self['ParticipationRate'] + (self.marketManager.getFeedInfo(evt.getVenueId(), 'TIME_NUM') - self.begin_time) * self.coeffRate)
                self.qmin = qmid - 0.01 
                self.qmax = qmid + 0.01
            
            self.qmid = 0.5 * (self.qmin + self.qmax)
         
        self.qslipp = self.qmin + (self.qmax - self.qmin)*(1.0 - self.exec_pct)        
        
    def estimateVolume(self):
        ''' 
            @summary: compute an estimation for the next period's volume
            @return: volume of market solely for the next period
            @todo: using a Moving Average with respect to the volume curve
        '''
        if len(self.last_trade_qty) >= self.lengthWindowVolumes:
            self.last_trade_qty.popleft()
        if self['SimulatedMode']:
            self.last_trade_qty.append(self.event_market['LIT_DEAL_SIZE'])
        else:    
            self.last_trade_qty.append(self.event_market['DEAL_SIZE'])
        estimate_volume = sum(self.last_trade_qty)
        return estimate_volume
    
    def computeCurrentZone(self):        
        ''' 
            @summary: determine the current zone
            @return: the current zone:
                3: FAR AHEAD
                2: AHEAD
                1: LATE
                0: VERY LATE             
        '''
        cur_zone = sum([d < 0 for d in self.delta_volumes])
        # if optimal_rate for slippage = qmin => correct zone: VERY LATE => LATE
        if (self.optimal_rate[0] == self.qmin) and (self.optimal_rate[1] == self.qmax) and (cur_zone == self.AlgoZone.VERY_LATE):
            cur_zone = self.AlgoZone.LATE          
        return cur_zone



    def printOrder(self, order_price, order_qty, order_type_str, action, function_name = None):
        '''
            @summary: print the order
        '''        
        if function_name :
            self.log('%s: Action: %s Order of type %s, P = %.4f, Q = %.2f' % (function_name, action, order_type_str, order_price , order_qty))
        else :
            self.log('Action: %s Order of type %s, P = %.4f, Q = %.2f' % (action, order_type_str, order_price , order_qty))
        
                    
    def mergeXfireParamsWithConfig(self, xfire_params):
        '''
            @summary: merge the CrossFire parameters with the set up config
            @param xfire_params: the received CrossFire params
            @return: new parameters for CrossFire occurrences 
        '''
        
        occurence_params = xfire_params
        self.log("mergeXfireParamsWithConfig: TAP PARAMS before MERGE: %s" % occurence_params)
        self.log("mergeXfireParamsWithConfig: self['ExcludeAggressiveType']: %s" % self['ExcludeAggressiveType'])
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
        if  self['ExcludePrimary'] == 1 or xfire_params['ExcludePrimary'] == 1:
            occurence_params['ExcludePrimary'] = 1 
        else:
            occurence_params['ExcludePrimary'] = 0 
        
        # keep configration parameters
        occurence_params['ExcludeAggressiveVenue'] = self['ExcludeAggressiveVenue']
        occurence_params['ExcludePassiveVenue'] = self['ExcludePassiveVenue'] 
        
        if 'MID' not in occurence_params['ExcludePassiveType'] or  'MID' not in occurence_params['ExcludeAggressiveType']:
            self.error('Invalid configuration for a xfire')
         
        occurence_params['MIFIDBestExecutionReqd'] = 1
        
        self.log("mergeXfireParamsWithConfig: TAP PARAMS : %s" % occurence_params)
    
        return occurence_params
    
    
                  
    def modifySlippageOccurence(self, market_id, new_xfire_params, curXFire):
        '''
            @summary: modify slippage  occurrences
            @param market_id: the market id
            @param new_xfire_params: new CrossFire occurrence's parameters
            @param curXFire: CrossFire occurrence
            @todo: if there is more than 2 CrossFire occurrences, then keep the best one    
        '''        
        
        if len(curXFire) > 1:
            # more than 1 SlippageOrder => cancel all except the one at the best price            
            self.cancelAllLivingOrders(order_prefix = 'SlippageOrder')
            curXFire = None
            
        if not curXFire:
            self.createOccurrence(tactic_type = 'TAP',
                                  venue_id = market_id,
                                  prefix = 'SlippageOrder',
                                  tactic_params = new_xfire_params,
                                  priority = self.ZonePriority.Priority[self.current_zone]['SlippageOrder'])
            
            self.printOrder(new_xfire_params["Price"], new_xfire_params["OrderQty"], 'SlippageOrder', 'Insert', function_name = 'modifySlippageOccurence')
            
        elif curXFire[curXFire.keys()[0]].has_key('ExcludePrimary'):
            # check if it is a "mid" or "aggressive" order
            if curXFire[curXFire.keys()[0]]['ExcludePrimary'] != new_xfire_params['ExcludePrimary'] or curXFire[curXFire.keys()[0]].has_key('ParticipationRate'):                    
                # we need a cancel & replace
                # check if delta quantity is sufficient large to modify
                delta_quantity = abs(new_xfire_params['OrderQty'] - curXFire[curXFire.keys()[0]]['OrderQty'])
                if (delta_quantity > self.ats * 0.1):             
                    self.log('modifySlippageOccurence: delta quantity sufficient to cancel/replace')
                
                    self.cancelOccurrence(market_id, curXFire.keys()[0])
                    self.createOccurrence(tactic_type = 'TAP',
                                          venue_id = market_id,
                                          prefix = 'SlippageOrder',
                                          tactic_params = new_xfire_params,
                                          priority = self.ZonePriority.Priority[self.current_zone]['SlippageOrder'])
                    
                    self.printOrder(new_xfire_params["Price"], new_xfire_params["OrderQty"], 'SlippageOrder', 'Cancel&Replace', function_name = 'modifySlippageOccurence')
                else:
                    self.log('modifySlippageOccurence: delta quantity is not sufficient to modify')
        else:            
            # we don't need a cancel & replace
            # check if delta quantity is sufficient large to modify
            delta_quantity = abs(new_xfire_params['OrderQty'] - curXFire[curXFire.keys()[0]]['OrderQty'])
            if (delta_quantity > self.ats * 0.1):             
                self.log('modifySlippageOccurence: delta quantity sufficient to modify')
                self.modifyOccurrence(tactic_type = 'TAP',
                                      venue_id = market_id,
                                      oc_id = curXFire.keys()[0],
                                      tactic_params = new_xfire_params,
                                      priority = self.ZonePriority.Priority[self.current_zone]['SlippageOrder'])
                self.printOrder(new_xfire_params["Price"], new_xfire_params["OrderQty"], 'SlippageOrder', 'Modify', function_name = 'modifySlippageOccurence')
            else:
                self.log('modifySlippageOccurence: delta quantity is not sufficient to modify')
                
    def updateIndicators(self, evt):
        '''
            @summary: update indicators 
        '''
        # update the current delta volumes
        self.delta_volumes = self.computeVolumeThreshold()    
         
#        Perf_VWAP = self.moneyManager.getStatistics()  if isinstance(self.moneyManager.getStatistics(), float) else  self.moneyManager.getStatistics()["vwap"]
                
        Market_VWAP = self.marketManager.getFeedInfo('AE', 'WTD_AVE1')
        
        exec_qty = self.moneyManager.getExecQty()
        total_qty = self.moneyManager.getTotalQty()
        mkt_qty = self.marketManager.getFeedInfo('AE', 'QTE_CNT1')
        sum_exec_qty = self.slippage_volume + self.tracking_volume + self.opportunity_volume + self.total_would_qty + self.total_auction_qty
        Perf_VWAP = self.total_amount / sum_exec_qty if sum_exec_qty > 0 else 0
                
        # Tracking Qty
        Tracking_occ = self.moneyManager.getPrivateBook('TrackingOrder')
        tracking_price = 0.0 if self.side == 1 else 999999999999999.0 
        tracking_qty = 0
        price_is_ok = False
        
        for occ in Tracking_occ.values():
            price_is_ok = True
            order_qty = occ["LeavesQty"]
            tracking_qty += int(order_qty)
            tracking_price = self.tools.worstBetween(float(occ["Price"]), tracking_price) 
        
        if not price_is_ok :
            tracking_price = 0
            
        # Slippage Qty
        Slippage_occ = self.moneyManager.getPrivateBook('SlippageOrder') 
        
        if Slippage_occ:
            slippage_price = float(Slippage_occ.values()[0]["Price"])
            slippage_qty = int(Slippage_occ.values()[0]["LeavesQty"])
        else:
            slippage_price = 0 
            slippage_qty = 0
            
        if slippage_qty < 0 :
            self.error("Invalid Quantity !!!!!")
            
        # Opportunity Qty
        Opportunity_occ = self.moneyManager.getPrivateBook('OpportunityOrder') 
        
        if Opportunity_occ:
            opportunity_price = float(Opportunity_occ.values()[0]["Price"])
            opportunity_qty = int(Opportunity_occ.values()[0]["LeavesQty"])
        else:
            opportunity_price = 0 
            opportunity_qty = 0
            
        if opportunity_qty < 0 :
            self.error("Invalid Quantity !!!!!")
      
        self.appendIndicator({ 'EffectiveAskedQty'  : total_qty,
                               'EffectiveExecQty'   : exec_qty,
                               'SumExecQty'         : sum_exec_qty,
                               'ZoneInter'          : self.current_zone, # +1 is for backward compatiobility with previous version 
                               'VolSlippage'        : self.delta_volumes[1], #positive when late
                               'CurVolPct'          : 0, #?
                               'ExecPct'            : 100.0 * exec_qty / total_qty,
                               'ExecSpread'         : 0, # ??? TBD
                               'VwapPrice'          : Perf_VWAP, 
                               'MarketVWAP'         : Market_VWAP,
                               'PLR'                : 100.0 * exec_qty / mkt_qty if mkt_qty > 0 else 0,
                               'WouldPLR'           : 0 , #TBD
                               'StdPLR'             : 100.0 * exec_qty / mkt_qty if mkt_qty > 0 else 0 , #TBD
                               'PnL'                : self.side * (Market_VWAP - Perf_VWAP) * exec_qty if isinstance(Market_VWAP, types.FloatType) and Market_VWAP > 0 else 0,
                               'PnLBp'              : self.side * 10000.0 * (Market_VWAP - Perf_VWAP) / Market_VWAP if isinstance(Market_VWAP, types.FloatType) and Market_VWAP > 0 else 0,
                               'MaxPrice'           : 0, #TBD
                               'SlippageAvgPx'      : self.slippage_amount / self.slippage_volume if self.slippage_volume > 0 else 0, #TODO
                               'TrackingAvgPx'      : self.tracking_amount / self.tracking_volume if self.tracking_volume > 0 else 0 , #TODO
                               'OpportunityAvgPx'   : self.opportunity_amount / self.opportunity_volume if self.opportunity_volume > 0 else 0 , #TODO
                               'TrackingQty'        : tracking_qty, #TODO
                               'TrackingPrice'      : tracking_price, #TODO                               
                               'SlippageQty'        : slippage_qty, #TODO
                               'SlippagePrice'      : slippage_price, #TODO
                               'OpportunityQty'     : opportunity_qty, #TODO
                               'OpportunityPrice'   : opportunity_price, #TODO
                               'WouldQty'           : self.total_would_qty, #TODO
                               'WouldPrice'         : self.would_amount / self.total_would_qty  if  self.total_would_qty > 0 else 0, #TODO                               
                               'ForwardOppSignal'   : 100.0 * self.fwd_signal,
                               'BackwardOppSignal'  : 100.0 * self.bwd_signal,
                               'SignalCondition'    : self.signal_condition,                           
                               'MarketTrend'        : self.marketManager.getFeedInfo('AE','trend_s_600') if self.marketManager.getFeedInfo('AE').has_key('trend_s_600') else None,
                               'GKVol10min'         : self.marketManager.getFeedInfo('AE','garman_klass_bp_s_600') if self.marketManager.getFeedInfo('AE').has_key('garman_klass_bp_s_600') else self.marketManager.getFeedInfo('AE', 'GEN_VAL15'),
                               'OpeningQty'         : 0 , #TBD                               
                               'IntradayQty'        : 0, #TBD ???
                               'ClosingQty'         : 0 , #TBD
                               'OpeningOccurrencePosition'  : 0 , #TBD
                               'IntradayOccurrencePosition' : 0, #TBD ???
                               'ClosingOccurrencePosition'  : 0, #TBD
                               'OpeningExecPercentage'      : 0, #TBD
                               'ClosingExecPercentage'      : 0, #TBD
                               'OpportunityExecPercentage'  : 100.0 * self.opportunity_volume / sum_exec_qty if sum_exec_qty > 0 else 0, #TOCHECK
                               'TrackingExecPercentage'     : 100.0 * self.tracking_volume / sum_exec_qty if sum_exec_qty > 0 else 0, #TOCHECK
                               'SlippageExecPercentage' : 100.0 * self.slippage_volume / sum_exec_qty if sum_exec_qty > 0 else 0, #TOCHECK
                               'WouldExecPercentage'    : 100.0 * self.total_would_qty / sum_exec_qty if sum_exec_qty > 0 else 0, #TBD
                               'VolLowerBound'          : self.delta_volumes[0],
                               'MinExecQty'             : mkt_qty * self.qmin,
                               'MedExecQty'             : mkt_qty * self.qmid,
                               'MaxExecQty'             : mkt_qty * self.qmax,
                               'EnveloppeWidth'         : self.width_enveloppe,                               
                               'tactic_id'              : self['tactic_id'],
                               'TrackingQty'            : self.tracking_volume,
                               'SlippageQty'            : self.slippage_volume,
                               'OpportunityQty'         : self.opportunity_volume,
                               'TrackingAmount'         : self.tracking_amount,
                               'SlippageAmount'         : self.slippage_amount,
                               'OpportunityAmount'      : self.opportunity_amount,
                               'ATS'                    : self.ats}, date = evt.timestamp)
            
    def estimateEndOfPeriodTime(self, evt):
        '''
            @summary: estimate the end of period time, used to compute backward signal
            @todo: use volume curve to estimate end of period, or use indicator in AE
        '''
        return self.moneyManager.getTotalQty()/self.moneyManager.getExecQty() * (evt.timestamp - self.begin_time) if self.moneyManager.getExecQty() > 0 else 0 

    def estimateEndOfPeriodVolume(self, evt):
        '''
            @summary: estimate end of period volume, used to compute backward signal
            @todo: use indicator in AE
        '''
        cond_ok = self.marketManager.getFeedInfo('AE').has_key('QTE_CNT1') and self.moneyManager.getExecQty() > 0
        return self.marketManager.getFeedInfo('AE', 'QTE_CNT1') * self.moneyManager.getTotalQty()/self.moneyManager.getExecQty() if cond_ok else 0
    