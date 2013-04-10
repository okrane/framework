'''Scripted PVOL Algorithm (% Volume) 

  CACVersion $File: refactored_ScriptedPVOL.py$ Version 1.1.0 $Branch: QuantitativeResearch$ $Date: 06/28/2012 01:00:04 PM$

'''
surround_branch = '$Branch: QuantitativeResearch$'
surround_header = '$Header: refactored_ScriptedPVOL.py: Revision: 1: Author: thped: Date: 06/28/2012 01:00:04 PM$'
tok_head = surround_header.split(': ') 
tok_branch = surround_branch.split(': ') 

local_version = 'r4'

__version__ = "CACVersion %s Version %s.%s %s" % (tok_head[1][:-3], tok_branch[1][:-1], local_version, tok_head[7])
__author__ = 'vleclercq@cheuvreux.com'
__reviewer__ = 'svlasceanu@cheuvreux.com'
__review_date__ = '2011/09/26'


# import under AlgoBox
#from simep.tactic import Tactic

# import under SIMEP
from simep.core.subtactic import Tactic
from collections import defaultdict
from collections import deque
from random import *
import math
from numpy import *




class refactored_ScriptedPVOL(Tactic):
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
        
    class Utils:
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
                    #print "utils:count_substring_in_list: %s is in %s" % (lower_sub_str, s.lower())
                #else :
                    #print "utils:count_substring_in_list: %s is not in %s" % (lower_sub_str, s.lower())
            return (counter, keys)   
        
        
        def bestBetween(self, *args):
            return min(args) if self.side == 1 else max(args)
                 
        def convertDateToSecond(self, datevalue):
            #print datestr, ':::', datestr[9:11], datestr[12:14], datestr[15:16]        
            return 24.0 * 3600 * (datevalue.toordinal() + datevalue.hour / 24.0 + datevalue.minute / (24.0 * 60) + datevalue.second / (24.0 * 60 * 60) + datevalue.microsecond / (24.0 * 60 * 60 * 1000000))
        
        def convertDateStrToSecond(self, datestr):        
            return int(datestr[0:1]) * 24.0 * 3600.0 + int(datestr[3:4]) * 3600.0 + int(datestr[6:7])            
        
    class DictDiffer(object):
        """
        Calculate the difference between two dictionaries as:
        (1) items added
        (2) items removed
        (3) keys same in both but changed values
        (4) keys same in both and unchanged values
        """
        def __init__(self, current_dict, past_dict):
            self.current_dict, self.past_dict = current_dict, past_dict
            self.set_current, self.set_past = set(current_dict.keys()), set(past_dict.keys())
            self.intersect = self.set_current.intersection(self.set_past)
        def added(self):
            return self.set_current - self.intersect 
        def removed(self):
            return self.set_past - self.intersect 
        def changed(self):
            return set(o for o in self.intersect if self.past_dict[o] != self.current_dict[o])
        def unchanged(self):
            return set(o for o in self.intersect if self.past_dict[o] == self.current_dict[o])
         

               
        
    def __init__(self, setup, context, parameters):
        
        Tactic.__init__(self, setup, context, parameters)
        
        self.next_slippage_deal = 0
        self.current_state = {}
        self.current_taps = {} 
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
        
        
        self.is_in_auction = False
        self.would_qty = 0
        self.total_would_qty = 0
        self.would_amount = 0
        #test 
        self.mkt_qty = 0
        
        self.spread_bid_ask_bp = 0
        
        # aggressiveness management 
        
        self.agressiveness_factor = 0
        
        self.width_enveloppe = 0
        
        # would level order
        self.would_occ_qty = 0
        self.would_level_is_used = False
        
        # Mono / Multi Destination
        
        self.is_mono_destination = True
        
        self.tick_size = None
        self.true_ats = 0
        
        self.last_auction_code = None
        self.last_auction_qty = 0
        self.latest_submitted_parameters = None
        self.is_limited = False
        
        
        self.executed_in_auction = 0
        self.delta_auction = 0
        self.auction_qty = 0
        self.total_auction_qty = 0
        self.last_trading_state = ''
        self.trading_state = ''
        #default mode is timer on, as we know we always have a timer info
        self.use_timers = True
        
        self.wait_images = 0
        self.max_wait_images = 10
        self.last_mkt_qty = 0
        self.current_mkt_qty = 0
        self.rate_is_adjusted = False
        
        self.delta_agressive_qty = 0
        
        self.end_of_algo_file = False
        
        self.total_continuous_market_qty = 0
        self.delta_volumes = [0.0, 0.0, 0.0]
        self.current_zone = -1
        
        self.market_qty_between_tracking_update = 0
        self.last_mkt_qty_placing_tracking = 0
        self.last_exec_qty_placing_tracking = 0
        
        self.total_amount = 0.0
        self.first_process = True
        self.exp_exec_time  = 0
        self.begin_time = 0
        self.exp_end_of_period_volume = 0
        
        self.adjusted_fwd_signal = 0.5
        self.adjusted_bwd_signal = 0.5
        self.mean_fwd_signal = 0.5
        self.mean_bwd_signal = 0.5
        self.fwd_signal = 0.5
        self.bwd_signal = 0.5
        self.last_fwd_signal = deque()
        self.last_bwd_signal = deque()
        
        try:
            from usr.dev.st_algo.funcs.OpportunitySignal import OpportunitySignal
            self.opportunity_signal = OpportunitySignal()            
        except ImportError:
            self.opportunity_signal = None
             
    @staticmethod    
    def getDefaultParametersOld():
        default_params = {"ExecutionStyle"     : 3,
                          "MinPctVolume"       : 10,
                          "ParticipationRate"  : 12,
                          "ExecutionStyle" : 3,                          
                          "benchmark" :'PVOL',
                          "ExcludeAggressiveType" :'MID DARK',
                          "ExcludePassiveType" :'MID DARK',
                          "ExcludePrimary" :0,
                          "maxAmount" :1000000000,
                          "MaxMultATS" :10,                          
                          "SlippageNbDeals" :5,
                          "SpreadBidAskBp" :50,
                          "SpreadBidAskBpLowerBound" :10,
                          "SpreadBidAskBpUpperBound" :100,                          
                          "OpeningAuctionPolyCoeff" :'1,1.7678,-9.6238,18.9054,1.1359',
                          "ClosingAuctionPolyCoeff" :'1,0.5892,-2.705,4.3429,1.05',
                          "ExcludeAuction" :'1 3 4',
                          "MinLateATSAuction" : 5
                          } 
        return default_params
        
    

    def checkParameters(self, parameters, error):
        
        self.log("Checking Parameters ...")
        self.log("Client Params : %s" % dict(self.client_params))
        if self['SimulatedMode']:
            self.log("Using Simulated Mode")
        else:
            self.log("NOT Using Simulated Mode")
        # Buy = 1, Sell = 2
        self.latest_submitted_parameters = self._parameters
        
        
        self.log("PrimaryVolumeOnly %s" % self['PrimaryVolume'])
        self.log("%s" % self._parameters)
        if self['PrimaryVolume'] == 1 or self['PrimaryVolume'] == None:
            self.log("Mono Destination") 
            self.is_mono_destination = True
        else:
            self.is_mono_destination = False
            self.log("Multi Destination")
            
        
        if float(self['OrderQty']) <= 0:
            error.message = "Invalid Asked Quantity, please check"
        
        
        if self['MinPctVolume'] == None and self['ParticipationRate'] == None :
            error.message = "Invalid Participation Rates : At least one percentage must be known"
            return False
        
        if not self['SpreadBidAskBp']: 
            if  self._parameters.has_key('SpreadBidAskBpLowerBound') and self._parameters.has_key('SpreadBidAskBpUpperBound'):
                self.spread_bid_ask_bp = 0.5 * (float(self['SpreadBidAskBpUpperBound']) + float(self['SpreadBidAskBpLowerBound']))
        else:
            self.spread_bid_ask_bp = max(float(self['SpreadBidAskBp']), float(self['SpreadBidAskBpLowerBound']))
            self.spread_bid_ask_bp = min(float(self['SpreadBidAskBp']), float(self['SpreadBidAskBpLowerBound']))
        
        
        if self['MinPctVolume'] == None and self['ParticipationRate'] != None :
            #value by default Min % = Max % - 0.1  * Max % 
            self._parameters['MinPctVolume'] = max(0, 0.9 * self['ParticipationRate'])
            
            
        if self['MinPctVolume'] != None and self['ParticipationRate'] == None :
            #value by default Max % = Min % + 0.1  * Min % 
            error.message = "You must specify a Max %"
            return False
        
        # now check with respect to the Maximum PLR  
        if self['MaximumPLR']:
            if int(self['ParticipationRate']) > int(self['MaximumPLR']) :
                self._parameters['ParticipationRate'] = self['ParticipationRate']
        
        
        if self['ExecutionStyle'] == None: 
            #value by default 2, from 1 to 5 
            self._parameters['ExecutionStyle'] = 3
      
        self.agressiveness_factor = 4 - int(self['ExecutionStyle']) / 5 
        print "ExecutionStyle : %d" % self.agressiveness_factor

        self.agressiveness = 0.01 * (20 - float(self['ExecutionStyle']))
        
        if  self['MinPctVolume'] > self['ParticipationRate']:
            error.message = "Invalid Participation Rates : MinPctVolume (%s) must be < to ParticipationRate(%s)" % (self['MinPctVolume'], self['ParticipationRate'])
            return False
            
        if self['Side'] == '1' or self['Side'] == 1:
            self.side = 1
        else:
            self.side = -1
            
        # add the Util class
        self.utils = self.Utils(self.side)
            
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

        if self['OpeningAuctionPolyCoeff']:
            self.openingAuctionPolyCoeff = [float(self['OpeningAuctionPolyCoeff'].split(',')[i]) for i in range(5)]
        if self['ClosingAuctionPolyCoeff']:
            self.closingAuctionPolyCoeff = [float(self['ClosingAuctionPolyCoeff'].split(',')[i]) for i in range(5)]        
        
        self.next_opportunity_deal = 0
        
        if self['ExcludeAuction']:            
            
            self.log('Auction Exclusions : %s' % self['ExcludeAuction'])
            excluded_auction_list = self['ExcludeAuction'].split(' ')
            
            self.participate_in_opening = '1' not in excluded_auction_list
            
            if self.participate_in_opening :
                self.log('Opening included')
            
            self.participate_in_closing = '4' not in excluded_auction_list
            
            if self.participate_in_closing :
                self.log('Closing included')
            
            self.participate_in_intraday = '3' not in excluded_auction_list
            
            if self.participate_in_intraday :
                self.log('Intraday included')
            
            self.participate_in_mid_auction = '8' not in excluded_auction_list
            
            if self.participate_in_mid_auction :
                self.log('Mid day included')  
            else:
                self.participate_in_mid_auction = False
                self.participate_in_intraday = False
                self.participate_in_closing = False
                self.participate_in_opening = False
             
        if self["destinationId"]:
            self.log("DestinationId :%d" % self["destinationId"])
    
        return True           
    
    
    def validateModifyParameters(self, client_params, rejection_message):
       
        if not self.checkParameters(client_params, rejection_message):
            return False
        else:
            if not client_params.has_key('ParticipationRate'):
                rejection_message.message = "validateModifyParameters: Order rejected because no max %"
                return False
            else :  
                if not client_params.has_key('MinPctVolume') :
                    client_params['MinPctVolume'] = max(0.0, 0.9 * client_params['ParticipationRate'])
                elif  client_params['ParticipationRate'] < client_params['MinPctVolume']:
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


    def reassesOnModify(self, OrderPrefix, qty_to_free):
        
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
        needed_parameters_to_check = set(['MinPctVolume', 'ParticipationRate', 'Price',
                                           'AdjustedPriceInd', 'ExecutionStyle', 'PrimaryVolumeOnly', 'OrderQty' ]) 
        new_params = self.convertToNumericValues(client_params) 
        dict_comparison = self.DictDiffer(new_params, self.latest_submitted_parameters)

        self.log("_ PARAMS :%s" % (self._parameters))
        self.log("CLIENT PARAMS :%s" % (client_params))
        self.log("SELF (td, primary) :(%s,%s)" % (self["destinationId"], self['PrimaryVolume']))
        
        if len(dict_comparison.changed() & needed_parameters_to_check) == 0 and len(dict_comparison.added() & needed_parameters_to_check) == 0 and len(dict_comparison.removed() & needed_parameters_to_check) == 0 : 
            self.log("[RESET] Processing  modification, but same parameters submitted")
            print "Differences between parameters :"
            print dict_comparison.changed() & needed_parameters_to_check
            print "Added  :"
            print dict_comparison.added() & needed_parameters_to_check
            print "Removed  :"
            print dict_comparison.removed() & needed_parameters_to_check 
            print "Original parameters  :"
            print self.latest_submitted_parameters 
            print "Resubmitted  parameters  :"
            print new_params
            self.latest_submitted_parameters = new_params
            self.rate_is_adjusted = False
            return Tactic.Modify.ACCEPT
        
        else :
            self.log("[RESET] Processing  modification,  parameters submitted have changed")
            print "Differences between parameters :"
            print dict_comparison.changed() & needed_parameters_to_check
            print "Added  :"
            print dict_comparison.added() & needed_parameters_to_check
            print "Removed  :"
            print dict_comparison.removed() & needed_parameters_to_check 
            print "Original parameters  :"
            print self.latest_submitted_parameters 
            print "Resubmitted  parameters  :"
            print new_params
        # check validity of the new parameters and reject if necessary 
        if client_params['OrderQty'] < self.moneyManager.getExecQty():
            # we need to reject 
            # display reason for rejection
            reject_msg.message = 'Modification rejected : Asked quantity is less than quantity already executed'
            return Tactic.Modify.REJECT
        
        
        
        # Cancel and Replace
        limit_price_has_not_changed = (client_params.has_key('Price') and self['Price'] and  self['Price'] == client_params['Price']) or  (not client_params.has_key('Price') and self['Price'] == None)
        would_price_has_not_changed = (client_params.has_key("AdjustedPriceInd") and self['AdjustedPriceInd'] and  self['AdjustedPriceInd'] == client_params['AdjustedPriceInd']) or  (not client_params.has_key('AdjustedPriceInd') and self['AdjustedPriceInd'] == None)
        
        
        # Primary Volume Management on modify(TOTEST)
        if self._parameters.has_key('PrimaryVolume'):
            if  self['PrimaryVolume'] != None:
            # we have a value : Compare to existing one
                primary_only_has_not_changed = client_params['PrimaryVolume'] == self['PrimaryVolume'] if client_params.has_key('PrimaryVolume') else True
        else :
            # Raise an error
            self.error("Primary Volume is not defined, something wrong here")
        
        # TVenue Management on modify (TOTEST)        
        if self._parameters.has_key('destinationId'):
            if  self["destinationId"] != None:
                # we have a value : Compare to existing one
                if client_params.has_key('destinationId'):
                    td_has_not_changed = client_params['destinationId'] == self['destinationId'] if  client_params['destinationId'] != None else True
                    self.log("destinationId : Old :%d, New : %d !" % (self['destinationId'], client_params['destinationId']))
                else:
                    td_has_not_changed = True
            else:
                self.error("destinationId is None, something wrong here")
        else :
            self.error("destinationId is undefined , something wrong here")
            
        # primary_only_has_not_changed = (client_params.has_key('PrimaryVolumeOnly') and self['PrimaryVolumeOnly'] and  self['PrimaryVolumeOnly'] == client_params['PrimaryVolumeOnly']) or  (not client_params.has_key('PrimaryVolumeOnly') and self['PrimaryVolumeOnly'] == None)
        # td_has_not_changed = (client_params.has_key('destinationId') and self['destinationId'] and  self['destinationId'] == client_params['destinationId']) or  (not client_params.has_key('destinationId') and self['destinationId'] == None)
        
        if client_params.has_key("AdjustedPriceInd") and  self['AdjustedPriceInd'] == None :
            self.would_level_is_used = False
        elif  (not client_params.has_key("AdjustedPriceInd")) and  self['AdjustedPriceInd'] != None :
            self.would_level_is_used = True
        
        
        if  not client_params.has_key('ParticipationRate') :  
            self.error("Please specify at least a max %")
        elif not client_params.has_key('MinPctVolume') :
            client_params['MinPctVolume'] = max(0, 0.9 * self['ParticipationRate'])
        
        vol_percent_changed = (client_params['ParticipationRate'] != self['ParticipationRate']) or (client_params['MinPctVolume'] != self['MinPctVolume'])          
    
        base_volume_computation_changed = vol_percent_changed or not (limit_price_has_not_changed and would_price_has_not_changed and td_has_not_changed and primary_only_has_not_changed)
        
        if base_volume_computation_changed :
            self.log("[RESET] Base Volume changed , reinitializing tactic")
            # Cancel & Replace
            # we restart completely the tactic, in any cases,  ? 
            # what we should do with the occurrences ? Cancel  all of them   
            for occ in self.moneyManager.getPrivateBook().keys():
                venue_id = self.moneyManager.getPrivateBook()[occ]["VenueId"]
                self.cancelOccurrence(venue_id, occ)
            # record the modification 
            # for market
            
            # market volume from new submission  
            
            self.total_continuous_market_qty += self.marketManager.getFeedInfo('AE', 'QTE_CNT1')   
            
            
            #self.delta_modify_qty = self.moneyManager.getExecQty()  + self.delta_modify_qty 
            self.log("vol_percent_changed :%d , limit_price_has_not_changed : %d,would_price_has_not_changed : %d , td_has_not_changed :%d, primary_only_has_not_changed : %d"
                                                                    % (vol_percent_changed, limit_price_has_not_changed, would_price_has_not_changed, td_has_not_changed, primary_only_has_not_changed))
            self.log("Updating delta qty to %d" % (self.delta_modify_qty))
            
            #reset the tactic
            return Tactic.Modify.REINITIALIZE
        
        # manage new quantity by respect to executed quantity
        self.log("The Order Qty is : %d, and the Exec Qty is : %d" % (client_params['OrderQty'], self.moneyManager.getExecQty()))    
        if client_params['OrderQty'] > self.moneyManager.getLeavesQty():
            # we can continue, with the new asked quantity
            # update the available Quantity with the new Amount
            self.moneyManager.setTotalQty(client_params['OrderQty'])
            self.log("Updating Total Qty")
            return Tactic.Modify.ACCEPT
            
        # manage new quantity by respect to leaves quantity
        
        if client_params['OrderQty'] < self.moneyManager.getLeavesQty():
            # we will need to free some quantity using the following order :
            # first cancel passive order, order by price , best price (far from the best) first

            # Initialize the Quantity to free 
            qty_to_free = self.moneyManager.getLeavesQty() - client_params['OrderQty'] 
            
            # Passive (Iceberg + TAP)
            qty_to_free = self.reassesOnModify("PassiveTracking", qty_to_free)
            
            # Aggressive : if qty_to_free == 0 , the following function will do nothing
            qty_to_free = self.reassesOnModify("AggressiveTracking", qty_to_free)
            return Tactic.Modify.REINITIALIZE
            
        # otherwise accept the modifications
        self.log("Accept Modifications")
        return Tactic.Modify.ACCEPT
   
    def getAuctionPositionPrice(self, occurence_position, last_price):
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
        
        self.log("Entering getAuctionPositionPrice : %s %d, base position is : %s " % (occurence_position, last_price, base_position))
        price_auction = None
        if look_for_offset:
            # we have an offset
            if len(parts) > 1 : 
                if 'bp' in parts[1]:
                    if '+' in occurence_position:
                        offset_bp = float(parts[1].split('bp')[0])
                    elif  '-' in occurence_position:
                        offset_bp = -1 * float(parts[1].split('bp')[0])       
                    else :
                        self.error("Not able to determine the price offset for %s" % occurence_position)
                else:
                    self.log("Not able to determine the price offset for %s, using 0" % occurence_position)
        else :
            self.log("No price offset for %s found, using 0" % occurence_position)
            offset_bp = 0
            # look for the right reference
        if base_position.lower() == 'last' :
            last = float(self.event_market['DEAL_PRICE'])
            self.log("Using Last , %f" % last) 
            price_auction = float(last) + self.side * last_price * offset_bp / 10000.0
        elif base_position.lower() == 'market': 
            self.log("Using Market order")
            price_auction = 'Market'
        elif base_position.lower() == 'close':
            close = float(self.marketManager.getFeedInfo('AE', 'HST_CLOSE'))
            self.log("Using close price : %f" % close)
            price_auction = float(close) + self.side * last_price * offset_bp / 10000.0
        elif base_position.lower() == 'open' or base_position.lower() == 'marketopen':
            open = float(self.marketManager.getFeedInfo('AE', 'OPEN1'))
            self.log("Using open  price : %f" % open)
            price_auction = float(open) + self.side * last_price * offset_bp / 10000.0
        else:
            self.error("Stopping, Using unknwown order position for auction: %s" % base_position)
        
        
        return price_auction        
         
    
    def computeAuctionOrder(self, trading_state, last_price):
        # compute (price, volume) of the order in the auction
        self.log("computeAuctionOrder, trading state is %s" % trading_state)
        
        if trading_state == 'O':
            if self['OpeningOccurrencePosition'] != None or self['OpeningOccurrencePosition'] != '':
                price_auction = self.getAuctionPositionPrice(self['OpeningOccurrencePosition'] , last_price)
            else:
                self.log(" %s seems invalid, please check" % self['OpeningOccurrencePosition'])
        elif trading_state == 'C':
            if self['ClosingOccurrencePosition'] != None or self['ClosingOccurrencePosition'] != '':
                price_auction = self.getAuctionPositionPrice(self['ClosingOccurrencePosition'] , last_price)
            else:
                self.log(" %s seems invalid, please check" % self['ClosingOccurrencePosition'])
        elif trading_state == 'I':
            if self['IntradayOccurrencePosition'] != None or self['IntradayOccurrencePosition'] != '':
                price_auction = self.getAuctionPositionPrice(self['IntradayOccurrencePosition'] , last_price)
            else:
                self.log(" %s seems invalid, please check" % self['IntradayOccurrencePosition'])
            
        else:
            self.log('Trading state is %s, not recognized' % trading_state)
  
        if isinstance(price_auction, float) :
            print "OccurrencePosition %s ,price_auction %f  " % (self['ClosingOccurrencePosition'], price_auction)
        elif isinstance(price_auction, basestring):
            print "OccurrencePosition %s ,price_auction %s  " % (self['ClosingOccurrencePosition'], price_auction)
        
        if self.is_limited and isinstance(price_auction, basestring):
            price_auction = self.limit_price
        elif self.is_limited and isinstance(price_auction, float):
            price_auction = self.utils.bestBetween(self.limit_price, price_auction)
            
            
        return price_auction
            
        
    def manageAuctionOrder(self, evt, price_auction, quantity_auction, auction_leaves_quantity):
        # get all exist orders, already in self.current_state        
        
        delta_volume_to_adjust = quantity_auction - auction_leaves_quantity 
        self.log ("delta_volume_to_adjust : %d" % int(delta_volume_to_adjust))
        auction_occurence = self.moneyManager.getPrivateBook("ClassicAuction")
        
        if len(auction_occurence) > 0 :
            #we have already an occurenc, do a modify
            if len(auction_occurence) > 1 :
                self.error("We shoudl have a single auction order")
            else :
                self.log("we have already only one order, do a modify")
                print auction_occurence
                auction_occurence = self.moneyManager.getPrivateBook("ClassicAuction").keys()[0]
                print "Existing order is %s" % auction_occurence
                auction_order_exist = True
        else :  
            auction_order_exist = False

        # we add a new order in all case if no auction living order
        if delta_volume_to_adjust > self.ats or not auction_order_exist:
            # we add a new order
            
            if isinstance(price_auction, basestring):
                # Market Order
                self.log ("price auction is a string !!")
                if not self.is_limited  :
                    if 'Market' in price_auction:
                        #do we have already an auciton order ? 
                        if auction_order_exist:
                            self.modifyOccurrence(tactic_type='MarketOrder',
                                         venue_id=evt.getVenueId(),
                                         oc_id=auction_occurence,
                                         tactic_params={'OrderQty': int(quantity_auction),
                                                        'OrderType' : 'market'})
                            self.printOrder(1000000, quantity_auction, 'Auction Market Order', 'Modify')   
                        else:
                            self.createOccurrence(tactic_type='MarketOrder',
                                 venue_id=evt.getVenueId(),
                                 prefix='ClassicAuction',
                                 tactic_params={'OrderQty': int(quantity_auction),
                                                'OrderType' : 'market'})
                        self.log(" Market order of Qty %d" % int(quantity_auction))
                    else:
                        self.log("Something wrong with the auction price")
                else:
                    # do we have already an auction order ? 
                    if auction_order_exist:
                        #TOCHECK : Modify allowed on all  markets for Auction orders ? 
                        self.modifyOccurrence(tactic_type='MarketOrder',
                                 venue_id=evt.getVenueId(),
                                 oc_id=auction_occurence,
                                 tactic_params={'Price': self.limit_price,
                                                'OrderQty': int(quantity_auction),
                                                'OrderType' : 'market'})
                        self.printOrder(self.limit_price, quantity_auction, 'Auction Limited Order', 'Modify')    
                    else:
                    # no order, apply the limit price
                        self.createOccurrence(tactic_type='MarketOrder',
                                 venue_id=evt.getVenueId(),
                                 prefix='ClassicAuction',
                                 tactic_params={'Price': self.limit_price,
                                                'OrderQty': int(quantity_auction),
                                                'OrderType' : 'limit'})
                        self.printOrder(self.limit_price, delta_volume_to_adjust, 'Auction Limited Order', 'Insert')
                    
            
            elif isinstance(price_auction, float):
                self.log ("price auction is a float !!")
                if auction_order_exist:
                    self.modifyOccurrence(tactic_type='MarketOrder',
                                 venue_id=evt.getVenueId(),
                                 oc_id=auction_occurence,
                                 tactic_params={'Price': self.utils.bestBetween(price_auction, self.limit_price),
                                                'OrderQty': int(quantity_auction),
                                                'OrderType' : 'limit'})
                    self.printOrder(price_auction, int(quantity_auction), 'AuctionLimitOrder', 'Modify')
                
                else :
                    self.createOccurrence(tactic_type='MarketOrder',
                                 venue_id=evt.getVenueId(),
                                 prefix='ClassicAuction',
                                 tactic_params={'Price': self.utils.bestBetween(price_auction, self.limit_price),
                                                'OrderQty': int(quantity_auction),
                                                'OrderType' : 'limit'})
                
                    self.printOrder(price_auction, int(delta_volume_to_adjust), 'AuctionLimitOrder', 'Insert')
                
        elif delta_volume_to_adjust < -self.ats:
            
            if auction_order_exist:
                if isinstance(price_auction, float):
                    self.modifyOccurrence(tactic_type='MarketOrder',
                                 venue_id=evt.getVenueId(),
                                 oc_id=auction_occurence,
                                 tactic_params={'Price': price_auction,
                                                'OrderQty': int(quantity_auction),
                                                'OrderType' : 'limit'})
                    self.printOrder(price_auction, int(quantity_auction), 'AuctionLimitOrder', 'Modify')
                elif isinstance(price_auction, basestring):
                    self.modifyOccurrence(tactic_type='MarketOrder',
                                 venue_id=evt.getVenueId(),
                                 oc_id=auction_occurence,
                                 tactic_params={'OrderQty': int(quantity_auction),
                                                'OrderType' : 'market'})
                    self.printOrder(1e99, int(quantity_auction), 'AuctionMarketOrder', 'Modify')
                else :
                    self.log("Type of the auciton price seems incorrect, plese check :")
                    print price_auction
            else:
                    self.error("You want to lower the qty, but no existing order !!! ")
        else:
            self.log("No order modified or inserted because of delta_volume_to_adjust")
       
        if self.would_level_is_used:
            curWouldAuction = self.moneyManager.getPrivateBook("WouldAcution").keys()[0]
            would_auction_qty = self.moneyManager.getTotalQty() - quantity_auction
            if would_auction_qty > 0:
                if curWouldAuction :
                    self.modifyOccurrence(tactic_type='MarketOrder',
                                 venue_id=evt.getVenueId(),
                                 oc_id=curWouldAuction,
                                 tactic_params={'Price': self.would_level,
                                                'OrderQty': int(would_auction_qty),
                                                'OrderType' : 'limit'})
                else:
                    self.createOccurrence(tactic_type='MarketOrder',
                                 venue_id=evt.getVenueId(),
                                 prefix='WouldAcution',
                                 tactic_params={'Price': self.would_level,
                                                'OrderQty': int(would_auction_qty),
                                                'OrderType' : 'limit'})
                            
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
                return self.openingAuctionPolyCoeff[0] + self.openingAuctionPolyCoeff * time_before_auction_end + self.openingAuctionPolyCoeff[2] * time_before_auction_end ^ 2 + self.openingAuctionPolyCoeff[3] * time_before_auction_end ^ 3
            elif trading_state == 'C':                
                return self.closingAuctionPolyCoeff[0] + self.closingAuctionPolyCoeff[1] * time_before_auction_end + self.closingAuctionPolyCoeff[2] * time_before_auction_end ^ 2 + self.closingAuctionPolyCoeff[3] * time_before_auction_end ^ 3
    
    
    def validateMarketConditions(self, trading_state):
        # if we have no best opposite, the raise an error and stop
   
        in_auction = False
        #TOREMOVE
        if not trading_state:
            self.log("validateMarketConditions Forcing Trading State to TR ...")
            trading_state = 'TR'
        #TOREMOVE
        if trading_state == "WAIT":
            return self.MarketState.WAIT
        
        if len(trading_state) > 0:
            if trading_state[0] == 'A':
                in_auction = True
                self.is_in_auction = True
                self.log("validateMarketConditions : in auction !!")
            else:
                self.is_in_auction = False
                self.log("validateMarketConditions : Not in auction !! %s" % trading_state)
        else:
                self.log("Not able to  determine market state")
                self.is_in_auction = False
                self.wait_images += 1
                if self.wait_images < self.max_wait_images :
                    return self.MarketState.WAIT
                else:
                    return self.MarketState.INVALID
                
            
        if not in_auction:
            self.is_in_auction = False
            if not self.event_market['BEST1'] :
                self.log("No Best 1")
                self.wait_images += 1
                if self.wait_images < self.max_wait_images :
                        return self.MarketState.WAIT
                else:
                        return self.MarketState.INVALID
                        
            if not self.event_market['BEST2'] :
                self.log("No Best 2")
                
                if self.wait_images < self.max_wait_images :
                        self.wait_images += 1
                        return self.MarketState.WAIT
                else:
                        return self.MarketState.INVALID
        
            best_opposite = self.event_market['BEST_OPPOSITE1']
        
            if not best_opposite :
                self.log("No Best Opposite")
                if self.wait_images < self.max_wait_images :
                        self.wait_images += 1
                        return self.MarketState.WAIT
                else:
                        return self.MarketState.INVALID
            # tocheck ?? why here ?         
            elif not (self.side * self.would_level < best_opposite + 10000 * self.side * (best_opposite * (2 * self.spread_bid_ask_bp + 10) / 10000)):
                #validate would level as we could not do this in the check_parameters
                self.log("validateMarketConditions:Would Level not validated", log_level = "ERROR")
                
                return self.MarketState.INVALID
            else:
                self.log("validateMarketConditions:Would Level validated", log_level = "DEBUG")
                
        else:
            self.is_in_auction = True
            # we are in auction
            if trading_state[1] == 'O' :
                return self.MarketState.WAIT if not self.participate_in_opening else self.MarketState.REGULAR
            elif trading_state[1] == 'I' :
                return self.MarketState.WAIT if not self.participate_in_intraday else self.MarketState.REGULAR
            elif trading_state[1] == 'C' :
                return self.MarketState.WAIT if not self.participate_in_closing else self.MarketState.REGULAR
            elif trading_state[1] == 'M' :  
                return self.MarketState.WAIT if not self.participate_in_mid_auction else self.MarketState.REGULAR
            else:
                self.log("validateMarketConditions:in auction, but with an invalid state : %s" % trading_state)
                return self.MarketState.INVALID
            
            
            
        if 'TL' in trading_state or 'TA' in trading_state or 'TH'in trading_state or (trading_state[0] == 'C' and trading_state[1] != 'F'):
            print('validateMarketConditions:Market is CLOSED, the trading state is :%s' % trading_state)
            return self.MarketState.CLOSED    
        
        return self.MarketState.REGULAR
    
    def cancelAllLivingOrders(self, order_prefix=None, exclude_prefix=None):
        PB = self.moneyManager.getPrivateBook(order_prefix)
        
        
        for occ in PB.keys():
            if exclude_prefix :
                if  exclude_prefix not in occ :
                    venue_id = PB[occ]["VenueId"]
                    self.cancelOccurrence(venue_id, occ)
                    self.printOrder(0, 0, order_prefix, "Cancel", function_name="cancelAllLivingOrders")
                else :
                    self.log("cancelAllLivingOrders:Not Cancelling Order with prefix : %s" % occ)
            else:  
                venue_id = PB[occ]["VenueId"]
                self.cancelOccurrence(venue_id, occ)
                self.printOrder(0, 0, order_prefix, "Cancel", function_name="cancelAllLivingOrders")
    
    def getLimitPriceLevelInLOB(self):        
        # verify if the limit price is in LOB
        # @return: 
        # - positive value in [1,5] = first best order <= limit price
        # - 6 : not in LOB  
        
        limit_price_not_found = True
        level_in_lob = 1
        
        if self.side * self.limit_price > self.side * self.event_market['BEST1']:
            limit_price_not_found = False
            level_in_lob = 1            
        
        while limit_price_not_found and level_in_lob <= 5:
            if self.event_market['BEST%d' % level_in_lob] == self.limit_price:
                limit_price_not_found = False
            level_in_lob += 1    
        
        return level_in_lob
     
    def mergeNewTrackingOrders(self, evt, orders) :
               
        dict_by_prices = self.utils.invert_nested_dict(orders)        
        
        for price in dict_by_prices.keys() :
            order_names = dict_by_prices[price].keys()            

            news = self.utils.count_substring_in_list(order_names, 'new')
            olds = self.utils.count_substring_in_list(order_names, 'old')
            
            nb_news = news[0]
            order_news = news[1]
            
            nb_olds = olds[0]
            order_olds = olds[1]
            
            # TOTEST
            if nb_news == 0 and nb_olds == 0:
                print 'problem, no news and no olds ???'
                self.log("news : %s, olds : %s" % (news, olds))
                
            if nb_news == 1 and nb_olds == 0:                
                # we need an order at a new price -> Insert
                volume_order = min(dict_by_prices[price][order_news[0]][0], self.moneyManager.getRemainingQty())
                self.createOccurrence(tactic_type='IcebergMono',
                                 venue_id=evt.getVenueId(),
                                 prefix='TrackingOrder%.3f' % price,
                                 tactic_params={'Price': price,
                                               'OrderQty': volume_order},
                                 priority= -2)
                self.printOrder(price, volume_order, "Tracking", "Insert", "mergeNewTrackingOrders")
                # we need to define the priority after ! bets price must have the best priority 
                
            if nb_news == 0 and nb_olds == 1:
                #retrieve the order name
                the_occ = dict_by_prices[price][order_olds[0]][1]
                #TOTEST : do we cancel the good one ? 
                tracking_occurence = self.moneyManager.getpvOrder(the_occ)
                if tracking_occurence:
                    venue_id = tracking_occurence["VenueId"]
                    self.cancelOccurrence(venue_id, the_occ)
                    self.printOrder(0, 0, "Tracking", "Cancel", "mergeNewTrackingOrders")
                else :
                    self.log("mergeNewTrackingOrders: Not able to find  %s in %s" % (the_occ, self.moneyManager.getPrivateBook("Tracking")))  
                
            if nb_news == 1 and nb_olds == 1:
                
                # do a modify if its necessary
                # if Qnew > Q_old + 1 ATS, then do a modify
                #if Q_new < Q_old, do a modify
                # else do nothing, doesn't worth a loss of priority
                
                new_volume = dict_by_prices[price][order_news[0]][0]
                old_volume = dict_by_prices[price][order_olds[0]][0]
                #TOTEST

                if new_volume > old_volume + self.ats :
                    # worth a modify, even if we lose the priority
                    self.log("computeTrackingOcurrences:worth a modify, even if we lose the priority")
                    the_old_order_id = dict_by_prices[price][order_olds[0]][1]
                    the_old_occurence = self.moneyManager.getpvOrder(the_old_order_id)
                    volume_order = min(dict_by_prices[price][order_news[0]][0], self.moneyManager.getRemainingQty())
                    
                    self.modifyOccurrence(tactic_type='IcebergMono',
                                          venue_id=the_old_occurence["VenueId"],
                                          oc_id=the_old_order_id,
                                          tactic_params={'Price':price, 'OrderQty': volume_order})
                    self.printOrder(price, volume_order, "Tracking", "Modify", "mergeNewTrackingOrders")
                elif new_volume < old_volume :
                    self.log("computeTrackingOcurrences: worth a modify, we do not lose the priority")
                    # worth a modify,  we do not lose the priority
                    the_old_order_id = dict_by_prices[price][order_olds[0]][1]
                    the_old_occurence = self.moneyManager.getpvOrder(the_old_order_id)
                    venue_id = the_old_occurence["VenueId"]
                    volume_order = min(dict_by_prices[price][order_news[0]][0], self.moneyManager.getRemainingQty())
                    
                    self.modifyOccurrence(tactic_type='IcebergMono',
                                          venue_id=venue_id,
                                          oc_id=the_old_order_id,
                                          tactic_params={'Price': price, 'OrderQty': volume_order})
                    self.printOrder(price, volume_order, "Tracking", "Modify", "mergeNewTrackingOrders")
                else :
                    self.log('mergeNewTrackingOrders: a modify was requested but not executed, because volume condition.')
                    
                
                
            if nb_news == 2 and nb_olds == 0:
                self.log("mergeNewTrackingOrders:  2 orders at new price")
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
                    self.log('mergeNewTrackingOrders: negative volume order requested')
                if volume_order2 > 0 :
                    self.createOccurrence(tactic_type='IcebergMono',
                                     venue_id=evt.getVenueId(),
                                     prefix='TrackingOrder',
                                     tactic_params={'Price': price,
                                                   'OrderQty': volume_order2},
                                     priority= -2)  
                else :
                    self.log('mergeNewTrackingOrders: negative volume order requested')
                    
            if nb_news == 0 and nb_olds == 2:
                
                #TOTEST : cancel the 1st one  
                the_occ = dict_by_prices[price][order_olds[0]][1]
                self.log("mergeNewTrackingOrders:  2 orders to cancel")
                tracking_occurence = self.moneyManager.getpvOrderId(the_occ)
                venue_id = self.moneyManager.getpvVenueId(the_occ)
                self.cancelOccurrence(venue_id, tracking_occurence)
                
                #TOTEST : cancel the 2nd  one
                the_occ = dict_by_prices[price][order_olds[1]][1]
                tracking_occurence = self.moneyManager.getpvOrderId(the_occ)
                venue_id = self.moneyManager.getpvVenueId(the_occ)
                self.cancelOccurrence(venue_id, tracking_occurence)
                
        #now, set the correct priority and name in the Pending books depending on the price and the side
        pending_all = self.moneyManager.getPendingBook()
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
            self.log("mergeNewTrackingOrders: Setting Priority %d to order with Price : %.2f" % (self.ZonePriority.Priority[self.current_zone][first_order_priority],
                                                                                                float(pending_tracking_orders[pending_tracking_orders.keys()[0]].tactic_params["Price"])),"INFO")
        if second_order_priority != None :
            self.moneyManager.setPriority(pending_tracking_orders.keys()[1] , self.ZonePriority.Priority[self.current_zone][second_order_priority])
            self.log("mergeNewTrackingOrders: Setting Priority %d to order with Price : %.2f" % (self.ZonePriority.Priority[self.current_zone][second_order_priority],
                                                                                             float(pending_tracking_orders[pending_tracking_orders.keys()[1]].tactic_params["Price"])),"INFO")
        
        
    
    def updateOrdersPriority(self, evt):
        new_prios = {}
        PB = self.moneyManager.getPrivateBook()
        TrackingPB = dict(self.moneyManager.getPrivateBook("Tracking"))
        
        TrackingPendingBook = self.moneyManager.getPendingBook("Tracking")
        
        PendingBook = dict(self.moneyManager.getPendingBook())
        
        # Tracking
        TrackingPendingPrices =  dict((  k, x.tactic_params["Price"] )  for k,x in self.moneyManager.getPendingBook("Tracking").iteritems())
        TrackingDictPrices =  dict((  k, x["Price"] )  for k,x in TrackingPB.iteritems())
        TrackingPrices  =  [ x["Price"] for x in TrackingPB.values()]
        if TrackingDictPrices :
            TrackingDictPrices.update(TrackingPendingPrices)
            update_tracking_priority = True
        else :   
            TrackingOrders = TrackingPendingPrices
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
                self.log("WTF")
        
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
            
        best_tracking_price = min(TrackingPendingPrices) if len(TrackingPendingPrices) > 0 else None 
        
        for occ_id, priority in new_prios.iteritems():            
            if old_prios[occ_id] != priority:
                self.log("updateOrdersPriority:Change Priority of order %s from %d to %d"%(occ_id,old_prios[occ_id],priority),"INFO")
                self.moneyManager.setPriority(occ_id, priority)        
        

    def computeTrackingOcurrences(self, evt):
        # verify living volume in the order book        
        
        if (self.current_zone == self.AlgoZone.FAR_AHEAD): # AVANCE
            # cancel all orders, except would occurrences
            self.log("computeTrackingOcurrences:algo is far ahead , Cancel all existing tracking order because zone == 3")            
            # and cancel 
            self.cancelAllLivingOrders(exclude_prefix="Would")
            return
            
        
        tracking_pb = self.moneyManager.getPrivateBook("Tracking")
        
        if len(tracking_pb.keys()) > 2 :
            self.log("computeTrackingOcurrences: More than 2 tracking occurrences, PB : %s" % tracking_pb)
            self.error("computeTrackingOcurrences: More than 2 tracking occurrences, Stop")
        else :
            self.log("computeTrackingOcurrences: We have %d occurrences, Correct" % (len(tracking_pb.keys())))
        
        if self.algo_starting:
                # at the beginning, special treatment to place orders
                self.sec_id_trading_hours = self.getReferenceData("SecurityTradingHours", venue_id=evt.getVenueId())
                self.algo_starting = False
        
        # determine level of limit price
        limit_price_level_in_lob = self.getLimitPriceLevelInLOB()        
        self.log("computeTrackingOcurrences: limit_price_level_in_lob = %d" % (limit_price_level_in_lob))
        
        if limit_price_level_in_lob > 5:
            self.log('computeTrackingOcurrences: limit price is not in lob')
            # limit price is out of lob
            self.createOccurrence(tactic_type='IcebergMono',
                                 venue_id=evt.getVenueId(),
                                 prefix='TrackingOrder',
                                 tactic_params={'Price': self.limit_price,
                                               'OrderQty': self.ats},
                                 priority=self.ZonePriority.Priority[self.current_zone]['TrackingOrder1'])
            self.printOrder(self.limit_price, self.ats, 'TrackingOrder1', 'Insert', function_name="computeTrackingOcurrences")
                      
                          
        else:
            # determine level of tracking occurrences
            tracking_orders = {} 
            
            old_orders = self.moneyManager.getPrivateBook("Tracking")
            order_index = 1                
            for o in old_orders.keys() :                    
                tracking_orders['OldTrackingOrder%d' % order_index] = {old_orders[o]["Price"]:(old_orders[o]["OrderQty"], o)}
                self.log("computeTrackingOcurrences: Old Order : %s" % tracking_orders['OldTrackingOrder%d' % order_index])
                order_index += 1
                
            tracking_orders['NewTrackingOrder1'] = { self.event_market['BEST1'] :   (self.ats, "TrackingOrder1") } 
            tracking_orders['NewTrackingOrder2'] = { self.event_market['BEST5'] :   (self.ats, "TrackingOrder2") }

            if (len(old_orders.keys()) == 0):
                # no tracking order presents and we in AHEAD                
                if self.checkTrackingPlacementCondition(self.qmid, 1):
                    # condition is verified, can place orders
                    self.mergeNewTrackingOrders(evt, tracking_orders)
                    # and restart the volume timer
                    self.market_qty_between_tracking_update = 0
                    self.last_mkt_qty_placing_tracking = self.marketManager.getFeedInfo('AE', 'QTE_CNT1')
                    self.last_exec_qty_placing_tracking = self.moneyManager.getExecQty()
                else:
                    self.log('computeTrackingOcurrences: condition to place tracking orders is not verified', 'INFO')
            else:
                # if there exists order, just do a modify
                self.mergeNewTrackingOrders(evt, tracking_orders)           
               
    
    def checkTrackingPlacementCondition(self, rate, level):
        exec_qty_between_tracking_update = self.moneyManager.getExecQty() - self.last_exec_qty_placing_tracking
        real_market_qty_between_tracking_update = self.market_qty_between_tracking_update - exec_qty_between_tracking_update
        return real_market_qty_between_tracking_update >= (1- rate)/rate * self.ats - sum(filter(lambda d: self.event_market['BEST_SIZE%d'%d], [1,level]))
    
    def computeSlippageOcurrences(self, evt):
        
        # get current XFire order
        curXFire = self.moneyManager.getPrivateBook("SlippageOrder")
        #TOTEST: only one order is allowed
        
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
                    # quantity
                    self.log('computeSlippageOcurrences: delta_volumes %s' % self.delta_volumes)
                    self.log('computeSlippageOcurrences: getRemainingQty %s' % self.moneyManager.getRemainingQty())
                    self.log('computeSlippageOcurrences: ats %s' % self.ats)
                    quantity = min(max(self.ats, int(0.5 * (self.delta_volumes[0] + self.delta_volumes[1]))), self.moneyManager.getRemainingQty(), self["MaxMultATS"] * self.ats)
                    # price
                    price = self.utils.bestBetween(self.event_market['BEST_OPPOSITE1'], self.limit_price)
                    # Manage config XFire Venues here : we need also Primary + MTF + Dark + Mid , so we do not excluse everything
                    xfire_params = self.mergeXfireParamsWithConfig({'OrderQty': int(quantity),
                                                                    'Price': price,
                                                                    'ExcludeAggressiveType' : '',
                                                                    'ExcludePassiveType' : '',
                                                                    'ExcludePrimary' : 'N',
                                                                    'ParticipationRate' : 99,
                                                                    'AmountValidation' : 0})
                    self.log("computeSlippageOcurrences : Algo is very late, sending aggressive XFire order")
                    self.modifySlippageTAPOccurence(evt.getVenueId(), xfire_params, curXFire)
                else:
                    # tempo is KO
                    self.log("computeSlippageOcurrences : Algo is very late, waiting to send aggressive XFire order")
            else:
                # deltaVolume condition is KO
                self.log("computeSlippageOcurrences : Algo is very late, but not yet enough to send aggressive XFire order")
                    
        elif self.current_zone == self.AlgoZone.LATE:
            # in [Qmin, Qmid]
            # send passive XFire order to DARK only (Mid and Dark) at the mid price to go back to Qmid
            mid_price = 0.5 * (self.event_market['BEST1'] + self.event_market['BEST_OPPOSITE1'])
            quantity = min(max(self.ats, self.delta_volumes[1]), self.moneyManager.getRemainingQty())
            xfire_params = self.mergeXfireParamsWithConfig({'OrderQty': int(quantity),
                                                            'Price': mid_price,
                                                            'ExcludeAggressiveType' : 'LIT',
                                                            'ExcludePassiveType' : 'LIT',
                                                            'ExcludePrimary' : 'Y',
                                                            'ParticipationRate' : 99,
                                                            'AmountValidation' : 0})
            self.log("computeSlippageOcurrences : Algo is late (between Qmid and Qmin), sending TAP on Mid order")
            self.modifySlippageTAPOccurence(evt.getVenueId(), xfire_params, curXFire)
            
        elif self.current_zone == self.AlgoZone.AHEAD:
            # in [Qmid, Qmax]
            # cancel all order
            self.log("computeSlippageOcurrences : Algo is ahead (between Qmid and Qmax), canceling all SlippageOrders")
            self.cancelAllLivingOrders(order_prefix='SlippageOrder')
            
        elif self.current_zone == self.AlgoZone.FAR_AHEAD:
            # above Qmax
            self.log("computeSlippageOcurrences : Algo is far ahead (above Qmax), canceling all SlippageOrders")
            self.cancelAllLivingOrders(order_prefix='SlippageOrder')
        
    def updateOpportunitySignal(self, evt):
        if self.opportunity_signal != None:
            # Update expected execution time and end of period volume
            self.exp_exec_time = self.estEndOfPeriodTime(evt)
            self.exp_end_of_period_volume = self.estEndOfPeriodVolume(evt)
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
            
        if len(self.last_fwd_signal) >= 60:
            self.last_fwd_signal.popleft()
            self.last_bwd_signal.popleft()
            
        self.last_fwd_signal.append(self.fwd_signal)
        self.last_bwd_signal.append(self.bwd_signal)
        
        if self.opportunity_signal != None:
            self.mean_fwd_signal = self.opportunity_signal.getMeanFwd()
            self.mean_bwd_signal = self.opportunity_signal.getMeanBwd()
        else:
            self.mean_fwd_signal = sum(self.last_fwd_signal)/60.0 
            self.mean_bwd_signal = sum(self.last_bwd_signal)/60.0
 
        self.adjusted_fwd_signal = max((self.mean_fwd_signal - self.fwd_signal)*self.side, 0.0)/ self.mean_fwd_signal if self.mean_fwd_signal > 0 else 0
        self.adjusted_bwd_signal = max((self.mean_bwd_signal - self.bwd_signal)*self.side, 0.0)/ self.mean_bwd_signal if self.mean_bwd_signal > 0 else 0
        
                
    def computeOpportunityOcurrences(self, evt):
        '''
            Tactic "opportunity"
        '''
        
        if self['ForwardSignalEnabled'] or self['BackwardSignalEnabled']:        
            # Opportunity signal                   
            adjusted_fwd_signal = self.adjusted_fwd_signal
            adjusted_bwd_signal = self.adjusted_bwd_signal
            
            opportunity_cnt_ok = (self.next_opportunity_deal <= self.marketManager.getFeedInfo('AE', 'C1_CONVNUM'))
            if  opportunity_cnt_ok:            
                best_opposite_qty = self.event_market['BEST_OPPOSITE_SIZE1']
                bo = self.event_market['BEST_OPPOSITE1']
                max_opp_qty = min(self.delta_volumes[2], best_opposite_qty, 2*self.ats)
                min_opp_qty = self.ats
                
                if self['ForwardSignalEnabled'] and not self['BackwardSignalEnabled']:
                    opp_qty = (adjusted_fwd_signal>0)*(min_opp_qty + adjusted_fwd_signal * (max_opp_qty - min_opp_qty))
                elif self['BackwardSignalEnabled'] and not self['ForwardSignalEnabled']:
                    opp_qty = (adjusted_bwd_signal>0)*(min_opp_qty + adjusted_bwd_signal * (max_opp_qty - min_opp_qty))
                elif self['BackwardSignalEnabled'] and self['ForwardSignalEnabled']:
                    opp_qty = 0.5*((adjusted_fwd_signal>0)*(min_opp_qty + adjusted_fwd_signal * (max_opp_qty - min_opp_qty)) + 
                                   (adjusted_bwd_signal>0)*(min_opp_qty + adjusted_bwd_signal * (max_opp_qty - min_opp_qty)))            
                
                # we need to check with the limit price too , to avoid sending an order which 
                # is not consistent with the limit price
                if opp_qty > 0 and self.utils.worstBetween(bo, self.limit_price) == self.limit_price :
                    self.next_opportunity_deal = self.marketManager.getFeedInfo('AE', 'C1_CONVNUM') + self['OpportunityNbDeals']                
    
                    # FAK Opportunistic order
                    price_opp = self.utils.bestBetween(bo, self.limit_price)
                    self.log("computeOpportunityOcurrences: sending opportunity order, P = %.2f, Q = %d" % (price_opp, int(opp_qty)))            
                
                    self.createOccurrence('MarketOrder',
                        venue_id = evt.getVenueId(),
                        prefix = 'OpportunityOrder',
                        tactic_params={'OrderQty': int(opp_qty),
                                       'Price': price_opp,
                                       'OrderType': 'limit',
                                       'MarketTimeInForce': 'Fak'},
                        priority=self.ZonePriority.Priority[self.current_zone]['OpportunityOrder'])
                else:
                    self.log("computeOpportunityOcurrences: ITM, but no qty available : Max Curve : %d ,best_opposite_qty - ats : %d  !!!" % (self.delta_volumes[2], best_opposite_qty - self.ats))
            else:
                self.log("computeOpportunityOcurrences: no opportunity")
    
    
    def manageAuctionOcurrences(self):
        pass
    
    def computeWouldOcurrences(self, evt):
        # TOTEST 
        # check if we have an order of would level        
        
        if self.would_level_is_used:
            curWouldOcc = self.moneyManager.getPrivateBook('WouldOrder')
            best_opposite = self.event_market['BEST_OPPOSITE1']
            is_in_would_area = self.utils.bestBetween(self.would_level, best_opposite) == best_opposite
            
            if not curWouldOcc :
                # Would level should be used, but not orders alive
                would_price = self.would_level                                
                if not is_in_would_area:
                    #Place an order with half of the remaining quantity to the market, and a low priority
                  
                    # TOTEST : determine the would quantity
                    self.would_occ_qty = 0.5 * (self.moneyManager.getRemainingQty() - self.moneyManager.getLeavesQty())
                     
                    self.createOccurrence(tactic_type='IcebergMono',
                                         venue_id=evt.getVenueId(),
                                         prefix='WouldOrder',
                                         tactic_params={'Price': self.would_level,
                                                        'OrderQty': int(self.would_occ_qty)},
                                         priority=self.ZonePriority.Priority[self.current_zone]['WouldOrder'])
                    self.printOrder(self.would_level, int(self.would_occ_qty), 'WouldLevelIcbgORder ', 'Insert', function_name="computeWouldOcurrences")
                      
                          
                else :  
                    #Send a FAK ORder with the remaining Qty
                    would_qty = self.moneyManager.getRemainingQty()
                    self.createOccurrence('MarketOrder',
                                          venue_id=evt.getVenueId(),
                                          prefix='WouldOrder',
                                          tactic_params={'OrderQty': int(would_qty),
                                                           'Price': would_price,
                                                           'OrderType': 'limit',
                                                           'MarketTimeInForce': 'Fak'},
                                                           priority=self.ZonePriority.Priority[self.current_zone]['WouldFAKOrder'])
                    self.printOrder(would_price, int(would_qty), 'WouldLevelFAKORder ', 'Insert', function_name="computeWouldOcurrences")
            
        else:
            # would is not used, we have to cancel any eTrackingxisting would order
            # find all 
            curWouldOcc = self.moneyManager.getPrivateBook('WouldOrder')
            # and cancel 
            for occ in curWouldOcc.keys():
                venue_id = curWouldOcc[occ]["VenueId"]
                self.cancelOccurrence(venue_id, occ)
    
    def computeEndOfLifeOccurrences(self, evt):
        remaining = self.moneyManager.getRemainingQty()
        quantity =min(self.ats, remaining)
        
        # price
        price = self.utils.bestBetween(self.event_market['BEST_OPPOSITE1'], self.limit_price)
        xfire_params = self.mergeXfireParamsWithConfig({'OrderQty': int(quantity),
                                                        'Price': price,
                                                        'ExcludeAggressiveType' : '',
                                                        'ExcludePassiveType' : '',
                                                        'ExcludePrimary' : 'N',
                                                        'ParticipationRate' : 99,
                                                        'AmountValidation' : 0})
        self.cancelAllLivingOrders()
        self.createOccurrence(tactic_type='TAP',
                                  venue_id=evt.getVenueId(),
                                  prefix='EnfOfLifeOrder',
                                  tactic_params=xfire_params,
                                  priority=-10)
                    
        self.log("computeEndOfLifeOccurrences:Entering end of life","WARNING")
        
    
    def manageOccurrences(self, marketConditions, evt):
        
        # manage occurrences during continuous trading
        
        
        #we need to determine our position by respect to the benchmark (zone)
        
        
        #process end of life orders 
        if self.moneyManager.getRemainingQty() <= self.ats:
            self.computeEndOfLifeOccurrences(evt)
        
        #process tracking orders 
        self.computeTrackingOcurrences(evt)
        
        #process slippage order
        self.computeSlippageOcurrences(evt)
        
        #process opportunity  order
        self.computeOpportunityOcurrences(evt)
        
        #process would order
        self.computeWouldOcurrences(evt)
        
        self.updateOrdersPriority(evt)
        
        pass
        
        
    def getMarketState(self):
        if self.event_market.has_key('CONDCODE_2'):
            trading_state = self.event_market[ 'CONDCODE_2']
        else:
            if "waiting" in self.marketManager.getFeedInfo('AE', 'GN_TXT32_2'):
                print "Using AE(Condcode_1, AE still not active)  for market state"
                trading_state = "WAIT"
                return trading_state 
            
    def getAverageTradingSize(self, venue_id):
        
        static_ats = self.marketManager.getVenue(venue_id)['ats']
        
        if static_ats > 0 :
            ats = static_ats
        else :  
            ats = self.marketManager.getFeedInfo('AE', 'QTE_CNT1') / self.marketManager.getFeedInfo('AE', 'C1_CONVNUM') if self.marketManager.getFeedInfo('AE', 'C1_CONVNUM') > 0 else 100
        
        self.log("getAverageTradingSize : static_ats : ats is  %d" % ats)
        return ats
        
         
    def process(self, evt):
        ''' Function to be called for each update on the market 
            @return: 
                - compute thresholds for delta volume --> the current zone
                - depending on current zone --> decide to launch (tracking and/or slippage) tactic
        '''

        self.event_market = self.marketManager.getFeedInfo(evt.getVenueId())
        self.rateAdjustment(evt)
        self.log("process:Private Book -> %s " % self.moneyManager.getPrivateBook())
        self.log("process:Tracking Private Book -> %s " % self.moneyManager.getPrivateBook("TrackingOrder"))
        # what is the current state (condcode_2)
        self.trading_state = self.getMarketState()
        
        self.ats = self.getAverageTradingSize(evt.getVenueId())
        
        # from this  current state, what should we do ?         
        marketConditions = self.validateMarketConditions(self.trading_state)       
        
        if marketConditions == self.MarketState.INVALID:
            self.error("Process : Stopping because of incorrect Market Conditions")
        elif marketConditions == self.MarketState.WAIT:
            self.log("Process : Waiting for correct market conditions")
            pass
        elif marketConditions == self.MarketState.CLOSED:
            self.error("Process : Stopping, Market is closed")

        self.delta_volumes = self.computeVolumeThreshold(evt)
        
        self.current_zone = self.currentZone()
        
        self.market_qty_between_tracking_update += self.marketManager.getFeedInfo('AE', 'QTE_CNT1') - self.last_mkt_qty_placing_tracking 
        #if we are here in this case, we are in the correct market conditions
        if self.is_in_auction :
            self.manageAuctionOcurrences()
        else :
            self.manageOccurrences(marketConditions, evt)            

        if self.first_process:
            self.begin_time = evt.timestamp
            self.first_process = False        
            
            
    def processReport(self, evt):
        
        #update qty for each zone
        for x in self.moneyManager.getLastExecutions():
            self.total_amount += int(x["Quantity"]) * float(x["Price"])
            self.log("processReport:Order Filled: ID : %s  Qty=%d Price=%f" % (x["OrderId"] , int(x['Quantity']), float(x['Price'])), log_level="INFO")
            if 'TrackingOrder' in x["OrderId"] :
                self.tracking_volume += int(x["Quantity"])
                self.tracking_amount += int(x["Quantity"]) * float(x["Price"])
            elif 'SlippageOrder' in x["OrderId"] : 
                self.slippage_volume += int(x["Quantity"])
                self.slippage_amount += int(x["Quantity"]) * float(x["Price"])
            elif "Would" in x["OrderId"] :
                self.total_would_qty += int(x["Quantity"])
                self.would_amount += int(x["Quantity"]) * float(x["Price"])
            elif "Opportunity" in x["OrderId"] :
                self.opportunity_volume += int(x["Quantity"])
                self.opportunity_amount += int(x["Quantity"]) * float(x["Price"])
            elif "Auction" in x["OrderId"] :
                self.rate_is_adjusted = False
                self.executed_in_auction += int(x["Quantity"])
                self.last_auction_qty = self.executed_in_auction
                self.log("Auction exec : %d" % int(x["Quantity"]))
        
        exec_qty = self.moneyManager.getExecQty()
        mkt_qty = self.marketManager.getFeedInfo('AE', 'QTE_CNT1')
            
        self.last_mkt_qty = self.mkt_qty
        self.mkt_qty = mkt_qty

        plr_no_would = 0 if mkt_qty == 0 else  100.0 * (exec_qty - self.total_would_qty) / mkt_qty
        plr_with_would = 0 if mkt_qty == 0 else  100.0 * (exec_qty) / mkt_qty
        plr_since_last_modify = 0 if mkt_qty == 0 else  100.0 * (exec_qty - self.delta_modify_qty) / mkt_qty
        
        self.log ("[Process Report] ExecQty = %d , Market Qty = %d , PLR (without would qty) = %.2f, PLR (with would qty) = %.2f, PLR since last Modify : %d" % (exec_qty, mkt_qty, plr_no_would, plr_with_would, plr_since_last_modify),"INFO")
        self.delta_volumes = self.computeVolumeThreshold(evt)
        
        self.updateOpportunitySignal(evt)
        self.updateIndicators(evt)
        
    def acceptSubscription(self, sub_id, sub):
        sub.mode = 0   
        return sub_id == self.primary_venue_id

    def acceptVenue(self, venue_id, venue):
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

        ''' Returns any extra subscriptions needed. 
            @return
                Dictionary: {name: (Source, Ric, ReqdProcess)}
        '''
        
        price_string = ''
        
        # Gestion du Limit Prices

        
        if self["Price"]:
            print "The limit price is : %s" % self["Price"]
            price_string = ";GE=%s" % (self["Price"])  if self.side == -1 else ";LE=%s" % (self["Price"])
        else:
            print "no limit price"
        
        if self["AdjustedPriceInd"]:
            price_string += ";LE=%s" % (self["AdjustedPriceInd"])  if self.side == -1 else ";GE=%s" % (self["AdjustedPriceInd"])
            print "Using Would Level : %s" % (self["AdjustedPriceInd"])
        else:
            print "no would level"
            
        
        if self.is_mono_destination: 
            td_id_string = self.trading_destination
            print "Using PVOL on only one td "
        else :
            td_id_string = 'all'
            print "Using PVOL on all td "
        
        # Manage Auction PArticipation    
        #        ParticipateInOpeningAuction = 1
        #        ParticipateInClosingAuction = 1
        #        ParticipateInIntradayAuction = 1
        
        auction_inclusion_string = ''
        
#        if  self.participate_in_opening:
#            auction_inclusion_string = 'IN=oau;'
#            
#        if  self.participate_in_closing:
#            auction_inclusion_string += 'IN=cau;'
#            
#        if  self.participate_in_intraday:
#            auction_inclusion_string += 'IN=iau;'
#            
#        if  self.participate_in_mid_auction:
#            auction_inclusion_string += 'IN=iau;'
#            
#            
# we exclude all auctions from the AE subscription
        auction_exclusion_string = 'EX = auc;'
        
        
        print 'Suscribing to AE, S=%d@%s;P=bEHRC;Z=UTC;H=4;F=%s;T=%s;EX=top;EX=cross;%s%s%s;PL=os;' % (self['InstrumentID'], td_id_string, self['EffectiveTime'][9:], self['ExpireTime'][9:], auction_inclusion_string, auction_exclusion_string, price_string) 
        print 'VWAP_SOURCE is : %s' % (self['vwapSource'])
        
        return {'AE':(self['vwapSource'], 'S=%d@%s;P=bEHRC;Z=UTC;H=4;F=%s;T=%s;EX=top;EX=cross;%s%s%s' % (self['InstrumentID'], self.trading_destination, self['EffectiveTime'][9:], self['ExpireTime'][9:], auction_inclusion_string, auction_exclusion_string, price_string))}
    
    
    def getCurrentSequenceQuantity(self):
        return self.moneyManager.getExecQty() - self.delta_modify_qty
    
    def computeVolumeThreshold(self, evt):
        
        ''' Compute the 3 thresholds for the delta volume
            @return:
                List: (Q_min(t), Q_max(t), Q_opt(t)) - Q(t)
        '''
        
        # adjust executed qty by respect to modify received
        # exclude Would Qty,as the would is not in the benchmark, and exclude also the auction volume
        
        
        # self.delta_auction  is the total aucuction qty missed during the life of the algo included in the benchmark
        
        # compute what we have executed already 
        # we must remove the delta_auction, as our bench is on continuous trading, and also the possible modify_qty , 
        # and the would exec qty, not in the benchmark
        
        
        excVolume = self.moneyManager.getExecQty() - self.delta_auction - self.total_would_qty
        
        # get the current market volume up to time t
        mktVolume = self.marketManager.getFeedInfo('AE', 'QTE_CNT1') + self.total_continuous_market_qty
        
        minDeltaVolume = (self.qmin * mktVolume - excVolume + self.qmin * self.estimateVolume()) / (1.0 - self.qmin)
        maxDeltaVolume = (self.qmax * mktVolume - excVolume + self.qmax * self.estimateVolume()) / (1.0 - self.qmax)
        
        # using predicted volume for the next period
        optDeltaVolume = (self.qmid * mktVolume - excVolume + self.qmid * self.estimateVolume()) / (1.0 - self.qmid)
        self.width_enveloppe = (maxDeltaVolume - minDeltaVolume) / self.ats if self.ats > 0 else 0 
        return [minDeltaVolume, optDeltaVolume, maxDeltaVolume]
    
        
    def rateAdjustment(self, evt):
        ''' Adjust the participation rate w.r.t. benchmark (EVP, Strict, PVOL)   
            @return:
                adjusted rate            
        '''
           
        if not self.rate_is_adjusted :
            if ((self['benchmark'] == 'SVT') or (self['benchmark'] == 'PVOL')):
                self.qmax = self['ParticipationRate'] * 0.01
                if self['ParticipationRate'] == self['MinPctVolume']:
                    self.log("Min pct and max pct are equal, something wrong here")
                    self.qmin = self['ParticipationRate'] * 0.9
                else:
                    self.qmin = self['MinPctVolume'] * 0.01
                    
                
            elif (self['benchmark'] == 'EVP'):
                qmid = 0.01 * (self['ParticipationRate'] + (self.marketManager.getFeedInfo(evt.getVenueId(), 'TIME_NUM') - self.beginTime) * self.coeffRate)
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
                the current zone:
                3: far ahead
                2: ahead
                1: late
                0: very late 
        '''
        curZone = sum([d < 0 for d in self.delta_volumes])
        return curZone



    def printOrder(self, order_price, order_qty, order_type_str, action, function_name=None):        
        if function_name :
            self.log('%s : Action :  %s  Order of type %s, P1 = %.4f,Q1 = %.2f' % (function_name, action, order_type_str, order_price , order_qty))
        else :
            self.log('Action :  %s  Order of type %s, P1 = %.4f,Q1 = %.2f' % (action, order_type_str, order_price , order_qty))
        
                    
    def mergeXfireParamsWithConfig(self, xfire_params):
        
        occurence_params = xfire_params
        self.log("TAP PARAMS before MERGE: %s" % occurence_params)
        self.log("self['ExcludeAggressiveType']: %s" % self['ExcludeAggressiveType'])
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
        
        if  self['ExcludePrimary'] == 1  or xfire_params['ExcludePrimary'] == 1 :
            occurence_params['ExcludePrimary'] = 1 
        else:
            occurence_params['ExcludePrimary'] = 0 
        
        # keep configration parameters
        occurence_params['ExcludeAggressiveVenue'] = self['ExcludeAggressiveVenue']
        occurence_params['ExcludePassiveVenue'] = self['ExcludePassiveVenue'] 
        
        if 'MID' not in occurence_params['ExcludePassiveType'] or  'MID' not in occurence_params['ExcludeAggressiveType']:
            self.error('Invalid configuration for a xfire')
         
        occurence_params['MIFIDBestExecutionReqd'] = 1
        
        self.log("TAP PARAMS : %s" % occurence_params)
    
        return occurence_params
    
    
                  
    def modifySlippageTAPOccurence(self, market_id, new_xfire_params, curXFire):
        
        if len(curXFire) > 1:
            # more than 1 SlippageOrder => cancel all except the one at the best price
            # TODO : be smarter, and jsut cancel the worst occurences
            self.cancelAllLivingOrders()
            curXFire = None
            
        if not curXFire:
            self.createOccurrence(tactic_type='TAP',
                                  venue_id=market_id,
                                  prefix='SlippageOrder',
                                  tactic_params=new_xfire_params,
                                  priority=self.ZonePriority.Priority[self.current_zone]['SlippageOrder'])
            
            self.printOrder(new_xfire_params["Price"], new_xfire_params["OrderQty"], 'SlippageOrder', 'Insert')
            
        elif curXFire[curXFire.keys()[0]].has_key('ExcludePrimary'):
            if curXFire[curXFire.keys()[0]]['ExcludePrimary'] != new_xfire_params['ExcludePrimary'] or curXFire[curXFire.keys()[0]].has_key('ParticipationRate') :
                #check if it is a "mid" order or "aggressive" one
                # we need a cancel & replace
                self.cancelOccurrence(market_id, curXFire.keys()[0])
                self.createOccurrence(tactic_type='TAP',
                                              venue_id=market_id,
                                              prefix='ModerateTrackingTAP',
                                              tactic_params=new_xfire_params,
                                              priority=self.ZonePriority.Priority[self.current_zone]['SlippageOrder'])
                self.printOrder(new_xfire_params["Price"], new_xfire_params["OrderQty"], 'SlippageTAP', 'Cancel&Replace')
        else:
            # we don't need a cancel & replace
            self.modifyOccurrence(tactic_type='TAP',
                                              venue_id=market_id,
                                              oc_id=curXFire.keys()[0],
                                              tactic_params=new_xfire_params,
                                              priority=self.ZonePriority.Priority[self.current_zone]['SlippageOrder'])
            self.printOrder(new_xfire_params["Price"], new_xfire_params["OrderQty"], 'SlippageTAP', 'Modify')
    
    def updateIndicators(self, evt):    
         
#        Perf_VWAP = self.moneyManager.getStatistics()  if isinstance(self.moneyManager.getStatistics(), float) else  self.moneyManager.getStatistics()["vwap"]
         
#        Market_VWAP = self.marketManager.getFeedInfo('AE', 'WTD_AVE1') if isinstance(self.marketManager.getFeedInfo('AE', 'WTD_AVE1'), float) else 0
        
        Market_VWAP = self.marketManager.getFeedInfo('AE', 'WTD_AVE1')
        
        exec_qty = self.moneyManager.getExecQty()
        total_qty = self.moneyManager.getTotalQty()
        mkt_qty = self.marketManager.getFeedInfo('AE', 'QTE_CNT1')
        sum_exec_qty = self.slippage_volume + self.tracking_volume + self.opportunity_volume + self.total_would_qty
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
            tracking_price = self.utils.worstBetween(float(occ["Price"]), tracking_price) 
        
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
                               'PnL'                : self.side * (Market_VWAP - Perf_VWAP) * exec_qty ,
                               'PnLBp'              : self.side * 10000.0 * (Market_VWAP - Perf_VWAP) / Market_VWAP if  Market_VWAP * Perf_VWAP != 0 else 0,
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
                               'ForwardOppSignalStd'   : 100.0 * self.opportunity_signal.getStdFwd() if self.opportunity_signal != None else std(self.last_fwd_signal),
                               'BackwardOppSignalStd'  : 100.0 * self.opportunity_signal.getStdBwd() if self.opportunity_signal != None else std(self.last_bwd_signal),
                               'ForwardOppSignalMean'  : 100.0 * self.opportunity_signal.getMeanFwd() if self.opportunity_signal != None else mean(self.last_fwd_signal),
                               'BackwardOppSignalMean' : 100.0 * self.opportunity_signal.getMeanBwd() if self.opportunity_signal != None else mean(self.last_bwd_signal),
                               'ForwardOppSignalRange'  : 100.0 * self.opportunity_signal.getRangeFwd() if self.opportunity_signal != None else ptp(self.last_fwd_signal),
                               'BackwardOppSignalRange' : 100.0 * self.opportunity_signal.getRangeBwd() if self.opportunity_signal != None else ptp(self.last_bwd_signal),
                               'MarketTrend'        : self.marketManager.getFeedInfo('AE','trend_s_600') if self.marketManager.getFeedInfo('AE','trend_s_600') else None,
                               'GKVol10min'         : self.marketManager.getFeedInfo('AE','garman_klass_bp_s_600') if self.marketManager.getFeedInfo('AE','garman_klass_bp_s_600') else None,
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
                               'AgressiveDelta'         : self.delta_agressive_qty,
                               'tactic_id'              : self['tactic_id']
                               }, date = evt.timestamp)
            
    def estEndOfPeriodTime(self, evt):
        return self.moneyManager.getTotalQty()/self.moneyManager.getExecQty() * (evt.timestamp - self.begin_time) if self.moneyManager.getExecQty() > 0 else 0 

    def estEndOfPeriodVolume(self, evt):
        cond_ok = self.marketManager.getFeedInfo('AE').has_key('QTE_CNT1') and (evt.timestamp - self.begin_time) > 0
        return self.marketManager.getFeedInfo('AE', 'QTE_CNT1') * self.exp_exec_time / (evt.timestamp - self.begin_time) if cond_ok else 0
    