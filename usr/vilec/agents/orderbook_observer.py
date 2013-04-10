from simep.bin.simepcore import EventTypes, SideTypes, Order
from simep.core.basetactic import BaseTactic
import time
from simep.tools import *
from simep.funcs.data.pyData import pyData
from usr.dev.vilec.funcs.orderbook_calculator import AlternationCalculator,QuadraticCalculator, get_max_value_key_in_dict
import copy
from usr.dev.vilec.profiling.profilehooks import profile, profile
import time


def print_dict_difference(dict1, dict2):
        diffkeys = [k for k in dict1 if dict1[k] != dict2[k]]
        for k in diffkeys:
          print k, ':', dict1[k], '->', dict2[k]
    


class OrderBookObserver(BaseTactic):
    @staticmethod
    def public_parameters():
        return {'parameters': {'slice_duration' : {'label' : 'Slice Duration (in s)', 'value' : 30},
                               'round_trip_qty' : {'label' : 'round_trip_qunatities', 'value' : (1, 3, 5,  8,10 ,15, 20)},
                               'dummy_parameter3' : {'label' : 'Dummy Parameter 3', 'value' : 'my_string'}}}
    
    @staticmethod
    def indicators_list():
        return []
    
    def build_last_image(self,venue_market_feed,venue_id):
       previous_mkt_feed = {'BEST_BID': copy.deepcopy(venue_market_feed['PREV_BEST_BID']),'BEST_ASK': copy.deepcopy(venue_market_feed['PREV_BEST_ASK']),
                            'BEST_BSIZ': copy.deepcopy(venue_market_feed['PREV_BEST_BSIZ']),'BEST_ASIZ': copy.deepcopy(venue_market_feed['PREV_BEST_ASIZ']),
                            'MID_PRICE': 0.5*(venue_market_feed['PREV_BEST_BID'][0] + venue_market_feed['PREV_BEST_ASK'][0]),
                            'TRADES':self.marketManager.getTrades(venue_id)}
       return previous_mkt_feed
   
    def update_the_feed(self, venue_id):
        self.ba['feed'][venue_id].update({'TRADES_VOLUMES':self.marketManager.getTotalTradesVolume(venue_id),
                                          'TRADES_TURNOVER':self.marketManager.getTotalTradesTurnover(venue_id),
                                          'TRADES_NB_MOVES':self.marketManager.getNbTrades(venue_id)})
        # set these trades in the 
        if self.is_multi :
            self.ba['feed']["all"].update({'TRADES_VOLUMES':self.marketManager.getTotalTradesVolume("all"),
                                          'TRADES_TURNOVER':self.marketManager.getTotalTradesTurnover("all"),
                                          'TRADES_NB_MOVES':self.marketManager.getNbTrades("all")})
   
    def smart_feed_copy(self,venue_market_feed,venue_id):
        previous_mkt_feed = {}
        if venue_market_feed != None :
            # copy simple types
            previous_mkt_feed = dict(venue_market_feed)
            #change list !
            previous_mkt_feed.update({'BEST_BID': copy.deepcopy(venue_market_feed['BEST_BID']),'BEST_ASK': copy.deepcopy(venue_market_feed['BEST_ASK']),
                                 'BEST_BSIZ': copy.deepcopy(venue_market_feed['BEST_BSIZ']),'BEST_ASIZ': copy.deepcopy(venue_market_feed['BEST_ASIZ']),
#                                 'LIT_DEALS_PRICES' : copy.deepcopy(venue_market_feed['LIT_DEALS_PRICES']),
#                                 'LIT_DEALS_QTYS' : copy.deepcopy(venue_market_feed['LIT_DEALS_QTYS']),
#                                 'LIT_DEALS_SIDES' : copy.deepcopy(venue_market_feed['LIT_DEALS_SIDES']),
#                                 'LIT_DEALS_TYPES' : copy.deepcopy(venue_market_feed['LIT_DEALS_TYPES']),
#                                 'LIT_DEALS_TIMES' : copy.deepcopy(venue_market_feed['LIT_DEALS_TIMES']),
#                                 'LIT_TRADE_EVENT' : copy.deepcopy(venue_market_feed['LIT_TRADE_EVENT']),
#                                 'LIT_LAST_DEALS_IDXS' : copy.deepcopy(venue_market_feed['LIT_LAST_DEALS_IDXS']),
                                 'MID_PRICE': venue_market_feed['MID_PRICE']})
#            if update_trade_info :
#                previous_mkt_feed.update({'TRADES_VOLUMES':self.marketManager.getTotalTradesVolume(venue_id),'TRDPRC_1':venue_market_feed['TRDPRC_1'],
#                                          'TRADES_TURNOVER':self.marketManager.getTotalTradesTurnover(venue_id),
#                                          'TRADES_NB_MOVES':self.marketManager.getNbTrades(venue_id)})
#            else:
#                previous_mkt_feed.update({'TRADES_VOLUMES':0,
#                                          'TRADES_TURNOVER':0,
#                                          'TRADES_NB_MOVES':0})
                
            return previous_mkt_feed
        else:
            return None
               
        
       
    def test_get_alternation_value(self, old_value, new_value):
        if old_value == new_value:
            return 0 # no move
        elif old_value < new_value:
            return 1 #move up
        else: 
            return -1 #move down  
# constructor
    def __init__(self, setup, context, params, trace):
        """
        First, we have to call the base class constructor, i.e. BaseAgent
        """
        BaseTactic.__init__(self, setup, context, params, trace)
        """
        Then, we have to set two properties of the agent :
        -> self.needExecReportEvt permits to enable/disable the callback to the function 'processReport'
        -> self.reactOnEvent      permits to call the 'process' callback function every time there is an update
                                  on the market. If you set it to 'False', then you have to control the parameter
                                  self.time2wakeup to have your agent waked up with the process function during 
                                  the day. 
        self.securityIdEventsOnly permits to make sure that the process function will be called only with market 
                                  events of the same security_id as our agent
                                  
        """
        self.needExecReportEvt    = True
        self.reactOnEvent         = True
        self.securityIdEventsOnly = True
        # retrive all the trading id, plus the aggregated venue ("all")
        self.venue_ids            = context["trading_venue_ids"]
        self.ob_venues  = context["venue_ids"]
        if len(context["trading_venue_ids"]) > 1 and 'all' not in self.venue_ids:
            self.venue_ids.append("all")
        
        self.ats  = None
        self.current_mid =  None
        self.round_trip_qty = params["round_trip_qty"]
        self.tick_sizes = context  
        #self.current_indicators = {'Volume' : 0, 'TurnOver':0,"BidQuantity" : 0, "AskQuantity" : 0}
        self.current_indicators = {}
        self.last_move_mid = {}
        self.last_move_last = {}
        self.last_move_bid = {}
        self.last_move_ask = {}
        self.time_at_best_ask = {}
        self.time_at_best_bid = {}
        self.best_ask = {}
        self.best_bid = {}
        self.last_alternator = {}
        self.mid_alternator = {}
        self.bid_alternator = {}
        self.ask_alternator = {}
        self.total_deal_volume = 0
        self.total_nb_deal = 0
        self.mid_quadratic_variator = {}
        self.last_quadratic_variator = {}
        self.spread_quadratic_variator = {}
        
        self.times_sum = {}
        self.min_tick = {}
        self.is_starting = True
        self.slice_duration = date2num("00:00:%s"%self['slice_duration'])
        
        self.aggregated_market_feed = None
        self.slice_start = None
        self.slice_end = None
        self.last_time_stamp = {}
        self.last_image = {}
        self.mid_tick = {}
        self.is_multi =len(self.venue_ids) > 1  
        self.previous_best_venue_ebbo = None 
        self.previous_best_venue_ebbo_qty = None
        self.trading_types = list()
        
        self.nb_update = 0
        self.previous_previous_image = {}
        self.bid_round_trip = {}
        self.ask_round_trip = {}
        self.round_trip_dict = {}
        
        for v in self.venue_ids:
            if v in self.ob_venues:
                self.tick_sizes[v] = self.ba['feed'][self.ob_venues[v]]['tick_sizes'] 
                self.min_tick[v] = min(self.tick_sizes[v])
            
            self.bid_round_trip[v] = {}
            self.ask_round_trip[v] = {}
            # init round trip structure
            for nb_ats in self.round_trip_qty:
                self.round_trip_dict["bid_round_trip_%d_ats"%nb_ats] = 0
                self.round_trip_dict["ask_round_trip_%d_ats"%nb_ats] = 0
                
                #self.ask_round_trip[v] = {1:None, 3 : None, 5 : None, 8 : None, 10 : None, 15 : None, 20 : None}
            self.init_slice_indicators(v)
            self.last_time_stamp[v] = None
            self.previous_previous_image[v] = None
            self.last_image[v] = None
            self.last_move_mid[v]   = None
            self.last_move_last[v]  = None
            self.last_move_bid[v]   = None
            self.last_move_ask[v]   = None
            # time at best limit indicators
            self.time_at_best_ask[v] = 0
            self.time_at_best_bid[v] = 0
            self.best_ask[v] = None
            self.best_bid[v] = None
            
            self.last_alternator[v] = AlternationCalculator(None, 0, 0)
            self.mid_alternator[v] =  AlternationCalculator(None, 0, 0)
            self.bid_alternator[v] =  AlternationCalculator(None, 0, 0)
            self.ask_alternator[v] =  AlternationCalculator(None, 0, 0)
            self.mid_quadratic_variator[v] = QuadraticCalculator(None)
            self.last_quadratic_variator[v] = QuadraticCalculator(None)
            self.spread_quadratic_variator[v] = QuadraticCalculator(None)
            self.mid_tick[v] = None
            
            
        self.in_min_spread = False
        self.in_max_spread = False
        self.spread_is_crossed = False
        self.current_spread = None 
        self.previous_spread = None
        
    def compute_vwap_price_for_qty(self,at_bid,market_feed, qty):
        
        prefix_q =  'BEST_BSIZ' if at_bid else 'BEST_ASIZ'
        prefix_p =  'BEST_BID' if at_bid else 'BEST_ASK'
        q_volume = 0
        q_turnover = 0
        q_remaining = qty
        limit = 1
        nb_limit = len(market_feed[prefix_q])
        
        while q_remaining >  0 and limit < nb_limit :
            qlimit = min( market_feed[prefix_q][limit], q_remaining)
            
            
            if qlimit == 0:
                # no more limit ....
                break
                
            q_remaining -= qlimit
            q_volume += qlimit 
            if market_feed[prefix_p][limit] != None :
                q_turnover +=  qlimit * market_feed[prefix_p][limit]
            else:
               break
            limit += 1
        
        if q_remaining == 0 :
            return  q_turnover / q_volume
        else:
            return None
     
        
    def compute_round_trips(self,market_feed, venue_id, dt):
        if self.ats != None and self.current_mid != None :
            for nb_ats in self.round_trip_qty :
                round_trip_price = self.compute_vwap_price_for_qty(True,market_feed,nb_ats * self.ats )
                if round_trip_price != None :
                    self.bid_round_trip[venue_id][nb_ats]["Slippage"] += (round_trip_price - self.current_mid)* dt
                    self.bid_round_trip[venue_id][nb_ats]["Time"] += dt
                
                
                round_trip_price = self.compute_vwap_price_for_qty(False,market_feed,nb_ats * self.ats )
                if round_trip_price != None :
                    self.ask_round_trip[venue_id][nb_ats]["Slippage"] += (round_trip_price - self.current_mid)* dt
                    self.ask_round_trip[venue_id][nb_ats]["Time"] += dt
             
              
    
    def get_ticksize(self,price, venue_id):
        i = 0
        while i+1 < len(self.tick_sizes[venue_id]) and self.tick_sizes[venue_id][i+1][0] < price:
            i += 1
        return self.tick_sizes[venue_id][i][1]
        
        
    def init_slice_indicators(self, venue_id):
        # all the inbdicators must be defined in this function, whi their starting value for each slice
        self.current_indicators[venue_id] ={'VenueId' : str(venue_id),'Volume' : 0, 'TurnOver':0,"BidQuantity_time" : 0, "AskQuantity_time" : 0,
                                    "NbUpdates" : 0, 'BidQuantity_trades':0, 'AskQuantity_trades':0,
                                    "NbContinuation_last" : 0,"NbAlternation_last" : 0,"NbContinuation_mid" : 0,"NbAlternation_mid" : 0,
                                    "NbContinuation_bid" : 0,"NbAlternation_bid" : 0,"NbDifferentSpreads" : 0,
                                    "NbEmpty_bid" : 0, "NbEmpty_ask" : 0, "NbCrossingSpread" : 0, "CrossingSpreadDuration": 0,
                                    "Spread_time" : 0, "Spread_trades" : 0, 'NbIncreasingSpread': 0, 'NbDecreasingSpread': 0,
                                    "MinSpread" : 0, "MaxSpread" : 0,"MinSpreadDuration" : 0,"MaxSpreadDuration" : 0,
                                    "TotalBidQty" : 0, "TotalAskQty" : 0,"NbOrders":0,
                                    "QuadraticVariation_last" : 0,"QuadraticVariation_mid" : 0,"QuadraticVariation_spread" : 0,
                                    "Mid_Tick_t" : 0, "Time_min_tick" : 0,
                                    "Discover_total_qty" : 0,"Discover_events" : 0, "Discover_mean_qty" : 0,
                                    "pct_time_ebbo" : 0, "pct_time_ebbo_max_qty" : 0, "time_best_bid_pct" : 0, "time_best_ask_pct" : 0}
        
        self.current_indicators[venue_id].update(self.round_trip_dict)
        
        for nb_ats in self.round_trip_qty:
            self.bid_round_trip[venue_id][nb_ats] = {}
            self.ask_round_trip[venue_id][nb_ats] = {}
            self.bid_round_trip[venue_id][nb_ats]["Slippage"]  = 0
            self.bid_round_trip[venue_id][nb_ats]["Time"] = 0
            self.ask_round_trip[venue_id][nb_ats]["Slippage"]  = 0
            self.ask_round_trip[venue_id][nb_ats]["Time"] = 0 
                
        self.times_sum[venue_id] = 0
        
        self.time_at_best_ask[venue_id] = 0
        self.time_at_best_bid[venue_id] = 0
        self.best_ask[venue_id] = None
        self.best_bid[venue_id] = None
            
        
    def normalize_indicators(self, venue_id):
         total_volume = self.current_indicators[venue_id]["Volume"]    
         if self.times_sum[venue_id] > 0 :
             if venue_id != "all":
                 self.current_indicators[venue_id]["Mid_Tick_t"] = self.mid_tick[venue_id] / self.times_sum[venue_id]
             else :
                 self.current_indicators[venue_id]["Mid_Tick_t"] = self.mid_tick[venue_id] / self.times_sum[venue_id] if self.mid_tick[venue_id] != None else 0
             #for nb_ats in self.bid_round_trip[venue_id]:
             #    self.current_indicators[venue_id]["bid_round_trip"] = self.bid_round_trip[venue_id]
             self.current_indicators[venue_id]["BidQuantity_time"] = self.current_indicators[venue_id]["BidQuantity_time"]  /self.times_sum[venue_id]
             self.current_indicators[venue_id]["AskQuantity_time"] =   self.current_indicators[venue_id]["AskQuantity_time"]/self.times_sum[venue_id]
             self.current_indicators[venue_id]["Spread_time"] = self.current_indicators[venue_id]["Spread_time"]/self.times_sum[venue_id]
             ## normalization of quadratic variations   
             self.current_indicators[venue_id]["QuadraticVariation_last"] =  self.last_quadratic_variator[venue_id].get_current_quadratic_variation()
             self.current_indicators[venue_id]["QuadraticVariation_mid"] = self.mid_quadratic_variator[venue_id].get_current_quadratic_variation()
             self.current_indicators[venue_id]["QuadraticVariation_spread"] = self.spread_quadratic_variator[venue_id].get_current_quadratic_variation()
             for nb_ats in self.round_trip_qty:
                 self.current_indicators[venue_id]["bid_round_trip_%d_ats"%nb_ats] = self.bid_round_trip[venue_id][nb_ats]["Slippage"] /  self.bid_round_trip[venue_id][nb_ats]["Time"] if self.bid_round_trip[venue_id][nb_ats]["Time"] != 0 else None 
                 self.current_indicators[venue_id]["ask_round_trip_%d_ats"%nb_ats] = self.ask_round_trip[venue_id][nb_ats]["Slippage"] /  self.ask_round_trip[venue_id][nb_ats]["Time"] if self.ask_round_trip[venue_id][nb_ats]["Time"] != 0 else None
                 
         else :
             for nb_ats in self.round_trip_qty:
                 self.current_indicators[venue_id]["bid_round_trip_%d_ats"%nb_ats] = 0
                 self.current_indicators[venue_id]["ask_round_trip_%d_ats"%nb_ats] = 0
             self.current_indicators[venue_id]["BidQuantity_time"] = 0
             self.current_indicators[venue_id]["AskQuantity_time"] =   0
             self.current_indicators[venue_id]["Spread_time"] = 0
             
         self.current_indicators[venue_id]["BidQuantity_trades"] =  self.current_indicators[venue_id]["BidQuantity_trades"]  /total_volume if total_volume > 0 else None
         self.current_indicators[venue_id]["AskQuantity_trades"] =   self.current_indicators[venue_id]["AskQuantity_trades"]/total_volume if total_volume > 0 else None
         self.current_indicators[venue_id]["Spread_trades"] = self.current_indicators[venue_id]["Spread_trades"] /total_volume if total_volume > 0 else None
         self.current_indicators[venue_id]["NbOrders"] = self.current_indicators[venue_id]["NbOrders"]  / total_volume if total_volume > 0 else None
         self.current_indicators[venue_id]["time_best_bid_pct"] = 100.0 * self.time_at_best_bid[venue_id]/float(self.slice_duration)
         self.current_indicators[venue_id]["time_best_ask_pct"] = 100.0 * self.time_at_best_ask[venue_id]/float(self.slice_duration)
         if self.current_indicators[venue_id]["Discover_events" ] >  0:
            self.current_indicators[venue_id]["Discover_mean_qty"] = self.current_indicators[venue_id]["Discover_total_qty"]/self.current_indicators[venue_id]["Discover_events" ]


    
    def process_end_of_slice(self, timestamp_str, venue_id):
        self.normalize_indicators(venue_id)
        
        
        # Alternation and Continuation
        self.current_indicators[venue_id]['NbContinuation_mid'] =  self.mid_alternator[venue_id].nb_continuation
        self.current_indicators[venue_id]['NbAlternation_mid']  =  self.mid_alternator[venue_id].nb_alternation
        
        self.current_indicators[venue_id]['NbContinuation_last']= self.last_alternator[venue_id].nb_continuation
        self.current_indicators[venue_id]['NbAlternation_last'] = self.last_alternator[venue_id].nb_alternation
        
        self.current_indicators[venue_id]['NbContinuation_bid'] =  self.bid_alternator[venue_id].nb_continuation
        self.current_indicators[venue_id]['NbAlternation_ask']  =  self.bid_alternator[venue_id].nb_alternation
        
        self.current_indicators[venue_id]['NbContinuation_bid']= self.ask_alternator[venue_id].nb_continuation
        self.current_indicators[venue_id]['NbAlternation_ask'] = self.ask_alternator[venue_id].nb_alternation
        # Resest the values
        # We here make the choice of using the last move 
        # of the preceding slice as the last_move at the beginnning of a slice
        self.mid_alternator[venue_id].reset_values(self.mid_alternator[venue_id].previous_move, 0,0)
        self.last_alternator[venue_id].reset_values(self.last_alternator[venue_id].previous_move, 0,0)
        self.bid_alternator[venue_id].reset_values(self.bid_alternator[venue_id].previous_move, 0,0)
        self.ask_alternator[venue_id].reset_values(self.ask_alternator[venue_id].previous_move, 0,0)
        # quad variations
        
        self.mid_quadratic_variator[venue_id].reset_values(self.mid_quadratic_variator[venue_id].last_value)
        self.last_quadratic_variator[venue_id].reset_values(self.mid_quadratic_variator[venue_id].last_value)
        self.spread_quadratic_variator[venue_id].reset_values(self.spread_quadratic_variator[venue_id].last_value)
        
        
#        self.bid_round_trip[v][nb_ats]  = 0
#            self.ask_round_trip[v][nb_ats]  = 0
#            
        ## we add to know which venue it is from !
        self.appendIndicator(self.current_indicators[venue_id], timestamp = self.slice_start)
        self.init_slice_indicators(venue_id)
                            
   
    
    def compute_spread_indicators(self,  event, venue_market_feed,last_image, dt, venue_id):
        #"NbDifferentSpreads" : 0,"NbEmpty_bid" : 0, "NbEmpty_ask" : 0, "NbCrossingSpread" : 0}
       #try :
        #self.current_indicators[venue_id]["VenueId"] = str(venue_id)
        bid_has_moved = last_image['BEST_BID1'] != venue_market_feed['BEST_BID1']
        ask_has_moved = last_image['BEST_ASK1'] != venue_market_feed['BEST_ASK1']
             
        if self.spread_is_crossed:
            self.current_indicators[venue_id]["CrossingSpreadDuration"] += dt
            
        
        if venue_market_feed['BEST_BID1'] == None :
            self.current_indicators[venue_id]["NbEmpty_bid"] += 1
        elif venue_market_feed['BEST_ASK1'] == None :
            self.current_indicators[venue_id]["NbEmpty_ask"] += 1
        elif venue_market_feed['BEST_BID1'] >= venue_market_feed['BEST_ASK1']:
            self.current_indicators[venue_id]["NbCrossingSpread"] += 1
            self.spread_is_crossed = True
        elif venue_market_feed['BEST_BID1'] < venue_market_feed['BEST_ASK1']:
            # Regular case
            # do we have a new spread ?
             if not self.spread_is_crossed:
                 if last_image['BEST_ASK1'] != None and last_image['BEST_BID1'] != None :
                     previous_spread = last_image['BEST_ASK1'] - last_image['BEST_BID1']
                     
                 else :
                     previous_spread = None
                    
                 if venue_market_feed['BEST_ASK1'] != None and venue_market_feed['BEST_BID1'] != None :
                     spread = venue_market_feed['BEST_ASK1'] - venue_market_feed['BEST_BID1']
                 else :
                     spread = None
                     
                 if self.is_multi and  self.previous_best_venue_ebbo != None:
                     for  venue_id in self.previous_best_venue_ebbo:
                         # this venue was one of the best 
                         self.current_indicators[v]["pct_time_ebbo_max_qty"] += dt
                         # reset the list ? 
                         
                     if self.previous_best_venue_ebbo_qty != None:
                         self.current_indicators[self.previous_best_venue_ebbo_qty]["pct_time_ebbo_max_qty"] += dt
                         self.previous_best_venue_ebbo_qty = None
                    
                    
                     # TODO add all venues ??? 
                     if venue_id != "all" :
                     # manage time @ EBBO, make no sense for the aggregated venue
                        if venue_market_feed['BEST_ASK1'] == self.aggregated_market_feed['BEST_ASK1'] and venue_market_feed['BEST_BID1'] == self.aggregated_market_feed['BEST_BID1'] :
                            # first criteria : no quantity to take into account
                            #self.previous_best_venue_ebbo = None 
                            self.previous_best_venue_ebbo.append(venue_id)
                            self.current_indicators[venue_id]["pct_time_ebbo"] += dt
                            venue_at_ebbo = {}
                            for ob in self.ob_venues:
                                if self.ba['feed'][self.ob_venues[ob]]['BEST_BID1'] == self.aggregated_market_feed['BEST_BID1'] and self.ba['feed'][self.ob_venues[ob]]['BEST_ASK1'] == self.aggregated_market_feed['BEST_ASK1'] :  
                                
                                    venue_at_ebbo[self.ob_venues[ob]] = 0.5*(self.ba['feed'][self.ob_venues[ob]]['BEST_BID1'] + self.ba['feed'][self.ob_venues[ob]]['BEST_ASK1']) 
                                    
                            if len(venue_at_ebbo.keys()) > 0:
                                self.previous_best_venue_ebbo_qty =  get_max_value_key_in_dict(venue_at_ebbo).split('#')[0]
                                
                                              
                 
                 if spread > previous_spread:
                     q_discovered = 0
                     divider = 0 
                     self.current_indicators[venue_id]["NbIncreasingSpread"] += 1
                     # manage discover indicator
                     if last_image['BEST_BID1'] > venue_market_feed['BEST_BID1']:
                         #some quantity discovered at the bid
                         q_discovered += venue_market_feed['BEST_BID1']
                         divider += 1
                     if last_image['BEST_ASK1'] < venue_market_feed['BEST_ASK1']:
                         #some quantity discovered at the ask
                         q_discovered += venue_market_feed['BEST_ASK1']
                         divider += 1 
                     
                     if self.current_indicators[venue_id]["Discover_total_qty"] == None :
                        self.current_indicators[venue_id]["Discover_total_qty"] = q_discovered / divider
                     else  :
                        if divider == 0 :
                            print "ouch 3"
                        else :
                            self.current_indicators[venue_id]["Discover_total_qty"] += q_discovered / divider
                    
                     self.current_indicators[venue_id]["Discover_events"] += 1
                    
                    # : 0,self.current_indicators[venue_id][: 0,  "Discover_mean_qty" : 0} 
                 elif spread < previous_spread: 
                     self.current_indicators[venue_id]["NbDecreasingSpread"] += 1
                 else :
                     # spread hasn t change : if it is the minimum, increase the duration
                      if self.in_min_spread:
                          self.current_indicators[venue_id]["MinSpreadDuration"] += dt
                      
                      if self.in_max_spread:
                          self.current_indicators[venue_id]["MaxSpreadDuration"] += dt
                 
                 self.current_indicators[venue_id]["Spread_time"] += spread*dt
                 self.previous_spread = self.current_spread
                 self.current_spread  = spread
                 
             else :
                 # the spread is crossed
                 spread = 0
                 self.current_spread = 0
                 self.current_indicators[venue_id]["NbIncreasingSpread"] += 1
                # print "spread is crossed, venue is : %s"%str(venue_id)
             self.spread_is_crossed = False
             
             if self.current_indicators[venue_id]["MinSpread"] == 0 :
                 self.in_min_spread = True
                 self.current_indicators[venue_id]["MinSpread"] =  spread
                 
             if self.current_indicators[venue_id]["MaxSpread"] == 0 :
                 self.current_indicators[venue_id]["MaxSpread"] =  spread
                 self.in_max_spread = True
                 
             if self.current_indicators[venue_id]["MinSpread"] > spread :
                 self.current_indicators[venue_id]["MinSpread"] =  spread 
                 self.in_min_spread = True
                 # new minimum spread, reset to zero the duration
                 self.current_indicators[venue_id]["MinSpreadDuration"] = 0
             
             if self.current_indicators[venue_id]["MaxSpread"] < spread :
                 self.current_indicators[venue_id]["MaxSpread"] =  spread 
                 self.in_max_spread = True
                 # new minimum spread, reset to zero the duration
                 self.current_indicators[venue_id]["MaxSpreadDuration"] = 0
             
             
             if bid_has_moved or ask_has_moved:
                # we have a new spread
                 self.current_indicators[venue_id]["NbDifferentSpreads"] += 1
                 
       #except:
       #     print "An error occured in spread computations"      
         
             
            
            
            
    def compute_order_book_indicators(self,event,  timestamp_num, venue_market_feed,previous_market_feed, venue_id):
        # compute necessary basic ob indicators 
       # try :
       
        dt = min(timestamp_num - self.last_time_stamp[venue_id],timestamp_num - self.slice_start)
       
       
        if venue_market_feed == None:
            self.times_sum[venue_id] += dt
            return
        
        
        
        
        if previous_market_feed != None:
            self.compute_spread_indicators(event, venue_market_feed,previous_market_feed, dt, venue_id) 
        self.compute_round_trips(venue_market_feed, venue_id, dt)
        
        if self.best_bid[venue_id] == None :
            self.best_bid[venue_id] =  venue_market_feed['BEST_BID1']
            self.time_at_best_bid[venue_id] = dt 
        elif self.best_bid[venue_id] <=  venue_market_feed['BEST_BID1']:
             self.time_at_best_bid[venue_id] += dt
            
        if self.best_ask[venue_id] == None :
            self.best_ask[venue_id] ==  venue_market_feed['BEST_ASK1']
            self.time_at_best_ask[venue_id] = dt
        elif self.best_ask[venue_id] >=  venue_market_feed['BEST_ASK1']:
             self.time_at_best_ask[venue_id] += dt
             
        #except :
        #    print "Error, please check"
        # compute necessary basic ob indicators, wihtout any normalization 
        self.times_sum[venue_id] += dt
        
        ## bid /ask Qty
        
        self.current_indicators[venue_id]["NbUpdates"] += 1
        if dt < 0:
            print"OUCH !!! events not in the right order ???? "
        elif  venue_market_feed['BEST_BID1'] >= venue_market_feed['BEST_ASK1'] : 
            #crossed spreads require a specific treatement   
            bid_crossed  = 0
            ask_crossed  = 0
            limit = 1
            while  limit < 6 and venue_market_feed['BEST_BID%d'%limit] >venue_market_feed['BEST_ASK1'] :
                 bid_crossed += venue_market_feed['BEST_BSIZ%d'%limit]
                 limit += 1
            limit = 1
            while  limit < 6 and venue_market_feed['BEST_ASK%d'%limit] <venue_market_feed['BEST_BID1'] :
                 ask_crossed += venue_market_feed['BEST_ASIZ%d'%limit]
                 limit += 1
            
            self.current_indicators[venue_id]["BidQuantity_time"] += bid_crossed * dt
            self.current_indicators[venue_id]["AskQuantity_time"] += ask_crossed * dt
        
        else:
            # we should be in the "normal" case : no crossing
            if self.current_indicators[venue_id]["BidQuantity_time"]  == None :
                self.current_indicators[venue_id]["BidQuantity_time"] = venue_market_feed['BEST_BSIZ1'] * dt if venue_market_feed['BEST_BSIZ1'] else 0
            else :
                self.current_indicators[venue_id]["BidQuantity_time"] += venue_market_feed['BEST_BSIZ1'] * dt if venue_market_feed['BEST_BSIZ1'] else 0
            if self.current_indicators[venue_id]["AskQuantity_time"]  == None :
                self.current_indicators[venue_id]["AskQuantity_time"] = venue_market_feed['BEST_ASIZ1'] * dt if venue_market_feed['BEST_ASIZ1'] else 0
            else:
                self.current_indicators[venue_id]["AskQuantity_time"] += venue_market_feed['BEST_ASIZ1'] * dt if venue_market_feed['BEST_ASIZ1'] else 0

                if venue_id != "all":
                    self.current_mid = venue_market_feed['MID_PRICE']
                    current_mid_tick = self.get_ticksize(self.current_mid , venue_id)
                    
                    if self.mid_tick[venue_id] == None:
                        self.mid_tick[venue_id] = current_mid_tick * dt
                    else:
                        self.mid_tick[venue_id] += current_mid_tick * dt 
                    if current_mid_tick == self.current_spread :
                        # time where spread = 1 tick
                        self.current_indicators[venue_id]["Time_min_tick"] += dt
                else :
                    self.current_mid = venue_market_feed['MID_PRICE']
                        
                        
        
        
        if previous_market_feed != None:
            #alternation and continuations 
            if venue_id != "all" and venue_market_feed['LIT_TRADE_EVENT']:
                self.last_alternator[venue_id].compute_alternation( venue_market_feed['TRDPRC_1'], previous_market_feed['TRDPRC_1'])
            
     
            self.mid_alternator[venue_id].compute_alternation( venue_market_feed['MID_PRICE'],previous_market_feed['MID_PRICE'])
            self.bid_alternator[venue_id].compute_alternation( venue_market_feed['BEST_BID1'], previous_market_feed['BEST_BID1'])
            self.ask_alternator[venue_id].compute_alternation( venue_market_feed['BEST_ASK1'], previous_market_feed['BEST_ASK1'])
            
            #Quadratic variation
            quad_var = self.mid_quadratic_variator[venue_id].compute_quadratic_variation(venue_market_feed['MID_PRICE'], previous_market_feed['MID_PRICE'])
            quad_var = self.last_quadratic_variator[venue_id].compute_quadratic_variation(venue_market_feed['TRDPRC_1'], previous_market_feed['TRDPRC_1'])
            self.spread_quadratic_variator[venue_id].compute_quadratic_variation(self.current_spread, self.previous_spread)
            
        #total bid and ask quantity
        total_bid = 0
        total_ask = 0
        total_nb = 0
        total_ask_nb = 0
        total_bid_nb = 0
        
        for limit in range(1, 5): 
             total_bid +=  venue_market_feed['BEST_BSIZ%d'%limit] if venue_market_feed['BEST_BSIZ%d'%limit] else 0 
             total_ask   +=  venue_market_feed['BEST_ASIZ%d'%limit] if venue_market_feed['BEST_ASIZ%d'%limit] else 0
             if limit > 1:
                 total_ask_nb += venue_market_feed['NO_ASKMMKR%d'%limit] if 'NO_ASKMMKR%d'%limit in venue_market_feed else 0 
                 total_bid_nb += venue_market_feed['NO_BIDMMKR%d'%limit] if 'NO_BIDMMKR%d'%limit in venue_market_feed else 0
             else:
                 total_ask_nb += venue_market_feed['NO_ASKMMKR'] if 'NO_ASKMMKR' in venue_market_feed else 0
                 total_bid_nb += venue_market_feed['NO_BIDMMKR'] if 'NO_BIDMMKR' in venue_market_feed else 0
             
        self.current_indicators[venue_id]["TotalAskQty"] += total_ask * dt
        self.current_indicators[venue_id]["TotalBidQty"] += total_bid * dt
        self.current_indicators[venue_id]["NbOrders"] += (total_ask_nb + total_bid_nb) * dt
        
    def compute_trade_indicators(self, event, venue_market_feed, venue_id, add_the_volume):
        # compute necessary basic indicators weighted by trade volumes 
         trade_volume = 0  
         #venue_market_feed['LIT_LAST_TRDS_GROUP']['VOLUME']
         #feed['LIT_LAST_TRDS_GROUP']['NUM_MOVES']
         
         trade_volume = venue_market_feed['TRADES_VOLUMES']
         
         spread = venue_market_feed['BEST_ASK1'] - venue_market_feed['BEST_BID1']
         
         if trade_volume >  0 and trade_volume != None:
             if add_the_volume :
                 self.current_indicators[venue_id]["Volume"] += trade_volume
                 self.current_indicators[venue_id]["TurnOver"] +=venue_market_feed['TRADES_TURNOVER']
             self.current_indicators[venue_id]["BidQuantity_trades"] += venue_market_feed['BEST_BSIZ1'] * trade_volume 
             self.current_indicators[venue_id]["AskQuantity_trades"] += venue_market_feed['BEST_ASIZ1'] * trade_volume
             
             if spread != None and spread >  0 : 
                 self.current_indicators[venue_id]["Spread_trades"] += spread * trade_volume
                  
    def manage_empty_slice(self, venue_id, venue_market_feed):
        # Compute indicators for a slide where no events occured  
        # We just have to fill the ob indicators, no need to call the normalization
        self.current_indicators[venue_id]['NbContinuation_mid'] = 0
        self.current_indicators[venue_id]['NbAlternation_mid']  = 0
        
        self.current_indicators[venue_id]['NbContinuation_last']= 0
        self.current_indicators[venue_id]['NbAlternation_last'] = 0
        
        self.current_indicators[venue_id]['NbContinuation_bid'] =  0
        self.current_indicators[venue_id]['NbAlternation_ask']  =  0
        
        self.current_indicators[venue_id]['NbContinuation_bid']= 0
        self.current_indicators[venue_id]['NbAlternation_ask'] = 0
        
        self.current_mid = venue_market_feed['MID_PRICE']
        current_mid_tick = self.get_ticksize(self.current_mid , venue_id)
        self.current_indicators[venue_id]["Mid_Tick_t"] =  current_mid_tick
        self.current_indicators[venue_id]["Time_min_tick"] =  self.slice_duration
        
        
             #for nb_ats in self.bid_round_trip[venue_id]:
             #    self.current_indicators[venue_id]["bid_round_trip"] = self.bid_round_trip[venue_id]
        self.current_indicators[venue_id]["BidQuantity_time"] =  venue_market_feed["BEST_BSIZ1"]
        self.current_indicators[venue_id]["AskQuantity_time"] = venue_market_feed["BEST_ASIZ1"]
        self.current_indicators[venue_id]["Spread_time"] = venue_market_feed["BEST_ASIZ1"] - venue_market_feed["BEST_BSIZ1"]
        ## normalization of quadratic variations   
        self.current_indicators[venue_id]["QuadraticVariation_last"] = 0  
        self.current_indicators[venue_id]["QuadraticVariation_mid"] = 0
        self.current_indicators[venue_id]["QuadraticVariation_spread"] = 0 
        
        self.current_indicators[venue_id]["Volume"] = 0
        self.current_indicators[venue_id]["TurnOver"] =0
        
        #compute the round trips         
        self.compute_round_trips(venue_market_feed, venue_id, 1)
        
        for nb_ats in self.round_trip_qty:
            self.current_indicators[venue_id]["bid_round_trip_%d_ats"%nb_ats] = self.bid_round_trip[venue_id][nb_ats]["Slippage"] /  self.bid_round_trip[venue_id][nb_ats]["Time"] if self.bid_round_trip[venue_id][nb_ats]["Time"] != 0 else None 
            self.current_indicators[venue_id]["ask_round_trip_%d_ats"%nb_ats] = self.ask_round_trip[venue_id][nb_ats]["Slippage"] /  self.ask_round_trip[venue_id][nb_ats]["Time"] if self.ask_round_trip[venue_id][nb_ats]["Time"] != 0 else None 
    
        self.current_indicators[venue_id]["BidQuantity_trades"] =  venue_market_feed["BEST_BSIZ1"]
        self.current_indicators[venue_id]["AskQuantity_trades"] =   venue_market_feed["BEST_ASIZ1"]
        self.current_indicators[venue_id]["Spread_trades"] = self.current_indicators[venue_id]["Spread_time"]
        self.current_indicators[venue_id]["Discover_events" ] = 0
        self.current_indicators[venue_id]["Discover_mean_qty"] = 0

        total_bid = 0
        total_ask = 0
        total_nb = 0
        total_ask_nb = 0
        total_bid_nb = 0
        
        for limit in range(1, 5): 
             total_bid +=  venue_market_feed['BEST_BSIZ%d'%limit] if venue_market_feed['BEST_BSIZ%d'%limit] else 0 
             total_ask   +=  venue_market_feed['BEST_ASIZ%d'%limit] if venue_market_feed['BEST_ASIZ%d'%limit] else 0
             if limit > 1:
                 total_ask_nb += venue_market_feed['NO_ASKMMKR%d'%limit] if 'NO_ASKMMKR%d'%limit in venue_market_feed else 0 
                 total_bid_nb += venue_market_feed['NO_BIDMMKR%d'%limit] if 'NO_BIDMMKR%d'%limit in venue_market_feed else 0
             else:
                 total_ask_nb += venue_market_feed['NO_ASKMMKR'] if 'NO_ASKMMKR' in venue_market_feed else 0
                 total_bid_nb += venue_market_feed['NO_BIDMMKR'] if 'NO_BIDMMKR' in venue_market_feed else 0

        self.current_indicators[venue_id]["TotalAskQty"] = total_ask
        self.current_indicators[venue_id]["TotalBidQty"] = total_bid
        self.current_indicators[venue_id]["NbOrders"] = total_bid_nb + total_ask_nb
        
        self.appendIndicator(self.current_indicators[venue_id], timestamp = self.slice_start)
        self.init_slice_indicators(venue_id)
           
    def process(self, event):
        try:
            """
            First of all, get all the event properties in local variables, so that you can SEE them with ECLIPSE
            in debug mode. If you don't do so, you won't have access to this information in debug mode, since
            the 'event' parameter comes from swig.
            """
            event_type    = event.type
            if event_type & EventTypes.IdleEvent:
                return False
            security_id   = event.securityId
            venue_id      = event.venueId
            timestamp_num = event.timestamp
            timestamp_str = event.getTimeStampStr()
  #          td_id = event.trading_destination_id
            """
            If we have a phase change event, then let's print the new starting phase
            """
            if   event_type & EventTypes.OpeningAuctionStarting:
                print 'Opening auction is starting right now!'
            elif event_type & EventTypes.ContinuousAuctionStarting:
                print 'Continuous auction is starting right now!'
            elif event_type & EventTypes.IntradayAuctionStarting:
                print 'An intraday auction is starting right now!'

            elif event_type & EventTypes.OpeningAuctionStarting:
                print 'Opening auction is starting right now!'
            """
            Then, if you use the optionnal indicators OR if your agent derives from BasePlotAgent, so
            you have to call the function below.
            """
            code = self.update(event)
            """
            Now, let's get the market feed on the destination which just changed. To do that, there are
            two different ways which are exactly equivalent.
            """
            if venue_id in self.ba['lit_venue_ids']:
                tv_id = int(venue_id.split('#')[0])
                self.update_the_feed(venue_id)
                if self.ba['feed'][venue_id]['LIT_TRADE_EVENT']:
                    print "T= %s, Trade Event : %d,  LIT_Volume = %d, Lit Num Moves : %d , Volume : %d"%(timestamp_str,self.ba['feed'][venue_id]['LIT_TRADE_EVENT'],self.ba['feed'][venue_id]['TRADES_VOLUMES'],self.ba['feed'][venue_id]['LIT_NUM_MOVES'],self.ba['feed'][venue_id]['VOLUME'])
                    trades = self.marketManager.getTrades(venue_id)
                        
                
                if self.ba['feed'][venue_id]['LIT_TRADE_EVENT'] >  0 :
                    #update ATS 
                    trades_volume = self.marketManager.getTotalTradesVolume(venue_id)
                    nb_deal =  len(self.ba['feed'][venue_id]['LIT_LAST_DEALS_IDXS'])
                    
                    self.total_deal_volume +=  trades_volume
                    self.total_nb_deal +=  nb_deal
                    self.ats = self.total_deal_volume / self.total_nb_deal
                    
                market_feed = self.marketManager.getFeedInfo(venue_id)
             
                
#                for l in range(0,5) :  
#                    if market_feed['BEST_ASK%d'%(l+1)] !=market_feed["BEST_ASK"][l]:
#                        print "error in market feed, limit  = %d"%l(l+1)
#                    if market_feed['BEST_BID%d'%(l+1)] !=market_feed["BEST_BID"][l]:
#                        print "error in market feed" 
#                    if market_feed['BEST_ASIZ%d'%(l+1)] !=market_feed["BEST_ASIZ"][l]:
#                        print "error in market feed"
#                    if market_feed['BEST_BSIZ%d'%(l+1)] !=market_feed["BEST_BSIZ"][l]:
#                        print "error in market feed" 
#                        
                    
                    
                if market_feed['CONDCODE_2'] != 'TR':
                    print "The CONDCODE_2 is %s"%market_feed['CONDCODE_2']
                    pass
                
                if self.is_multi :
                    self.aggregated_market_feed=self.ba['feed']["all"] 
                
                
                if self.slice_start == None:
                    time_parts = timestamp_str.split(':');
                    nb_seconds =  time_parts[2]
                    if int(nb_seconds) < 30 :
                        second_slice_start = '00'
                    else:
                        second_slice_start = '30'
                    
                    self.slice_start = date2num("%s:%s:%s"%(time_parts[0],time_parts[1],second_slice_start))
                    self.slice_end = self.slice_start + self.slice_duration
                    
                    for v in self.venue_ids:
                        # initialize timestamps to slice start
                        self.last_time_stamp[v] = self.slice_start
                    
                
                
                
                #print "Time is %s, Venue is : %s , Slice start : %s, Slice end : %s "%(timestamp_str ,venue_id, num2date(self.slice_start), num2date(self.slice_end))
                # TODO do something
                if timestamp_num >= self.slice_end:
                    print "End of slice, T =  %s, Slice start: %s, Slice end :%s "%(timestamp_str,num2date(self.slice_start), num2date(self.slice_end))
                    #compute the indicators on the last part of the slice, limiting to the slice_end
#                    if self.slice_end - self.last_time_stamp[tv_id] > self.slice_duration :
#                        print "T = %s, No update during one slide, specific treatment of empty slice"%timestamp_str
#                        
#                        # compute indicators nutil the end of the non empty slide
#                        self.compute_all_indicators(event,venue_id, tv_id, self.slice_end, False, True)
#                        
#                        self.slice_start = self.slice_end
#                        self.slice_end += self.slice_duration
#                        
#                        # process the non empty slice
#                        for v in self.venue_ids:
#                            self.process_end_of_slice(timestamp_str, v)
#                            self.last_time_stamp[v] =self.slice_start
#                            
#                        self.slice_start = self.slice_end
#                        self.slice_end += self.slice_duration
#                        
#                        for v in self.venue_ids:
#                            self.manage_empty_slice( v, self.last_image[v])
#                            self.last_time_stamp[v] =self.slice_start
                    if False :
                        print "Woooow"
                    else:
                    # compute indicators nutil the end of the current slide
                        self.compute_all_indicators(event,venue_id, tv_id, self.slice_end, False, True)
                        #update the slice
                        self.slice_start = self.slice_end
                        self.slice_end += self.slice_duration
                        #then, one can compute the normalization
                        for v in self.venue_ids:
                            self.process_end_of_slice(timestamp_str, v)
                            self.last_time_stamp[v] =self.slice_start  
                        # compute indicators from the beginning of the new slice up to the last time stamp
                         
                            
                        self.compute_all_indicators(event,venue_id, tv_id, timestamp_num, True, False)
                        if timestamp_num > self.slice_end :
                            # in this case, the previous update will still be used to manage the next slice,
                            # as the next OB update happen after the end of the curretn slice
                            
                            self.slice_start = self.slice_end
                            self.slice_end += self.slice_duration
                                
                            for v in self.venue_ids:
                                self.manage_empty_slice( v, self.last_image[v])
                                self.last_time_stamp[v] =self.slice_start
                                
                    
                        
                else:
                    self.compute_all_indicators(event,venue_id, tv_id, timestamp_num, True, True)
            return False
        except:
            """
            Please copy-paste the piece of code below to catch and print any exception raised by your python code
            """
            import sys, traceback
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
    
    def processReport(self, event):
        try:
            venue_id      = event.venueId
            security_id   = event.securityId
            timestamp     = event.timestamp
            remain_qty    = event.remainQty
            exec_quantity = event.quantity
            price         = event.price
            order_id      = event.orderId
            type          = event.type
            print 'EXECUTION : %d @ %f, remain_qty=%d' %(exec_quantity, price, remain_qty)
            time.sleep(1)
        except:
            """
            Please copy-paste the piece of code below to catch and print any exception raised by your python code
            """
            import sys, traceback
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
    
    def compute_all_indicators(self,event, venue_id, tv_id, timestamp_num, update_market_images, take_trade_volume_in_account):
        
       
            
        
        #market_feed = copy.deepcopy(self.last_image[tv_id]) if self.last_image[tv_id] != None else None
        market_feed = self.smart_feed_copy(self.last_image[tv_id], venue_id) if self.last_image[tv_id] != None else None
            
        #temp = copy.deepcopy(self.last_image[tv_id])if self.last_image[tv_id] != None else None
        temp = self.smart_feed_copy(self.last_image[tv_id], venue_id) if self.last_image[tv_id] != None else None
        #temp = copy.deepcopy(self.last_image[tv_id])if self.last_image[tv_id] != None else None
       #compute_order_book_indicators(self, event, venue_market_feed,previous_market_feed, venue_id)
       
        #if self.last_image[tv_id] == self.previous_previous_image[tv_id] :
        #   print "Equality !!"
#        else :
#           print_dict_difference(self.last_image[tv_id], self.previous_previous_image[tv_id])
#           
        previous_image = self.smart_feed_copy(self.ba['feed'][venue_id], venue_id)
         
        self.compute_order_book_indicators(event,timestamp_num, self.last_image[tv_id], self.previous_previous_image[tv_id], int(tv_id))
        
        if update_market_images :
            #self.last_image[tv_id] = copy.deepcopy(self.ba['feed'][venue_id])
            self.last_image[tv_id] = self.smart_feed_copy(self.ba['feed'][venue_id],venue_id)
            #self.previous_previous_image[tv_id] = copy.deepcopy(temp) if temp != None else None
            self.previous_previous_image[tv_id] = self.smart_feed_copy(temp, venue_id) if temp != None else None
        
        if self.is_multi:
            #always be late of one updfate, because of time based indicators
            
            if update_market_images :
                if self.last_image["all"] != None :
                    #self.aggregated_market_feed = copy.deepcopy(self.last_image["all"])
                    self.aggregated_market_feed = self.smart_feed_copy(self.last_image["all"],"all")
                    #temp = copy.deepcopy(self.last_image["all"])
                    temp = self.smart_feed_copy(self.last_image["all"],"all")
                if self.ba['feed']["all"] != None:
                    #self.last_image["all"] = copy.deepcopy(self.ba['feed']["all"])
                    self.last_image["all"] = self.smart_feed_copy(self.ba['feed']["all"],"all")
                    self.previous_previous_image["all"] = self.smart_feed_copy(temp,"all")
            else:
                    self.aggregated_market_feed = self.smart_feed_copy( self.aggregated_market_feed, venue_id)
                    
                    #self.previous_previous_image["all"] = dict(temp)
                
            self.compute_order_book_indicators(event,timestamp_num, self.last_image["all"],self.previous_previous_image["all"], 'all')
            self.last_time_stamp["all"] = timestamp_num
            nb_trades = self.aggregated_market_feed['TRADES_NB_MOVES'] if self.aggregated_market_feed != None else 0 
            if nb_trades > 0:
                    self.compute_trade_indicators(event, self.aggregated_market_feed, "all", take_trade_volume_in_account)

            
        self.last_time_stamp[tv_id] = timestamp_num
        
        #self.previous_previous_image[tv_id] =self.last_image[tv_id]
        #self.last_image[tv_id] = dict(self.ba['feed'][venue_id])
         
        if market_feed != None and market_feed['TRADES_NB_MOVES'] >= 1:
             self.compute_trade_indicators(event, market_feed, int(tv_id), take_trade_volume_in_account)
    
    def last_process(self):
        """
        Here we can implement what will be done when the simulation is over. For example, we can print the volume, turnover, vwap of the while day.
        """
       
        
        