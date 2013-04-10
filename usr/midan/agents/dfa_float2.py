from simep.agents.basetrader import BaseTrader
from simep.sched import Order, Trade
from simep.funcs.data.pyData import pyData
from simep.funcs.stdio.utils import pyLog
from simep.tools import date2num
from random import *
import math


class dfa_float_bt(BaseTrader): 
    # trading rules
    # R1: order size  = 20% agg size
    # R2: order price = agg limit price
    # R3:   
    
    @staticmethod
    def public_parameters():
        
        setup      = {'name'         : {'label' : 'Name'             , 'value' : 'dfaFloat001'},
                      'indicators'   : {'label' : 'Indicators'       , 'value' : 'None'}}
        parameters = {'ref_idx'       : {'label' : 'Reference Index', 'value' : 'FTE.PA'},
                      'limit_price'   : {'label' : 'Price Limit'      , 'value' : 10}, 
                      'soft_limit'    : {'label' : 'Soft Limit'       , 'value' : 1}, 
                      'side'           : {'label' : 'Buy'              , 'value' : Order.Buy},
                      'rel_del'       : {'label' : 'Reload Delay'     , 'value' : 30},
                      'child_size'    : {'label' : 'Child Order Size (%)' , 'value' : 20},
                      'child_fin'     : {'label' : 'Child Order Finished (%)', 'value' : 100},
                      'min_size_2_join' : {'label' : 'Min Size to Join (%)' , 'value' : 20},
                      'hist_avg_t_size' : {'label' : 'Historic Average Touch Size', 'value': 100},
                      'hist_trades'   : {'label' : 'Historic ADV', 'value': 10000},
                      'idx_rel_off'   : {'label' : 'Index Relative Offset (%)', 'value' : 30},
                      'o_rel_trig'    : {'label' : 'Order Reload Trigger Volume', 'value' : 0},                      
                      'tag_vol_src'   : {'label' : 'Target Volume Source = all', 'value' : True},
                      'rnd_fact'      : {'label' : 'Random Factor (%)', 'value' : 20},
                      'const_max_size': {'label' : 'Max Size override Rule Const (%)', 'value' : 50},
                      'one_tick'      : {'label' : 'One Tick Size', 'value' : 0.01},
                      'ats_width'     : {'label' : 'ATS Window Width (business time)', 'value' : 60},
                      'plot_mode'     : {'label' : 'Plot Mode', 'value' : 1},
                      'asked_qty'     : {'label' : 'asked qty', 'value' : 100000}}
        
        return {'setup': setup, 'parameters': parameters}
    
    @staticmethod
    def check_parameters(parameters):        
        pass
    
    def __init__(self, setup, context, parameters, trace):        
        self._figure_title = 'title'
        BaseTrader.__init__(self, setup, context, parameters, trace)
        self.time_stamp = 0
        # volume timer
        self.vol_timer = 0
        # calendar timer 
        self.cal_timer = 0
        # volume of child orders that have been sent 
        self.sent_vol_child_order = 0
        # volume of child orders that have been executed
        self.exec_vol_child_order = 0
        # pct of finished child order
        self.pct_chld_order_fin = 0
        # if true then save all indicators
        self.exploration_mode = True
        # define a const 1 if Buy, -1 if Sell
        self.buy_const = 2*self['side'] - 1
        # the first value for index
        self.first_idx_value = None
        # the first value for stock
        self.first_stock_value = None
        # vector of all market trades
        self.market_trades = []
        # initialize return for index 
        self.idx_ret = 0
        # initialize avg_price 
        self.avg_price = self['limit_price']
        
    def initialize(self, market, bus):
        BaseTrader.initialize(self, market, bus)
        # get index LOB
        self.idx_LOB = self.market.findOrderBook(self['ref_idx'])
        # from historic trades and price limit compute ADV
        # actually take the whole ADV no constraint on limit price
        # compute the threshold for volume      
        self.thres_vol = self['o_rel_trig'] * self['hist_trades'] / 100
        self.no_delay  = (self['rel_del']==0) and (self['o_rel_trig']==0)
       
        # randomize the next reload timing +/- 20%    
        rnd_vol_fact  = 100 + uniform(-self['rnd_fact'],self['rnd_fact'])
        rnd_time_fact = 100 + uniform(-self['rnd_fact'],self['rnd_fact'])
        self.thres_vol_rnd  = self.thres_vol * rnd_vol_fact /100
        if self.no_delay:
            self.thres_time_rnd = self['rel_del'] * rnd_time_fact /100
        else:
            self.thres_time_rnd = uniform(1,120) 
       
    def enabled(self, context = None):
        return True
    
    def process(self, evt):
        code = self.update(evt)
        if self._exec_qty != 0:
            self.avg_price = self._weighted_price_algo/self._exec_qty
        else:
            self.avg_price = 37.0
        # depending on which event (from index LOB or stock LOB) 
        if evt.getOrderBookRIC() == self['ref_idx']:
            all_new_trades = evt.getTrades()
            len_trades = len(all_new_trades)
            last_trade = all_new_trades[len_trades-1]
            # uniquely for the first observation of index
            if self.first_idx_value is None:
                self.first_idx_value = all_new_trades[0].price
            
            # compute the index return
            self.idx_ret = (last_trade.price - self.first_idx_value)/self.first_idx_value 
                               
        elif evt.getOrderBookRIC() == self['ric']:
            # get all trades since last received event
            all_new_trades = evt.getTrades()
            len_trades = len(all_new_trades)
            last_trade = all_new_trades[len_trades-1]
            
            # uniquely for the first observation of stock
            if self.first_stock_value is None:
                self.first_stock_value = all_new_trades[0].price    
                
            # increment 2 timers and also memorize the market trades
            # ? explicit cumulative volume
            self.cal_timer += 1000*(evt.getEvtTime() - self.time_stamp)
            self.time_stamp = evt.getEvtTime()
            for trade in evt.getTrades():
                self.vol_timer += trade.size
                if len(self.market_trades)>=self['ats_width']:
                    self.market_trades.pop(0)
                self.market_trades.append((self.time_stamp, trade.price, trade.size))
            
            
            # reset 2 timers follow R4
            if (self.pct_chld_order_fin >= self['child_fin']):
                self.vol_timer = 0
                self.cal_timer = 0                
                 
            # then compute the index relative limit price
            idx_rel_lim_price = (self.idx_ret - self['idx_rel_off']*self.buy_const / 100)*self.first_stock_value
            
            # get the order book
            lob = evt.getLob()                  
            bid_price = lob.getBid(0).price            
            ask_price = lob.getAsk(0).price        
            bid_size  = lob.getBid(0).size            
            ask_size  = lob.getAsk(0).size
            
            # compute ATS(60)
            last_60_trade = map(lambda x: x[2], self.market_trades)            
            ats = dfa_float_bt.mean(last_60_trade) 
            
            # Live Touch Minus 1 tick, false if not exist
            if self['side']:
                live_touch_minus_1 = (lob.getBid(1).price < bid_price - self['one_tick'])
                live_touch_minus_1_tick_price = lob.getBid(1).price if live_touch_minus_1 else 0
                live_touch_minus_1_tick_size = lob.getBid(1).size if live_touch_minus_1 else 0
            else:
                live_touch_minus_1 = (lob.getAsk(1).price > ask_price + self['one_tick'])
                live_touch_minus_1_tick_price = lob.getAsk(1).price if live_touch_minus_1 else 0
                live_touch_minus_1_tick_size = lob.getAsk(1).size if live_touch_minus_1 else 0

                                
            # compute all secondary parameters
            # update aggregate average size
            agg_size = (self['hist_avg_t_size'] + bid_size + ask_size + live_touch_minus_1_tick_size)/4
                        
            # compute the child order limit size 
            ord_size = self['child_size'] * agg_size / 100
            
            # compute the price of child order              
            ord_prc = (min(self['limit_price'], idx_rel_lim_price, bid_price) if (self['side']) \
                      else max(self['limit_price'], idx_rel_lim_price, ask_price))
                
            # R7 Max Size Override Price Rule
            far_touch = (lob.getBid(0) if (self['side']) else lob.getAsk(0))            
            if ord_size > self['const_max_size']*far_touch.size/100:
                ord_prc -= self['one_tick']*self.buy_const
                 
            # R8 Random Price Offset Rule
            # generate a random number
            a_rnd_value = uniform(0,101)
            ord_prc -= (max(self['one_tick'],ord_prc * uniform(5,150)/10000) if a_rnd_value > 99 else 0)*self.buy_const

            # expiration of timers rule R3 + R5
            ok_2_place_order = ((((self.vol_timer >= self.thres_vol_rnd) or (self.cal_timer >= self.thres_time_rnd)) and not (self.no_delay)) \
                or ((self.cal_timer >= self.thres_time_rnd) and self.no_delay)) and ((self.avg_price - self['soft_limit'])*self.buy_const > 0) \
                and (bid_size > self['min_size_2_join']*agg_size/100) 
                       
            if ok_2_place_order:                
                # trading rule R1 + R2
                self._activeOrder = self.send_limit_day_order(ord_size, ord_prc, self['name'])
                # update sent volume of child orders 
                self.sent_vol_child_order += ord_size
                # and process the child order                
                
                # randomize the next reload timing +/- 20%    
                rnd_vol_fact = 100 + uniform(-self['rnd_fact'],self['rnd_fact'])
                rnd_time_fact = 100 + uniform(-self['rnd_fact'],self['rnd_fact'])
                self.thres_vol_rnd  = self.thres_vol * rnd_vol_fact /100
                if self.no_delay:
                    self.thres_time_rnd = self['rel_del'] * rnd_time_fact /100
                else:
                    self.thres_time_rnd = uniform(1,120)
                
                # also reset the two timers
                self.vol_timer = 0
                self.cal_timer = 0
                
            if self.exploration_mode:
                self.appendIndicator( \
                                      pyData('init', date = [self.time_stamp], \
                                      value = {'ats': [ats], 'bid size': [bid_size], 'ask size' : [ask_size]}))
        

               
            
        return None
    
    def processReport(self, evtReport):
        if evtReport.price > 100000.0 or evtReport.price < 0.1:
            pyLog("FATAL ERROR : eventReport.price > 100000.0 or eventReport.price < 0.1")
            return None
        # update average execution price 
        # update executed volume of child orders
        self.exec_vol_child_order += evtReport.size
        
        self.pct_chld_order_fin = 100* self.exec_vol_child_order / self.sent_vol_child_order
        self._exec_qty            += evtReport.size
        self._weighted_price_algo += evtReport.size * evtReport.price
        return None
    
    @staticmethod
    def mean(x):
        return sum(x)/len(x)
    
    def _pvtl__update_child_plot_curves(self):
        pass
    
    def _pvtl__update_child_plot_figure(self):
        pass
    
    def _pvtl__update_last_plot(self):
        pass