'''
Created on 1 juil. 2010

@author: benca
'''

from simep.funcs.stdio.utils import pyLog
import math



class VwapCompTacticVWAP:
    
    def __init__(self, VWAPManager):
        # VWAP Manager instance
        self.B                         = VWAPManager
        self._use_business_time        = True
        # Pursuit parameters
        self._cycle_time               = 4
        self._next_order_business_time = self._cycle_time
        self._max_mult_ATS             = 4.0
        self._min_mult_ATS             = 1.0
        self._price_distance           = 50.0
        self._overweight_coeff         = 1.5
        self._percen_to_go_next_bidask = 0.8
        self._window_size_to_get_delta = 100
        self._delta_prices_list        = [-1.0 for i in range(self._window_size_to_get_delta)]
        self._delta_prices_index       = 0
        self._reserved_qty             = 0
        self._order                    = None
        self._order_time               = self.B['start_time']
    
    def _prvt__check_time(self):
        # RETURN
        if self._order == None:
            return True
        elif self._use_business_time:
            if self.B._bus['NUM_MOVES'] < self._next_order_business_time:
                return False
            else:
                self._next_order_business_time = self.B._bus['NUM_MOVES'] + self._cycle_time
                return True
        else:
            if self.B._time_t < (self._pursuit_order_time+self.B._dt*self._cycle_time):
                return False
            else:
                return True
    
    def update_reserved_qty(self, qty=-1):
        if qty != -1:
            self._reserved_qty = qty
        elif self._order != None:
            self._reserved_qty = self._order.remain
        else:
            self._reserved_qty = 0
        self.B.update_reserved_qty()
    
    def send_order(self, lob):
        if self.B._market_volume == 0:
            return
        mark_vwap = self.B._WeightedPriceMarket/self.B._market_volume
        if self.B._side == 1:
            best_price = lob.getBid(0).price
            delta_price = mark_vwap - (1+self._price_distance/10000)*best_price
        else:
            best_price = lob.getAsk(0).price
            delta_price = (1-self._price_distance/10000)*best_price - mark_vwap
        if delta_price < 0.0:
            return
        self._delta_prices_list[self._delta_prices_index] = delta_price
        self._delta_prices_index += 1
        self._delta_prices_index = (self._delta_prices_index % self._window_size_to_get_delta)
        if self._delta_prices_list[-1] == -1.0:
            return
        average_delta_price = 0.0
        for price in self._delta_prices_list:
            average_delta_price += price
        average_delta_price /= self._window_size_to_get_delta
        # check cycle
        if not self._prvt__check_time():
            return
        # define Q(t), Q(t+dt) and DQ
        Qty = int(self._overweight_coeff*self.B._bus['avg_trade_size_300'])
        if delta_price > average_delta_price:
            Qty = int( (self._max_mult_ATS*self.B._bus['avg_trade_size_300']-Qty)*(1-math.exp(-delta_price/average_delta_price)) + Qty )
        # take min
        Qty = int(min(self.B['asked_qty'] - self.B._exec_qty - self.B._all_reserved_qty + self._reserved_qty, Qty))
        # check price
        ask_price = lob.getAsk(0).price
        if ask_price < 0.1 or ask_price > 100000.0:
            pyLog('ERROR : pursuit tactic received a weird ask price')
        bid_price = lob.getBid(0).price
        if bid_price < 0.1 or bid_price > 100000.0:
            pyLog('ERROR : pursuit tactic received a weird bid price')
        # best price
        if self._order != None:
            if self.B._side == 1:
                qty        = int(round(lob.getBid(0).size * self._percen_to_go_next_bidask))
                if qty <= self._order.remain and bid_price <= self._order.price:
                    best_price = lob.getBid(1).price
            else:
                qty        = int(round(lob.getAsk(0).size * self._percen_to_go_next_bidask))
                if qty <= self._order.remain and ask_price >= self._order.price:
                    best_price = lob.getAsk(1).price
        else:
            if self.B._side == 1:
                best_price = lob.getBid(0).price
            else:
                best_price = lob.getAsk(0).price
        # check with limit price
        if self.B['limit_price'] > 0.0 and best_price*self.B._side > self.B['limit_price']*self.B._side:
            return
        # create order
        self.cancel_order()
        self._order_time = self.B._time_t
        self._order      = self.B.send_limit_day_order(Qty, best_price, self.B['name']+"_VWAPCOMP")
        self.update_reserved_qty()
    
    def cancel_order(self):
        self.B.cancel_order(self._order)
        self._order = None
        self.update_reserved_qty()
