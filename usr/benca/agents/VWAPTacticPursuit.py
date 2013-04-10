'''
Created on 1 juil. 2010

@author: benca
'''

from simep.funcs.stdio.utils import pyLog



class PursuitTacticVWAP:
    
    def __init__(self, VWAPManager, cycle_time, distance, use_constant_size, use_business_time):
        # VWAP Manager instance
        self.B                         = VWAPManager
        self._use_business_time        = use_business_time
        self._use_constant_size        = use_constant_size
        # Pursuit parameters
        self._cycle_time               = cycle_time   # which corresponds to n*dt
        self._overweight_coefficient   = 1.2
        self._max_mult_ATS             = 4.0
        self._min_mult_ATS             = 1.0
        self._ratio_default            = 2.0
        self._distance                 = distance
        self._next_order_business_time = self._cycle_time
        ba['event']['TIME_NUM']dt                 = self.B['start_time']
        self._reserved_qty             = 0
        # order parameters
        self._percen_to_go_next_bidask = 0.90
        self._order                    = None
        self._order_time               = self.B['start_time']
    
    def _prvt__check_time(self):
        dt = self.B._bus['avg_exec_time']
        # UPDATE PI_TDT
        ba['event']['TIME_NUM']dt =  self.B._time_t + dt*self._cycle_time
        self._PI_tdt   = (ba['event']['TIME_NUM']dt - self.B._prev_slice_time) * self.B._PIs[0][self.B._next_slice_index] + \
                         (self.B._next_slice_time - ba['event']['TIME_NUM']dt) * self.B._PIs[0][self.B._prev_slice_index]
        self._PI_tdt  /=  self.B._slice_timestep
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
            if self.B._time_t < (self._pursuit_order_time+dt*self._cycle_time):
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
        # Update internal parameters
        if not self._prvt__check_time():
            return
        if self._use_constant_size:
            DQ        = min(self.B['asked_qty'] - self.B._exec_qty - self.B._all_reserved_qty + self._reserved_qty, int(self._overweight_coefficient*self.B._bus['avg_trade_size_300']))
        else:
            # define Q(t), Q(t+dt) and DQ
            Q_t       = self.B._PI_t   * self.B['asked_qty']
            Q_tdt     = self._PI_tdt   * self.B['asked_qty']
            DQ        = Q_tdt - Q_t
            # compute ratio and multiply DQ with it
            ratio     = Q_t / self.B._exec_qty if self.B._exec_qty != 0 else self._ratio_default
            DQ       *= ratio
            # multiply DQ with the overweight coefficient
            DQ       *= self._overweight_coefficient
            # compare with ATS and coefficients
            DQ        = max(self._min_mult_ATS*self.B._bus['avg_trade_size_300'], DQ)
            DQ        = min(self._max_mult_ATS*self.B._bus['avg_trade_size_300'], DQ)
            # check with remaining quantity
            DQ        = int(min(self.B['asked_qty'] - self.B._exec_qty - self.B._all_reserved_qty + self._reserved_qty, DQ))
        # check price
        ask_price = lob.getAsk(0).price
        if ask_price < 0.1 or ask_price > 100000.0:
            pyLog('ERROR : pursuit tactic received a weird ask price')
        bid_price = lob.getBid(0).price
        if bid_price < 0.1 or bid_price > 100000.0:
            pyLog('ERROR : pursuit tactic received a weird bid price')
        # determine if we are the only ones at best price
        if self._order != None:
            if self.B._side == 1:
                qty       = int(round(lob.getBid(0).size * self._percen_to_go_next_bidask))
                if qty <= self._order.remain and bid_price <= self._order.price:
                    index = 1
                else:
                    index = 0
            else:
                qty       = int(round(lob.getAsk(0).size * self._percen_to_go_next_bidask))
                if qty <= self._order.remain and ask_price >= self._order.price:
                    index = 1
                else:
                    index = 0
        else:
            index = 0
        # get best opposite price
        if self.B._side == 1:
            best_price     = lob.getBid(index).price
        else:
            best_price     = lob.getAsk(index).price
        price     = best_price - self.B._side*self._distance*best_price/10000
        # check with limit price
        if self.B['limit_price'] > 0.0 and price*self.B._side > self.B['limit_price']*self.B._side:
            return
        # create order
        self.cancel_order()
        self._order_time = self.B._time_t
        self._order      = self.B.send_limit_day_order(DQ, price, self.B['name']+"_PURSUIT")
        self.update_reserved_qty()
    
    def cancel_order(self):
        self.B.cancel_order(self._order)
        self._order = None
        self.update_reserved_qty()
