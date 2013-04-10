'''
Created on 1 juil. 2010

@author: benca
'''



class TrackingTacticIS:
    
    def __init__(self, ISManager, cycle_time, use_business_time, use_constant_size):
        # VWAP Manager instance
        self.B                         = ISManager
        self._use_business_time        = use_business_time
        self._use_constant_size        = use_constant_size
        # Pursuit parameters
        self._cycle_time               = cycle_time   # which corresponds to n*dt
        self._overweight_coefficient   = 1.2
        self._max_mult_ATS             = 8.0
        self._min_mult_ATS             = 1.0
        self._ratio_default            = 2.0
        self._distance                 = 0
        self._next_order_business_time = self._cycle_time
        ba['event']['TIME_NUM']dt                 = self.B['start_time']
        self._reserved_qty             = 0
        # order parameters
        self._percen_to_go_next_bidask = 0.90
        self._order                    = None
        self._order_time               = self.B['start_time']
    
    def _prvt__check_time(self):
        # UPDATE PI_TDT
        ba['event']['TIME_NUM']dt =  self.B._time_t + self.B._bus['avg_exec_time']*self._cycle_time
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
            if self.B._time_t < (self._pursuit_order_time+self.B._bus['avg_exec_time']*self._cycle_time):
                return False
            else:
                return True
    
    def _prvt__perform_first_order(self, lob):
        if self._order == None:
            if self.B._side == 1:
                best_opposite = lob.getAsk(2).price
                price_condition = (best_opposite <= self.B['limit_price'])
            else:
                best_opposite = lob.getBid(2).price
                price_condition = (best_opposite >= self.B['limit_price'])
            if self.B['ExecutionStyle'] != 'Neutral' and self.B['ExecutionStyle'] != 'Passive' and price_condition:
                qty                      = int(round(self._max_mult_ATS*self.B._bus['avg_trade_size_300']))
                self._order_time = self.B._time_t
                self._order      = self.B.send_limit_day_order(qty, best_opposite, self.B['Name'])
                return True
        return False
    
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
        if self._prvt__perform_first_order(lob):
            return
        if self._use_constant_size:
            DQ        = min(self.B['asked_qty']-self.B._exec_qty, int(self._overweight_coefficient*self.B._bus['avg_trade_size_300']))
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
            DQ        = max(self._min_mult_ATS*self.B._ATS, DQ)
            DQ        = min(self._max_mult_ATS*self.B._ATS, DQ)
            # check with remaining quantity
            DQ        = min( self.B['asked_qty'] - self.B._exec_qty - self.B._all_reserved_qty + self._reserved_qty , DQ)
            DQ        = int(round(DQ))
        # determine if we are the only ones at best price
        if self._order != None:
            if self.B._side == 1:
                price     = lob.getBid(0).price
                qty       = int(round(lob.getBid(0).size * self._percen_to_go_next_bidask))
                if qty <= self._order.remain and price <= self._order.price:
                    index = 1
                else:
                    index = 0
            else:
                price     = lob.getAsk(0).price
                qty       = int(round(lob.getAsk(0).size * self._percen_to_go_next_bidask))
                if qty <= self._order.remain and price >= self._order.price:
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
        if self.B['limit_price'] >= 0.0:
            if price*self.B._side > self.B['limit_price']*self.B._side:
                return
        # create order
        self.cancel_order()
        self._order_time = self.B._time_t
        self._order      = self.B.send_limit_day_order(DQ, price, self.B['name']+'_TRACKING')
        self.update_reserved_qty()
    
    def cancel_order(self):
        self.B.cancel_order(self._order)
        self._order = None
        self.update_reserved_qty()