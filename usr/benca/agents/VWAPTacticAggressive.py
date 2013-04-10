'''
Created on 1 juil. 2010

@author: benca
'''

from simep.funcs.stdio.utils import pyLog



class AggressiveTacticVWAP:
    
    def __init__(self, VWAPManager, use_business_time):
        # VWAP Manager instance
        self.B                            = VWAPManager
        self._use_business_time           = use_business_time
        self._min_slippg_ntrvntion_period = 10
        self._next_order_business_time    = self._min_slippg_ntrvntion_period
        self._reserved_qty                = self.B['asked_qty']
        # order parameters
        self._order                       = None
        self._order_time                  = self.B['start_time']
    
    def _prvt__check_time(self):
        if self._order == None:
            return True
        if self._use_business_time:
            if self.B._bus['NUM_MOVES'] < self._next_order_business_time:
                return False
            else:
                self._next_order_business_time = self.B._bus['NUM_MOVES'] + self._min_slippg_ntrvntion_period
                return True
        else:
            if self.B._time_t > (self._order_time+self.B._dt*self._min_slippg_ntrvntion_period):
                return True
            else:
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
        ShouldWePlaceAnOrder = self._prvt__check_time()
        if not ShouldWePlaceAnOrder:
            return
        # order size
        MaxQtyAvailable = 0
        if self.B._side == 1:
            MaxQtyAvailable += lob.getAsk(0).size
            MaxQtyAvailable += lob.getAsk(1).size
        else:
            MaxQtyAvailable += lob.getBid(0).size
            MaxQtyAvailable += lob.getBid(1).size
        Qty = int( self.B['asked_qty']*self.B._PI_t_L - self.B._exec_qty )
        Qty = max( Qty, self.B._bus['avg_trade_size_300'] )
        Qty = min( Qty, MaxQtyAvailable)
        Qty = min( Qty, self.B['asked_qty'] - self.B._exec_qty - self.B._all_reserved_qty + self._reserved_qty )
        # check price
        ask_price = lob.getAsk(0).price
        if ask_price < 0.1 or ask_price > 100000.0:
            pyLog('ERROR : agressive tactic received a weird ask price')
        bid_price = lob.getBid(0).price
        if bid_price < 0.1 or bid_price > 100000.0:
            pyLog('ERROR : agressive tactic received a weird bid price')
        # check with limit price
        if self.B['limit_price'] > 0.0:
            if self.B._side == 1:
                if ask_price > self.B['limit_price']:
                    self.cancel_order()
                    return
            else:
                if bid_price < self.B['limit_price']:
                    self.cancel_order()
                    return
        # create order
        self.cancel_order()
        self._order_time = self.B._time_t
        self._order      = self.B.send_market_day_order(Qty, self.B['name']+"_AGGRESSIVE")
        self.update_reserved_qty()
    
    def cancel_order(self):
        self.B.cancel_order(self._order)
        self._order = None
        self.update_reserved_qty()

