'''
Created on 13 sept. 2010

@author: benca
'''

from simep.sched import Order
from simep.agents.baseagent import BaseAgent
from simep.tools import date2num
from numpy.random import *
import math


# ------------------------------------------------- #
# Set random set for repeatability                  #
# ------------------------------------------------- #
seed(42)




# ------------------------------------------------- #
# This is the zero intelligence model we use in the #
# simulation. This model sends n random limit order #
# (day execution, no iceberg) per step. The time    #
# between two steps's a random variable following a #
# normal law :                                      #
# ( self['mu_delta_time'], self['std_delta_time'] ) #
# The random orders have the following parameters : #
# - side (BUY or SELL) is random (uniformly)        #
# - quantity is random (log-normal, parameters      #
#   depend on the side)                             #
# - price is random (log_normal, parameters         #
#   depend on the side)                             #
# ------------------------------------------------- #
class ZIMModel(BaseAgent):
    
    
    '''######################################################################################################
    ########################################   IMPORTANT FUNCTIONS   ########################################
    ######################################################################################################'''
    
    # ------------------------------------------------- #
    # Constructor. We start by converting string time   #
    # into numerical time, so that it is easier to      #
    # manipulate (this is done with the function        #
    # 'date2num').                                      #
    # The base class is then initialized. And some      #
    # parameters are eventually set.                    #
    # ------------------------------------------------- #
    def __init__(self, setup, context, params, trace):
        self._timestamp          = params['start_time']
        params['start_time']     = int(date2num(params['start_time']))
        params['end_time']       = int(date2num(params['end_time']))
        params['mu_delta_time']  = int(date2num(params['mu_delta_time']))
        params['std_delta_time'] = int(date2num(params['std_delta_time']))
        super(ZIMModel,self).__init__(setup, context, params, trace)
        self.needAllEvts       = False
        self.needExecReportEvt = False
        self.setIsSimulator(True)
        self._order_number     = 0
        self._log_file         = params['file']
        self.time2wakeup       = self['start_time']
        self.round             = lambda (x): math.floor(x/self['tick_size'])*self['tick_size']
    
    # ------------------------------------------------- #
    # process : very important function ! This one is   #
    # the function called each time there is an         #
    # orderbook modification event.                     # 
    # ------------------------------------------------- #
    def process(self, event):
        if (self.time2wakeup == self['start_time']):
            self.initialize_orderbook()
            self.increase_time()
            return None
        if(self.time2wakeup >= self['end_time']):            
            return True
        self._timestamp = event.getEvtTimeStr()
        self.record_orderbook(event.getLob())
        for i in range(self['number_of_orders']):
            epsilon = (2 * randint(2,size=1)[0]) - 1
            if epsilon > 0:
                side = Order.Buy
            else:
                side = Order.Sell
            X = randn()
            Y = randn()
            qty = round(math.exp(self['std_order_qty']*Y + self['mu_order_qty']))
            qty = int(max(qty, 100))
            price = self.round(self['reference_price'] + (epsilon * self['half_spread']) - (epsilon * math.exp(self['std_price'] * X + self['mu_price'])))
            self.create_order(side,price, qty)
        self.increase_time()
        return None
    
    
    
    
    '''######################################################################################################
    ########################################   INTERNAL FUNCTIONS   #########################################
    ######################################################################################################'''
    
    # ------------------------------------------------- #
    # Initialize. This function's a pure simep internal #
    # function. Its goal is to set a pointer            #
    # (self._orderbook) to the public orderbook of the  #
    # right stock, so that orders can easily be created #
    # ------------------------------------------------- #
    def initialize(self, market, bus):
        BaseAgent.initialize(self, market, bus)
        self._orderbook = self.market.findOrderBook(self['ric'])
        if self._orderbook is None:
            raise ValueError("%s orderbook could not be found ..." % self['ric'])
    
    # ------------------------------------------------- #
    # A simple function which records the current       #
    # orderbook into the log file of the zimmodel.      #
    # ------------------------------------------------- #
    def record_orderbook(self, lob):
        if date2num(self._timestamp) > self['start_time']:
            lob_str  = '%s, BID1: %04d @ %05.02f' %(self._timestamp, lob.getBid(0).size, lob.getBid(0).price)
            lob_str += '    '
            lob_str += 'ASK1: %04d @ %05.02f' %(lob.getAsk(0).size, lob.getAsk(0).price)
            self._log_file.write(lob_str + '\n')
            for i in range(1,5):
                lob_str  = '%s, BID%d: %04d @ %05.02f' %('            ', i+1, lob.getBid(i).size, lob.getBid(i).price)
                lob_str += '    '
                lob_str += 'ASK%d: %04d @ %05.02f' %(i+1, lob.getAsk(i).size, lob.getAsk(i).price)
                self._log_file.write(lob_str + '\n')
    
    # ------------------------------------------------- #
    # The create order function. We use only limit      #
    # orders in this model, with day execution and no   #
    # iceberg. This is why we always have :             #
    # order.orderType = Order.Limit                     #
    # order.execType  = Order.DAY                       #
    # order.shownQty  = qty                             #
    # ------------------------------------------------- #
    def create_order(self, side, price, qty):
        order                  = Order()
        self._order_number    += 1
        order.orderId          = "%s$%i" % (self['name'], self._order_number)
        order.brokerId         = self.brokerId        
        order.side             = side
        order.price            = price
        order.orderType        = Order.Limit
        order.execType         = Order.DAY
        order.remain           = qty
        order.initialQty       = qty
        order.shownQty         = qty
        order.minQty           = 0
        order.creationTimeMs   = self.time2wakeup
        order.expirationTimeMs = 0
        self._orderbook.processCreateOrder(order)
        if side == Order.Buy:
            side_str = 'BUY '
        else:
            side_str = 'SELL'
        lob_str = '%s, LIMIT ORDER: %s %04d @ %05.02f' % (self._timestamp, side_str, qty, price)
        self._log_file.write(lob_str + '\n')
        return order
    
    # ------------------------------------------------- #
    # Initialize orberbook function. This is the        #
    # function which sends the very first orders in     #
    # order not to have an empty orderbook.             #
    # ------------------------------------------------- #
    def initialize_orderbook(self):
        for i in range(5):
            for j in range(i+1):
                qty = int(max(math.exp(self['std_order_qty']*randn() + self['mu_order_qty']), 100))
                self.create_order( Order.Buy, self['reference_price']-((i+3)*self['tick_size']), qty)
        for i in range(5):
            for j in range(i+1):
                qty = int(max(math.exp(self['std_order_qty']*randn() + self['mu_order_qty']), 100))
                self.create_order( Order.Sell, self['reference_price']+((i+3)*self['tick_size']), qty)
    
    # ------------------------------------------------- #
    # This is the way time is increased (that is to say #
    # randomly).                                        #
    # ------------------------------------------------- #
    def increase_time(self):
        T = randn()
        delta_time = self['mu_delta_time'] + T*self['std_delta_time']
        delta_time = int(max(delta_time, 1000))
        self.time2wakeup += delta_time

