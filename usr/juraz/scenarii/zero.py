'''
Created on 24 juin 2010

@author: juraz
'''
from simep.sched import PyAgent
from simep.agents.agents import BaseAgent
from simep.sched import Order

class Zero(BaseAgent):
    def __init__(self,name,ric):
        super(Zero,self).__init__(name)
        self.setIsSimulator(True)
        self.nbOrders = 0
        self.brokerId = 1
        self.ric = ric
        
    def initialize(self, market, bus):
        """ Initialize method called by C++ Agent """
        BaseAgent.initialize(self, market, bus)
        self._orderbook = self.market.findOrderBook(self.ric)
        if self._orderbook is None:
            raise ValueError("%s orderbook could not be found ..." % ric)
        
    def _createOrder(self, prefix, side, price, type, qty, execType):
        order = Order()
        
        self.nbOrders += 1
        order.orderId = "%s_%i" % (prefix, self.nbOrders)
        order.brokerId = self.brokerId
        
        order.side = side
        order.price = price
        order.orderType = type # Limit, Stop, Market, MarketToLimit ...
        order.execType = execType # DAY, GTD, GTC, IOC, FOK
        
        # By default, it not a iceberg order
        order.remain = qty
        order.initialQty = qty
        order.shownQty = qty
        order.minQty = 0
        
        # Default: no expiration
        order.creationTimeMs = 0
        order.expirationTimeMs = 0
                
        return order    
    
    def process(self, evts):
        if(self.nbOrders >= 10):            
            return True
        side = 0
        price = 10.2
        qty = 100
        order = self._createOrder(self.name,side,price, Order.Limit, qty, Order.DAY)
        self._orderbook.processCreateOrder(order)
        