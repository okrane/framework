from simep.agents.agents import BaseAgent
from simep.sched import Order
from datetime import datetime

class BaseAlgo(BaseAgent):
    
    def __init__(self, name, ric, side, size, startTime, endTime, needExecReportEvt=True):
        BaseAgent.__init__(self, name)
        self._parameters = {'Size': size, 'StartTime': datetime.strptime(startTime, "%H:%M:%S").time(),
                            'EndTime': datetime.strptime(endTime, "%H:%M:%S").time(),
                            'Side': side, 'RIC': ric }
        self._nb_order = 0
        self._remaining_qty = self._parameters['Size']
        self.trades = []
        
        self.needExecReportEvt = needExecReportEvt
        
        
    def initialize(self, market, bus):
        """ Initialize method called by C++ Agent """
        BaseAgent.initialize(self, market, bus)
        self._orderbook = self.market.findOrderBook(self._parameters['RIC'])
        if self._orderbook is None:
            raise ValueError("%s orderbook could not be found ..." % self['RIC'])
        
    def averagePrice(self):
        avg = sum(tr.size * tr.price for tr in self.trades)
        
        qty = self['Size'] - self._remaining_qty
        if qty == 0:
            return 0    
        return avg / qty

    def __getitem__(self, key):
        return self._parameters[key]
    
    def processCreateOrder(self, order):
        evt = self._orderbook.processCreateOrder(self._activeOrder)
        if not evt.isOrderProcessed():
            raise RuntimeError("Error while processing create order %s !" % order.orderId)
    
    def _createOrder(self, prefix, side, price, type, qty, execType):
        order = Order()
        
        self._nb_order += 1
        order.orderId = "%s_%i" % (prefix, self._nb_order)
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
    
    def _getOppositeLimit(self, lob, index=0):
        """ Returns a tuple (price, size) of opposite side
            @param lob: Limit Order Book
            @param index: Order place in the lob (default: 0 i.e BestOpposite)  
        """
        if self['Side'] == Order.Buy:
            return (lob.getAsk(index).price, lob.getAsk(index).size)
        else: # Order.Sell
            return (lob.getBid(index).price, lob.getBid(index).size)
        
    def _getLimit(self, lob, index=0):
        """ Returns a tuple (price, size) of our side
            @param lob: Limit Order Book
            @param index: Order place in the lob (default: 0 i.e BestPrice)  
        """
        if self['Side'] == Order.Buy:
            return (lob.getBid(index).price, lob.getBid(index).size)
        else: # Order.Sell
            return (lob.getAsk(index).price, lob.getAsk(index).size)
    
    def _getAvailableQty(self, lob):
        if self['Side'] == Order.Buy:
            lob_func = lob.getAsk
        else:
            lob_func = lob.getBid
        
        return sum(volume for (price, volume) in self._lobGen(lob_func))
            
    
    def _cancelOrder(self, order):
        cancel = self._orderbook.processCancelOrder(order.orderId)
        if not cancel.isOrderProcessed():
            # TODO: If cancel order failed, it should be better handled
            #       Currently, we raise an exception and restore state
            msg = "[BaseAlgo] Cancel order (%s) can't be cancelled!" % order.orderId
#            raise StandardError(msg)
            print msg
        return cancel
    
    def _lobGen (self, lob_func):
        """ Generator function for navigating into the lob
            lob_func : pointer to lob function (getBid or getAsk)
        """
        for i in xrange(5):
            yield lob_func(i).price, lob_func(i).size
        
    def reportGen(self):
        """ Generator function for algo execution reporting
        """
        yield ['deal_id', 'hour', 'price', 'size']
        for t in self.trades:
            yield {'deal_id' : t.orderId1, 'hour': t.hour,
                   'price': t.price, 'size': t.size } 
            
        
