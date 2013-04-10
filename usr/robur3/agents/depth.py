from basealgo import BaseAlgo
from simep.sched import Order, Trade
from simep.tools import date2time
from simep.sched import numDate


class Depth(BaseAlgo):
    """ Agent implementing Hunt Algorithm """
    
    def __init__(self, name, ric, side, size, startTime, endTime ):
        BaseAlgo.__init__(self, name, ric, side, size, startTime, endTime, True)
                
    def process(self, evt):
        trades = evt.getTrades()
        if (len(trades) > 0):
            lob = self._orderbook.getLob()                
            currentTime = date2time(lob.evtTime)
            order = self._createOrder(Order.Buy,  self._parameters['Size'])
            self._orderbook.processCreateOrder(order)
            order = self._createOrder(Order.Sell,  self._parameters['Size'])
            self._orderbook.processCreateOrder(order)
        return None
    
    def processReport(self, event):
        """ Called by the Scheduler on each trade executed """
        t = Trade()
        t.price = event.price
        t.orderId1 = event.orderId
        t.size = event.size
        t.hour = numDate(event.orderSnapshot.creationTimeMs)
        self.trades.append(t)
        
        return None
    
    def __str__(self):
        return ('Depth Algorithm (' + self.name + ')\n'
                'From %(StartTime)s to %(EndTime)s \n'
                '%(Size)i %(RIC)s \n'
                'Remaining: %(Remaining)i  Avg Price: %(AvgPrice).4f'
                % (dict(self._parameters.items() + 
                        {'Remaining': self._remaining_qty,
                         'AvgPrice': self.averagePrice()}.items())))
        
    def _createOrder(self, side, qty):
        
        order = Order()
        
        self._nb_order += 1
        order.orderId = "Depth_%i" % (self._nb_order)
        order.brokerId = self.brokerId
        
        order.side = side
        order.orderType = Order.Market # Limit, Stop, Market, MarketToLimit ...
        order.execType = Order.DAY # DAY, GTD, GTC, IOC, FOK
        
        # By default, it not a iceberg order
        order.remain = qty
        order.initialQty = qty
        order.shownQty = qty
        order.minQty = 0
        
        # Default: no expiration
        order.creationTimeMs = 0
        order.expirationTimeMs = 0
        order.needExecReport = True
        
        return order
        
if __name__ == "__main__":
#    event = Event2()
#    event.brokerId = 1
#    event.toto()
#    print event.brokerId
#    
#    a = Agent2()
#    a.name = "toto"
#    a.brokerId = 10
#    a.needAllEvts = True
#    a.needExecReportEvt = True
#    
#    print a
    #algo = BaseAgent("toto")
    
    algo = Depth ('Test', 'FTE.PA', Order.Buy, 100000, '09:00:00', '15:30:00')
    print algo   
        
        
