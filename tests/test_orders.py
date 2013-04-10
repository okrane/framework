import unittest

import simep.sched
import simep.agents
from simep.sched import OrderBook
from simep.sched import Order
from simep.sched import Trade
from simep.sched import Event


class TestMultiOrder(unittest.TestCase):

    def createTrade(self,id1,id2,price,qty,hour,over):
        trade = Trade()
        trade.orderId1 = id1
        trade.orderId2 = id2
        trade.price = price
        trade.size = qty
        trade.hour = hour
        trade.overAsk = over
        return trade

    def createGenericOrder(self,orderId,side,price,qty,type,execType,expirationTime):
        order = Order()
        order.orderId = orderId
        order.side = side
        order.price = price

        if type == Order.Stop or type == Order.StopLimit :
            order.stopPrice = price

        order.initialQty = qty
        order.shownQty = qty
        order.remain = qty
        order.orderType = type
        order.creationTimeMs = 0
        order.execType = execType
        order.expirationTimeMs = expirationTime
        order.minQty = 0
        return order
    
    def createOrder(self,orderId,side,price,qty,type):
        return self.createGenericOrder(orderId,side,price,qty,type,Order.DAY,0)

    def createMinQtyOrder(self,orderId,side,price,qty,minQty,type):
        order = self.createOrder(orderId,side,price,qty,type)
        order.minQty = minQty
        return order
    
    def createIcebergOrder(self,orderId,side,price,qty,shownQty,type):
        order = self.createOrder(orderId,side,price,qty,type)
        order.shownQty = shownQty
        order.remain = shownQty
        order.hiddenQty = qty - shownQty
        return order

#####################
##Tests
#####################
    def test_LimitOrders1(self):
        # create an empty order book
        order_book = OrderBook("OB")
        
        # add an ask order in the order book
        order = self.createOrder('ASK1',Order.Sell,10,250,Order.Limit)
        evt = order_book.processCreateOrder(order)
        
        # try to make a BID deal on the ASK order
        bid_order = self.createOrder('BID1',Order.Buy,30,100,Order.Limit)
        trade = order_book.processCreateOrder(bid_order).getTrades()[0]
        
        ref_Trade = self.createTrade('ASK1','BID1',10,100,trade.hour,1)
            
        # compare the results
        self.assert_(trade==ref_Trade)
                
    def test_LimitOrders2(self):
        # create an empty order book
        order_book = OrderBook("OB")
        
        # add an ask order in the order book
        order = self.createOrder('BID1',Order.Buy,40,250,Order.Limit)
        evt = order_book.processCreateOrder(order)
        
        # try to make a BID deal on the ASK order
        ask_order = self.createOrder('ASK1',Order.Sell,30,100,Order.Limit)
        trade = order_book.processCreateOrder(ask_order).getTrades()[0]

        ref_Trade = self.createTrade('BID1','ASK1',40,100,trade.hour,0)
            
        # compare the results
        self.assert_(trade==ref_Trade)
        
    def test_StopOrder1(self):
        # create an empty order book
        order_book = OrderBook("OB")
        
        # add a bid order in the order book
        order = self.createOrder('BID1',Order.Buy,12,50,Order.Limit)
        evt = order_book.processCreateOrder(order)
        
        # add a bid stop order in the order book
        order = self.createOrder('BID2',Order.Buy,14,100,Order.Stop)
        evt = order_book.processCreateOrder(order)
        
        # add an ask order in the order book
        order = self.createOrder('ASK1',Order.Sell,14,100,Order.Limit)
        evt = order_book.processCreateOrder(order)
        nbTrades = len(evt.getTrades())
        
        # no order should be executed
        if nbTrades == 0 :
            self.assert_(True)
        else:
            self.assert_(False)

    def test_StopOrder2(self):
        # create an empty order book
        order_book = OrderBook("OB")
        
        # add a bid stop order in the order book
        order = self.createOrder('BID1',Order.Buy,14,100,Order.Stop)
        evt = order_book.processCreateOrder(order)

        # add a bid order in the order book
        order = self.createOrder('BID2',Order.Buy,12,50,Order.Limit)
        evt = order_book.processCreateOrder(order)
        
        # add an ask order in the order book
        order = self.createOrder('ASK1',Order.Sell,12,100,Order.Limit)
        evt = order_book.processCreateOrder(order)
        nbTrades = len(evt.getTrades())
        
        # 1 trade should be executed
        if nbTrades == 1 :
            trade = evt.getTrades()[0]
            ref_Trade = self.createTrade('BID2','ASK1',12,50,trade.hour,0)
            self.assert_(trade==ref_Trade)
        else:
            self.assert_(False)
            
    def test_StopOrder3(self):
        # create an empty order book
        order_book = OrderBook("OB")
        
        # add a bid stop order in the order book
        order = self.createOrder('BID1',Order.Buy,14,100,Order.Stop)
        evt = order_book.processCreateOrder(order)

        # add a bid order in the order book
        order = self.createOrder('BID2',Order.Buy,14,50,Order.Limit)
        evt = order_book.processCreateOrder(order)
        
        # add an ask order in the order book
        order = self.createOrder('ASK1',Order.Sell,14,100,Order.Limit)
        evt = order_book.processCreateOrder(order)
        nbTrades = len(evt.getTrades())
        
        # 2 trades should be generated
        if nbTrades == 2 :
            trade1 = evt.getTrades()[0]
            trade2 = evt.getTrades()[1]
            ref_Trade1 = self.createTrade('BID2','ASK1',14,50,trade1.hour,0)
            ref_Trade2 = self.createTrade('ASK1','BID1',14,50,trade2.hour,1)
            self.assert_(trade1==ref_Trade1 and trade2==ref_Trade2)
        else:
            self.assert_(False)

    def test_StopOrder4(self):
        # create an empty order book
        order_book = OrderBook("OB")

        # add an ask order in the order book
        order = self.createOrder('ASK1',Order.Sell,16,50,Order.Limit)
        evt = order_book.processCreateOrder(order)
        
        # add a bid order in the order book
        order = self.createOrder('BID1',Order.Buy,16,100,Order.Limit)
        evt = order_book.processCreateOrder(order)
        
        # add an ask stop order in the order book
        order = self.createOrder('ASK2',Order.Sell,15,100,Order.Stop)
        evt = order_book.processCreateOrder(order)

        # add an ask order in the order book
        order = self.createOrder('ASK3',Order.Sell,15,100,Order.Limit)
        evt = order_book.processCreateOrder(order)
        
        # add a bid order in the order book
        order = self.createOrder('BID2',Order.Buy,15,100,Order.Limit)
        evt = order_book.processCreateOrder(order)
        nbTrades = len(evt.getTrades())
        
        # 2 trades should be generated
        if nbTrades == 2 :
            trade1 = evt.getTrades()[0]
            trade2 = evt.getTrades()[1]
            ref_Trade1 = self.createTrade('ASK3','BID2',15,50,trade1.hour,1)
            ref_Trade2 = self.createTrade('BID2','ASK2',15,50,trade2.hour,0)
            self.assert_(trade1==ref_Trade1 and trade2==ref_Trade2)
        else:
            self.assert_(False)
        
    def test_CancelLimitOrder(self):
        # create an empty order book
        order_book = OrderBook("OB")
        
        # add an ask order in the order book
        order1 = self.createOrder('ASK1',Order.Sell,10,250,Order.Limit)
        evt = order_book.processCreateOrder(order1)
        
        # add an ask order in the order book
        order2 = self.createOrder('ASK2',Order.Sell,10,250,Order.Limit)
        evt = order_book.processCreateOrder(order2)
        
        # try to remove an order
        evt = order_book.processCancelOrder(order1.orderId)
        
        self.assert_(evt.isOrderProcessed())
        
    def test_CancelStopOrder(self):
        # create an empty order book
        order_book = OrderBook("OB")
        
        # add a bid stop order in the order book
        order1 = self.createOrder('BID',Order.Buy,14,100,Order.Stop)
        evt = order_book.processCreateOrder(order1)
        
         # add an ask stop order in the order book
        order2 = self.createOrder('ASK',Order.Sell,14,100,Order.Stop)
        evt = order_book.processCreateOrder(order2)
        
        # try to remove an order
        evt = order_book.processCancelOrder(order1.orderId)

        self.assert_(evt.isOrderProcessed())

    def test_CancelStopLimitOrder(self):
        # create an empty order book
        order_book = OrderBook("OB")
        
        # add a bid stop order in the order book
        order1 = self.createOrder('BID',Order.Buy,14,100,Order.StopLimit)
        evt = order_book.processCreateOrder(order1)
        
         # add an ask stop order in the order book
        order2 = self.createOrder('ASK',Order.Sell,14,100,Order.StopLimit)
        evt = order_book.processCreateOrder(order2)
        
        # try to remove an order
        evt = order_book.processCancelOrder(order1.orderId)

        self.assert_(evt.isOrderProcessed())
        
    def test_CancelMarketOrder(self):
        # create an empty order book
        order_book = OrderBook("OB")
        
        # add a BID market order in the order book
        order1 = self.createOrder('BID1',Order.Buy,0,100,Order.Market)
        evt = order_book.processCreateOrder(order1)
        
        # try to remove an order
        evt = order_book.processCancelOrder(order1.orderId)

        self.assert_(evt.isOrderProcessed())

    def test_CancelMarketToLimitOrder(self):
        # create an empty order book
        order_book = OrderBook("OB")
        
        # add a BID market order in the order book
        order1 = self.createOrder('BID1',Order.Buy,0,100,Order.MarketToLimit)
        evt = order_book.processCreateOrder(order1)
        
        # try to remove an order
        evt = order_book.processCancelOrder(order1.orderId)

        self.assert_(evt.isOrderProcessed())
        
    def test_ReplaceLimitOrder(self):
        # create an empty order book
        order_book = OrderBook("OB")

        # add an ASK order in the order book
        order1 = self.createOrder('ASK1',Order.Sell,10,250,Order.Limit)
        evt = order_book.processCreateOrder(order1)

        # create an ASK order
        order2 = self.createOrder('ASK2',Order.Sell,10,250,Order.Limit)

        # try to replace the order 1
        evt = order_book.processReplaceOrder(order1.orderId, order2)
        
        self.assert_(evt.isOrderProcessed())
        
    def test_ReplaceStopOrder(self):
        # create an empty order book
        order_book = OrderBook("OB")
        
        # add a BID stop order in the order book
        order1 = self.createOrder('BID1',Order.Buy,14,100,Order.Stop)
        evt = order_book.processCreateOrder(order1)
        
        # add an ASK stop order in the order book
        order2 = self.createOrder('BID2',Order.Buy,5,100,Order.Stop)
        
        # try to replace the order 1
        evt = order_book.processReplaceOrder(order1.orderId, order2)

        self.assert_(evt.isOrderProcessed())
        
    def test_ReplaceStopLimitOrder(self):
        # create an empty order book
        order_book = OrderBook("OB")
        
        # add a bid stop order in the order book
        order1 = self.createOrder('BID1',Order.Buy,14,100,Order.StopLimit)
        evt = order_book.processCreateOrder(order1)
        
        # add an ASK stop order in the order book
        order2 = self.createOrder('BID2',Order.Buy,5,100,Order.StopLimit)
        
        # try to replace the order 1
        evt = order_book.processReplaceOrder(order1.orderId, order2)

        self.assert_(evt.isOrderProcessed())
        
    def test_ReplaceMarketOrder(self):
        # create an empty order book
        order_book = OrderBook("OB")
        
        # add a bid market order in the order book
        order1 = self.createOrder('BID1',Order.Buy,0,100,Order.Market)
        evt = order_book.processCreateOrder(order1)
        
        # create a bid market order in the order book
        order2 = self.createOrder('BID2',Order.Buy,0,200,Order.Market)
        
        # try to replace the order 1
        evt = order_book.processReplaceOrder(order1.orderId, order2)

        self.assert_(evt.isOrderProcessed())
        
    def test_ReplaceMarketToLimitOrder(self):
        # create an empty order book
        order_book = OrderBook("OB")
        
        # add a bid market order in the order book
        order1 = self.createOrder('BID1',Order.Buy,0,100,Order.MarketToLimit)
        evt = order_book.processCreateOrder(order1)
        
        # create a bid market order in the order book
        order2 = self.createOrder('BID2',Order.Buy,0,200,Order.MarketToLimit)
        
        # try to replace the order 1
        evt = order_book.processReplaceOrder(order1.orderId, order2)

        self.assert_(evt.isOrderProcessed())
        
    def test_StopLimitOrder1(self):
        # create an empty order book
        order_book = OrderBook("OB")
        
        # add a bid order in the order book
        order = self.createOrder('BID1',Order.Buy,12,50,Order.Limit)
        evt = order_book.processCreateOrder(order)
        
        # add a bid stop order in the order book
        order = self.createOrder('BID2',Order.Buy,14,100,Order.StopLimit)
        evt = order_book.processCreateOrder(order)
        
        # add an ask order in the order book
        order = self.createOrder('ASK1',Order.Sell,14,200,Order.Limit)
        evt = order_book.processCreateOrder(order)
        nbTrades = len(evt.getTrades())
        
        # no order should be executed
        if nbTrades == 0 :
            self.assert_(True)
        else:
            self.assert_(False)
            
    def test_StopLimitOrder2(self):
        # create an empty order book
        order_book = OrderBook("OB")
        
        # add a bid stop limit order in the order book
        order = self.createOrder('BID1',Order.Buy,14,100,Order.StopLimit)
        evt = order_book.processCreateOrder(order)

        # add a bid order in the order book
        order = self.createOrder('BID2',Order.Buy,12,50,Order.Limit)
        evt = order_book.processCreateOrder(order)
        
        # add an ask order in the order book
        order = self.createOrder('ASK1',Order.Sell,12,100,Order.Limit)
        evt = order_book.processCreateOrder(order)
        nbTrades = len(evt.getTrades())
        
        # 1 trade should be executed
        if nbTrades == 1 :
            trade = evt.getTrades()[0]
            ref_Trade = self.createTrade('BID2','ASK1',12,50,trade.hour,0)
            self.assert_(trade==ref_Trade)
        else:
            self.assert_(False)
            
    def test_StopLimitOrder3(self):
        # create an empty order book
        order_book = OrderBook("OB")
        
        # add a bid stop order in the order book
        order = self.createOrder('BID1',Order.Buy,14,100,Order.StopLimit)
        evt = order_book.processCreateOrder(order)

        # add a bid order in the order book
        order = self.createOrder('BID2',Order.Buy,15,50,Order.Limit)
        evt = order_book.processCreateOrder(order)
        
        # add an ask order in the order book
        order = self.createOrder('ASK1',Order.Sell,14,100,Order.Limit)
        evt = order_book.processCreateOrder(order)
        nbTrades = len(evt.getTrades())
        
        # 2 trades should be generated
        if nbTrades == 2 :
            trade1 = evt.getTrades()[0]
            trade2 = evt.getTrades()[1]
            ref_Trade1 = self.createTrade('BID2','ASK1',15,50,trade1.hour,0)
            ref_Trade2 = self.createTrade('ASK1','BID1',14,50,trade2.hour,1)
            self.assert_(trade1==ref_Trade1 and trade2==ref_Trade2)
        else:
            self.assert_(False)

    def test_StopLimitOrder4(self):
        # create an empty order book
        order_book = OrderBook("OB")

        # add an ask order in the order book
        order = self.createOrder('ASK1',Order.Sell,16,50,Order.Limit)
        evt = order_book.processCreateOrder(order)
        
        # add a bid order in the order book
        order = self.createOrder('BID1',Order.Buy,16,100,Order.Limit)
        evt = order_book.processCreateOrder(order)
        
        # add an ask stop order in the order book
        order = self.createOrder('ASK2',Order.Sell,15,100,Order.StopLimit)
        evt = order_book.processCreateOrder(order)

        # add an ask order in the order book
        order = self.createOrder('ASK3',Order.Sell,15,100,Order.Limit)
        evt = order_book.processCreateOrder(order)
        
        # add an bid order in the order book
        order = self.createOrder('BID2',Order.Buy,15,100,Order.Limit)
        evt = order_book.processCreateOrder(order)
        nbTrades = len(evt.getTrades())
        
        # 2 trades should be generated
        if nbTrades == 2 :
            trade1 = evt.getTrades()[0]
            trade2 = evt.getTrades()[1]
            ref_Trade1 = self.createTrade('ASK3','BID2',15,50,trade1.hour,1)
            ref_Trade2 = self.createTrade('BID2','ASK2',15,50,trade2.hour,0)
            self.assert_(trade1==ref_Trade1 and trade2==ref_Trade2)
        else:
            self.assert_(False)
            
    def test_MarketToLimitOrder1(self):
        # create an empty order book
        order_book = OrderBook("OB")

        # add a bid order in the order book
        order = self.createOrder('BID1',Order.Buy,15,100,Order.Limit)
        evt = order_book.processCreateOrder(order)
        
        # add an ask market to limit order in the order book
        order = self.createOrder('ASK1',Order.Sell,0,50,Order.MarketToLimit)
        evt = order_book.processCreateOrder(order)
        nbTrades = len(evt.getTrades())
        
        if nbTrades == 1 :
            trade1 = evt.getTrades()[0]
            ref_Trade1 = self.createTrade('BID1','ASK1',15,50,trade1.hour,0)
            self.assert_(trade1==ref_Trade1)
        else:
            self.assert_(False)
        
    def test_MarketToLimitOrder2(self):
        # create an empty order book
        order_book = OrderBook("OB")
       
        # add a bid order in the order book
        order = self.createOrder('ASK1',Order.Sell,15,100,Order.Limit)
        evt = order_book.processCreateOrder(order)
        
        # add a bid order in the order book
        order = self.createOrder('ASK2',Order.Sell,10,100,Order.Limit)
        evt = order_book.processCreateOrder(order)
        
        # add a bid market to limit order in the order book
        order = self.createOrder('BID1',Order.Buy,0,100,Order.MarketToLimit)
        evt = order_book.processCreateOrder(order)
        nbTrades = len(evt.getTrades())
         
        if nbTrades == 1 :
            trade1 = evt.getTrades()[0]
            ref_Trade1 = self.createTrade('ASK2','BID1',10,100,trade1.hour,1)
            self.assert_(trade1==ref_Trade1)
        else:
            self.assert_(False)
            
    def test_MarketOrder1(self):
        # create an empty order book
        order_book = OrderBook("OB")

        # add a bid order in the order book
        order = self.createOrder('BID1',Order.Buy,15,100,Order.Limit)
        evt = order_book.processCreateOrder(order)
        
        # add an ask market to limit order in the order book
        order = self.createOrder('ASK1',Order.Sell,0,50,Order.Market)
        evt = order_book.processCreateOrder(order)
        nbTrades = len(evt.getTrades())
        
        if nbTrades == 1 :
            trade1 = evt.getTrades()[0]
            ref_Trade1 = self.createTrade('BID1','ASK1',15,50,trade1.hour,0)
            self.assert_(trade1==ref_Trade1)
        else:
            self.assert_(False)
            
    def test_MarketOrder2(self):
        # create an empty order book
        order_book = OrderBook("OB")

        # add a ask order in the order book
        order = self.createOrder('ASK1',Order.Sell,15,100,Order.Limit)
        evt = order_book.processCreateOrder(order)
        
        # add an ask market to limit order in the order book
        order = self.createOrder('BID1',Order.Buy,0,50,Order.Market)
        evt = order_book.processCreateOrder(order)
        nbTrades = len(evt.getTrades())
        
        if nbTrades == 1 :
            trade1 = evt.getTrades()[0]
            ref_Trade1 = self.createTrade('ASK1','BID1',15,50,trade1.hour,1)
            self.assert_(trade1==ref_Trade1)
        else:
            self.assert_(False)

    def test_IceberOrder1(self):
        # create an empty order book
        order_book = OrderBook("OB")

        # add an iceberg bid order in the order book
        order = self.createIcebergOrder('BID',Order.Buy,15,100,50,Order.Limit)
        evt = order_book.processCreateOrder(order)
        
        # add an ask order in the order book
        order = self.createOrder('ASK',Order.Sell,15,80,Order.Limit)
        evt = order_book.processCreateOrder(order)
        nbTrades = len(evt.getTrades())
        
        if nbTrades == 1 :
            trade1 = evt.getTrades()[0]
            ref_Trade1 = self.createTrade('BID','ASK',15,80,trade1.hour,0)
            self.assert_(trade1==ref_Trade1)
        else:
            self.assert_(False)

    def test_IceberOrder2(self):
        # create an empty order book
        order_book = OrderBook("OB")

        # add an iceberg bid order in the order book
        order = self.createIcebergOrder('BID',Order.Buy,15,100,50,Order.Limit)
        evt = order_book.processCreateOrder(order)
        
        # add an ask order in the order book
        order = self.createOrder('ASK',Order.Sell,15,120,Order.Limit)
        evt = order_book.processCreateOrder(order)
        nbTrades = len(evt.getTrades())
        
        if nbTrades == 1 :
            trade1 = evt.getTrades()[0]
            ref_Trade1 = self.createTrade('BID','ASK',15,100,trade1.hour,0)
            self.assert_(trade1==ref_Trade1)
        else:
            self.assert_(False)

    def test_IceberOrder3(self):
        # create an empty order book
        order_book = OrderBook("OB")

        # add an ask order in the order book
        order = self.createOrder('ASK',Order.Sell,15,80,Order.Limit)
        evt = order_book.processCreateOrder(order)
        
        # add an iceberg bid order in the order book
        order = self.createIcebergOrder('BID',Order.Buy,15,100,50,Order.Limit)
        evt = order_book.processCreateOrder(order)
        nbTrades = len(evt.getTrades())
        
        if nbTrades == 1 :
            trade1 = evt.getTrades()[0]
            ref_Trade1 = self.createTrade('ASK','BID',15,80,trade1.hour,1)
            self.assert_(trade1==ref_Trade1)
        else:
            self.assert_(False)

    def test_IceberOrder4(self):
        # create an empty order book
        order_book = OrderBook("OB")

        # add an ask order in the order book
        order = self.createOrder('ASK',Order.Sell,15,120,Order.Limit)
        evt = order_book.processCreateOrder(order)
        
        # add an iceberg bid order in the order book
        order = self.createIcebergOrder('BID',Order.Buy,15,100,50,Order.Limit)
        evt = order_book.processCreateOrder(order)
        nbTrades = len(evt.getTrades())
        
        if nbTrades == 1 :
            trade1 = evt.getTrades()[0]
            ref_Trade1 = self.createTrade('ASK','BID',15,100,trade1.hour,1)
            self.assert_(trade1==ref_Trade1)
        else:
            self.assert_(False)

    def test_MinQtyOrder(self):
        # create an empty order book
        order_book = OrderBook("OB")

        # add an ask order in the order book
        order = self.createOrder('ASK',Order.Sell,15,200,Order.Limit)
        evt = order_book.processCreateOrder(order)

        # add a bid order with a minimal quantity in the order book
        order = self.createMinQtyOrder('BID',Order.Buy,15,300,210,Order.Limit)
        evt = order_book.processCreateOrder(order)

        nbTrades = len(evt.getTrades())
        
        if nbTrades > 0 :
            self.assert_(False)
        else:
            self.assert_(True)

    def test_MinQtyOrder2(self):
        # create an empty order book
        order_book = OrderBook("OB")

        # add an ask order in the order book
        order = self.createOrder('ASK',Order.Sell,15,200,Order.Limit)
        evt = order_book.processCreateOrder(order)

        # add a bid order with a minimal quantity in the order book
        order = self.createMinQtyOrder('BID',Order.Buy,15,300,200,Order.Limit)
        evt = order_book.processCreateOrder(order)

        nbTrades = len(evt.getTrades())
        
        if nbTrades == 1 :
            trade1 = evt.getTrades()[0]
            ref_Trade1 = self.createTrade('ASK','BID',15,200,trade1.hour,1)
            self.assert_(trade1==ref_Trade1)
        else:
            self.assert_(False)

    def test_MinQtyOrder3(self):
        # create an empty order book
        order_book = OrderBook("OB")

        # add an ask order in the order book
        order = self.createOrder('ASK',Order.Sell,15,300,Order.Limit)
        evt = order_book.processCreateOrder(order)

        # add a bid order with a minimal quantity in the order book
        order = self.createMinQtyOrder('BID',Order.Buy,15,300,200,Order.Limit)
        evt = order_book.processCreateOrder(order)

        nbTrades = len(evt.getTrades())
        
        if nbTrades == 1 :
            trade1 = evt.getTrades()[0]
            ref_Trade1 = self.createTrade('ASK','BID',15,300,trade1.hour,1)
            self.assert_(trade1==ref_Trade1)
        else:
            self.assert_(False)

    def test_MinQtyOrder4(self):
        # create an empty order book
        order_book = OrderBook("OB")

        # add an ask order in the order book
        order = self.createOrder('ASK',Order.Sell,15,300,Order.Limit)
        evt = order_book.processCreateOrder(order)

        # add a bid order with a minimal quantity in the order book
        order = self.createMinQtyOrder('BID',Order.Buy,15,300,200,Order.Market)
        evt = order_book.processCreateOrder(order)

        nbTrades = len(evt.getTrades())
        
        if nbTrades > 0 :
            self.assert_(False)
        else:
            self.assert_(True)

    def test_MinQtyOrder5(self):
        # create an empty order book
        order_book = OrderBook("OB")

        # add a bid order in the order book
        order = self.createOrder('BID',Order.Buy,15,200,Order.Limit)
        evt = order_book.processCreateOrder(order)

        # add an ask order with a minimal quantity in the order book
        order = self.createMinQtyOrder('ASK',Order.Sell,15,300,200,Order.Limit)
        evt = order_book.processCreateOrder(order)

        nbTrades = len(evt.getTrades())
        
        if nbTrades == 1 :
            trade1 = evt.getTrades()[0]
            ref_Trade1 = self.createTrade('BID','ASK',15,200,trade1.hour,0)
            self.assert_(trade1==ref_Trade1)
        else:
            self.assert_(False)

    def test_MinQtyOrder6(self):
        # create an empty order book
        order_book = OrderBook("OB")

        # add a bid order in the order book
        order = self.createOrder('BID',Order.Buy,15,200,Order.Limit)
        evt = order_book.processCreateOrder(order)

        # add an ask order with a minimal quantity in the order book
        order = self.createMinQtyOrder('ASK',Order.Sell,0,300,200,Order.MarketToLimit)
        evt = order_book.processCreateOrder(order)

        nbTrades = len(evt.getTrades())
        
        if nbTrades == 1 :
            trade1 = evt.getTrades()[0]
            ref_Trade1 = self.createTrade('BID','ASK',15,200,trade1.hour,0)
            self.assert_(trade1==ref_Trade1)
        else:
            self.assert_(False)

    def test_TIF_DAY(self):
        # create an empty order book
        order_book = OrderBook("OB")

        # add an ask order in the order book, this one will have
        # an expiration time of 1 day after calling the processCreateOrder function.
        order = self.createGenericOrder('ASK1',Order.Sell,10,250,Order.Limit,Order.DAY,86400000)
        evt = order_book.processCreateOrder(order)

        # set the time over the order expiration day time
        order_book.currentTime = 86400001

        # the order has been canceled
        if order_book.checkTimeInForce() == 1: self.assert_(True)
        else: self.assert_(False)

    def test_TIF_GTD(self):
        # create an empty order book
        order_book = OrderBook("OB")

        # set the start time before 1h00 am
        order_book.currentTime = 3500000

        # add an ask order in the order book with a good till time TIF, this one have
        # an expiration time of 1h00 am
        order = self.createGenericOrder('ASK1',Order.Sell,10,250,Order.Limit,Order.GTD,3600000)
        evt = order_book.processCreateOrder(order)

        if order_book.checkTimeInForce() == 0:
            # the current time is now 1h00 am, the order has to
            # be removed from the order book
            order_book.currentTime = 3600000

            if order_book.checkTimeInForce() == 1: self.assert_(True)
            else: self.assert_(True)
        else: self.assert_(False)

    def test_TIF_GTC(self):
        # create an empty order book
        order_book = OrderBook("OB")

        # set the start time before 1h00 am
        order_book.currentTime = 3500000

        # add an ask order in the order book with a good till cancel TIF, this one have
        # no expiration time
        order = self.createGenericOrder('ASK1',Order.Sell,10,250,Order.Limit,Order.GTC,0)
        evt = order_book.processCreateOrder(order)

        if order_book.checkTimeInForce() == 0:
            # the current time is now 1h00 am, the order can't
            # be removed until the user cancel it
            order_book.currentTime = 3600000

            if order_book.checkTimeInForce() == 1: self.assert_(False)
            else: self.assert_(True)
        else: self.assert_(False)

    def test_TIF_FOK(self):
        # create an empty order book
        order_book = OrderBook("OB")

        # create a simple bid order in the order book
        order = self.createOrder('BID1',Order.Buy,10,200,Order.Limit)
        order_book.processCreateOrder(order)
        
        # add an ask order in the order book with a fill or kill TIF, this one have
        # no expiration time. Here the order can be executed completely
        order = self.createGenericOrder('ASK1',Order.Sell,10,150,Order.Limit,Order.FOK,0)
        trade = order_book.processCreateOrder(order).getTrades()[0]

        ref_Trade = self.createTrade('BID1','ASK1',10,150,trade.hour,0)
            
        # compare the results
        self.assert_(trade==ref_Trade)

    def test_TIF_IOC(self):
        # create an empty order book
        order_book = OrderBook("OB")

        # create a simple bid order in the order book
        order = self.createOrder('BID1',Order.Buy,10,200,Order.Limit)
        order_book.processCreateOrder(order)
        
        # add an ask order in the order book with a immediate or cancel TIF, this one have
        # no expiration time. Here the order should be patially executed and canceled.
        order = self.createGenericOrder('ASK1',Order.Sell,10,250,Order.Limit,Order.IOC,0)
        trade = order_book.processCreateOrder(order).getTrades()[0]

        ref_Trade = self.createTrade('BID1','ASK1',10,200,trade.hour,0)
        
        if trade == ref_Trade:
            if order_book.checkTimeInForce() == 1:
                o = order_book.findOrder(order.orderId)
                if o == None: self.assert_(True)
                else: self.assert_(False)
            else: self.assert_(False)
        else: self.assert_(False)


        

