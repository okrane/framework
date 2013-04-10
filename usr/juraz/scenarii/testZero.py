from simep.sched import *
from simep.lightmodel import *
from simep.pbgmodel import *
from usr.dev.sivla.agents.LobRecorder import LobRecorder
from simep.sched import PyAgent
from simep.agents.agent import BaseAgent
from simep.sched import Order
from numpy.random import *
import math

class Zero(BaseAgent):
    def __init__(self,name,ric,S0=100.0,psy=1.0,qty=2.30,sigma=1.0,mu=0.25,tick_size=1.0/100.0):
        super(Zero,self).__init__(name)
        self.setIsSimulator(True)
        self.nbOrders = 0
        self.brokerId = 1
        self.ric = ric
        self.time2wakeup = dateNum('09:00:00:000')
        self.S0 = S0
        self.psy = psy
        self.qty = qty
        self.sigma = sigma
        self.mu = mu
        self.round = lambda (x): math.floor(x/tick_size)*tick_size
        
    def initialize(self, market, bus):
        """ Initialize method called by C++ Agent """
        BaseAgent.initialize(self, market, bus)
        self._orderbook = self.market.findOrderBook(self.ric)
        if self._orderbook is None:
            raise ValueError("%s orderbook could not be found ..." % self.ric)
        
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
        order.creationTimeMs = self.time2wakeup
        order.expirationTimeMs = 0                
        return order    
    
    def process(self, evts):
        if(self.nbOrders >= 6120):   
            print "end of simulation..."         
            return True
        epsilon = (2 * randint(2,size=1)[0]) - 1
        if epsilon > 0:
            side = Order.Buy
        else:
            side = Order.Sell
        Y = randn();
        vol = (int)(math.ceil(math.exp(self.sigma * Y + self.qty)))
        X = randn()
        price = self.round(self.S0 + (epsilon * self.psy) - (epsilon * math.exp(self.sigma * X + self.mu)))
        order = self._createOrder(self.name,side,price, Order.Limit, vol, Order.DAY)
        self._orderbook.processCreateOrder(order)
        self.nbOrders = self.nbOrders + 1
        self.time2wakeup = self.time2wakeup + 10000
        return None

# ------------------------------------------------- #
#                Define Variables                   #
# ------------------------------------------------- #
# If auto_run is not set, it means that the script is
# run stand alone
if 'auto_run' not in locals().keys():
        auto_run=True
sched = Scheduler("C:/st_sim/simep/Log",False)
sched.addOrderBook('normal', 'FTE')
agent = Zero('essai','FTE')
sched.addAgent(agent)
record = LobRecorder( '20100625', 110)
sched.addAgent( record)
print 'run'
sched.run()

from usr.dev.sivla.funcs.data.pyData import pyData
record.run_visualization(record.results())

