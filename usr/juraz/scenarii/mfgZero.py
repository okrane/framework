from simep.sched import *
from simep.lightmodel import *
from simep.pbgmodel import *
from simep.mfgen import *
from numpy.random import *
from simep.sched import PyAgent
from simep.agents.baseagent import BaseAgent
from simep.sched import Order
from numpy.oldnumeric.random_array import gamma

class MfgZero(BaseAgent):
    def __init__(self, setup, context, parameters, trace):
        super(MfgZero,self).__init__(setup, context, parameters, trace)
        self.nbOrders = 0
        self.time2wakeup = 0
        self.setIsSimulator(True)
        self.prefix = "relu"
        self.exprice = 0
        self.exspread = 0
        
    def initialize(self, market, bus):
        """ Initialize method called by C++ Agent """
        BaseAgent.initialize(self, market, bus)
        self._orderbook = self.market.findOrderBook(self['ric'])
        if self._orderbook is None:
            print "orderbook could not be found"
            raise ValueError("%s orderbook could not be found ..." % self['ric'])
        self.mfg = MFGen();
        self.sigma = 0.20
        self.sigma_l = 0.06
        self.p_ref = 10.0
        self.l_ref = -100000.0
        self.stats = self.mfg.build(self.sigma,self.sigma_l,self.p_ref,self.l_ref)
        print "mfg built"
        self.fic = open('C:/outputPython50.csv','w')
        self.oldIndex = 0
        self.brokerId = 999
        
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
    
    def pente(self, lob):
        m_bar = 0.0;
        p_bar = 0.0;
        n_bid =0
        n_ask= 0
        for i in range(0,5):
            if (lob.getBid(i).size > 0):
                m_bar = m_bar + lob.getBid(i).size 
                p_bar = p_bar + lob.getBid(i).price
                n_bid = n_bid + 1
            if (lob.getAsk(i).size > 0):
                m_bar = m_bar - lob.getAsk(i).size
                p_bar = p_bar + lob.getAsk(i).price
                n_ask = n_ask + 1
                
        
        m_bar = m_bar / (2*n_bid) 
        p_bar = p_bar / (2*n_ask)
        mid_p = (lob.getBid(0).price + lob.getAsk(0).price)/2.0
        cov = 0.0
        var = 0.0
        x2 = 0.0
        x3 = 0.0
        x4 = 0.0
        fxx = 0.0
        fxx2 = 0.0
        for i in range(0,n_ask):
            cov = cov + (-lob.getAsk(i).size - m_bar) * (lob.getAsk(i).price -p_bar)
            var = var + (lob.getAsk(i).price - p_bar)*(lob.getAsk(i).price - p_bar)
            
        for i in range(0,n_bid):
            cov = cov + (lob.getBid(i).size - m_bar) * (lob.getBid(i).price -p_bar)
            var = var + (lob.getBid(i).price - p_bar)*(lob.getBid(i).price - p_bar)
        
        slope = cov/var       
        return slope   

    def process(self, evt):
        if(self.time2wakeup >= 0.33*3600*1000):
            print "end of simulation..."    
            self.fic.close()     
            return True
        print self.time2wakeup
        tick = 0.005
        if (self.time2wakeup == 0):
            order = self._createOrder( self.prefix, Order.Buy, 10.0-3*tick, Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Buy, 10.0-(4*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Buy, 10.0-(4*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Buy, 10.0-(5*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Buy, 10.0-(5*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Buy, 10.0-(5*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Buy, 10.0-(6*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Buy, 10.0-(6*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Buy, 10.0-(6*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Buy, 10.0-(6*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Buy, 10.0-(7*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Buy, 10.0-(7*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Buy, 10.0-(7*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Buy, 10.0-(7*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Buy, 10.0-(7*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            
            order = self._createOrder( self.prefix, Order.Sell, 10.0+3*tick, Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Sell, 10.0+(4*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Sell, 10.0+(4*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Sell, 10.0+(5*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Sell, 10.0+(5*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Sell, 10.0+(5*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Sell, 10.0+(6*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Sell, 10.0+(6*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Sell, 10.0+(6*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Sell, 10.0+(6*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Sell, 10.0+(7*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Sell, 10.0+(7*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Sell, 10.0+(7*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Sell, 10.0+(7*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            order = self._createOrder( self.prefix, Order.Sell, 10.0+(7*tick), Order.Limit, 500, Order.DAY)
            self._orderbook.processCreateOrder(order)
            self.exprice = 10
            self.exspread = 6*tick;
    
        lob = self._orderbook.getLob()
        midPrice = (lob.getAsk(0).price + lob.getBid(0).price)/2.0
        spread = (lob.getAsk(0).price - lob.getBid(0).price)
        if (lob.getAsk(0).size == 0):
            midPrice = self.exprice + tick
            spread = self.exspread +2*tick
        else:
            if (lob.getBid(0).size == 0):
                midPrice = self.exprice - tick
                spread = self.exspread +2*tick
        evtTime = evt.getEvtTime()
        index = self.time2wakeup / 1000
        p_star = self.stats[index].p
        l_star = self.stats[index].l
        l = self.pente(lob)
        dll = (l_star - l)/l
        
        dp = p_star-midPrice
        if(index > self.oldIndex):
            str = "%0.4f;%0.4f;%0.4f\n" %(midPrice, p_star, dll)
            self.fic.write(str)
            self.oldIndex = index
        pGauche = -6.5* dp + 0.5 # merges proba of Trade touching the bid, Bid Cancel and Ask Insert
        pI = 0.508+0.13*dll
        pC = 0.459-0.13*dll
        pT = 0.033
        u = random()
           
        if (u < pGauche): # trois types d'ordre possibles
            v = random()
            if (v < pI):
               # insert a l'Ask
               sizeI = int(gamma(1.5, 700, 1))
               # Calcul du prix d'insertion
               mean_p = 2.0-3.0*dp
               var_p = 1.3
               k_p = mean_p*mean_p/var_p
               theta_p = var_p/mean_p
               priceI = midPrice-spread/2.0 + spread * gamma(k_p,theta_p,1)
               priceI = tick*int(priceI/tick)
               order = self._createOrder( self.prefix, Order.Sell, priceI, Order.Limit, sizeI, Order.DAY)
               self._orderbook.processCreateOrder(order)
            else:
                if ((v >= pI) & (v<pI+pC)):
                # cancel au Bid
                    mean_p = 1.0+1.0*dp
                    var_p = 1.2
                    k_p = mean_p*mean_p/var_p
                    theta_p = var_p/mean_p
                    priceC = midPrice+spread/2.0 - spread *(1+ gamma(k_p,theta_p,1))
                    priceC = tick*int(priceC/tick)
                                  
                    os = self.market.findOrders(self['ric'],Order.Buy,priceC)
                    n = len(os)
                    dist = 1000000
                    cible = int(gamma(1.5, 700, 1))
                    nearest_i = 0
                    if (n==0):
                        priceC=priceC+tick
                        os = self.market.findOrders(self['ric'],Order.Buy,priceC)
                        n = len(os)
                    if (n==0):
                        priceC=priceC-tick
                        os = self.market.findOrders(self['ric'],Order.Buy,priceC)
                        n = len(os)
                        
                        
                    for i in range(0,n):
                        if ((os[i].brokerId == self.brokerId) & (abs(cible - os[i].remain) < dist)):
                            dist = abs(cible - os[i].remain)
                            nearest_i = i
                    if (n != 0):        
                        self._orderbook.processCancelOrder(os[nearest_i].orderId)
                
                else:
                # Trade touchant le Bid
                    priceT = lob.getBid(0).price
                    sizeT = int(gamma(0.8, 1500, 1))    
                    order = self._createOrder( self.prefix, Order.Sell, priceT, Order.Market, sizeT, Order.DAY)
                    self._orderbook.processCreateOrder(order)

        else:
            v = random()
            if (v < pI):
               # insert au Bid
               sizeI = int(gamma(1.5, 700, 1))
               mean_p = 2.0+3.0*dp
               var_p = 1.3
               k_p = mean_p*mean_p/var_p
               theta_p = var_p/mean_p
               priceI = midPrice+spread/2.0 - spread * gamma(k_p,theta_p,1)
               priceI = tick*int(priceI/tick)
               order = self._createOrder( self.prefix, Order.Buy, priceI, Order.Limit, sizeI, Order.DAY)
               self._orderbook.processCreateOrder(order)
            else:
                if ((v >= pI) & (v<pI+pC)):
                # cancel a l'ask
                    mean_p = 1.0-1.0*dp
                    var_p = 1.2
                    k_p = mean_p*mean_p/var_p
                    theta_p = var_p/mean_p
                    priceC = midPrice-spread/2.0 + spread *(1+ gamma(k_p,theta_p,1))
                    priceC = tick*int(priceC/tick)
                    os = self.market.findOrders(self['ric'],Order.Sell,priceC)
                    n = len(os)
                    dist = 1000000
                    cible = int(gamma(1.5, 700, 1))
                    nearest_i = 0
                    if (n==0):
                        priceC=priceC-tick
                        os = self.market.findOrders(self['ric'],Order.Buy,priceC)
                        n = len(os)
                    if (n==0):
                        priceC=priceC+tick
                        os = self.market.findOrders(self['ric'],Order.Buy,priceC)
                        n = len(os)
                    for i in range(0,n):
                        if ((os[i].brokerId == self.brokerId) & (abs(cible - os[i].remain) < dist)):
                            dist = abs(cible - os[i].remain)
                            nearest_i = i
                    if (n != 0):        
                        self._orderbook.processCancelOrder(os[nearest_i].orderId)
                
                else:
                # Trade touchant l'Ask
                    priceT = lob.getAsk(0).price
                    sizeT = int(gamma(0.8, 1500, 1))    
                    order = self._createOrder( self.prefix, Order.Buy, priceT, Order.Market, sizeT, Order.DAY)
                    self._orderbook.processCreateOrder(order)
         
               
        delta_t = int(round(max(60.0,min(1500.0,exponential(300000.0/(1300.0+140000.0*(p_star - midPrice)*(p_star - midPrice)),1)))))
        self.time2wakeup = self.time2wakeup + delta_t
        print self.time2wakeup
        return None
        
if 'auto_run' not in locals().keys():
        auto_run=True
sched = Scheduler("C:/st_sim/simep/Log",False)
sched.addOrderBook('normal', 'FTE')    

agent = MfgZero({'name':'mfgen001'}, {'ric':'FTE'}, {}, sched.getTrace())
agent.needAllEvts =  False
sched.addAgent(agent)
print 'run2'
sched.run()
print 'ending'