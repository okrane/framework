from simep.sched import *
from simep.lightmodel import *
from simep.pbgmodel import *
from simep.sched import PyAgent
from simep.agents.agent import BaseAgent
from simep.sched import Order

class AgentFindOrders(BaseAgent):
    def __init__(self,name,ric):
        super(AgentFindOrders,self).__init__(name)
        self.ric = ric
        self.nb = 0
        
    def initialize(self, market, bus):
        """ Initialize method called by C++ Agent """
        BaseAgent.initialize(self, market, bus)
        self._orderbook = self.market.findOrderBook(self.ric)
        if self._orderbook is None:
            raise ValueError("%s orderbook could not be found ..." % self.ric)
        
    def process(self, evt):
        self.nb = self.nb + 1
        lob = self._orderbook.getLob()       
        print "%d: %0.3f" %(self.nb,lob.getBid(0).price)       
        os = self.market.findOrders(self.ric,Order.Buy,lob.getBid(0).price)
        n = len(os)
        for i in range(0,n):
            str = "%d : id = %s, price = %0.3f, size = %d" %(self.nb,os[i].orderId,os[i].price,os[i].remain)
            print str
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
pbg = PBGModel('PBG', 'FTE')
sched.addAgent(pbg)
pbg.setLogin('juraz')
pbg.setFull(False)
pbg.setDate('20090202')
pbg.setNbBT(10)
pbg.setInputFileName('C:/histo/lobTrade_26_20090202.binary')
pbg.needExecReportEvt = False
pbg.needAllEvts = False
agent = AgentFindOrders('agent','FTE')
agent.needAllEvts =  True
sched.addAgent(agent)

print 'run'
sched.run()


