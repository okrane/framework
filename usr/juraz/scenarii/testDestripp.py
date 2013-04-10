from simep.sched import *
from simep.lightmodel import *
from simep.pbgmodel import *
from simep.sched import PyAgent
from simep.agents.agent import BaseAgent
from simep.sched import Order
from simep.sched import Destripp
from time import strftime

class AgentDes(BaseAgent):
    def __init__(self,name,ric,tr):
        super(AgentDes,self).__init__(name,tr)
        self.ric = ric
        self.nb = 0
        
    def initialize(self, market, bus):
        """ Initialize method called by C++ Agent """
        BaseAgent.initialize(self, market, bus)
        self._orderbook = self.market.findOrderBook(self.ric)
        if self._orderbook is None:
            raise ValueError("%s orderbook could not be found ..." % self.ric)
        
    def process(self, evt):
        lob = self._orderbook.getLob()
        print "============= LOB ================"
        for i in range(0,5):
            bid = lob.getBid(i)
            ask = lob.getAsk(i)
            str = "%d\t%0.3f\t%0.3f\t%d" %(bid.size,bid.price,ask.price,ask.size)
            print str
        print "=================================="
        ts = evt.getTrades()
        nts = len(ts)
        if(nts>0):
            str = "******* %d trades **********" %(nts)
            print str
            for i in range(0,nts):
                str = "price:%0.3f,size:%d,overAsk:%d" %(ts[i].price,ts[i].size,ts[i].overAsk)
                print str
            print "*****************************"
        destripp = evt.getDestripp()
        if destripp is None:
            return None
        if(destripp.getUnsure()):
            unsure = "True"
        else:
            unsure = "False"
        evtTime = destripp.getEventTime()
        ms = destripp.getMicroseconds()
        actions = destripp.getActions()
        trades = destripp.getTrades()
        nbActions = len(actions)
        nbTrades = len(trades)
        print "----------------------------------------------------"
        str = "unsure:%s,evtTime:%s,ms:%d,nbActions:%d,nbTrades:%d" %(unsure,evtTime,ms,nbActions,nbTrades)
        print str
        print "actions"
        print "- - - - - - - - - - - - - - - - - - - - - - - - - - "
        for i in range(0,nbActions):
            if(actions[i].side == ACTION.buy):
                side = "buy"
            else:
                side = "sell"
            if(actions[i].type == ACTION.insert):
                type = "insert"
            if(actions[i].type == ACTION.cancel):
                type = "cancel"
            if(actions[i].type == ACTION.trade):
                type = "trade"
            if(actions[i].type == ACTION.update):
                type = "update"
            str = "side:%s,type:%s,price:%0.3f,size:%d" %(side,type,actions[i].price,actions[i].size)
            print str
        print "- - - - - - - - - - - - - - - - - - - - - - - - - - "
        print "trades"
        print "- - - - - - - - - - - - - - - - - - - - - - - - - - "
        for i in range(0,nbTrades):
            str = "price:%0.3f,size:%0.0f,overAsk:%d" %(trades[i].price,trades[i].size,trades[i].overAsk)
            print str
        print "- - - - - - - - - - - - - - - - - - - - - - - - - - "
        return None
        
# ------------------------------------------------- #
#                Define Variables                   #
# ------------------------------------------------- #
# If auto_run is not set, it means that the script is
# run stand alone
if 'auto_run' not in locals().keys():
        auto_run=True
sched = Scheduler("C:/st_sim/simep/Log",False)
sched.addOrderBook('light', 'FTE')   
light = LightModel('LiModl', 'FTE',sched.getTrace(),True) 
light.setSecurity('C:/st_project/tools/TBT2/tick_ged','FTE.PA','20100519',4,'07:02:00','07:25:00')
light.setLogin('LiModl')
light.setDate('20100519')
# 2/ Specific Params
light.setFull(True)
#model_2.setNbBT(10)
light.needExecReportEvt = False
light.needAllEvts = False
sched.addAgent(light)

des = AgentDes('agent','FTE',sched.getTrace())
des.needAllEvts =  True
sched.addAgent(des)

t = strftime("%Y-%m-%d %H:%M:%S")
print t
print 'run'
sched.run()
print 'end'
t = strftime("%Y-%m-%d %H:%M:%S")
print t

