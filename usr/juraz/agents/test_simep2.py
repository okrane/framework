'''
Created on 4 mai 2010

@author: juraz
'''

from simep.pbgmodel import PBGModel
from simep.sched import Order, Scheduler
from simep.rfaagent import RFAAgent

def test_simep():
    """ Test simep apophis with rfa """
    sched = Scheduler(True)
    sched.addOrderBook('normal', 'FR0000131104')
    pbg = PBGModel('PBG', 'FR0000131104')
    sched.addAgent(pbg)
    pbg.setLogin('juraz')
    pbg.setFull(False)
    pbg.setDate('20090202')
    pbg.setNbBT(10)
    pbg.setInputFileName('C:/histo/lobTrade_26_20090202.binary')
    pbg.needExecReportEvt = False
    pbg.needAllEvts = False
    
    agent = RFAAgent()
    agent.addSecurity(
        RIC="FR0000131104",
        RFA="BNP",
        securityId=110,
        tradingDestinationId=4,
        inputFileName="")
    agent.initRFA(r"C:/st_sim/dev/usr/juraz/agents/slcPublisher.log","topcac","padev016")
    agent.startRFA()
    sched.addAgent(agent)    
    sched.run()

if __name__ == '__main__':
    test_simep()

