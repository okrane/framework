from depth import Depth
from simep.pbgmodel import PBGModel
from simep.agents.observer import Observer
from simep.sched import Order, Scheduler
from simep.lightmodel import LightModel
from simep.funcs.stdio.utils import writeCSV
from simep.funcs.stdio.sqlite import sqliteWriter
#from simep import utils


def test_depth():
    """ Simple Depth test case """
    sched = Scheduler()
    #sched.setRandomGenerator(42)
    sched.addOrderBook('light', 'FTE.PA')
    
    light = LightModel('LiModl', 'FTE.PA')
    sched.addAgent(light)
    
    light.setLogin('juraz')
    light.setFull(True)
    light.setDate('20090202')
    light.setInputFileName('C:/histo/lobTrade_26_20090202.binary')
    light.needExecReportEvt = False
    light.needAllEvts = False
        
#    light = LightModel('LiModl', 'FTE.PA')
#    sched.addAgent(light)
#        
#    light.setLogin('juraz')
#    light.setFull(True)
#    light.setDate('20090202')
#    light.setInputFileName('C:/histo/lobTrade_26_20090202.binary')
#    light.needExecReportEvt = False
#    light.needAllEvts = False
    
#    pbg2 = PBGModel('PBG2', 'FTE.PA')
#    sched.addAgent(pbg2)
#    
#    pbg2.setLogin('juraz2')
#    pbg2.setFull(False)
#    pbg2.setDate('20090202')
#    pbg2.setNbBT(10)
#    pbg2.setInputFileName('C:/histo/lobTrade_26_4_20100129.binary')
#    pbg2.needExecReportEvt = False
#    pbg2.needAllEvts = False
#    
    
    depth1 = Depth('Test', 'FTE.PA', Order.Buy, 1000000, '00:00:00', '23:59:59')
    sched.addAgent(depth1)
    #hunt2 = Hunt('FTE.PA', Order.Buy, 1000000, '00:00:00', '23:59:59', 27.1, 2000, 'Test2')
    #sched.addAgent(hunt2)
    
    #obs = Observer('agent')
    #sched.addAgent(obs)
#    obs.setNeedAllEvts(True)

    sched.run()
   
    #print depth1
#    print hunt2
    writeCSV('./depth1.csv', depth1.reportGen())
    writer = sqliteWriter('./testdb.db') 
    writer.write('test', depth1.reportGen())

if __name__ == '__main__':
    test_depth()
