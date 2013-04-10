import simep
from simep.scenarii.scenario import Scenario
from simep.sched import Order, Trade
from simep.funcs.dbtools.repository import Repository
import matplotlib.pyplot as plt

def run_simulation(size):

    colors = ['red', 'green', 'cyan', 'blue', 'black', 'yellow']
    result = []
    for s in size:

        testCycle = Scenario('Cycle', Repository('C:/st_sim/simep/projects/databases/repository'), 'C:/st_sim/dev/usr/sivla/scenarii')
        testCycle.setEngine('Light', {'Full': 'True'})
        testCycle.setUniverse({'20100104': ['ACCP.PA']})        
        testCycle.registerTrader(secId = 'ACCP.PA', 
                         classname = 'Cycle', 
                         params = {'side': Order.Buy, 'distance': s, 'cycle': 15, 'size': 500}, 
                         module="dev.usr.sivla.agents.Cycle", 
                         date = '20100104')
        testCycle.generateSimulations()

        testCycle.loadSimulations('C:/test_simep', ".*\.py");
        a = testCycle.runSimulations()

        a = a[0]
        a['main key'] = a.keys()[0]
        a['size'] = s
        
        result.append(a)
        
        #plt.plot(range(len(result['phi'])), result['phi'], color = colors[size.index(s)])
    

    #plt.show()
    return result
    
r = run_simulation([1])
#r1 = r[0][('trader_Cycle_ACCP_PA_0', '20100104', 'ACCP.PA')]
#print r1['phi']
