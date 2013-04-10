'''
Created on 23 juil. 2010

@author: benca
'''


from simep.scenarii.metascenario import MetaScenario


def run_simulation(distance):
    
    testCycle = MetaScenario('C:/st_sim/simep/st_sim.xml')
    testCycle.SetEngine('LightModel', {'Full': 'True'})
    testCycle.SetUniverse({'20100104' : ['ACCP.PA']})
    testCycle.AddTrader('Cycle', {'parameters' : {'distance'  : distance, 
                                                 'size'       : 50000, 
                                                 'side'       : 'Order.Buy', 
                                                 'cycle_time' : 15}})
    r = testCycle.GenerateAndRunSimulations()
    
    return r
    
    
    
r = run_simulation([0,1,2])
print r['20100104']