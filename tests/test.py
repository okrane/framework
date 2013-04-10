from simep.sched import Scheduler
from simep.agents import Observer
from simep.lightmodel import LightModel

sched = Scheduler()
sched.addOrderBook('light','TestRIC')


light = LightModel('LiModl', 'TestRIC')
        
sched.addAgent(light)

obs = Observer('agent')
sched.addAgent(obs)  
