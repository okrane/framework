# -*- coding: utf-8 -*-
import random as rnd

class Agent:
    def __init__ (self, parameters):
        self.parameters = parameters
        self.indicators = []
        
    def publish(self, m):
        self.indicators.append(m)
    
    def process(self, evt):
        pass
    
    def processReport(self, evt):
        pass
    

class Event:
    def __init__(self, x):
        self.x = x
        
class Simulation:
    @staticmethod    
    def run_simulation(rang, duration, agent_class, parameters):                           
        agent = agent_class(parameters)
        
        for i in range(duration):
            if rnd.randrange(1, 10) < 2:
                evt = Event({'type': "Report", 'value': 1})                
                agent.processReport(evt)
            else:
                evt = Event({'type': "Process", 'value': rnd.randrange(10, 100)})                
                agent.process(evt)
        
        return agent.indicators
            