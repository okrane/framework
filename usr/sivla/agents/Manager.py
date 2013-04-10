from simep.agents.agents import *

class Manager (BaseAgent):
    def __init__(self, tactic_list, params):
        self.tactics = tactic_list
        self.context = []
        
    def process(self, evt):
        self.update_context(self.context)
        for tactic in self.tactics:
            if tactic.enabled(self.context):
                tactic.process(evt)
    
    def processReport(self, evtReport):
        self.flow_data = []
        self.update_context(self.context)
        
        for tactic in self.tactics:
            if tactic.enabled(self.context):
                tactic.process(evtReport)
                self.flow_data.append(tactic.flow_data())
        
        tactic_score = self.evaluate(self.flow_data)
        
        for tactic in self.tactics:
            if tactic.enabled(self.context):
                tactic.update(tactic_score)
                
    def evaluate(self, flow_data):
        pass
        
        
        