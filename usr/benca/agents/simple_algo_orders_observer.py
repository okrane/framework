'''
Created on 24 oct. 2011

@author: benca
'''

from simep.funcs.stdio.utils import pyLog
from simep.core.baseagent import BaseAgent
from simep.funcs.data.pyData import pyData
from simep.tools import date2num



class SimpleAlgoOrdersObserver(BaseAgent):
    
    def __init__(self, setup, context, params, trace):
        super(SimpleAlgoOrdersObserver, self).__init__(setup, context, params, trace)
        
    def process(self, event):
        return False
       
    def processReport(self, event):
        print 'EXECUTION : %d @ %f' %(event.size, event.price)
    