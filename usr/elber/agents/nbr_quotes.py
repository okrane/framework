'''
Created on 17 juin 2011

@author: elber
'''

from simep.core.baseagent import BaseAgent
from simep.tools import date2num



class nbr_quotes(BaseAgent):
    
    
    
    '''######################################################################################################
    ##########################################   STATIC METHODS   ###########################################
    ######################################################################################################'''
    
    @staticmethod
    def public_parameters():
        parameters = {}
        return {'setup': {}, 'parameters': parameters}
    
    @staticmethod
    def indicators_list():
        return []
    
    
    
    '''######################################################################################################
    ############################################   CONSTRUCTOR   ############################################
    ######################################################################################################'''
    
    def __init__(self, setup, context, parameters, trace):
        super(nbr_quotes, self).__init__(setup, context, parameters, trace)
        self.needExecReportEvt    = False        
        self.needAllEvts          = True
        self.securityIdEventsOnly = True
        ba                        = self.ba
        ba['mvrs'] = {'VENUE_ID'    : self.ba['venue_ids'][0] if len(self.ba['venue_ids']) == 1 else 'all',
                      'tick_size'   : 0.01}
        
         
        
        self.counter = 0
        
        
        
    

    '''######################################################################################################
    #################################   FUNCTIONS CALLED BY THE SCHEDULER   #################################
    ######################################################################################################'''
        
    def process(self, event):
        if self.update(event) <= 0:
            return False
        #self.print_orderbooks()
        self.counter += 1
        name_fichier = self.ba['venue_ids'][0]
        self.append_to_m_file('C:/matlab_results/'+name_fichier, {'nbr_quotes' : self.counter})
    def last_process(self):
        print self.counter
        print 'finish'
        