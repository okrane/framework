'''
Created on 19 janv. 2011

@author: benca
'''

from simep.core.baseobserver import BaseObserver
from simep.funcs.data.pyData import pyData
from simep.tools import date2num



class OrderbookPydataWriter(BaseObserver):
    
    
    
    @staticmethod
    def public_parameters():
        
        setup      = {'name'                   : {'label' : 'Name'                        , 'value' : 'OrderbookPydtaWriter001'}}
        parameters = {'plot_mode'              : {'label' : 'Plot Mode (0, 1, 2)'         , 'value' : 0}}
        return {'setup': setup, 'parameters': parameters}
    
    @staticmethod
    def indicators_list():
        return []
    
    
    
    '''######################################################################################################
    ############################################   CONSTRUCTOR   ############################################
    ######################################################################################################'''
    
    def __init__(self, setup, context, parameters, trace):
        BaseObserver.__init__(self, setup, context, parameters, trace)
        self._last_time_t = 0
        self._time_limit  = date2num('18:00:00:000000')
        
    

    '''######################################################################################################
    #################################   FUNCTIONS CALLED BY THE SCHEDULER   #################################
    ######################################################################################################'''
        
    def process(self, event):
        code = self.update(event)
        if code >= 0 and ba['event']['TIME_NUM'] > self._last_time_t and ba['event']['TIME_NUM'] < self._time_limit:
            indicators = {}
            for venue_id in self.ba['venue_ids']:
                trad_id = '%02d' %self.ba['destination_ids'][venue_id]
                indicators.update({trad_id + 'bid_price_1'   : [ba['feed'][venue_id]['bid_price_1']],
                                   trad_id + 'ask_price_1'   : [ba['feed'][venue_id]['ask_price_1']]})
            self.append_indicator(pyData('init',  date=[ba['event']['TIME_NUM']], value=indicators))
            self._last_time_t = ba['event']['TIME_NUM']

