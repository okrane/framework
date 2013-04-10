'''
Created on 18 nov. 2011

@author: benca
'''


from simep.funcs.stdio.utils import pyLog
from simep.core.baseagent import BaseAgent
from simep.funcs.data.pyData import pyData
from simep.tools import date2num



class BlinkAntigamingIndicators(BaseAgent):
    
    def __init__(self, setup, context, params, trace):
        super(BlinkAntigamingIndicators, self).__init__(setup, context, params, trace)
        ba                            = self.ba
        ba['venue_id']                = ba['venue_ids'][0]
        ba['agi1']                    = {}
        ba['agi2']                    = {}
        ba['turnover_price_ag1']      = {}
        ba['volume']                  = {}
        ba['client_start_time']       = {}
        ba['client_end_time']         = {}
        ba['client_side']             = {}
        ba['agi2_buffer']             = list()
    
    def initialize(self):
        pass
        
    def process(self, event):
        return False
    
    def results(self):
        ba = self.ba
        for c_o_id in ba['client_start_time'].iterkeys():
            self.appendIndicator({'client_order_id' : c_o_id,
                                  'start_time'      : ba['client_start_time'][c_o_id],
                                  'end_time'        : ba['client_end_time'][c_o_id],
                                  'side'            : ba['client_side'][c_o_id],
                                  'agi1'            : ba['agi1'][c_o_id],
                                  'agi2'            : ba['agi2'][c_o_id]},
                                 ba['client_end_time'][c_o_id])
        
    def processAction(self, algo_action):
        if algo_action['Type'] == 'Z':
            ba                                   = self.ba
            venue_id                             = ba['venue_id']
            feed                                 = ba['feed'][venue_id][0]
            price                                = algo_action['Price']
            qty                                  = algo_action['OrderQty']
            side                                 = algo_action['Side']
            c_o_id                               = algo_action['ClientOrderId']
            time_num                             = algo_action['Timestamp']
            """
            Detect side switch and save results
            """
            if ba['client_start_time'].has_key(c_o_id):
                if ba['client_side'][c_o_id] != side:
                    end_time                     = ba['client_end_time'].pop(c_o_id)
                    self.appendIndicator({'client_order_id' : c_o_id,
                                          'start_time'      : ba['client_start_time'].pop(c_o_id),
                                          'end_time'        : end_time,
                                          'side'            : ba['client_side'].pop(c_o_id),
                                          'agi1'            : ba['agi1'].pop(c_o_id),
                                          'agi2'            : ba['agi2'].pop(c_o_id)},
                                         end_time)
            """
            Create a new key if client_order_id doesn't exist
            """
            if not ba['client_start_time'].has_key(c_o_id):
                ba['client_start_time'][c_o_id]  = time_num
                ba['client_side'][c_o_id]        = side
                ba['agi1'][c_o_id]               = 0.0
                ba['agi2'][c_o_id]               = 0.0
                ba['volume'][c_o_id]             = 0
                ba['turnover_price_ag1'][c_o_id] = 0.0
            ba['client_end_time'][c_o_id]        = time_num
            """
            Increment turnover_price_ag1 and volume over all active client_order_ids
            """
            turnover_price_diff                  = qty*(price - feed['MID_PRICE'])
            for coid in ba['client_start_time'].iterkeys():
                ba['turnover_price_ag1'][coid]  += turnover_price_diff
                ba['volume'][coid]              += qty
            """
            Update agi1 only for the current client_order_id
            """
            ba['agi1'][c_o_id]                   = ba['turnover_price_ag1'][c_o_id]/ba['volume'][c_o_id]
            """
            Update agi2 buffer
            """
            ba['agi2_buffer'].append( (time_num, turnover_price_diff, qty) )
            """
            Update agi2 only for the current client_order_id
            """
            end_time                             = ba['client_start_time'][c_o_id]
            start_time                           = end_time - (time_num - end_time)
            volume                               = 0
            turnover_price_ag2                   = 0.0       
            for a in ba['agi2_buffer']:
                if a[0] >= start_time and a[0] <= end_time:
                    turnover_price_ag2          += a[1]
                    volume                      += a[2]
            ba['agi2'][c_o_id]                   = turnover_price_ag2 / volume
            

            
        