'''
Created on 23 mars 2011

@author: benca
'''

from simep import __tvfo_mode__
from simep.sched import Scheduler, Order
from simep.core.analyticsmanager import AnalyticsManager
from simep.models.robmodel import *
from simep.agents.PassivePVOLTracking import *

# ------------------------------------------------- #
#                Define Variables                   #
# ------------------------------------------------- #
sched = Scheduler('C:/st_sim/simep/Log')
trace = sched.getTrace()
# ------------------------------------------------- #


# ------------------------------------------------- #
#              Create the bus manager               #
# ------------------------------------------------- #
AnalyticsManager.set_sched(sched)
AnalyticsManager.set_trace(trace)
# ------------------------------------------------- #
AnalyticsManager.new_bus({'trading_destination_id'  : 8, 
                          'name'                    : 'stock_SAPG_DE', 
                          'data_type'               : 'TBT2', 
                          'opening'                 : {8: '08:00:00:000'}, 
                          'input_file_names'        : {8: 'Q:/tick_ged'}, 
                          'trading_destination_names' : ['XETRA'], 
                          'rics'                    : {8: 'SAPG.DE'}, 
                          'trading_destination_ids' : [8], 
                          'date'                    : '20110118', 
                          'security_id'             : 6724, 
                          'closing'                 : {8: '16:30:00:000'}, 
                          'ric'                     : 'SAPG.DE', 
                          'tick_sizes'              : {8: [(10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003), (0.0, 0.001)]}},
                         [])
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Create traders                    #
# ------------------------------------------------- #
PassivePVOLTracking000_SAPG_DE = PassivePVOLTracking({'class_name'              : 'PassivePVOLTracking', 
                                                      'counter'                 : 0, 
                                                      'name'                    : 'PassivePVOLTracking000_SAPG_DE'},
                                                     {'trading_destination_ids' : [8], 
                                                      'date'                    : '20110118', 
                                                      'security_id'             : 6724, 
                                                      'trading_destination_names' : ['XETRA'], 
                                                      'ric'                     : 'SAPG.DE', 
                                                      'output_filename'         : 'C:/st_repository/simep_scenarii/gui/ROB/PassivePVOLTracking/SAPG_DE_008/DAY_20110118/cfa5301358b9fcbe7aa45b1ceea088c6'},
                                                     {'c'                       : 15, 
                                                      'k'                       : 0, 
                                                      'side'                    : 0},
                                                     trace)
PassivePVOLTracking000_SAPG_DE.isTvfoAgent(__tvfo_mode__)
sched.addAgent(PassivePVOLTracking000_SAPG_DE)
# ------------------------------------------------- #



PassivePVOLTracking000_SAPG_DE.append_to_m_file('C:/test2.m', {'avg_spread_paris' : 0.3,
                                                 'avg_spread_milan' : 0.4,
                                                 'std_spread_paris' : 0.1,
                                                 'std_spread_milan' : 0.12})

