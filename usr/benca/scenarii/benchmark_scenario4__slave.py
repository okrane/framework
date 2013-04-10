from simep import __tvfo_mode__
from simep.bin.simepcore import *
from simep.core.analyticsmanager import *
from simep.agents.empty_agent import *
from simep.models.robmodel import *



# ------------------------------------------------- #
#                Define Variables                   #
# ------------------------------------------------- #
sched = Scheduler('C:/st_sim/simep/Log')
trace = sched.getTrace()
# ------------------------------------------------- #



# ------------------------------------------------- #
#             Create stock(s) model(s)              #
# ------------------------------------------------- #
sched.addOrderBook('light', '4BNPP.PA', 26)
stock_BNPP_PA_004 = ROBModel({'login'                   : 'a_lambda_guy', 
                              'name'                    : 'stock_BNPP_PA', 
                              'engine_type'             : 'light', 
                              'allowed_data_types'      : 'BINARY;TBT2'},
                             {'trading_destination_id'  : 4, 
                              'name'                    : 'stock_BNPP_PA', 
                              'data_type'               : 'TBT2', 
                              'opening'                 : {81: '08:00:00:000', 4: '08:00:00:000', 61: '08:00:00:000', 89: '08:00:00:000'}, 
                              'input_file_names'        : {81: 'Q:/tick_ged', 4: 'Q:/tick_ged', 61: 'Q:/tick_ged', 89: 'Q:/tick_ged'}, 
                              'trading_destination_names' : ['ENPA', 'CHIXPA', 'TRQXLT', 'BATE'], 
                              'rics'                    : {81: 'BNPPpa.TQ', 4: 'BNPP.PA', 61: 'BNPPpa.CHI', 89: 'BNPPpa.BS'}, 
                              'trading_destination_ids' : [4, 61, 81, 89], 
                              'date'                    : '20110118', 
                              'security_id'             : 26, 
                              'closing'                 : {81: '16:30:00:000', 4: '16:30:00:000', 61: '16:30:00:000', 89: '16:30:00:000'}, 
                              'ric'                     : 'BNPP.PA', 
                              'tick_sizes'              : {81: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 61: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 89: [(0.001, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                             {'number_of_bt'            : 10, 
                              'full'                    : True},
                             trace)
sched.addAgent(stock_BNPP_PA_004)
# ------------------------------------------------- #
sched.addOrderBook('light', '61BNPP.PA', 26)
stock_BNPP_PA_061 = ROBModel({'login'                   : 'a_lambda_guy', 
                              'name'                    : 'stock_BNPP_PA', 
                              'engine_type'             : 'light', 
                              'allowed_data_types'      : 'BINARY;TBT2'},
                             {'trading_destination_id'  : 61, 
                              'name'                    : 'stock_BNPP_PA', 
                              'data_type'               : 'TBT2', 
                              'opening'                 : {81: '08:00:00:000', 4: '08:00:00:000', 61: '08:00:00:000', 89: '08:00:00:000'}, 
                              'input_file_names'        : {81: 'Q:/tick_ged', 4: 'Q:/tick_ged', 61: 'Q:/tick_ged', 89: 'Q:/tick_ged'}, 
                              'trading_destination_names' : ['ENPA', 'CHIXPA', 'TRQXLT', 'BATE'], 
                              'rics'                    : {81: 'BNPPpa.TQ', 4: 'BNPP.PA', 61: 'BNPPpa.CHI', 89: 'BNPPpa.BS'}, 
                              'trading_destination_ids' : [4, 61, 81, 89], 
                              'date'                    : '20110118', 
                              'security_id'             : 26, 
                              'closing'                 : {81: '16:30:00:000', 4: '16:30:00:000', 61: '16:30:00:000', 89: '16:30:00:000'}, 
                              'ric'                     : 'BNPP.PA', 
                              'tick_sizes'              : {81: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 61: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 89: [(0.001, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                             {'number_of_bt'            : 10, 
                              'full'                    : True},
                             trace)
sched.addAgent(stock_BNPP_PA_061)
# ------------------------------------------------- #
sched.addOrderBook('light', '81BNPP.PA', 26)
stock_BNPP_PA_081 = ROBModel({'login'                   : 'a_lambda_guy', 
                              'name'                    : 'stock_BNPP_PA', 
                              'engine_type'             : 'light', 
                              'allowed_data_types'      : 'BINARY;TBT2'},
                             {'trading_destination_id'  : 81, 
                              'name'                    : 'stock_BNPP_PA', 
                              'data_type'               : 'TBT2', 
                              'opening'                 : {81: '08:00:00:000', 4: '08:00:00:000', 61: '08:00:00:000', 89: '08:00:00:000'}, 
                              'input_file_names'        : {81: 'Q:/tick_ged', 4: 'Q:/tick_ged', 61: 'Q:/tick_ged', 89: 'Q:/tick_ged'}, 
                              'trading_destination_names' : ['ENPA', 'CHIXPA', 'TRQXLT', 'BATE'], 
                              'rics'                    : {81: 'BNPPpa.TQ', 4: 'BNPP.PA', 61: 'BNPPpa.CHI', 89: 'BNPPpa.BS'}, 
                              'trading_destination_ids' : [4, 61, 81, 89], 
                              'date'                    : '20110118', 
                              'security_id'             : 26, 
                              'closing'                 : {81: '16:30:00:000', 4: '16:30:00:000', 61: '16:30:00:000', 89: '16:30:00:000'}, 
                              'ric'                     : 'BNPP.PA', 
                              'tick_sizes'              : {81: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 61: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 89: [(0.001, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                             {'number_of_bt'            : 10, 
                              'full'                    : True},
                             trace)
sched.addAgent(stock_BNPP_PA_081)
# ------------------------------------------------- #
sched.addOrderBook('light', '89BNPP.PA', 26)
stock_BNPP_PA_089 = ROBModel({'login'                   : 'a_lambda_guy', 
                              'name'                    : 'stock_BNPP_PA', 
                              'engine_type'             : 'light', 
                              'allowed_data_types'      : 'BINARY;TBT2'},
                             {'trading_destination_id'  : 89, 
                              'name'                    : 'stock_BNPP_PA', 
                              'data_type'               : 'TBT2', 
                              'opening'                 : {81: '08:00:00:000', 4: '08:00:00:000', 61: '08:00:00:000', 89: '08:00:00:000'}, 
                              'input_file_names'        : {81: 'Q:/tick_ged', 4: 'Q:/tick_ged', 61: 'Q:/tick_ged', 89: 'Q:/tick_ged'}, 
                              'trading_destination_names' : ['ENPA', 'CHIXPA', 'TRQXLT', 'BATE'], 
                              'rics'                    : {81: 'BNPPpa.TQ', 4: 'BNPP.PA', 61: 'BNPPpa.CHI', 89: 'BNPPpa.BS'}, 
                              'trading_destination_ids' : [4, 61, 81, 89], 
                              'date'                    : '20110118', 
                              'security_id'             : 26, 
                              'closing'                 : {81: '16:30:00:000', 4: '16:30:00:000', 61: '16:30:00:000', 89: '16:30:00:000'}, 
                              'ric'                     : 'BNPP.PA', 
                              'tick_sizes'              : {81: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 61: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 89: [(0.001, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                             {'number_of_bt'            : 10, 
                              'full'                    : True},
                             trace)
sched.addAgent(stock_BNPP_PA_089)
# ------------------------------------------------- #



# ------------------------------------------------- #
#              Create the bus manager               #
# ------------------------------------------------- #
AnalyticsManager.set_sched(sched)
AnalyticsManager.set_trace(trace)
# ------------------------------------------------- #
AnalyticsManager.new_bus({'trading_destination_id'  : 89, 
                    'name'                    : 'stock_BNPP_PA', 
                    'data_type'               : 'TBT2', 
                    'opening'                 : {81: '08:00:00:000', 4: '08:00:00:000', 61: '08:00:00:000', 89: '08:00:00:000'}, 
                    'input_file_names'        : {81: 'Q:/tick_ged', 4: 'Q:/tick_ged', 61: 'Q:/tick_ged', 89: 'Q:/tick_ged'}, 
                    'trading_destination_names' : ['ENPA', 'CHIXPA', 'TRQXLT', 'BATE'], 
                    'rics'                    : {81: 'BNPPpa.TQ', 4: 'BNPP.PA', 61: 'BNPPpa.CHI', 89: 'BNPPpa.BS'}, 
                    'trading_destination_ids' : [4, 61, 81, 89], 
                    'date'                    : '20110118', 
                    'security_id'             : 26, 
                    'closing'                 : {81: '16:30:00:000', 4: '16:30:00:000', 61: '16:30:00:000', 89: '16:30:00:000'}, 
                    'ric'                     : 'BNPP.PA', 
                    'tick_sizes'              : {81: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 61: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 89: [(0.001, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                   [])
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Create traders                    #
# ------------------------------------------------- #
EmptyAgent000_BNPP_PA = EmptyAgent({'class_name'              : 'EmptyAgent', 
                                    'counter'                 : 0, 
                                    'name'                    : 'EmptyAgent000_BNPP_PA'},
                                   {'date'                    : '20110118', 
                                    'security_id'             : 26, 
                                    'trading_destination_ids' : [81, 4, 61, 89], 
                                    'ric'                     : 'BNPP.PA', 
                                    'output_filename'         : 'C:/st_sim/usr/dev/benca/scenarii/ROB/EmptyAgent/BNPP_PA_all/DAY_20110118/4a7d1ed414474e4033ac29ccb8653d9b'},
                                   {'plot_mode'               : 0, 
                                    'get_lob_and_bidasks'     : False, 
                                    'update_bus'              : True, 
                                    'append_into_pydata'      : True},
                                   trace)
EmptyAgent000_BNPP_PA.isTvfoAgent(__tvfo_mode__)
sched.addAgent(EmptyAgent000_BNPP_PA)
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Run simulation                    #
# ------------------------------------------------- #
sched.run()
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Return Results                   #
# ------------------------------------------------- #
SimulationResults = {}
SimulationResults['EmptyAgent000_BNPP_PA']  = EmptyAgent000_BNPP_PA.results()
globals()['return_SIMEP']=SimulationResults
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Destroy objects                  #
# ------------------------------------------------- #
del(EmptyAgent000_BNPP_PA)
del(stock_BNPP_PA_004)
del(stock_BNPP_PA_061)
del(stock_BNPP_PA_081)
del(stock_BNPP_PA_089)
AnalyticsManager.delete_all()
del(trace)
del(sched)
# ------------------------------------------------- #



