from simep import __username__
from simep.bin.simepcore import Scheduler
from simep.core.analyticsmanager import AnalyticsManager
from simep.models.robmodel import ROBModel
from simep.agents.empty_agent import EmptyAgent
import cProfile
import pstats


# ------------------------------------------------- #
#                Define Variables                   #
# ------------------------------------------------- #
sched = Scheduler(__username__, False)
sched.setPythonInterAgentBus()
trace = sched.getTrace()
# ------------------------------------------------- #



# ------------------------------------------------- #
#              Create the bus manager               #
# ------------------------------------------------- #
AnalyticsManager.set_sched(sched)
AnalyticsManager.set_trace(trace)
# ------------------------------------------------- #
AnalyticsManager.new_orderbook('light', {'name'                    : 'stock_BNPP_PA', 
                                         'data_type'               : 'TBT2', 
                                         'opening'                 : {81: '08:00:00:000', 4: '08:00:00:000', 61: '08:00:00:000', 89: '08:00:00:000'}, 
                                         'input_file_names'        : {81: 'Y:/tick_ged', 4: 'Y:/tick_ged', 61: 'Y:/tick_ged', 89: 'Y:/tick_ged'}, 
                                         'trading_destination_names' : ['ENPA', 'CHIXPA', 'TRQXLT', 'BATE'], 
                                         'rics'                    : {81: 'BNPPpa.TQ', 4: 'BNPP.PA', 61: 'BNPPpa.CHI', 89: 'BNPPpa.BS'}, 
                                         'trading_destination_ids' : [4, 61, 81, 89], 
                                         'date'                    : '20110118', 
                                         'security_id'             : 26, 
                                         'closing'                 : {81: '16:30:00:000', 4: '16:30:00:000', 61: '16:30:00:000', 89: '16:30:00:000'}, 
                                         'ric'                     : 'BNPP.PA', 
                                         'tick_sizes'              : {81: [(0.0, 0.001), (10.0, 0.005), (50.0, 0.01), (100.0, 0.05)], 4: [(0.0, 0.001), (10.0, 0.005), (50.0, 0.01), (100.0, 0.05)], 61: [(0.0, 0.001), (10.0, 0.005), (50.0, 0.01), (100.0, 0.05)], 89: [(0.001, 0.001), (10.0, 0.005), (50.0, 0.01), (100.0, 0.05)]}},
                                        [])
# ------------------------------------------------- #



# ------------------------------------------------- #
#             Create stock(s) model(s)              #
# ------------------------------------------------- #
stock_BNPP_PA_004 = ROBModel({'login'                   : 'a_lambda_guy', 
                              'name'                    : 'stock_BNPP_PA', 
                              'engine_type'             : 'light', 
                              'allowed_data_types'      : 'BINARY;TBT2'},
                             {'trading_destination_id'  : 4, 
                              'name'                    : 'stock_BNPP_PA', 
                              'data_type'               : 'TBT2', 
                              'opening'                 : {81: '08:00:00:000', 4: '08:00:00:000', 61: '08:00:00:000', 89: '08:00:00:000'}, 
                              'input_file_names'        : {81: 'Y:/tick_ged', 4: 'Y:/tick_ged', 61: 'Y:/tick_ged', 89: 'Y:/tick_ged'}, 
                              'trading_destination_names' : ['ENPA', 'CHIXPA', 'TRQXLT', 'BATE'], 
                              'rics'                    : {81: 'BNPPpa.TQ', 4: 'BNPP.PA', 61: 'BNPPpa.CHI', 89: 'BNPPpa.BS'}, 
                              'trading_destination_ids' : [4, 61, 81, 89], 
                              'date'                    : '20110118', 
                              'security_id'             : 26, 
                              'closing'                 : {81: '16:30:00:000', 4: '16:30:00:000', 61: '16:30:00:000', 89: '16:30:00:000'}, 
                              'ric'                     : 'BNPP.PA', 
                              'tick_sizes'              : {81: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 61: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 89: [(0.001, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                             {'include_dark_trades'     : False, 
                              'include_cross_trades'    : False, 
                              'number_of_bt'            : 10, 
                              'full'                    : True, 
                              'include_stopauction_trades' : False, 
                              'include_intradayauction_trades' : False, 
                              'include_openingauction_trades' : False, 
                              'include_tradatlast_trades' : False, 
                              'include_closingauction_trades' : False, 
                              'include_tradafterhours_trades' : False},
                             trace)
sched.addAgent(stock_BNPP_PA_004)
# ------------------------------------------------- #
stock_BNPP_PA_061 = ROBModel({'login'                   : 'a_lambda_guy', 
                              'name'                    : 'stock_BNPP_PA', 
                              'engine_type'             : 'light', 
                              'allowed_data_types'      : 'BINARY;TBT2'},
                             {'trading_destination_id'  : 61, 
                              'name'                    : 'stock_BNPP_PA', 
                              'data_type'               : 'TBT2', 
                              'opening'                 : {81: '08:00:00:000', 4: '08:00:00:000', 61: '08:00:00:000', 89: '08:00:00:000'}, 
                              'input_file_names'        : {81: 'Y:/tick_ged', 4: 'Y:/tick_ged', 61: 'Y:/tick_ged', 89: 'Y:/tick_ged'}, 
                              'trading_destination_names' : ['ENPA', 'CHIXPA', 'TRQXLT', 'BATE'], 
                              'rics'                    : {81: 'BNPPpa.TQ', 4: 'BNPP.PA', 61: 'BNPPpa.CHI', 89: 'BNPPpa.BS'}, 
                              'trading_destination_ids' : [4, 61, 81, 89], 
                              'date'                    : '20110118', 
                              'security_id'             : 26, 
                              'closing'                 : {81: '16:30:00:000', 4: '16:30:00:000', 61: '16:30:00:000', 89: '16:30:00:000'}, 
                              'ric'                     : 'BNPP.PA', 
                              'tick_sizes'              : {81: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 61: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 89: [(0.001, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                             {'include_dark_trades'     : False, 
                              'include_cross_trades'    : False, 
                              'number_of_bt'            : 10, 
                              'full'                    : True, 
                              'include_stopauction_trades' : False, 
                              'include_intradayauction_trades' : False, 
                              'include_openingauction_trades' : False, 
                              'include_tradatlast_trades' : False, 
                              'include_closingauction_trades' : False, 
                              'include_tradafterhours_trades' : False},
                             trace)
sched.addAgent(stock_BNPP_PA_061)
# ------------------------------------------------- #
stock_BNPP_PA_081 = ROBModel({'login'                   : 'a_lambda_guy', 
                              'name'                    : 'stock_BNPP_PA', 
                              'engine_type'             : 'light', 
                              'allowed_data_types'      : 'BINARY;TBT2'},
                             {'trading_destination_id'  : 81, 
                              'name'                    : 'stock_BNPP_PA', 
                              'data_type'               : 'TBT2', 
                              'opening'                 : {81: '08:00:00:000', 4: '08:00:00:000', 61: '08:00:00:000', 89: '08:00:00:000'}, 
                              'input_file_names'        : {81: 'Y:/tick_ged', 4: 'Y:/tick_ged', 61: 'Y:/tick_ged', 89: 'Y:/tick_ged'}, 
                              'trading_destination_names' : ['ENPA', 'CHIXPA', 'TRQXLT', 'BATE'], 
                              'rics'                    : {81: 'BNPPpa.TQ', 4: 'BNPP.PA', 61: 'BNPPpa.CHI', 89: 'BNPPpa.BS'}, 
                              'trading_destination_ids' : [4, 61, 81, 89], 
                              'date'                    : '20110118', 
                              'security_id'             : 26, 
                              'closing'                 : {81: '16:30:00:000', 4: '16:30:00:000', 61: '16:30:00:000', 89: '16:30:00:000'}, 
                              'ric'                     : 'BNPP.PA', 
                              'tick_sizes'              : {81: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 61: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 89: [(0.001, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                             {'include_dark_trades'     : False, 
                              'include_cross_trades'    : False, 
                              'number_of_bt'            : 10, 
                              'full'                    : True, 
                              'include_stopauction_trades' : False, 
                              'include_intradayauction_trades' : False, 
                              'include_openingauction_trades' : False, 
                              'include_tradatlast_trades' : False, 
                              'include_closingauction_trades' : False, 
                              'include_tradafterhours_trades' : False},
                             trace)
sched.addAgent(stock_BNPP_PA_081)
# ------------------------------------------------- #
stock_BNPP_PA_089 = ROBModel({'login'                   : 'a_lambda_guy', 
                              'name'                    : 'stock_BNPP_PA', 
                              'engine_type'             : 'light', 
                              'allowed_data_types'      : 'BINARY;TBT2'},
                             {'trading_destination_id'  : 89, 
                              'name'                    : 'stock_BNPP_PA', 
                              'data_type'               : 'TBT2', 
                              'opening'                 : {81: '08:00:00:000', 4: '08:00:00:000', 61: '08:00:00:000', 89: '08:00:00:000'}, 
                              'input_file_names'        : {81: 'Y:/tick_ged', 4: 'Y:/tick_ged', 61: 'Y:/tick_ged', 89: 'Y:/tick_ged'}, 
                              'trading_destination_names' : ['ENPA', 'CHIXPA', 'TRQXLT', 'BATE'], 
                              'rics'                    : {81: 'BNPPpa.TQ', 4: 'BNPP.PA', 61: 'BNPPpa.CHI', 89: 'BNPPpa.BS'}, 
                              'trading_destination_ids' : [4, 61, 81, 89], 
                              'date'                    : '20110118', 
                              'security_id'             : 26, 
                              'closing'                 : {81: '16:30:00:000', 4: '16:30:00:000', 61: '16:30:00:000', 89: '16:30:00:000'}, 
                              'ric'                     : 'BNPP.PA', 
                              'tick_sizes'              : {81: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 61: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 89: [(0.001, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                             {'include_dark_trades'     : False, 
                              'include_cross_trades'    : False, 
                              'number_of_bt'            : 10, 
                              'full'                    : True, 
                              'include_stopauction_trades' : False, 
                              'include_intradayauction_trades' : False, 
                              'include_openingauction_trades' : False, 
                              'include_tradatlast_trades' : False, 
                              'include_closingauction_trades' : False, 
                              'include_tradafterhours_trades' : False},
                             trace)
sched.addAgent(stock_BNPP_PA_089)
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Create traders                    #
# ------------------------------------------------- #
EmptyAgent000_BNPP_PA = EmptyAgent({'class_name'              : 'EmptyAgent', 
                                    'counter'                 : 0, 
                                    'name'                    : 'EmptyAgent000_BNPP_PA'},
                                   {'date'                    : '20110118', 
                                    'security_id'             : 26, 
                                    'trading_destination_ids' : [4, 61, 81, 89], 
                                    'ric'                     : 'BNPP.PA', 
                                    'output_filename'         : 'C:/st_sim/usr/dev/benca/scenarii/ROB/EmptyAgent/BNPP_PA_004/DAY_20110118/c6f057b86584942e415435ffb1fa93d4'},
                                   {'plot_mode'               : 0, 
                                    'update_bus'              : True, 
                                    'append_into_pydata'      : False},
                                   trace)
sched.addAgent(EmptyAgent000_BNPP_PA)
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Run simulation                    #
# ------------------------------------------------- #
cProfile.run('sched.run()','schedprofiling2')
p = pstats.Stats('schedprofiling2')
p.sort_stats('name')
p.print_stats()
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



