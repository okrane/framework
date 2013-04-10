from simep import __tvfo_mode__, __release__
from simep.sched import Scheduler, Order
from simep.core.analyticsmanager import AnalyticsManager
from simep.agents.empty_agent import *
from simep.models.robmodel import *



# ------------------------------------------------- #
#            Create scheduler and trace             #
# ------------------------------------------------- #
sched = Scheduler('C:/st_sim/simep/Log')
trace = sched.getTrace()
# ------------------------------------------------- #



# ------------------------------------------------- #
#             Create stock(s) model(s)              #
# ------------------------------------------------- #
sched.addOrderBook('light', '61BNPP.PA', 26)
stock_BNPP_PA_061 = ROBModel({'login'                   : 'a_lambda_guy', 
                              'name'                    : 'stock_BNPP_PA', 
                              'engine_type'             : 'light', 
                              'allowed_data_types'      : 'BINARY;TBT2'},
                             {'data_type'               : 'BINARY', 
                              'opening'                 : {81: '07:00:00:000', 4: '07:00:00:000', 61: '07:00:00:000'}, 
                              'input_file_names'        : {81: 'C:/Histo/lobTrade_26_81_20100520.binary', 4: 'C:/Histo/lobTrade_26_4_20100520.binary', 61: 'C:/Histo/lobTrade_26_61_20100520.binary'}, 
                              'trading_destination_names' : ['CHIXPA', 'ENPA', 'TRQXLT'], 
                              'date'                    : '20100520', 
                              'ric'                     : 'BNPP.PA', 
                              'trading_destination_id'  : 61, 
                              'name'                    : 'stock_BNPP_PA', 
                              'rics'                    : {81: 'BNPPpa.TQ', 4: 'BNPP.PA', 61: 'BNPPpa.CHI'}, 
                              'trading_destination_ids' : [61, 4, 81], 
                              'security_id'             : 26, 
                              'closing'                 : {81: '15:30:00:000', 4: '15:30:00:000', 61: '15:30:00:000'}, 
                              'tick_sizes'              : {81: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 61: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                             {'number_of_bt'            : 10, 
                              'full'                    : True},
                             trace)
sched.addAgent(stock_BNPP_PA_061)
# ------------------------------------------------- #
sched.addOrderBook('light', '4BNPP.PA', 26)
stock_BNPP_PA_004 = ROBModel({'login'                   : 'a_lambda_guy', 
                              'name'                    : 'stock_BNPP_PA', 
                              'engine_type'             : 'light', 
                              'allowed_data_types'      : 'BINARY;TBT2'},
                             {'data_type'               : 'BINARY', 
                              'opening'                 : {81: '07:00:00:000', 4: '07:00:00:000', 61: '07:00:00:000'}, 
                              'input_file_names'        : {81: 'C:/Histo/lobTrade_26_81_20100520.binary', 4: 'C:/Histo/lobTrade_26_4_20100520.binary', 61: 'C:/Histo/lobTrade_26_61_20100520.binary'}, 
                              'trading_destination_names' : ['CHIXPA', 'ENPA', 'TRQXLT'], 
                              'date'                    : '20100520', 
                              'ric'                     : 'BNPP.PA', 
                              'trading_destination_id'  : 4, 
                              'name'                    : 'stock_BNPP_PA', 
                              'rics'                    : {81: 'BNPPpa.TQ', 4: 'BNPP.PA', 61: 'BNPPpa.CHI'}, 
                              'trading_destination_ids' : [61, 4, 81], 
                              'security_id'             : 26, 
                              'closing'                 : {81: '15:30:00:000', 4: '15:30:00:000', 61: '15:30:00:000'}, 
                              'tick_sizes'              : {81: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 61: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                             {'number_of_bt'            : 10, 
                              'full'                    : True},
                             trace)
sched.addAgent(stock_BNPP_PA_004)
# ------------------------------------------------- #
sched.addOrderBook('light', '81BNPP.PA', 26)
stock_BNPP_PA_081 = ROBModel({'login'                   : 'a_lambda_guy', 
                              'name'                    : 'stock_BNPP_PA', 
                              'engine_type'             : 'light', 
                              'allowed_data_types'      : 'BINARY;TBT2'},
                             {'data_type'               : 'BINARY', 
                              'opening'                 : {81: '07:00:00:000', 4: '07:00:00:000', 61: '07:00:00:000'}, 
                              'input_file_names'        : {81: 'C:/Histo/lobTrade_26_81_20100520.binary', 4: 'C:/Histo/lobTrade_26_4_20100520.binary', 61: 'C:/Histo/lobTrade_26_61_20100520.binary'}, 
                              'trading_destination_names' : ['CHIXPA', 'ENPA', 'TRQXLT'], 
                              'date'                    : '20100520', 
                              'ric'                     : 'BNPP.PA', 
                              'trading_destination_id'  : 81, 
                              'name'                    : 'stock_BNPP_PA', 
                              'rics'                    : {81: 'BNPPpa.TQ', 4: 'BNPP.PA', 61: 'BNPPpa.CHI'}, 
                              'trading_destination_ids' : [61, 4, 81], 
                              'security_id'             : 26, 
                              'closing'                 : {81: '15:30:00:000', 4: '15:30:00:000', 61: '15:30:00:000'}, 
                              'tick_sizes'              : {81: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 61: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                             {'number_of_bt'            : 10, 
                              'full'                    : True},
                             trace)
sched.addAgent(stock_BNPP_PA_081)
# ------------------------------------------------- #



# ------------------------------------------------- #
#           Create the analytics manager            #
# ------------------------------------------------- #
AnalyticsManager.set_sched(sched)
AnalyticsManager.set_trace(trace)
# ------------------------------------------------- #
AnalyticsManager.new_bus({'data_type'               : 'BINARY', 
                          'opening'                 : {81: '07:00:00:000', 4: '07:00:00:000', 61: '07:00:00:000'}, 
                          'input_file_names'        : {81: 'C:/Histo/lobTrade_26_81_20100520.binary', 4: 'C:/Histo/lobTrade_26_4_20100520.binary', 61: 'C:/Histo/lobTrade_26_61_20100520.binary'}, 
                          'trading_destination_names' : ['CHIXPA', 'ENPA', 'TRQXLT'], 
                          'date'                    : '20100520', 
                          'ric'                     : 'BNPP.PA', 
                          'trading_destination_id'  : 81, 
                          'name'                    : 'stock_BNPP_PA', 
                          'rics'                    : {81: 'BNPPpa.TQ', 4: 'BNPP.PA', 61: 'BNPPpa.CHI'}, 
                          'trading_destination_ids' : [61, 4, 81], 
                          'security_id'             : 26, 
                          'closing'                 : {81: '15:30:00:000', 4: '15:30:00:000', 61: '15:30:00:000'}, 
                          'tick_sizes'              : {81: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 61: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                         [])
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Create traders                    #
# ------------------------------------------------- #
EmptyAgent000_BNPP_PA = EmptyAgent({'class_name'              : 'EmptyAgent', 
                                    'counter'                 : 0, 
                                    'name'                    : 'EmptyAgent000_BNPP_PA'},
                                   {'trading_destination_ids' : [81, 4, 61], 
                                    'date'                    : '20100520', 
                                    'security_id'             : 26, 
                                    'trading_destination_names' : ['CHIXPA', 'ENPA', 'TRQXLT'], 
                                    'ric'                     : 'BNPP.PA', 
                                    'output_filename'         : 'C:/st_repository/simep_scenarii/gui/ROB/EmptyAgent/BNPP_PA_all/DAY_20100520/d3d9446802a44259755d38e6d163e820'},
                                   {'update_bus'              : True, 
                                    'append_into_pydata'      : False},
                                   trace)
EmptyAgent000_BNPP_PA.isTvfoAgent(False)
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
EmptyAgent000_BNPP_PA.results()
# ------------------------------------------------- #



# ------------------------------------------------- #
#           Analytics manager postprocess           #
# ------------------------------------------------- #
AnalyticsManager.postprocess()
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Destroy objects                  #
# ------------------------------------------------- #
del(EmptyAgent000_BNPP_PA)
del(stock_BNPP_PA_061)
del(stock_BNPP_PA_004)
del(stock_BNPP_PA_081)
AnalyticsManager.delete_all()
del(trace)
del(sched)
# ------------------------------------------------- #



