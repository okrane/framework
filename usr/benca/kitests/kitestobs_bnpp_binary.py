from simep import __tvfo_mode__, __release__
from simep.bin.simepcore import Scheduler, Order
from simep.core.analyticsmanager import AnalyticsManager
from simep.agents.kitestobs import *
from simep.models.robmodel import *



# ------------------------------------------------- #
#            Create scheduler and trace             #
# ------------------------------------------------- #
sched = Scheduler('C:/st_sim/simep/Log')
sched.setRandomGenerator(42)
trace = sched.getTrace()
# --------------------------0----------------------- #



# ------------------------------------------------- #
#             Create stock(s) model(s)              #
# ------------------------------------------------- #
sched.addOrderBook('light', '4BNPP.PA', 26)
stock_BNPP_PA_004 = ROBModel({'login'                   : 'a_lambda_guy', 
                              'name'                    : 'stock_BNPP_PA', 
                              'engine_type'             : 'light', 
                              'allowed_data_types'      : 'BINARY;TBT2'},
                             {'data_type'               : 'BINARY', 
                              'opening'                 : {4: '07:00:00:000'}, 
                              'input_file_names'        : {4: 'C:/Histo/lobTrade_2_4_20100520.binary'}, 
                              'trading_destination_names' : ['ENPA'], 
                              'date'                    : '20100520', 
                              'ric'                     : 'BNPP.PA', 
                              'trading_destination_id'  : 4, 
                              'name'                    : 'stock_BNPP_PA', 
                              'rics'                    : {4: 'BNPP.PA'}, 
                              'trading_destination_ids' : [4], 
                              'security_id'             : 26, 
                              'closing'                 : {4: '15:30:00:000'}, 
                              'tick_sizes'              : {4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                             {'number_of_bt'            : 10, 
                              'full'                    : True},
                             trace)
sched.addAgent(stock_BNPP_PA_004)
# ------------------------------------------------- #



# ------------------------------------------------- #
#           Create the analytics manager            #
# ------------------------------------------------- #
AnalyticsManager.set_sched(sched)
AnalyticsManager.set_trace(trace)
# ------------------------------------------------- #
AnalyticsManager.new_bus({'data_type'               : 'BINARY', 
                          'opening'                 : {4: '07:00:00:000'}, 
                          'input_file_names'        : {4: 'C:/Histo/lobTrade_26_4_20100520.binary'}, 
                          'trading_destination_names' : ['ENPA'], 
                          'date'                    : '20100520', 
                          'ric'                     : 'BNPP.PA', 
                          'name'                    : 'stock_BNPP_PA', 
                          'rics'                    : {4: 'BNPP.PA'}, 
                          'trading_destination_ids' : [4], 
                          'security_id'             : 26, 
                          'closing'                 : {4: '15:30:00:000'}, 
                          'tick_sizes'              : {4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                         [])
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Create traders                    #
# ------------------------------------------------- #
KiTestObs000=KiTestObs({'class_name'              : 'KiTestObs', 
                        'counter'                 : 0, 
                        'name'                    : 'KiTestObs000'},
                       {'trading_destination_ids' : [4], 
                        'date'                    : '20100520', 
                        'security_id'             : 26, 
                        'trading_destination_names' : ['ENPA'], 
                        'ric'                     : 'BNPP.PA', 
                        'output_filename'         : 'C:/st_repository/simep_scenarii/gui/ROB/EmptyAgent/BNPP_PA_4/DAY_20100520/d3d9446802a44259755d38e6d163e820'},
                       {'log_filename'            : 'C:/kitestobs_bnpp_binary.log'},
                       trace)
KiTestObs000.isTvfoAgent(False)
sched.addAgent(KiTestObs000)
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Run simulation                    #
# ------------------------------------------------- #
sched.run()
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Return Results                   #
# ------------------------------------------------- #
KiTestObs000.results()
# ------------------------------------------------- #



# ------------------------------------------------- #
#           Analytics manager postprocess           #
# ------------------------------------------------- #
AnalyticsManager.postprocess()
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Destroy objects                  #
# ------------------------------------------------- #
del(KiTestObs000)
del(stock_BNPP_PA_004)
AnalyticsManager.delete_all()
del(trace)
del(sched)
# ------------------------------------------------- #



