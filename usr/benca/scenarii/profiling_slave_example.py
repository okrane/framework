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
AnalyticsManager.new_orderbook('light', {'trading_destination_id'  : 4, 
                                         'name'                    : 'stock_TOTF_PA', 
                                         'data_type'               : 'TBT2', 
                                         'opening'                 : {4: '08:00:00:000'}, 
                                         'input_file_names'        : {4: 'Q:/tick_ged'}, 
                                         'trading_destination_names' : ['ENPA'], 
                                         'rics'                    : {4: 'TOTF.PA'}, 
                                         'trading_destination_ids' : [4], 
                                         'date'                    : '20110209', 
                                         'security_id'             : 276, 
                                         'closing'                 : {4: '16:30:00:000'}, 
                                         'ric'                     : 'TOTF.PA', 
                                         'tick_sizes'              : {4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                                        [])
# ------------------------------------------------- #



# ------------------------------------------------- #
#             Create stock(s) model(s)              #
# ------------------------------------------------- #
stock_TOTF_PA_004 = ROBModel({'login'                   : 'a_lambda_guy', 
                              'name'                    : 'stock_TOTF_PA', 
                              'engine_type'             : 'light', 
                              'allowed_data_types'      : 'BINARY;TBT2'},
                             {'trading_destination_id'  : 4, 
                              'name'                    : 'stock_TOTF_PA', 
                              'data_type'               : 'TBT2', 
                              'opening'                 : {4: '08:00:00:000'}, 
                              'input_file_names'        : {4: 'Y:/tick_ged'}, 
                              'trading_destination_names' : ['ENPA'], 
                              'rics'                    : {4: 'TOTF.PA'}, 
                              'trading_destination_ids' : [4], 
                              'date'                    : '20110209', 
                              'security_id'             : 276, 
                              'closing'                 : {4: '16:30:00:000'}, 
                              'ric'                     : 'TOTF.PA', 
                              'tick_sizes'              : {4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
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
sched.addAgent(stock_TOTF_PA_004)
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Create traders                    #
# ------------------------------------------------- #
StockObserver000_TOTF_PA = EmptyAgent({'class_name'              : 'StockObserver', 
                                       'counter'                 : 0, 
                                       'name'                    : 'StockObserver000_TOTF_PA'},
                                      {'trading_destination_ids' : [4], 
                                       'date'                    : '20110209', 
                                       'security_id'             : 276, 
                                       'trading_destination_names' : ['ENPA'], 
                                       'ric'                     : 'TOTF.PA', 
                                       'output_filename'         : 'C:/st_repository/simep_scenarii/gui/ROB/StockObserver/TOTF_PA_004/DAY_20110209/c6f057b86584942e415435ffb1fa93d4'},
                                      {'append_into_pydata'      : False, 
                                       'update_bus'              : True},
                                      trace)
sched.addAgent(StockObserver000_TOTF_PA)
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Run simulation                    #
# ------------------------------------------------- #
cProfile.run('sched.run()','schedprofiling')
p = pstats.Stats('schedprofiling')
p.sort_stats('name')
p.print_stats()
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Return Results                   #
# ------------------------------------------------- #
StockObserver000_TOTF_PA.results()
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Destroy objects                  #
# ------------------------------------------------- #
del(StockObserver000_TOTF_PA)
del(stock_TOTF_PA_004)
AnalyticsManager.delete_all()
del(trace)
del(sched)
# ------------------------------------------------- #



