from simep import __tvfo_mode__
from simep.bin.simepcore import Scheduler
from simep.core.analyticsmanager import AnalyticsManager
from simep.agents.stockobserver import StockObserver
from simep.models.mfgmodel_d import MFGModel2



# ------------------------------------------------- #
#                Define Variables                   #
# ------------------------------------------------- #
sched = Scheduler('C:/st_sim/simep/Log')
trace = sched.getTrace()
# ------------------------------------------------- #



# ------------------------------------------------- #
#             Create stock(s) model(s)              #
# ------------------------------------------------- #
sched.addOrderBook('normal', '0', 'MFGEN', '0')
stock_MFGEN_000 = MFGModel2({'login'                   : 'a_lambda_guy', 
                             'name'                    : 'stock_MFGEN', 
                             'engine_type'             : 'normal'},
                            {'trading_destination_id'  : 0, 
                             'name'                    : 'stock_MFGEN', 
                             'data_type'               : None, 
                             'opening'                 : {0: '09:00:00:000000'}, 
                             'input_file_names'        : {0: None}, 
                             'trading_destination_ids' : [0], 
                             'date'                    : '20100520', 
                             'security_id'             : 0, 
                             'closing'                 : {0: '17:30:00:000000'}, 
                             'ric'                     : 'MFGEN', 
                             'tick_sizes'              : {0: []}},
                            {'reference_price'         : 15.6, 
                             'tick_size'               : 0.005, 
                             'sigma_price'             : 0.2, 
                             'reference_slope'         : -600000.0, 
                             'sigma_slope'             : 0.06, 
                             'initial_order_size'      : 3000, 
                             'reference_spread'        : 0.01},
                            trace)
sched.addAgent(stock_MFGEN_000)
# ------------------------------------------------- #



# ------------------------------------------------- #
#              Create the bus manager               #
# ------------------------------------------------- #
AnalyticsManager.set_sched(sched)
AnalyticsManager.set_trace(trace)
AnalyticsManager.set_engine_params({'reference_price'             : 15.6, 
                              'tick_size'                   : 0.005, 
                              'name'                        : 'stock_MFGEN', 
                              'sigma_price'                 : 0.2, 
                              'reference_slope'             : -600000.0, 
                              'class_name'                  : 'MFGModel', 
                              'sigma_slope'                 : 0.06, 
                              'login'                       : 'a_lambda_guy', 
                              'initial_order_size'          : 3000, 
                              'engine_type'                 : 'normal', 
                              'reference_spread'            : 0.01})
# ------------------------------------------------- #
AnalyticsManager.new_bus({'trading_destination_id'  : 0, 
                    'name'                    : 'stock_MFGEN', 
                    'data_type'               : None, 
                    'opening'                 : {0: '09:00:00:000000'}, 
                    'input_file_names'        : {0: None}, 
                    'trading_destination_ids' : [0], 
                    'date'                    : '20100520', 
                    'security_id'             : 0, 
                    'closing'                 : {0: '17:30:00:000000'}, 
                    'ric'                     : 'MFGEN', 
                    'tick_sizes'              : {0: []}},
                   ['curves'])
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Create traders                    #
# ------------------------------------------------- #
StockObserver000_MFGEN = StockObserver({'class_name'              : 'StockObserver', 
                                        'counter'                 : 0, 
                                        'name'                    : 'StockObserver000'},
                                       {'date'                    : '20100520', 
                                        'security_id'             : 0, 
                                        'trading_destination_ids' : [0], 
                                        'ric'                     : 'MFGEN', 
                                        'output_filename'         : 'C:/st_repository/simep_scenarii/demos/other/MFG/StockObserver/MFGEN_000/DAY_20100520/3644a684f98ea8fe223c713b77189a77'},
                                       {'plot_mode'               : 2, 
                                        'print_orderbook'         : False, 
                                        'save_into_file'          : False},
                                       trace)
StockObserver000_MFGEN.isTvfoAgent(__tvfo_mode__)
sched.addAgent(StockObserver000_MFGEN)
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
SimulationResults['StockObserver000_MFGEN']  = StockObserver000_MFGEN.results()
globals()['return_SIMEP']=SimulationResults
# ------------------------------------------------- #



# ------------------------------------------------- #
#                   Write Results                   #
# ------------------------------------------------- #
StockObserver000_MFGEN.write_mat_file('C:/st_repository/simep_scenarii/demos/other/MFG/StockObserver/MFGEN_000/DAY_20100520/3644a684f98ea8fe223c713b77189a77.mat')
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Destroy objects                  #
# ------------------------------------------------- #
del(StockObserver000_MFGEN)
del(stock_MFGEN_000)
AnalyticsManager.delete_all()
del(trace)
del(sched)
# ------------------------------------------------- #



