from simep import __tvfo_mode__, __release__
from simep.sched import Scheduler, Order
from simep.core.analyticsmanager import AnalyticsManager
if __release__:
    from simep.funcs.dbtools.scenario_dictionnary import ScenarioDict
from simep.agents.tvfo_is_tactic_triggerer import *
from simep.models.robmodel import *



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
AnalyticsManager.new_bus({'name'                    : 'stock_FTE_PA', 
                          'data_type'               : 'TBT2', 
                          'opening'                 : {4: '07:00:00:000'}, 
                          'input_file_names'        : {4: 'Y:/tick_ged'}, 
                          'trading_destination_names' : ['ENPA'], 
                          'rics'                    : {4: 'FTE.PA'}, 
                          'trading_destination_ids' : [4], 
                          'date'                    : '20110331', 
                          'security_id'             : 110, 
                          'closing'                 : {4: '15:30:00:000'}, 
                          'ric'                     : 'FTE.PA', 
                          'tick_sizes'              : {4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                         ['avg_spread_bp_t_60', 'garman_klass_bp_s_900'])
# ------------------------------------------------- #



# ------------------------------------------------- #
#             Create stock(s) model(s)              #
# ------------------------------------------------- #
sched.addOrderBook('light', '4FTE.PA', 110)
stock_FTE_PA_004 = ROBModel({'login'                   : 'a_lambda_guy', 
                             'name'                    : 'stock_FTE_PA', 
                             'engine_type'             : 'light', 
                             'allowed_data_types'      : 'BINARY;TBT2'},
                            {'trading_destination_id'  : 4, 
                             'name'                    : 'stock_FTE_PA', 
                             'data_type'               : 'TBT2', 
                             'opening'                 : {4: '07:00:00:000'}, 
                             'input_file_names'        : {4: 'Y:/tick_ged'}, 
                             'trading_destination_names' : ['ENPA'], 
                             'rics'                    : {4: 'FTE.PA'}, 
                             'trading_destination_ids' : [4], 
                             'date'                    : '20110331', 
                             'security_id'             : 110, 
                             'closing'                 : {4: '15:30:00:000'}, 
                             'ric'                     : 'FTE.PA', 
                             'tick_sizes'              : {4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}},
                            {'include_dark_trades'     : False, 
                             'number_of_bt'            : 10, 
                             'full'                    : False, 
                             'include_intradayauction_trades' : False, 
                             'include_tradatlast_trades' : False, 
                             'include_closingauction_trades' : False, 
                             'include_cross_trades'    : False, 
                             'include_tradafterhours_trades' : False, 
                             'include_stopauction_trades' : False, 
                             'include_openingauction_trades' : False},
                            trace)
sched.addAgent(stock_FTE_PA_004)
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Create traders                    #
# ------------------------------------------------- #
TvfoTriggeredIs000_FTE_PA = TvfoTriggeredIs({'class_name'              : 'TvfoTriggeredIs', 
                                             'counter'                 : 0, 
                                             'name'                    : 'TvfoTriggeredIs000'},
                                            {'date'                    : '20110331', 
                                             'security_id'             : 110, 
                                             'trading_destination_ids' : [4], 
                                             'ric'                     : 'FTE.PA', 
                                             'output_filename'         : 'C:/st_sim/usr/dev/sivla/scenarii/ROB/TvfoTriggeredIs/FTE_PA_004/DAY_20110331/f9892427330362626b697131d4628eb9'},
                                            {'tactic_max_lifetime'     : '00:10:00:000000', 
                                             'tactic_class'            : 'Cruiser', 
                                             'companion_class'         : 'CruiserCompanion', 
                                             'delta_trigger'           : '00:10:00:000000', 
                                             'order_indicators'        : True, 
                                             'limit_price'             : 1000.0, 
                                             'total_qty'               : 50000, 
                                             'companion_module'        : 'usr.dev.st_algo.agents.CruiserCompanion', 
                                             'algo_start_time'         : '+00:03:00:000000', 
                                             'full_filename_skeleton'  : 'C:/Results/results', 
                                             'algo_end_time'           : '+06:30:00:000000', 
                                             'lob_indicators'          : False, 
                                             'companion_indicators'    : True, 
                                             'arrival_price_offset'    : 0, 
                                             'market_indicators'       : False, 
                                             'optimal_qty'             : 20000, 
                                             'tactic_module'           : 'usr.dev.st_algo.agents.Cruiser', 
                                             'side'                    : Order.Buy},
                                            trace)
TvfoTriggeredIs000_FTE_PA.isTvfoAgent(__tvfo_mode__)
sched.addAgent(TvfoTriggeredIs000_FTE_PA)
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Run simulation                    #
# ------------------------------------------------- #
sched.run()
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Return Results                   #
# ------------------------------------------------- #
TvfoTriggeredIs000_FTE_PA.results()
# ------------------------------------------------- #



# ------------------------------------------------- #
#                   Write Results                   #
# ------------------------------------------------- #
if __release__:
    TvfoTriggeredIs000_FTE_PA.write_mat_file('C:/st_sim/usr/dev/sivla/scenarii/ROB/TvfoTriggeredIs/FTE_PA_004/DAY_20110331/f9892427330362626b697131d4628eb9.mat')
    ScenarioDict.insertScenarioParameters(3, 'TvfoTriggeredIs', 'C:/st_sim/usr/dev/sivla/scenarii/ROB/TvfoTriggeredIs/FTE_PA_004/DAY_20110331/f9892427330362626b697131d4628eb9.mat', 'FTE.PA', 110, [4], '20110331', 'ROBModel', "{'tactic_max_lifetime': '00:10:00:000000', 'tactic_class': 'Cruiser', 'companion_class': 'CruiserCompanion', 'delta_trigger': '00:10:00:000000', 'order_indicators': True, 'limit_price': 1000.0, 'total_qty': 50000, 'companion_module': 'usr.dev.st_algo.agents.CruiserCompanion', 'algo_start_time': '+00:03:00:000000', 'full_filename_skeleton': 'C:/Results/results', 'algo_end_time': '+06:30:00:000000', 'lob_indicators': False, 'companion_indicators': True, 'arrival_price_offset': 0, 'market_indicators': False, 'optimal_qty': 20000, 'tactic_module': 'usr.dev.st_algo.agents.Cruiser', 'side': 'Order.Buy'}")
# ------------------------------------------------- #



# ------------------------------------------------- #
#                   Post process                    #
# ------------------------------------------------- #
AnalyticsManager.postprocess()
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Destroy objects                  #
# ------------------------------------------------- #
del(TvfoTriggeredIs000_FTE_PA)
del(stock_FTE_PA_004)
AnalyticsManager.delete_all()
del(trace)
del(sched)
# ------------------------------------------------- #



