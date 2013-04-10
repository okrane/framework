from simep import __simep_directory__, __release__, __username__
from simep.bin.simepcore import PyScheduler, Order
from simep.core.analyticsmanager import AnalyticsManager
if __release__:
    from simep.funcs.dbtools.scenario_dictionnary import ScenarioDict
from simep.models.robmodel import *
from usr.dev.vilec.agents.orderbook_observer import *


# ------------------------------------------------- #
#                Define Variables                   #
# ------------------------------------------------- #
sched = PyScheduler('%s/logs/%s' %(__simep_directory__,__username__), False)
sched.setPythonInterAgentBus()
trace = sched.getTrace()
# ------------------------------------------------- #



# ------------------------------------------------- #
#              Create the bus manager               #
# ------------------------------------------------- #
AnalyticsManager.set_sched(sched)
AnalyticsManager.set_trace(trace)
# ------------------------------------------------- #
AnalyticsManager.new_orderbook('light', {'input_file_names'               : {4: 'Y:/tick_ged/20111104/20111104_44.tbt2', 23: 'Y:/tick_ged/20111104/20111104_6686.tbt2'}, 
                                         'trading_venue_types'            : {4: 'L', 23: 'L'}, 
                                         'enable_opening_auctions'        : {4: False, 23: False}, 
                                         'ric'                            : 'TOTF.PA', 
                                         'enable_stop_auctions'           : {4: False, 23: False}, 
                                         'closing_auctions'               : {4: '15:30:00:000', 23: ''}, 
                                         'enable_intraday_auctions'       : {4: False, 23: False}, 
                                         'rics'                           : {4: 'TOTF.PA', 23: 'TOTFpa.CHI'}, 
                                         'opening_auctions'               : {4: '05:15:00:000', 23: ''}, 
                                         'venue_ids'                      : {4: '4#FR0000120271', 23: '23#FPp'}, 
                                         'tick_sizes'                     : {4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 23: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}, 
                                         'trading_venue_ids'              : [23, 4], 
                                         'data_type'                      : 'TBT2', 
                                         'closings'                       : {4: '15:30:00:000', 23: '15:30:00:000'}, 
                                         'trading_venue_names'            : ['CHI-X PARIS', 'NYSE EURONEXT PARIS'], 
                                         'date'                           : '20111104', 
                                         'openings'                       : {4: '07:00:00:000', 23: '07:00:00:000'}, 
                                         'name'                           : 'stock_TOTF_PA', 
                                         'is_primary'                     : {4: True, 23: False}, 
                                         'enable_closing_auctions'        : {4: False, 23: False}, 
                                         'trading_destination_ids'        : {4: 4, 23: 61}, 
                                         'security_id'                    : 276},
                                        [])
# ------------------------------------------------- #



# ------------------------------------------------- #
#             Create stock(s) model(s)              #
# ------------------------------------------------- #
stock_TOTF_PA_004 = ROBModel({'default_data_type'              : 'TBT2', 
                              'name'                           : 'stock_TOTF_PA', 
                              'engine_type'                    : 'light', 
                              'allowed_data_types'             : 'TBT2;TBTL'},
                             {'input_file_names'               : {4: 'Y:/tick_ged/20111104/20111104_44.tbt2', 23: 'Y:/tick_ged/20111104/20111104_6686.tbt2'}, 
                              'trading_venue_types'            : {4: 'L', 23: 'L'}, 
                              'enable_opening_auctions'        : {4: False, 23: False}, 
                              'mid_trading_venue_id'           : None, 
                              'ric'                            : 'TOTF.PA', 
                              'trading_destination_id'         : 4, 
                              'enable_stop_auctions'           : {4: False, 23: False}, 
                              'closing_auctions'               : {4: '15:30:00:000', 23: ''}, 
                              'enable_intraday_auctions'       : {4: False, 23: False}, 
                              'rics'                           : {4: 'TOTF.PA', 23: 'TOTFpa.CHI'}, 
                              'opening_auctions'               : {4: '05:15:00:000', 23: ''}, 
                              'venue_ids'                      : {4: '4#FR0000120271', 23: '23#FPp'}, 
                              'tick_sizes'                     : {4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 23: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}, 
                              'trading_venue_ids'              : [23, 4], 
                              'data_type'                      : 'TBT2', 
                              'closings'                       : {4: '15:30:00:000', 23: '15:30:00:000'}, 
                              'trading_venue_names'            : ['CHI-X PARIS', 'NYSE EURONEXT PARIS'], 
                              'date'                           : '20111104', 
                              'openings'                       : {4: '07:00:00:000', 23: '07:00:00:000'}, 
                              'name'                           : 'stock_TOTF_PA', 
                              'is_primary'                     : {4: True, 23: False}, 
                              'enable_closing_auctions'        : {4: False, 23: False}, 
                              'trading_destination_ids'        : {4: 4, 23: 61}, 
                              'lit_trading_venue_id'           : 4, 
                              'security_id'                    : 276},
                             {'include_cross_trades'           : False, 
                              'number_of_bt'                   : 1, 
                              'full'                           : True, 
                              'include_stopauction_trades'     : True, 
                              'include_intradayauction_trades' : True, 
                              'include_openingauction_trades'  : True, 
                              'include_tradatlast_trades'      : False, 
                              'include_closingauction_trades'  : True, 
                              'include_tradafterhours_trades'  : False, 
                              'prefixing_qty_bias'             : 0.0},
                             trace)
sched.addAgent(stock_TOTF_PA_004)
# ------------------------------------------------- #
stock_TOTF_PA_061 = ROBModel({'default_data_type'              : 'TBT2', 
                              'name'                           : 'stock_TOTF_PA', 
                              'engine_type'                    : 'light', 
                              'allowed_data_types'             : 'TBT2;TBTL'},
                             {'input_file_names'               : {4: 'Y:/tick_ged/20111104/20111104_44.tbt2', 23: 'Y:/tick_ged/20111104/20111104_6686.tbt2'}, 
                              'trading_venue_types'            : {4: 'L', 23: 'L'}, 
                              'enable_opening_auctions'        : {4: False, 23: False}, 
                              'mid_trading_venue_id'           : None, 
                              'ric'                            : 'TOTF.PA', 
                              'trading_destination_id'         : 61, 
                              'enable_stop_auctions'           : {4: False, 23: False}, 
                              'closing_auctions'               : {4: '15:30:00:000', 23: ''}, 
                              'enable_intraday_auctions'       : {4: False, 23: False}, 
                              'rics'                           : {4: 'TOTF.PA', 23: 'TOTFpa.CHI'}, 
                              'opening_auctions'               : {4: '05:15:00:000', 23: ''}, 
                              'venue_ids'                      : {4: '4#FR0000120271', 23: '23#FPp'}, 
                              'tick_sizes'                     : {4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)], 23: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}, 
                              'trading_venue_ids'              : [23, 4], 
                              'data_type'                      : 'TBT2', 
                              'closings'                       : {4: '15:30:00:000', 23: '15:30:00:000'}, 
                              'trading_venue_names'            : ['CHI-X PARIS', 'NYSE EURONEXT PARIS'], 
                              'date'                           : '20111104', 
                              'openings'                       : {4: '07:00:00:000', 23: '07:00:00:000'}, 
                              'name'                           : 'stock_TOTF_PA', 
                              'is_primary'                     : {4: True, 23: False}, 
                              'enable_closing_auctions'        : {4: False, 23: False}, 
                              'trading_destination_ids'        : {4: 4, 23: 61}, 
                              'lit_trading_venue_id'           : 23, 
                              'security_id'                    : 276},
                             {'include_cross_trades'           : False, 
                              'number_of_bt'                   : 1, 
                              'full'                           : True, 
                              'include_stopauction_trades'     : True, 
                              'include_intradayauction_trades' : True, 
                              'include_openingauction_trades'  : True, 
                              'include_tradatlast_trades'      : False, 
                              'include_closingauction_trades'  : True, 
                              'include_tradafterhours_trades'  : False, 
                              'prefixing_qty_bias'             : 0.0},
                             trace)
sched.addAgent(stock_TOTF_PA_061)
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Create traders                    #
# ------------------------------------------------- #
OrderBookObserver000_TOTF_PA = OrderBookObserver({'class_name'                     : 'OrderBookObserver', 
                                                  'counter'                        : 0, 
                                                  'name'                           : 'OrderBookObserver000'},
                                                 {'venue_ids'                      : {4: '4#FR0000120271', 23: '23#FPp'}, 
                                                  'trading_venue_ids'              : [23, 4], 
                                                  'closing_auctions'               : {4: '15:30:00:000', 23: ''}, 
                                                  'is_primary'                     : {4: True, 23: False}, 
                                                  'trading_venue_types'            : {4: 'L', 23: 'L'}, 
                                                  'closings'                       : {4: '15:30:00:000', 23: '15:30:00:000'}, 
                                                  'output_filename'                : 'C:/st_repository/simep_scenarii/gui/ROB/OrderBookObserver/TOTF_PA_all/DAY_20111104/38cf522964cbc3df050b95bd01b9a737', 
                                                  'trading_venue_names'            : None, 
                                                  'trading_destination_ids'        : {4: 4, 23: 61}, 
                                                  'opening_auctions'               : {4: '05:15:00:000', 23: ''}, 
                                                  'date'                           : '20111104', 
                                                  'security_id'                    : 276, 
                                                  'openings'                       : {4: '07:00:00:000', 23: '07:00:00:000'}, 
                                                  'ric'                            : 'TOTF.PA'},
                                                 {'dummy_parameter3'               : 'my_string', 
                                                  'dummy_parameter2'               : 387, 
                                                  'slice_duration'                 : 30},
                                                 trace)
sched.addAgent(OrderBookObserver000_TOTF_PA)
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Run simulation                    #
# ------------------------------------------------- #
sched.main()
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Return Results                   #
# ------------------------------------------------- #
OrderBookObserver000_TOTF_PA.results()
# ------------------------------------------------- #



# ------------------------------------------------- #
#                   Write Results                   #
# ------------------------------------------------- #
if __release__:
    OrderBookObserver000_TOTF_PA.write_mat_file('C:/st_repository/simep_scenarii/gui/ROB/OrderBookObserver/TOTF_PA_all/DAY_20111104/38cf522964cbc3df050b95bd01b9a737.mat')
    ScenarioDict.insertScenarioParameters(3, 'OrderBookObserver', 'C:/st_repository/simep_scenarii/gui/ROB/OrderBookObserver/TOTF_PA_all/DAY_20111104/38cf522964cbc3df050b95bd01b9a737.mat', 'TOTF.PA', 276, [23, 4], '20111104', 'ROBModel', "{'dummy_parameter3': 'my_string', 'dummy_parameter2': 387, 'slice_duration': 30}")
# ------------------------------------------------- #



# ------------------------------------------------- #
#                   Post process                    #
# ------------------------------------------------- #
AnalyticsManager.postprocess()
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Destroy objects                  #
# ------------------------------------------------- #
del(OrderBookObserver000_TOTF_PA)
del(stock_TOTF_PA_004)
del(stock_TOTF_PA_061)
AnalyticsManager.delete_all()
del(trace)
del(sched)
# ------------------------------------------------- #


