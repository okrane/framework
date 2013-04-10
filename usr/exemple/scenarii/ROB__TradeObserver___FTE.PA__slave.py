from simep import __simep_directory__, __release__, __username__
from simep.bin.simepcore import PyScheduler, Order
from simep.core.analyticsmanager import AnalyticsManager
if __release__:
    from simep.funcs.dbtools.scenario_dictionnary import ScenarioDict
from usr.dev.exemple.agents.TradeObserver import *
from simep.models.robmodel import *



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
AnalyticsManager.new_orderbook('light', {'input_file_names'               : {4: 'Y:/tick_ged/20111101/20111101_22.tbt2'}, 
                                         'trading_venue_types'            : {4: 'L'}, 
                                         'enable_opening_auctions'        : {4: False}, 
                                         'ric'                            : 'FTE.PA', 
                                         'enable_stop_auctions'           : {4: False}, 
                                         'closing_auctions'               : {4: '15:30:00:000'}, 
                                         'enable_intraday_auctions'       : {4: False}, 
                                         'rics'                           : {4: 'FTE.PA'}, 
                                         'opening_auctions'               : {4: '05:15:00:000'}, 
                                         'venue_ids'                      : {4: '4#FR0000133308'}, 
                                         'tick_sizes'                     : {4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}, 
                                         'trading_venue_ids'              : [4], 
                                         'data_type'                      : 'TBT2', 
                                         'closings'                       : {4: '15:30:00:000'}, 
                                         'trading_venue_names'            : ['NYSE EURONEXT PARIS'], 
                                         'date'                           : '20111101', 
                                         'openings'                       : {4: '07:00:00:000'}, 
                                         'name'                           : 'stock_FTE_PA', 
                                         'is_primary'                     : {4: True}, 
                                         'enable_closing_auctions'        : {4: False}, 
                                         'trading_destination_ids'        : {4: 4}, 
                                         'security_id'                    : 110},
                                        [])
# ------------------------------------------------- #



# ------------------------------------------------- #
#             Create stock(s) model(s)              #
# ------------------------------------------------- #
stock_FTE_PA_004 = ROBModel({'default_data_type'              : 'TBT2', 
                             'name'                           : 'stock_FTE_PA', 
                             'engine_type'                    : 'light', 
                             'allowed_data_types'             : 'TBT2;TBTL'},
                            {'input_file_names'               : {4: 'Y:/tick_ged/20111101/20111101_22.tbt2'}, 
                             'trading_venue_types'            : {4: 'L'}, 
                             'enable_opening_auctions'        : {4: False}, 
                             'mid_trading_venue_id'           : None, 
                             'ric'                            : 'FTE.PA', 
                             'trading_destination_id'         : 4, 
                             'enable_stop_auctions'           : {4: False}, 
                             'closing_auctions'               : {4: '15:30:00:000'}, 
                             'enable_intraday_auctions'       : {4: False}, 
                             'rics'                           : {4: 'FTE.PA'}, 
                             'opening_auctions'               : {4: '05:15:00:000'}, 
                             'venue_ids'                      : {4: '4#FR0000133308'}, 
                             'tick_sizes'                     : {4: [(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]}, 
                             'trading_venue_ids'              : [4], 
                             'data_type'                      : 'TBT2', 
                             'closings'                       : {4: '15:30:00:000'}, 
                             'trading_venue_names'            : ['NYSE EURONEXT PARIS'], 
                             'date'                           : '20111101', 
                             'openings'                       : {4: '07:00:00:000'}, 
                             'name'                           : 'stock_FTE_PA', 
                             'is_primary'                     : {4: True}, 
                             'enable_closing_auctions'        : {4: False}, 
                             'trading_destination_ids'        : {4: 4}, 
                             'lit_trading_venue_id'           : 4, 
                             'security_id'                    : 110},
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
sched.addAgent(stock_FTE_PA_004)
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Create traders                    #
# ------------------------------------------------- #
TradeObserver000_FTE_PA = TradeObserver({'class_name'                     : 'TradeObserver', 
                                         'counter'                        : 0, 
                                         'name'                           : 'TradeObserver000'},
                                        {'venue_ids'                      : {4: '4#FR0000133308'}, 
                                         'trading_venue_ids'              : [4], 
                                         'closing_auctions'               : {4: '15:30:00:000'}, 
                                         'is_primary'                     : {4: True}, 
                                         'trading_venue_types'            : {4: 'L'}, 
                                         'closings'                       : {4: '15:30:00:000'}, 
                                         'output_filename'                : 'C:/st_sim2.5/usr/dev/exemple/scenarii/ROB/TradeObserver/FTE_PA_004/DAY_20111101/51054b25d2392de390f5516471c16e81', 
                                         'trading_venue_names'            : None, 
                                         'trading_destination_ids'        : {4: 4}, 
                                         'opening_auctions'               : {4: '05:15:00:000'}, 
                                         'date'                           : '20111101', 
                                         'security_id'                    : 110, 
                                         'openings'                       : {4: '07:00:00:000'}, 
                                         'ric'                            : 'FTE.PA'},
                                        {'dummy_parameter1'               : False, 
                                         'dummy_parameter2'               : 387, 
                                         'dummy_parameter3'               : 'my_string'},
                                        trace)
sched.addAgent(TradeObserver000_FTE_PA)
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Run simulation                    #
# ------------------------------------------------- #
sched.main()
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Return Results                   #
# ------------------------------------------------- #
TradeObserver000_FTE_PA.results()
# ------------------------------------------------- #



# ------------------------------------------------- #
#                   Write Results                   #
# ------------------------------------------------- #
if __release__:
    TradeObserver000_FTE_PA.write_mat_file('C:/st_sim2.5/usr/dev/exemple/scenarii/ROB/TradeObserver/FTE_PA_004/DAY_20111101/51054b25d2392de390f5516471c16e81.mat')
    ScenarioDict.insertScenarioParameters(34, 'TradeObserver', 'C:/st_sim2.5/usr/dev/exemple/scenarii/ROB/TradeObserver/FTE_PA_004/DAY_20111101/51054b25d2392de390f5516471c16e81.mat', 'FTE.PA', 110, [4], '20111101', 'ROBModel', "{'dummy_parameter1': False, 'dummy_parameter2': 387, 'dummy_parameter3': 'my_string'}")
    TradeObserver000_FTE_PA.write_csv_file('C:/st_sim2.5/usr/dev/exemple/scenarii/ROB/TradeObserver/FTE_PA_004/DAY_20111101/51054b25d2392de390f5516471c16e81.csv')
# ------------------------------------------------- #



# ------------------------------------------------- #
#                   Post process                    #
# ------------------------------------------------- #
AnalyticsManager.postprocess()
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Destroy objects                  #
# ------------------------------------------------- #
del(TradeObserver000_FTE_PA)
del(stock_FTE_PA_004)
AnalyticsManager.delete_all()
del(trace)
del(sched)
# ------------------------------------------------- #



