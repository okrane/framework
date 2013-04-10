from simep.sched import *
from simep.core.busmanager import *
from simep.models.robmodel import *
from simep.agents.stockobserver import *
from simep.agents.orderbookobserver import *



# ------------------------------------------------- #
#                Define Variables                   #
# ------------------------------------------------- #
sched = Scheduler('C:/st_sim/simep/Log')
trace = sched.getTrace()
# ------------------------------------------------- #



# ------------------------------------------------- #
#             Create stock(s) model(s)              #
# ------------------------------------------------- #
sched.addOrderBook('light', 'ACCP.PA')
stock_ACCP_PA_4 = ROBModel({'login'                  : 'a_lambda_guy', 
                            'name'                   : 'stock_ACCP_PA_4', 
                            'engine_type'            : 'light'},
                           {'input_file_name'        : 'Q:/tick_ged', 
                            'trading_destination_id' : 4, 
                            'name'                   : 'stock_ACCP_PA_4', 
                            'data_type'              : 'TBT2', 
                            'opening'                : '07:00:00:000', 
                            'date'                   : '20100519', 
                            'security_id'            : 2, 
                            'closing'                : '15:30:00:000', 
                            'ric'                    : 'ACCP.PA'},
                           {'number_of_bt'           : 1, 
                            'full'                   : True},
                           trace)
sched.addAgent(stock_ACCP_PA_4)
# ------------------------------------------------- #



# ------------------------------------------------- #
#              Create the bus manager               #
# ------------------------------------------------- #
BusManager.set_sched(sched)
BusManager.set_trace(trace)
BusManager.set_engine_params({'number_of_bt'                : 1, 
                              'full'                        : True, 
                              'name'                        : 'stock_ACCP_PA_4', 
                              'class_name'                  : 'ROBModel', 
                              'login'                       : 'a_lambda_guy', 
                              'engine_type'                 : 'light'})
# ------------------------------------------------- #
BusManager.new_bus({'input_file_name'        : 'Q:/tick_ged', 
                    'trading_destination_id' : 4, 
                    'name'                   : 'stock_ACCP_PA_4', 
                    'data_type'              : 'TBT2', 
                    'opening'                : '07:00:00:000', 
                    'date'                   : '20100519', 
                    'security_id'            : 2, 
                    'closing'                : '15:30:00:000', 
                    'ric'                    : 'ACCP.PA'},
                   ['garman_klass_15', 'spread_avg_in_s_1', 'avg_trade_size_300', 'vwap_inst_in_s_1', 'realized_avg_spread_s_1', 'trade_size_avg_s_1'])
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Create traders                    #
# ------------------------------------------------- #
StockObserver001_ACCP_PA_4 = StockObserver({'class_name'             : 'StockObserver', 
                                            'indicators'             : 'avg_trade_size_300;garman_klass_15', 
                                            'counter'                : 1, 
                                            'name'                   : 'StockObserver001_ACCP_PA_4'},
                                           {'date'                   : '20100519', 
                                            'security_id'            : 2, 
                                            'trading_destination_id' : 4, 
                                            'ric'                    : 'ACCP.PA'},
                                           {'plot_mode'              : 0, 
                                            'print_orderbook'        : False, 
                                            'save_into_file'         : True},
                                           trace)
sched.addAgent(StockObserver001_ACCP_PA_4)
# ------------------------------------------------- #
OrderbookObserver000_ACCP_PA_4 = OrderbookObserver({'class_name'             : 'OrderbookObserver', 
                                                    'indicators'             : 'spread_avg_in_s_1;trade_size_avg_s_1;vwap_inst_in_s_1;realized_avg_spread_s_1', 
                                                    'counter'                : 0, 
                                                    'name'                   : 'OrderbookObserver000_ACCP_PA_4'},
                                                   {'date'                   : '20100519', 
                                                    'security_id'            : 2, 
                                                    'trading_destination_id' : 4, 
                                                    'ric'                    : 'ACCP.PA'},
                                                   {'plot_mode'              : 0, 
                                                    'delta_t'                : '00:00:01:000', 
                                                    'number_of_limits'       : 3, 
                                                    'tick_size_minimum'      : 0.0001, 
                                                    'price_impact_volumes'   : '500|1000|1500'},
                                                   trace)
sched.addAgent(OrderbookObserver000_ACCP_PA_4)
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
SimulationResults['StockObserver001_ACCP_PA_4']  = StockObserver001_ACCP_PA_4.results()
SimulationResults['OrderbookObserver000_ACCP_PA_4']  = OrderbookObserver000_ACCP_PA_4.results()
globals()['return_SIMEP']=SimulationResults
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Destroy objects                  #
# ------------------------------------------------- #
del(StockObserver001_ACCP_PA_4)
del(OrderbookObserver000_ACCP_PA_4)
del(stock_ACCP_PA_4)
BusManager.delete_all()
del(trace)
del(sched)
# ------------------------------------------------- #



