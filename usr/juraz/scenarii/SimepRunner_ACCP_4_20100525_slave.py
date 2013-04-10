from simep.sched import *
from simep.core.busmanager import *
from simep.models.robmodel import *
from simep.agents.stockobserver import *



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
                           {'trading_destination_id' : 4, 
                            'name'                   : 'stock_ACCP_PA_4', 
                            'data_type'              : 'BINARY', 
                            'opening'                : '07:00:00:000', 
                            'trading_destination_name' : 'ENPA', 
                            'input_file_name'        : 'C:/Histo/lobTrade_2_4_20100525.binary', 
                            'date'                   : '20100525', 
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
BusManager.new_bus({'trading_destination_id' : 4, 
                    'name'                   : 'stock_ACCP_PA_4', 
                    'data_type'              : 'BINARY', 
                    'opening'                : '07:00:00:000', 
                    'trading_destination_name' : 'ENPA', 
                    'input_file_name'        : 'C:/Histo/lobTrade_2_4_20100525.binary', 
                    'date'                   : '20100525', 
                    'security_id'            : 2, 
                    'closing'                : '15:30:00:000', 
                    'ric'                    : 'ACCP.PA'},
                   ['avg_trade_size_300', 'garman_klass_15'])
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Create traders                    #
# ------------------------------------------------- #
StockObserver000_ACCP_PA_4 = StockObserver({'indicators'             : 'avg_trade_size_300;garman_klass_15', 
                                            'class_name'             : 'StockObserver', 
                                            'counter'                : 0, 
                                            'name'                   : 'StockObserver000_ACCP_PA_4'},
                                           {'date'                   : '20100525', 
                                            'security_id'            : 2, 
                                            'trading_destination_id' : 4, 
                                            'ric'                    : 'ACCP.PA'},
                                           {'plot_mode'              : 0, 
                                            'print_orderbook'        : False, 
                                            'save_into_file'         : True},
                                           trace)
sched.addAgent(StockObserver000_ACCP_PA_4)
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
SimulationResults['StockObserver000_ACCP_PA_4']  = StockObserver000_ACCP_PA_4.results()
globals()['return_SIMEP']=SimulationResults
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Destroy objects                  #
# ------------------------------------------------- #
del(StockObserver000_ACCP_PA_4)
del(stock_ACCP_PA_4)
BusManager.delete_all()
del(trace)
del(sched)
# ------------------------------------------------- #



