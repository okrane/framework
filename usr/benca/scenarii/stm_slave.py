from simep.bin.simepcore import *
from simep.core.analyticsmanager import *
from simep.models.robmodel import *
from simep.agents.stocks_comparator import *



# ------------------------------------------------- #
#                Define Variables                   #
# ------------------------------------------------- #
sched = Scheduler('C:/st_sim/simep/Log')
sched.setRandomGenerator(4)
trace = sched.getTrace()
# ------------------------------------------------- #



# ------------------------------------------------- #
#             Create stock(s) model(s)              #
# ------------------------------------------------- #
sched.addOrderBook('light', 'STM.MI')
stock_STM_MI_6 = ROBModel({'login'                  : 'a_lambda_guy', 
                           'name'                   : 'stock_STM_MI_6', 
                           'engine_type'            : 'light'},
                          {'input_file_name'        : 'Q:/tick_ged', 
                           'trading_destination_id' : 6, 
                           'name'                   : 'stock_STM_MI_6', 
                           'data_type'              : 'TBT2', 
                           'opening'                : '08:00:00:000', 
                           'date'                   : '20100104', 
                           'security_id'            : 7153, 
                           'closing'                : '16:25:00:000', 
                           'ric'                    : 'STM.MI'},
                          {'number_of_bt'           : 10, 
                           'full'                   : True, 
                           'seed'                   : 4},
                          trace)
sched.addAgent(stock_STM_MI_6)
# ------------------------------------------------- #
sched.addOrderBook('light', 'STM.PA')
stock_STM_PA_4 = ROBModel({'login'                  : 'a_lambda_guy', 
                           'name'                   : 'stock_STM_PA_4', 
                           'engine_type'            : 'light'},
                          {'input_file_name'        : 'Q:/tick_ged', 
                           'trading_destination_id' : 4, 
                           'name'                   : 'stock_STM_PA_4', 
                           'data_type'              : 'TBT2', 
                           'opening'                : '08:00:00:000', 
                           'date'                   : '20100104', 
                           'security_id'            : 247, 
                           'closing'                : '16:30:00:000', 
                           'ric'                    : 'STM.PA'},
                          {'number_of_bt'           : 10, 
                           'full'                   : True, 
                           'seed'                   : 4},
                          trace)
sched.addAgent(stock_STM_PA_4)
# ------------------------------------------------- #



# ------------------------------------------------- #
#              Create the bus manager               #
# ------------------------------------------------- #
AnalyticsManager.set_sched(sched)
AnalyticsManager.set_trace(trace)
AnalyticsManager.set_engine_params({'number_of_bt'                : 10, 
                              'full'                        : True, 
                              'name'                        : 'stock_STM_PA_4', 
                              'class_name'                  : 'ROBModel', 
                              'login'                       : 'a_lambda_guy', 
                              'seed'                        : 4, 
                              'engine_type'                 : 'light'})
# ------------------------------------------------- #
AnalyticsManager.new_bus({'input_file_name'        : 'Q:/tick_ged', 
                    'trading_destination_id' : 6, 
                    'name'                   : 'stock_STM_MI_6', 
                    'data_type'              : 'TBT2', 
                    'opening'                : '08:00:00:000', 
                    'date'                   : '20100104', 
                    'security_id'            : 7153, 
                    'closing'                : '16:25:00:000', 
                    'ric'                    : 'STM.MI'},
                   [])
# ------------------------------------------------- #
AnalyticsManager.new_bus({'input_file_name'        : 'Q:/tick_ged', 
                    'trading_destination_id' : 4, 
                    'name'                   : 'stock_STM_PA_4', 
                    'data_type'              : 'TBT2', 
                    'opening'                : '08:00:00:000', 
                    'date'                   : '20100104', 
                    'security_id'            : 247, 
                    'closing'                : '16:30:00:000', 
                    'ric'                    : 'STM.PA'},
                   ['curves'])
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Create traders                    #
# ------------------------------------------------- #
StocksComparator000_STM_PA_4 = StocksComparator({'class_name'             : 'StocksComparator', 
                                                 'counter'                : 0, 
                                                 'name'                   : 'StocksComparator000_STM_PA_4'},
                                                {'date'                   : '20100104', 
                                                 'security_id'            : 247, 
                                                 'trading_destination_id' : 4, 
                                                 'ric'                    : 'STM.PA'},
                                                {'plot_mode'              : 1, 
                                                 'trading_destination_id2' : 6, 
                                                 'ric2'                   : 'STM.MI', 
                                                 'save_into_file'         : True},
                                                trace)
sched.addAgent(StocksComparator000_STM_PA_4)
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
SimulationResults['StocksComparator000_STM_PA_4']  = StocksComparator000_STM_PA_4.results()
globals()['return_SIMEP']=SimulationResults
# ------------------------------------------------- #



# ------------------------------------------------- #
#                   Write Results                   #
# ------------------------------------------------- #
StocksComparator000_STM_PA_4.write_mat_file('C:/st_repository/simep_scenarii/ROB/StocksComparator/STM_PA_004/DAY_20100104/b55b13a34317fc8a3e9faf63dadd98df.mat')
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Destroy objects                  #
# ------------------------------------------------- #
del(StocksComparator000_STM_PA_4)
del(stock_STM_MI_6)
del(stock_STM_PA_4)
AnalyticsManager.delete_all()
del(trace)
del(sched)
# ------------------------------------------------- #



