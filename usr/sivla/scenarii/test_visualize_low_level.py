from simep.sched import *
from simep.core.busmanager import *
from simep.funcs.dbtools.scenario_dictionnary import ScenarioDict
from usr.dev.sivla.agents.LobRecorder import *
from simep.models.sbbmodel import *



# ------------------------------------------------- #
#                Define Variables                   #
# ------------------------------------------------- #
sched = Scheduler('C:/st_sim/simep/Log')
trace = sched.getTrace()
# ------------------------------------------------- #



# ------------------------------------------------- #
#             Create stock(s) model(s)              #
# ------------------------------------------------- #
sched.addOrderBook('normal', 'ACCP.PA')
stock_ACCP_PA_4 = SBBModel({'login'                  : 'a_lambda_guy', 
                            'name'                   : 'stock_ACCP_PA_4', 
                            'engine_type'            : 'normal'},
                           {'trading_destination_id' : 4, 
                            'name'                   : 'stock_ACCP_PA_4', 
                            'data_type'              : 'BINARY', 
                            'opening'                : '08:00:00:000', 
                            'input_file_name'        : 'C:/Histo/lobTrade_2_4_20100104.binary', 
                            'date'                   : '20100104', 
                            'security_id'            : 2, 
                            'closing'                : '16:30:00:000', 
                            'ric'                    : 'ACCP.PA', 
                            'tick_sizes'             : '[(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]'},
                           {'number_of_bt'           : 400, 
                            'full'                   : False},
                           trace)
sched.addAgent(stock_ACCP_PA_4)
# ------------------------------------------------- #



# ------------------------------------------------- #
#              Create the bus manager               #
# ------------------------------------------------- #
BusManager.set_sched(sched)
BusManager.set_trace(trace)
BusManager.set_engine_params({'number_of_bt'                : 400, 
                              'full'                        : False, 
                              'name'                        : 'stock_ACCP_PA_4', 
                              'class_name'                  : 'SBBModel', 
                              'login'                       : 'a_lambda_guy', 
                              'engine_type'                 : 'normal'})
# ------------------------------------------------- #
BusManager.new_bus({'trading_destination_id' : 4, 
                    'name'                   : 'stock_ACCP_PA_4', 
                    'data_type'              : 'BINARY', 
                    'opening'                : '08:00:00:000', 
                    'input_file_name'        : 'C:/Histo/lobTrade_2_4_20100104.binary', 
                    'date'                   : '20100104', 
                    'security_id'            : 2, 
                    'closing'                : '16:30:00:000', 
                    'ric'                    : 'ACCP.PA', 
                    'tick_sizes'             : '[(0.0, 0.001), (10.0, 0.0050000000000000001), (50.0, 0.01), (100.0, 0.050000000000000003)]'},
                   [])
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Create traders                    #
# ------------------------------------------------- #
LobRecorder000_ACCP_PA_4 = LobRecorder({'class_name'             : 'LobRecorder', 
                                        'counter'                : 0, 
                                        'name'                   : 'LobRecorder000'},
                                       {'date'                   : '20100104', 
                                        'security_id'            : 2, 
                                        'trading_destination_id' : 4, 
                                        'ric'                    : 'ACCP.PA'},
                                       {'plot_mode'              : 0, 
                                        'start'                  : '2010-01-04 14:30:00', 
                                        'end'                    : '2010-01-04 15:00:00', 
                                        'record_only_trade_events' : True},
                                       trace)
sched.addAgent(LobRecorder000_ACCP_PA_4)
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
SimulationResults['LobRecorder000_ACCP_PA_4']  = LobRecorder000_ACCP_PA_4.results()
globals()['return_SIMEP']=SimulationResults
# ------------------------------------------------- #



# ------------------------------------------------- #
#                   Write Results                   #
# ------------------------------------------------- #
LobRecorder000_ACCP_PA_4.write_mat_file('C:/st_sim/usr/dev/sivla/scenarii/SBB/LobRecorder/ACCP_PA_004/DAY_20100104/01a1f71dcfd6f9e21671a55bc121818b.mat')
#ScenarioDict.insertScenarioParameters(3, 'LobRecorder', 'C:/st_sim/usr/dev/sivla/scenarii/SBB/LobRecorder/ACCP_PA_004/DAY_20100104/01a1f71dcfd6f9e21671a55bc121818b.mat', 'ACCP.PA', 2, 4, '20100104', 'SBBModel', "{'plot_mode': 0, 'start': datetime.datetime(2010, 1, 4, 14, 30), 'end': datetime.datetime(2010, 1, 4, 15, 0), 'record_only_trade_events': True}")
LobRecorder000_ACCP_PA_4.write_csv_file('C:/st_sim/usr/dev/sivla/scenarii/SBB/LobRecorder/ACCP_PA_004/DAY_20100104/01a1f71dcfd6f9e21671a55bc121818b.csv')
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Destroy objects                  #
# ------------------------------------------------- #
del(LobRecorder000_ACCP_PA_4)
del(stock_ACCP_PA_4)
BusManager.delete_all()
del(trace)
del(sched)
# ------------------------------------------------- #



