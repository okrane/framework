from simep.bin.simepcore import *
from simep.core.analyticsmanager import *
from simep.agents.tvfo_is_tactic_triggerer import *
from simep.models.sbbmodel import *



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
sched.addOrderBook('normal', 'AXAF.PA')
stock_AXAF_PA_4 = SBBModel({'login'                  : 'a_lambda_guy', 
                            'name'                   : 'stock_AXAF_PA_4', 
                            'engine_type'            : 'normal'},
                           {'input_file_name'        : 'Q:/tick_ged', 
                            'trading_destination_id' : 4, 
                            'name'                   : 'stock_AXAF_PA_4', 
                            'data_type'              : 'TBT2', 
                            'opening'                : '07:00:00:000', 
                            'date'                   : '20100810', 
                            'security_id'            : 18, 
                            'closing'                : '15:30:00:000', 
                            'ric'                    : 'AXAF.PA'},
                           {'number_of_bt'           : 10, 
                            'full'                   : False, 
                            'seed'                   : 4},
                           trace)
sched.addAgent(stock_AXAF_PA_4)
# ------------------------------------------------- #



# ------------------------------------------------- #
#              Create the bus manager               #
# ------------------------------------------------- #
AnalyticsManager.set_sched(sched)
AnalyticsManager.set_trace(trace)
AnalyticsManager.set_engine_params({'number_of_bt'                : 10, 
                              'full'                        : False, 
                              'name'                        : 'stock_AXAF_PA_4', 
                              'class_name'                  : 'SBBModel', 
                              'login'                       : 'a_lambda_guy', 
                              'seed'                        : 4, 
                              'engine_type'                 : 'normal'})
# ------------------------------------------------- #
AnalyticsManager.new_bus({'input_file_name'        : 'Q:/tick_ged', 
                    'trading_destination_id' : 4, 
                    'name'                   : 'stock_AXAF_PA_4', 
                    'data_type'              : 'TBT2', 
                    'opening'                : '07:00:00:000', 
                    'date'                   : '20100810', 
                    'security_id'            : 18, 
                    'closing'                : '15:30:00:000', 
                    'ric'                    : 'AXAF.PA'},
                   ['imb_p_s_900', 'vwavg_spread_bp_s_900', 'curves', 'garman_klass_bp_s_900', 'avg_spread_bp_t_60', 'ret_p_s_900', 'imb_p_s_600', 'volume_less_arrival_p_s_600'])
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Create traders                    #
# ------------------------------------------------- #
TvfoTriggeredIs000_AXAF_PA_4 = TvfoTriggeredIs({'class_name'             : 'TvfoTriggeredIs', 
                                                'counter'                : 0, 
                                                'name'                   : 'TvfoTriggeredIs000'},
                                               {'date'                   : '20100810', 
                                                'security_id'            : 18, 
                                                'trading_destination_id' : 4, 
                                                'ric'                    : 'AXAF.PA'},
                                               {'tactic_max_lifetime'    : '00:10:00:000000', 
                                                'tactic_class'           : 'TrackingPlacement', 
                                                'algo_start_time'        : '+00:10:00:000000', 
                                                'low_envelope'           : 0.03, 
                                                'full_filename_skeleton' : 'C:/Results/test_dummy_', 
                                                'plot_mode'              : 1, 
                                                'delta_trigger'          : '00:10:00:000000', 
                                                'algo_end_time'          : '+07:00:00:000000', 
                                                'limit_price'            : 1000.0, 
                                                'asked_qty'              : 50000, 
                                                'arrival_price_offset'   : 0, 
                                                'high_envelope'          : 0.03, 
                                                'optimal_qty'            : 1000, 
                                                'tactic_module'          : 'usr.dev.sivla.agents.TrackingPlacement', 
                                                'side'                   : 0},
                                               trace)
sched.addAgent(TvfoTriggeredIs000_AXAF_PA_4)
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Run simulation                    #
# ------------------------------------------------- #
sched.run()
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Destroy objects                  #
# ------------------------------------------------- #
del(TvfoTriggeredIs000_AXAF_PA_4)
del(stock_AXAF_PA_4)
AnalyticsManager.delete_all()
del(trace)
del(sched)
# ------------------------------------------------- #



