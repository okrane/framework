from simep.bin.simepcore import *
from simep.agents.analyticsmanager import *
from simep.robmodel import *
from usr.dev.midan.agents.dfa_float import *



# ------------------------------------------------- #
#                Define Variables                   #
# ------------------------------------------------- #
sched = Scheduler('C:/st_sim/simep/Log')
trace = sched.getTrace()
# ------------------------------------------------- #



# ------------------------------------------------- #
#             Create stock(s) model(s)              #
# ------------------------------------------------- #
sched.addOrderBook('light', 'BARC.L')
stock_BARC_L_1 = ROBModel({'login'                  : 'a_lambda_guy', 
                           'name'                   : 'stock_BARC_L_1', 
                           'engine_type'            : 'light'},
                          {'trading_destination_id' : 1, 
                           'name'                   : 'stock_BARC_L_1', 
                           'input_file_name'        : 'Y:/tick_ged', 
                           'date'                   : '20100107', 
                           'security_id'            : 10762, 
                           'ric'                    : 'BARC.L'},
                          {'number_of_bt'           : 10, 
                           'full'                   : False},
                          trace)
sched.addAgent(stock_BARC_L_1)
# ------------------------------------------------- #



# ------------------------------------------------- #
#              Create the bus manager               #
# ------------------------------------------------- #
AnalyticsManager.set_sched(sched)
AnalyticsManager.set_trace(trace)
# ------------------------------------------------- #
AnalyticsManager.new_bus({'trading_destination_id' : 1, 
                    'name'                   : 'stock_BARC_L_1', 
                    'input_file_name'        : 'Y:/tick_ged', 
                    'date'                   : '20100107', 
                    'security_id'            : 10762, 
                    'ric'                    : 'BARC.L'},
                   [])
# ------------------------------------------------- #



# ------------------------------------------------- #
#                 Create traders                    #
# ------------------------------------------------- #
dfaFloat001_BARC_L_1 = dfa_float({'class_name'             : 'dfa_float', 
                                  'name'                   : 'dfaFloat001'},
                                 {'date'                   : '20100107', 
                                  'security_id'            : 10762, 
                                  'trading_destination_id' : 1, 
                                  'ric'                    : 'BARC.L'},
                                 {'buy'                    : 1, 
                                  'one_tick'               : 0.01, 
                                  'rnd_fact'               : 20, 
                                  'tag_vol_src'            : True, 
                                  'child_size'             : 20, 
                                  'const_max_size'         : 50, 
                                  'o_rel_trig'             : 0, 
                                  'child_fin'              : 100, 
                                  'hist_avg_t_size'        : 3551, 
                                  'min_size_2_join'        : 20, 
                                  'ref_idx_td_id'          : 13, 
                                  'ref_idx'                : 'VOD.L', 
                                  'idx_rel_off'            : None, 
                                  'ats_width'              : 60, 
                                  'rel_del'                : 0, 
                                  'soft_limit'             : 10000.0, 
                                  'hist_trade_volume'      : 63298427.0, 
                                  'price_limit'            : 318.96, 
                                  'ref_idx_sec_id'         : 12058},
                                 trace)
sched.addAgent(dfaFloat001_BARC_L_1)
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
SimulationResults['dfaFloat001_BARC_L_1']  = dfaFloat001_BARC_L_1.results()
globals()['return_SIMEP']=SimulationResults
# ------------------------------------------------- #



# ------------------------------------------------- #
#                  Destroy objects                  #
# ------------------------------------------------- #
del(dfaFloat001_BARC_L_1)
del(stock_BARC_L_1)
AnalyticsManager.delete_all()
del(trace)
del(sched)
# ------------------------------------------------- #



