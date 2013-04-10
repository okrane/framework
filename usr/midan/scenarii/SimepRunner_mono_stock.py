#--------------------------------------------------------------------
# Import modules
#--------------------------------------------------------------------

from simep.scenarii.metascenario import MetaScenario
import math
from simep.sched import Order
from simep.funcs.dbtools.securities_tools import SecuritiesTools
# select characteristics daily
import sqlite3
conn = sqlite3.connect('C:/st_repository/simep_databases/trading_daily')
conn.row_factory = sqlite3.Row
c = conn.cursor()

order_side = Order.Buy

date_str_beg = '2010-01-01'
date_str_end = '2010-01-30'

      
security_id  = 276
trading_destination_id = 4
ric = SecuritiesTools.get_ric('sqlite', security_id, trading_destination_id)


ref_idx_sec_id = 110
ref_idx_td_id  = trading_destination_id
ref_idx = SecuritiesTools.get_ric('sqlite', ref_idx_sec_id, ref_idx_td_id)



global dates_dict, dictUniverse
dates_dict = {}
dictUniverse = {}

price_limit = []
ats         = []
adv         = [] 
c.execute("select date, volume, nb_deal, open_prc, high_prc, low_prc from characteristics where date between date('%s') and  date('%s') and trading_destination_id = %d and security_id = %d" \
          % (date_str_beg ,date_str_end, trading_destination_id, security_id))
for data in c:
    date_str = str(data['date'])
    date_str = date_str[0:4] + date_str[5:7] + date_str[8:10]
    dates_dict[date_str] = {}
    if (order_side == Order.Buy):        
        dates_dict[date_str]['soft_limit'] = 1.0
        price_thrsh = data['high_prc']
    else:
        dates_dict[date_str]['soft_limit'] = 10000.0
        price_thrsh = data['low_prc']
    dates_dict[date_str]['price_limit']       = 1.0/3.0*data['open_prc']+ 2.0/3.0*price_thrsh
    dates_dict[date_str]['hist_trade_volume'] = data['volume']
    dates_dict[date_str]['hist_avg_t_size']   = int(math.floor(data['volume']/data['nb_deal']))
    dates_dict[date_str]['buy'] = order_side
    # update dictUniverse
    dictUniverse[date_str] = [ric]  

conn.close()
 

#--------------------------------------------------------------------
# Define slave file : this is the file which will be generated just before simulation
#--------------------------------------------------------------------

SimulationSlaveFile = 'C:/st_sim/dev/tests/gui/SimepRunner_ACCP_4_20100106_slave.py'
InputXMLFile = 'C:/st_sim/simep/st_sim.xml'



#--------------------------------------------------------------------
# Write all the dictionaries of parameters
#--------------------------------------------------------------------

EngineParametersDictionary = {
    'number_of_bt'           : 10,
    'full'                   : False
}

def custom_func(date, context, params):
    for (key, val) in dates_dict[date].iteritems():
        params[key] = val
    return params

dfaFloat001_params = {
    'setup'      : {'name'                   : 'dfaFloat001'},
    'context'    : {'security_id'            : security_id, 
                    'trading_destination_id' : trading_destination_id, 
                    'ric'                    : None},
    'parameters' : {'min_size_2_join'        : 20, 
                    'buy'                    : None, 
                    'ref_idx'                : ref_idx, 
                    'ref_idx_sec_id'         : ref_idx_sec_id,
                    'ref_idx_td_id'          : ref_idx_td_id,
                    'idx_rel_off'            : 0.1, 
                    'one_tick'               : 0.01, 
                    'child_fin'              : 100, 
                    'rel_del'                : 0, 
                    'rnd_fact'               : 20, 
                    'tag_vol_src'            : True,
                    'child_size'             : 20, 
                    'hist_trade_volume'      : None, 
                    'soft_limit'             : None, 
                    'const_max_size'         : 50, 
                    'ats_width'              : 60, 
                    'price_limit'            : None,
                    'o_rel_trig'             : 0, 
                    'hist_avg_t_size'        : None},
    'custom_func' : custom_func }



#--------------------------------------------------------------------
# Define meta-scenario
#--------------------------------------------------------------------

MyScenario = MetaScenario(InputXMLFile, SimulationSlaveFile)
MyScenario.SetEngine('ROBModel', EngineParametersDictionary)
MyScenario.SetUniverse(dictUniverse)
MyScenario.AddTrader('dfa_float', dfaFloat001_params)
MyScenario.GenerateAndRunSimulations('C:/st_repository/simep_scenarii/DFA')




