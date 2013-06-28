# -*- coding: utf-8 -*-
"""
Created on Mon Jun 24 12:13:37 2013

@author: whuang
"""

import lib.dbtools.get_algodata as get_algodata
import matplotlib.pyplot as plt
import datetime
from lib.plots.color_schemes import kc_main_colors
import scipy.stats as stats
from projects.RobotAnalyst.robot_analyst import *

today = datetime.date.today()
if today.day < 10:
    today_matlab = '0' + str(today.day)
else:
    today_matlab = str(today.day)

if today.month < 10:
    today_matlab = today_matlab + '/0' + str(today.month) + '/' + str(today.year)
else:
    today_matlab = today_matlab + '/' + str(today.month) + '/' + str(today.year)

data_occ = get_algodata.occurence_info(start_date="13/05/2013",end_date=today_matlab)
data_seq = get_algodata.sequence_info(start_date="13/05/2013",end_date="13/06/2013")
date_idx = data_occ.index
var_occ = date_idx.dayofweek
data_occ['WeekDay'] = var_occ
date_idx = data_seq.index
var_occ = date_idx.dayofweek
data_seq['WeekDay'] = var_occ
date_idx = data_occ.index
var_occ = date_idx.hour
data_occ['StartHour'] = var_occ
date_idx = data_seq.index
var_occ = date_idx.hour
data_seq['StartHour'] = var_occ
# aggregate seq to occ
data_seq['turnover_euro'] = data_seq['rate2euro']*data_seq['turnover']
nb_occ = len(data_occ)
turnover_euro_occ = np.empty(nb_occ)
strategy_occ = np.empty(nb_occ)
side_occ = np.empty(nb_occ)
for i_occ in range(nb_occ):
    idx_seq = data_seq['occ_ID'] == data_occ['occ_ID'][i_occ]
    # turnover occurence
    turnover_euro_occ[i_occ] = sum(data_seq['turnover_euro'][idx_seq])
    # strategy_name occurence
    strategy_seq = data_seq['StrategyName'][idx_seq]
    if len(unique(strategy_seq)) == 1:
        strategy_occ[i_occ] = strategy_seq[0]
    else:
        # 100 for multi strategy
        strategy_occ[i_occ] = 100
    # execution side occurence
    side_seq = data_seq['Side'][idx_seq]
    if len(unique(side_seq)) == 1:
        side_occ[i_occ] = side_seq[0]
    else:
        # 0 for multi side
        side_occ[i_occ] = 0

data_occ['turnover_euro'] = turnover_euro_occ
data_occ['StrategyName'] = strategy_occ

# test robot
kbase = {}
name = 'robot1'
target = {}
target['Test_Uniform_Discrete'] = 'WeekDay'
robot_stat = robot_analyst_statistics(kbase = kbase, name = name, target = target)
question = 'whether number at StrategyName 5.0 is big'
robot_stat.answer(data_occ, question)
question = 'why number at StrategyName 5.0 is big'
kbase = {}
knowledge_key = 'det+' + 'number' + '+' + '' + '+' + 'WeekDay'
knowledge_value = 'Side' + '+' + 'StrategyName' + '+' + 'StartHour'
kbase[knowledge_key] = knowledge_value
knowledge_key = 'det+' + 'number' + '+' + '' + '+' + 'StrategyName'
knowledge_value = 'Side' + '+' + 'WeekDay' + '+' + 'StartHour'
kbase[knowledge_key] = knowledge_value

robot_stat = robot_analyst_statistics(kbase = kbase, name = name, target = target)
conclusion = robot_stat.answer(data_occ, question)

# figure_out = robot_stat.bar_discrete_number(data_seq, 'WeekDay', 'StrategyName')
