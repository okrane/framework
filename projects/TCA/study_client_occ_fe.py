# -*- coding: utf-8 -*-
"""
Created on Fri Sep 20 10:57:31 2013

@author: njoseph
"""

import matplotlib.pyplot as plt
import matplotlib.cm as cm
import pandas as pd
import datetime as dt
import time as time
import numpy as np
import pytz
# import lib.data.matlabutils as matlabutils
from lib.dbtools.connections import Connections
from lib.tca.referentialdata import ReferentialDataProcessor
from lib.tca.marketdata import MarketDataProcessor
from lib.tca.algostats import AlgoStatsProcessor
from lib.tca.algodata import AlgoDataProcessor
import lib.data.dataframe_tools as dftools
import lib.data.dataframe_plots as dfplots
from lib.data.ui.Explorer import Explorer
from lib.tca.algoplot import PlotEngine
import scipy.stats as stats
import lib.tca.algoanalyst as autoanalyst
reload(autoanalyst)
#----------
# params
client_prefix='BLKUK'
start_date=dt.datetime(2013,1,1)
end_date=dt.datetime(2013,10,1)

#----------
# OCC FE Analysis
# -- get data
# without client filter
# Connections.change_connections('production')
algo_data=AlgoStatsProcessor(start_date = start_date, end_date = end_date)

# with client filter
# algo_data=AlgoStatsProcessor(start_date = start_date, end_date = end_date,
                             # filter = {'Account': {'$regex' : client_prefix+'.*'}})
                             
# algo_data.get_occ_fe_data()
algo_data.get_db_data(level = 'sequence')
# algo_data.get_db_data(level = 'occurrence')
# algo_data_byalgo['VWAP']
# analyst
analyst = autoanalyst.Client_Analyst()
analyst.study_parameter(algo_data)
# analyst.study_pie_chart(algo_data)

#plot_allp=PlotEngine()
#tmp = plot_allp.plot_piechart(level = 'sequence', algo_data = algo_data, var='mturnover_euro', gvar = 'place')
#
#analyst.study_evolution(algo_occ_fe_data)
#close('all')
#
#turnover_algo = analyst.stats['occ_fe_evolution_monthly']
#slippage_algo = analyst.stats['occ_fe_evolution_slippage_spread_monthly']
#
#diff_turnover = turnover_algo['mturnover_euro'][1:].values - turnover_algo['mturnover_euro'][:-1].values
#plt.plot(slippage_algo['avg_slippage_spread'][:-1].values,diff_turnover,'.')
#
#turnover_algo = analyst.stats['occ_fe_evolution_weekly_byalgo']
#slippage_algo = analyst.stats['occ_fe_evolution_slippage_spread_week_byalgo']
#
#strategy_vec = turnover_algo['occ_fe_strategy_name_mapped']
#stats_dict = {}
#slippage_dict = {}
#for name_strategy in unique(strategy_vec):
#    stats_dict[name_strategy] = turnover_algo[strategy_vec==name_strategy]['mturnover_euro']
#    
#strategy_vec = slippage_algo['occ_fe_strategy_name_mapped']
#for name_strategy in unique(strategy_vec):
#    slippage_dict[name_strategy] = slippage_algo[strategy_vec==name_strategy]['avg_slippage_spread']
#
#diff_vwap = stats_dict['DYNVOL'][1:].values - stats_dict['DYNVOL'][:-1].values
#plt.plot(slippage_dict['DYNVOL'][:-1].values,diff_vwap,'.')
#
#dataframe.tohtml()
#
##plot_allp=PlotEngine()
##algo_data = algo_occ_fe_data
##plot_allp.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='mturnover_euro',agg_step='day')
##plot_allp.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='mturnover_euro',agg_step='day')  
### weekly evolution
##plot_allp.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='mturnover_euro',agg_step='week')
### monthly evolution
##plot_allp.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='mturnover_euro',agg_step='month')  
### monthly evolution by algo
##plot_allp.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='mturnover_euro',gvar = 'occ_fe_strategy_name_mapped', agg_step='month')
### monthly evolution by place
##plot_allp.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='mturnover_euro',gvar = 'place', agg_step='month')
### ----------------------------------------------          
### --------- slippage evolution study -----------
### daily evolution
##plot_allp.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='avg_slippage_spread',agg_step='day')  
### weekly evolution
##plot_allp.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='avg_slippage_spread',agg_step='week')
### monthly evolution
##plot_allp.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='avg_slippage_spread',agg_step='month')
### monthly performance evolution by algo
##plot_allp.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='avg_slippage_spread',gvar = 'occ_fe_strategy_name_mapped', agg_step='month')                    
### monthly performance evolution by place
##plot_allp.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='avg_slippage_spread',gvar = 'place', agg_step='month')
#
#
## distribution density estimation for slippage spread
#idx_finite = np.isfinite(algo_occ_fe_data.data_occurrence['slippage_spread']) 
#slippage_result = algo_occ_fe_data.data_occurrence['slippage_spread'][idx_finite]
#slippage_quantile = stats.mstats.mquantiles(slippage_result.tolist(),[0.01,0.99])
#pointstep = np.arange(start = slippage_quantile[0],stop = slippage_quantile[1],step = (slippage_quantile[1] - slippage_quantile[0])/50)
#idx_restraint = (slippage_result >= slippage_quantile[0]) & (slippage_result <= slippage_quantile[1])
#slippage_result = slippage_result[idx_restraint]
#histplot = plt.hist(slippage_result,bins = 50)
#
## distribution density estimation for slippage bp
#idx_finite = np.isfinite(algo_occ_fe_data.data_occurrence['slippage_bp']) 
#slippage_result = algo_occ_fe_data.data_occurrence['slippage_bp'][idx_finite==1]
#slippage_quantile = stats.mstats.mquantiles(slippage_result.tolist(),[0.01,0.99])
#pointstep = np.arange(start = slippage_quantile[0],stop = slippage_quantile[1],step = (slippage_quantile[1] - slippage_quantile[0])/50)
#idx_restraint = slippage_result >= slippage_quantile[0]
#slippage_result = slippage_result[idx_restraint]
#idx_restraint = slippage_result <= slippage_quantile[1]
#slippage_result = slippage_result[idx_restraint]
#histplot = plt.hist(slippage_result,bins = 100)
#
#
#            if var == 'mturnover_euro' or var == 'nb_occurrence':
#                stats_config={'mturnover_euro': lambda df : np.sum(df.rate_to_euro*df.occ_fe_exec_shares*df.occ_fe_avg_price.apply(lambda x : float(x))*1e-6),
#                              'nb_occurrence': lambda df : len(df.p_occ_id)}
#            elif var == 'avg_slippage_bp':
#                stats_config={'nb_occurrence': lambda df : len(df.p_occ_id),
#                              'avg_slippage_bp': lambda df: np.average(df.slippage_bp[np.isfinite(df.slippage_bp)])}
#            elif var == 'avg_slippage_spread':
#                stats_config={'nb_occurrence': lambda df : len(df.p_occ_id),
#                              'avg_slippage_spread': lambda df: np.average(df.slippage_spread[np.isfinite(df.slippage_spread)])}
#            else:
#                raise ValueError('undefined format: var unknown')
## compute agg stats
#data = algo_occ_fe_data.agg_stats(level='occ_fe',var='mturnover_euro',agg_step='year')
#data = algo_occ_fe_data.agg_stats(level='occ_fe',var='mturnover_euro',gvar = 'occ_fe_strategy_name_mapped',agg_step='month') 
## PLot on all the period
#plot_allp=PlotEngine()
#
#plt=plot_allp.plot_algo_evolution(algo_data=algo_occ_fe_data,level='occ_fe',var='avg_slippage_bp',gvar = 'occ_fe_strategy_name_mapped',agg_step = None)
#plt.show()
#=plot_allp.plot_algo_evolution(algo_data=algo_occ_fe_data,level='occ_fe',var='avg_slippage_spread',agg_step='day')
#
##-- evolution / turnover by algo
#tmp=plot_allp.plot_algo_evolution(algo_data=algo_occ_fe_data,level='occ_fe',var='mturnover_euro',gvar='occ_fe_strategy_name_mapped',agg_step='day')
#plt.show()
#
##-- evolution / turnover by algo
#tmp=plot_allp.plot_algo_evolution(algo_data=algo_occ_fe_data,level='occ_fe',var='mturnover_euro',gvar='occ_fe_strategy_name_mapped',agg_step='day')
#plt.show()
#
##-- algo usage in turnover
#tmp=plot_allp.plot_algo_hbar(algo_data=algo_occ_fe_data,level='occ_fe',var='mturnover_euro',gvar='occ_fe_strategy_name_mapped',gvar_vals = 'is_dma')
#plt.show()     
# 
#tmp=plot_allp.plot_algo_hbar(algo_data=algo_occ_fe_data,level='occ_fe',var='mturnover_euro',gvar='occ_fe_strategy_name_mapped')
#
##-- algo usage in nb occurence
#tmp=plot_allp.plot_algo_hbar(algo_data=algo_occ_fe_data,level='occ_fe',var='nb_occurrence',gvar='occ_fe_strategy_name_mapped')
#plt.show()   
#
#data = algo_occ_fe_data.agg_stats(level='occ_fe',var='avg_slippage_is_bp',agg_step='week')
#
#
#
#
#
