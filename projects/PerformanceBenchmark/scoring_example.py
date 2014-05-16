# -*- coding: utf-8 -*-
"""
Created on Tue Jan 28 11:44:11 2014

@author: whuang
"""

# scoring client's execution

# client_list =  ['BLUEC','LGFRLGAM_C','ALKEN','AGIDIT','AMUNDI','MWAM','ARKEON','OTUS','CACIB','BANKIA','BLKUK','DANSKEB','AKO','DEGROOF','AKRO','RBC','SNS',
#                 'INSINGER','ADMI','ALETPR','ALETTI','CARAX','FISKE','LOOP','MONUM','PMUR','OUDART','ROBUR','ESPIRITO','NUMS']
                
import matplotlib
import os
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import pandas as pd
import datetime as dt
import time as time
import numpy as np
from lib.tca.referentialdata import ReferentialDataProcessor
from lib.tca.marketdata import MarketDataProcessor
from lib.tca.algostats import AlgoStatsProcessor
from lib.tca.algodata import AlgoDataProcessor
import lib.data.dataframe_tools as dftools
import lib.data.dataframe_plots as dfplots
import scipy.stats as stats
from lib.tca.marketdata import MarketDataProcessor
import lib.data.dataframe_tools as dftools
from lib.data.ui.Explorer import Explorer
from projects.PerformanceBenchmark.random_slicing import *

def not_array(a):
    out = np.ones((len(a),), dtype=bool)
    for i in range(0,len(a)):
        out[i] = not a[i]
    return out

def and_array(a,b):
    out = np.ones((len(a),), dtype=bool)
    for i in range(0,len(a)):
        out[i] = a[i] and b[i]
    return out

def or_array(a,b):
    out = np.ones((len(a),), dtype=bool)
    for i in range(0,len(a)):
        out[i] = a[i] or b[i]
    return out
    
def split_by_algo(algo_data=None):
    algo_list_tmp = np.unique(algo_data['occ_fe_strategy_name_mapped'].values)
    if len(algo_list_tmp) == 0:
        return [], None
    algo_list = []
    algo_data_byalgo = {}
    for algo in algo_list_tmp:
        if not my_isnan(algo):
            algo_list.append(algo)
            algo_data_byalgo[algo] = algo_data[algo_data['occ_fe_strategy_name_mapped']==algo]
    return algo_list, algo_data_byalgo
    

def my_isnan(var):
    return var != var

if __name__ == "__main__":
    client = 'BLUEC'
    now = dt.datetime.now() - dt.timedelta(days=1)
    day = dt.datetime(year = now.year, month=now.month, day=now.day, hour = 23, minute = 59, second = 59)
    for delta_day in range(365):
        sdate = day - dt.timedelta(days=delta_day)
        edate = day - dt.timedelta(days=delta_day-1)
        algo_data = AlgoStatsProcessor(start_date = sdate, end_date = edate)
        algo_data.get_db_data(level='sequence')
        if len(algo_data.data_sequence)==0:
            print delta_day
            continue
        idx_strategy = algo_data.data_sequence['strategy_name_mapped'] == 'VOL'
        idx_duration = algo_data.data_sequence['duration'] > 600
        idx_valid = and_array(idx_strategy,idx_duration)
        idx_qty = and_array(idx_duration, algo_data.data_sequence['exec_qty'] > 0)
        idx_valid = and_array(idx_valid,idx_qty)

        data_base = algo_data.data_sequence[idx_valid]
    
        # getting market data
    
        sec_id_vec = data_base['cheuvreux_secid']
        starttime_vec = data_base.index
        duration_vec = data_base['occ_duration']
        slippage_is = data_base['slippage_is_bp']
        slippage_vwap = data_base['slippage_vwap_bp']
        quantity = data_base['occ_fe_order_qty']
        side_vec = data_base['Side']
        vwap_vec = data_base['occ_fe_vwap']
    
        # paramaters of simulations
        nb_sim = 1000
        sim_price = np.zeros(nb_sim)
        sim_price2 = np.zeros(nb_sim)
        score_res = np.zeros(len(sec_id_vec))
        score_res2 = np.zeros(len(sec_id_vec))
        for i in range(len(sec_id_vec)):
            sec_id = sec_id_vec[i]
            starttime = starttime_vec[i]
            endtime = starttime + dt.timedelta(seconds=duration_vec[i])
            marketdata = MarketDataProcessor(date = starttime, security_id = sec_id)
            try:
                marketdata.get_data_tick()
            except:
                continue
            tickdata = marketdata.data_tick
            idx_period = and_array(tickdata.index >= starttime,tickdata.index <= endtime)
            marketdata.data_tick = marketdata.data_tick[idx_period]
            marketdata_slicer=dftools.agg(marketdata.data_tick,step_sec=300,
                                          stats={'nb_deal': lambda df : np.size(df.volume),
                                          'volume': lambda df : np.sum(df.volume),
                                          'vwap': lambda df : np.sum(df.price*df.volume)/np.sum(df.volume),
                                          'maxprice': lambda df : np.max(df.price),
                                          'minprice': lambda df : np.min(df.price)})
            if len(marketdata_slicer) == 0:
                continue
            order_qty = quantity[i]
            target_pct = order_qty/np.sum(marketdata_slicer['volume'])
            env_max = cumsum(marketdata_slicer['volume'].values)*target_pct*1.40
            env_min = cumsum(marketdata_slicer['volume'].values)*target_pct*0.60
            env_max = np.min(np.array([np.zeros(len(env_max))+order_qty,env_max]),axis = 0)
            env_min = np.max(np.array([np.zeros(len(env_min)),env_min]),axis = 0)
            env_min[len(env_min)-1] = order_qty
            env_max = np.insert(env_max,0,0)
            env_min = np.insert(env_min,0,0)
            # plt.plot(env_max,'r.-')
            # plt.hold(True)
            # plt.plot(env_min,'r.-')
            # gamma_centered_2
            
            for i_sim in range(nb_sim):
                trajectory = random_slicing(env_max,env_min,'uniform_2')
                sim_qty = trajectory[1:] - trajectory[:-1]
                sim_price[i_sim] = np.sum(sim_qty * marketdata_slicer['vwap'])/order_qty
                tmp = 0
                for i_slice in range(len(marketdata_slicer)):
                    tmp += sim_qty[i_slice]*(marketdata_slicer['minprice'][i_slice]+random.random()*(marketdata_slicer['maxprice'][i_slice]-marketdata_slicer['minprice'][i_slice])) 
                sim_price2[i_sim] = tmp/order_qty
                # plt.plot(trajectory,'b.-')
            side = side_vec[i]
            vwap = vwap_vec[i]
            if side == 1:
                score_res[i] = sum(sim_price > vwap)/nb_sim
            else:
                score_res[i] = sum(sim_price < vwap)/nb_sim
            if side == 1:
                score_res2[i] = sum(sim_price2 > vwap)/nb_sim
            else:
                score_res2[i] = sum(sim_price2 < vwap)/nb_sim

# date_start = starttime_vec[0].to_datetime()
#date_end = date_start + dt.timedelta(seconds=duration_vec[0])
#marketdata = MarketDataProcessor(date = date_start, security_id = 110)
#marketdata.get_data_tick()
#tickdata = marketdata.data_tick
#idx_period = and_array(tickdata.index >= date_start,tickdata.index <= date_end)
#marketdata.data_tick = marketdata.data_tick[idx_period]
#marketdata_slicer=dftools.agg(marketdata.data_tick,step_sec=300,
#                              stats={'nb_deal': lambda df : np.size(df.volume),
#                              'volume': lambda df : np.sum(df.volume),
#                              'vwap': lambda df : np.sum(df.price*df.volume)/np.sum(df.volume)})


        
    








