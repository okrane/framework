# -*- coding: utf-8 -*-
"""
Created on Wed Oct 09 14:36:36 2013

@author: whuang
"""

import pandas as pd
import datetime as dt
import time as time
import pytz
import numpy as np
# import lib.data.matlabutils as matlabutils
import logging
import lib.tca.mapping as mapping
from lib.tca.algodata import *
import lib.data.dataframe_tools as dftools
import lib.data.dataframe_plots as dfplots
import lib.data.matlabutils as mutils
import lib.io.toolkit as toolkit
import re
import lib.dbtools.get_repository as get_repository
from lib.tca.algodata import *
from lib.tca.algoplot import PlotEngine
import matplotlib.pyplot as plt
from lib.tca.algopara_list import *
import datetime as dt

DEFAULT_FIGSIZE = (13,7)

class Client_Analyst(object):
    def __init__(self, kbase=None):
        self.kbase = kbase
        self.stats = {}
        self.images = {}
        self.conclusion = {}
        self.plotengine=PlotEngine()
        self.algolist = []
                        
    def study_evolution(self,algo_data=None):
        # occ_fe_data data analysis
        if algo_data.entry_level == 'occ_fe':
            # ----------------------------------------------    
            # --------- volume evolution study -------------
            fig = plt.figure(figsize=(16, 12))
            plt.subplot(2,3,1)
            # daily evolution
            image_tmp,self.stats['occ_fe_evolution_daily'] = self.plotengine.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='mturnover_euro',agg_step='day',fig = fig,rotation_xtick = -15)  
            plt.subplot(2,3,2)            
            # weekly evolution
            image_tmp,self.stats['occ_fe_evolution_weekly'] = self.plotengine.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='mturnover_euro',agg_step='week',fig = fig,rotation_xtick = -15)
            # monthly evolution
            plt.subplot(2,3,3)  
            image_tmp,self.stats['occ_fe_evolution_monthly'] = self.plotengine.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='mturnover_euro',agg_step='month',fig = fig,rotation_xtick = -15)  
            
            # ----------------------------------------------          
            # --------- slippage evolution study -----------
            # daily evolution
            plt.subplot(2,3,4)
            image_tmp,self.stats['occ_fe_evolution_slippage_spread_daily'] = self.plotengine.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='avg_slippage_spread',agg_step='day',fig = fig,rotation_xtick=-15)  
            # weekly evolution
            plt.subplot(2,3,5)
            image_tmp,self.stats['occ_fe_evolution_slippage_spread_weekly'] = self.plotengine.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='avg_slippage_spread',agg_step='week',fig = fig,rotation_xtick=-15)
            # monthly evolution
            plt.subplot(2,3,6)
            image_tmp,self.stats['occ_fe_evolution_slippage_spread_monthly'] = self.plotengine.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='avg_slippage_spread',agg_step='month',fig = fig,rotation_xtick=-15)            

            self.images['occ_fe_evolution']= fig           
            
            # daily evolution by algo
            # self.images['occ_fe_evolution_daily_byalgo'],self.stats['occ_fe_evolution_daily_byalgo'] = self.plotengine.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='mturnover_euro',gvar = 'occ_fe_strategy_name_mapped', agg_step='day')
            # daily evolution by place
            # self.images['occ_fe_evolution_daily_byplace'],self.stats['occ_fe_evolution_daily_byplace'] = self.plotengine.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='mturnover_euro',gvar = 'place', agg_step='day')
            # weekly evolution by algo
            # self.images['occ_fe_evolution_weekly_byalgo'],self.stats['occ_fe_evolution_weekly_byalgo'] = self.plotengine.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='mturnover_euro',gvar = 'occ_fe_strategy_name_mapped', agg_step='week')
            # weekly evolution by place
            # self.images['occ_fe_evolution_weekly_byplace'],self.stats['occ_fe_evolution_weekly_byplace'] = self.plotengine.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='mturnover_euro',gvar = 'place', agg_step='week')
            # monthly evolution by algo
            # self.images['occ_fe_evolution_monthly_byalgo'],self.stats['occ_fe_evolution_monthly_byalgo'] = self.plotengine.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='mturnover_euro',gvar = 'occ_fe_strategy_name_mapped', agg_step='month')
            # monthly evolution by place
            # self.images['occ_fe_evolution_monthly_byplace'],self.stats['occ_fe_evolution_monthly_byplace'] = self.plotengine.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='mturnover_euro',gvar = 'place', agg_step='month')
            # ----------------------------------------------          
            # --------- slippage evolution study -----------
            # daily evolution
            # self.images['occ_fe_evolution_slippage_spread_daily'],self.stats['occ_fe_evolution_slippage_spread_daily'] = self.plotengine.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='avg_slippage_spread',agg_step='day')  
            # weekly evolution
            # self.images['occ_fe_evolution_slippage_spread_weekly'],self.stats['occ_fe_evolution_slippage_spread_weekly'] = self.plotengine.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='avg_slippage_spread',agg_step='week')
            # monthly evolution
            # self.images['occ_fe_evolution_slippage_spread_monthly'],self.stats['occ_fe_evolution_slippage_spread_monthly'] = self.plotengine.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='avg_slippage_spread',agg_step='month')            
            # daily evolution by algo
            # self.images['occ_fe_evolution_slippage_spread_daily_byalgo'],self.stats['occ_fe_evolution_slippage_spread_daily_byalgo'] = self.plotengine.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='avg_slippage_spread',gvar = 'occ_fe_strategy_name_mapped', agg_step='day')           
            # daily evolution by place
            # self.images['occ_fe_evolution_slippage_spread_daily_byplace'],self.stats['occ_fe_evolution_slippage_spread_daily_byplace'] = self.plotengine.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='avg_slippage_spread',gvar = 'place', agg_step='day')
            # week evolution by algo
            # self.images['occ_fe_evolution_slippage_spread_weekly_byalgo'],self.stats['occ_fe_evolution_slippage_spread_weekly_byalgo'] = self.plotengine.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='avg_slippage_spread',gvar = 'occ_fe_strategy_name_mapped', agg_step='week')           
            # week evolution by place
            # self.images['occ_fe_evolution_slippage_spread_weekly_byplace'],self.stats['occ_fe_evolution_slippage_spread_weekly_byplace'] = self.plotengine.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='avg_slippage_spread',gvar = 'place', agg_step='week')
            # monthly performance evolution by algo
            # self.images['occ_fe_evolution_slippage_spread_monthly_byalgo'],self.stats['occ_fe_evolution_slippage_spread_monthly_byalgo'] = self.plotengine.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='avg_slippage_spread',gvar = 'occ_fe_strategy_name_mapped', agg_step='month')                    
            # monthly performance evolution by place
            # self.images['occ_fe_evolution_slippage_spread_monthly_byplace'],self.stats['occ_fe_evolution_slippage_spread_monthly_byplace'] = self.plotengine.plot_algo_evolution(algo_data = algo_data, level='occ_fe',var='avg_slippage_spread',gvar = 'place', agg_step='month')

    def study_parameter(self,algo_data=None):
        
        if algo_data.entry_level != 'sequence':
            return
        algo_list, algo_data_byalgo = split_by_algo(algo_data.data_sequence)
        self.algo_list = algo_list
        
        # average number of sequence per occurence
        avg_seq = {}
        for algo in algo_list:
            if algo in ['DYNAMIC VOLUME', 'VOLUME']:
                continueocc_fe
            tmp_data = algo_data_byalgo[algo]
            occ_num = len(np.unique(tmp_data['p_occ_id']))
            seq_num = len(tmp_data)
            avg_seq[algo] = seq_num/occ_num
        self.stats['avg_seq_per_occ'] = avg_seq

        fig, ax = plt.subplots()
        ind = np.arange(len(avg_seq))
        width = 0.35
        ax.bar(ind,avg_seq.values(),width)
        ax.set_ylabel('avg_seq_per_occ')
        ax.set_title('avg_seq_per_occ')
        ax.set_xticks(ind+width)
        ax.set_xticklabels(avg_seq.keys())
        self.images['avg_seq_per_occ'] = fig
        
        # percentage of empty parameters, weighted by number of sequence
        for algo in algo_list:
            if algo in ['DYNAMIC VOLUME', 'VOLUME']:
                continue
            tmp_data = algo_data_byalgo[algo]
            # para_mand = get_para_mandatory(algo)
            para_opt = get_para_optional(algo)
            para_res = {}
            for para in para_opt:
                pct_empty = sum(tmp_data[para].values == np.nan) / len(tmp_data[para])
                para_res[para] = pct_empty
            self.stats['pct_empty_para_' + algo] = para_res
#            fig, ax = plt.subplots()
#            ind = np.arange(len(para_opt))
#            width = 0.35
#            ax.bar(ind,para_res.values(),width)
#            ax.set_ylabel('Pct_empty')
#            ax.set_title('pct empty algo submission parameters for algo ' + algo)
#            ax.set_xticks(ind+width)
#            ax.set_xticklabels(para_res.keys())
#            self.images['pct_empty_para_' + algo] = fig
            
        # number of sequence per occurrence pie chart
        fig = plt.figure(figsize=(16, 12))
        fig_count = 1
        for algo in ['VWAP','TWAP','VOL','ICEBERG','DYNVOL','IS','CLOSE','HUNT','CROSSFIRE','BLINK']:
            if not algo in algo_data_byalgo.keys():
                fig_count += 1
                continue
            tmp_data = algo_data_byalgo[algo]
            idx_multisequence = tmp_data['nb_replace'] > 0
            if sum(idx_multisequence) == 0:
                labels = ['Nb seq = 1']
                sizes = [len(tmp_data)]
                plt.subplot(3,3,fig_count)
                plt.pie(sizes, labels=labels,
                        autopct='%1.1f%%', startangle=90)
                plt.title('NbSeq/Occ: ' + algo)
                fig_count += 1
                continue
            sizes = [len(np.unique(tmp_data['p_occ_id'])), len(np.unique(tmp_data[idx_multisequence]['p_occ_id']))]
            labels = ['Nb seq = 1','Nb seq > 1']
            plt.subplot(3,3,fig_count)
            plt.pie(sizes, labels=labels,
                    autopct='%1.1f%%', startangle=90)
            plt.title('NbSeq/Occ: ' + algo)
            fig_count += 1
        self.images['nb_seq_dist'] = fig
        
        # for para change between sequences
        fig = plt.figure(figsize=(16, 12))
        fig_count = 1
        for algo in ['VWAP','TWAP','VOL','ICEBERG','DYNVOL','IS','CLOSE','HUNT','CROSSFIRE','BLINK']:
            if not algo in algo_data_byalgo.keys():
                fig_count += 1
                continue
            tmp_data = algo_data_byalgo[algo]
            para_mand = get_para_mandatory(algo)
            para_opt = get_para_optional(algo)
            para_total = para_mand + para_opt
            dict_change = {}
            for para in para_total:
                dict_change[para] = 0
            dict_change['NoParaChange'] = 0
            dict_change['StrategyChange'] = 0

            idx_multisequence = tmp_data['nb_replace'] > 0
            if sum(idx_multisequence) == 0:
                fig_count += 1
                continue
            clr_vec = np.unique(tmp_data[idx_multisequence]['p_occ_id'])
            for clr_id in clr_vec:
                idx_clr = tmp_data['p_occ_id'] == clr_id
                tmp_data_seq = tmp_data[idx_clr]
                nb_sequence = len(tmp_data_seq)
                for i_seq in range(0,nb_sequence-1):
                    idx_seq_last = tmp_data_seq['nb_replace'] == i_seq
                    idx_seq_current = tmp_data_seq['nb_replace'] == i_seq + 1
                    if sum(idx_seq_last) > 0 and sum(idx_seq_current) == 0:
                        dict_change['StrategyChange'] += 1
                        continue
                    elif sum(idx_seq_last) == 0:
                        continue
                    no_change = True
                    for para in para_total:
                        value_seq_last = tmp_data_seq[idx_seq_last][para].values[0]
                        value_seq_current = tmp_data_seq[idx_seq_current][para].values[0]
                        if not my_isequal(value_seq_last, value_seq_current):
                            dict_change[para] += 1
                            no_change = False
                    if no_change:
                        dict_change['NoParaChange'] += 1
            self.stats['Para_change' + algo] = dict_change
            labels = np.array(dict_change.keys())
            sizes = np.array(dict_change.values())
            pct_size = 1.0*sizes/sum(sizes)
            idx_other = pct_size < 0.05
            idx_keep = pct_size >= 0.05
            size_other = sum(sizes[idx_other])
            labels = labels[idx_keep]
            sizes = sizes[idx_keep]
            if size_other > 0:
                sizes = np.append(sizes, size_other)
                labels = np.append(labels, 'Others')
            plt.subplot(3,3,fig_count)
            plt.pie(sizes, labels=labels,
                    autopct='%1.1f%%', startangle=90)
            plt.title('para change: ' + algo)
            fig_count += 1
                    
        self.images['para_change'] = fig
            
    def study_pie_chart(self, algo_data):
        if algo_data.entry_level == 'sequence' or algo_data.entry_level == 'occ_fe':
            self.stats['distribution'] = {}
            # strategy turnover distribution
            fig = plt.figure()
            if algo_data.entry_level == 'sequence':
                self.stats['distribution_byalgo'] = self.plotengine.plot_piechart(level = algo_data.entry_level, algo_data = algo_data, var='mturnover_euro', gvar = 'strategy_name_mapped')     
            else:
                self.stats['distribution']['algo'] = self.plotengine.plot_piechart(level = algo_data.entry_level, algo_data = algo_data, var='mturnover_euro', gvar = 'occ_fe_strategy_name_mapped')     
            plt.title('turnover distribution by algo')
            self.images['distribution_byalgo'] = fig
            # place turnover distribution
            fig = plt.figure()   
            self.stats['distribution_byplace'] = self.plotengine.plot_piechart(level = algo_data.entry_level, algo_data = algo_data, var='mturnover_euro', gvar = 'place')     
            plt.title('turnover distribution by place')
            self.images['distribution_byplace'] = fig

    def show_figures(self, stats):
        if stats == 'pct_empty_para':
            for algo in self.algo_list:
                self.images['pct_empty_para' + algo].show()
        elif stats == 'para_change':
            self.images['para_change'].show()
        elif stats == 'avg_seq_per_occ':
            self.images['avg_seq_per_occ'].show()
        else:
            self.images[stats].show()
            
def split_by_algo(algo_data=None):
    algo_list_tmp = np.unique(algo_data['strategy_name_mapped'].values)
    if len(algo_list_tmp) == 0:
        return [], None
    algo_list = []
    algo_data_byalgo = {}
    for algo in algo_list_tmp:
        if not my_isnan(algo):
            algo_list.append(algo)
            algo_data_byalgo[algo] = algo_data[algo_data['strategy_name_mapped']==algo]
    return algo_list, algo_data_byalgo
    

def my_isnan(var):
    return var != var
    
def my_isequal(var1,var2):
    if my_isnan(var1) and my_isnan(var2):
        return True
    elif my_isnan(var1) and not my_isnan(var2):
        return False
    elif not my_isnan(var1) and my_isnan(var2):
        return False
    else:
        return var1 == var2
            




