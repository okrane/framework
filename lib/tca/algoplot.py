# -*- coding: utf-8 -*-
"""
Created on Thu Sep 12 09:21:26 2013

@author: njoseph
"""

import pandas as pd
import datetime as dt
import time as time
import pytz
import numpy as np
import logging
import lib.tca.mapping as mapping
from lib.tca.algodata import *
import lib.data.dataframe_tools as dftools
import lib.data.dataframe_plots as dfplots
import lib.data.matlabutils as mutils
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from lib.plots.color_schemes import kc_main_colors, algo_colors
import lib.dbtools.get_repository as get_repository
import lib.tca.algodata_formula as statsformula

DEFAULT_FIGSIZE = (13,7)

class PlotEngine(object):
    ###########################################################################
    # INIT : NOTHING FOR NOW
    ###########################################################################
    def __init__(self,start_date = None, end_date = None):
        # - plot bounds
        self.start_date = start_date
        self.end_date = end_date
    
    ###########################################################################
    # PLOT whithout self infos
    ###########################################################################
    def plot_algo_evolution(self,algo_data=None,level='sequence',var='mturnover_euro',gvar=None,agg_step='day',fig = None,rotation_xtick= 0):
        
        """
        evolution
        """
        data=pd.DataFrame()
        h=None
        #-----------------------------------
        # INPUT
        #-----------------------------------
        if not isinstance(algo_data, AlgoDataProcessor):
            raise ValueError('algo_data is not a proper algostatsprocessor class')
        
        # data aggregation            
        # a remplacer le var par le formule ou le clé
        stats_config = statsformula.get_formula(level = level, key = var)
        if stats_config is None:
            raise ValueError('Formula is not defined for %s'%var)
        data = algo_data.agg_stats(level=level,stats_config=stats_config,gvar=gvar,agg_step=agg_step)    
        
        # -- check  
        if data is None or data.shape[0]==0:
            logging.info('no data')
            return h,data
            
        # -- filter
        if data.shape[0]>0 and self.start_date is not None and self.end_date is not None:
            # handle timezone
            if self.start_date.tzinfo is None and data.index[0].to_datetime().tzinfo is not None:
                self.start_date=pytz.UTC.localize(self.start_date)
                self.end_date=pytz.UTC.localize(self.end_date)
            # do
            data=data[map(lambda x : x.to_datetime()>=self.start_date and x.to_datetime()<=self.end_date,data.index)]
            
        # -- check  
        if data.shape[0]==0:
            logging.info('no data in the asked timeframe')
            return h,data
            
        # -- handle datetime
        if self.start_date is not None:
            min_day=self.start_date.date()
            max_day=self.end_date.date()
        elif algo_data.start_date is not None:
            min_day=algo_data.start_date.date()
            max_day=algo_data.end_date.date()
        else:          
            min_day=min([x.to_datetime().date() for x in data.index])
            max_day=max([x.to_datetime().date() for x in data.index]) 
        #-----------------------------------
        # PLOT
        #-----------------------------------
        ylabel=''
        title='Evolution by '+ agg_step +'\n From ' + dt.datetime.strftime(min_day,'%d/%m/%Y') + ' To '+ dt.datetime.strftime(max_day,'%d/%m/%Y')
        
        if var == 'mturnover_euro':
            ylabel='Turnover(,000,000 Euros)'
        elif var == 'avg_slippage_spread':
            ylabel='Slippage(in spread size)'
            
        cmap = cm.spectral
        if gvar == 'is_dma':
            cmap = [kc_main_colors()['blue_1'],kc_main_colors()['blue_2']]
        tzname='Europe/London'
        h = dfplots.stackbar(data, 
                           var = var,
                           gvar ='tmp_date_end',
                           gvar_sbucket = 'tmp_date_start',
                           gvar_vals =gvar,
                           gvar_tzname = tzname,
                           title = title,
                           ylabel = ylabel,
                           show = False,
                           cmap = cmap,
                           legend_loc = 'upper center',
                           FIG_SIZE = DEFAULT_FIGSIZE,
                           FIG = fig,
                           ROTATION_XTICK = rotation_xtick,
                           alpha = 0.7)        
        return h,data
        
        
    def plot_algo_hbar(self,algo_data=None,level='sequence',var='mturnover_euro',gvar=None,gvar_vals=None):
        
        """
        intraday_exec_curve : 
        Plot the daily exec curve in turnover cross by group_var
        """
        data=pd.DataFrame()
        h=None
        #-----------------------------------
        # INPUT
        #-----------------------------------
        if not isinstance(algo_data, AlgoDataProcessor):
            raise ValueError('algo_data is not a proper algostatsprocessor class')

        # data aggregation
        stats_config = statsformula.get_formula(level=level, key=var)
        if stats_config is None:
            raise ValueError('Formula is not defined for %s'%var)
            
        data = algo_data.agg_stats(level=level,stats_config=stats_config,gvar=gvar,gvar_vals=gvar_vals)   

        # -- check  
        if data is None or data.shape[0]==0:
            logging.info('no data')
            return h,data
            
        # -- filter
        if data.shape[0]>0 and self.start_date is not None and self.end_date is not None:
            # handle timezone
            if self.start_date.tzinfo is None and data.index[0].to_datetime().tzinfo is not None:
                self.start_date=pytz.UTC.localize(self.start_date)
                self.end_date=pytz.UTC.localize(self.end_date)
            # do
            data=data[map(lambda x : x.to_datetime()>=self.start_date and x.to_datetime()<=self.end_date,data.index)]
            
        # -- check  
        if data.shape[0]==0:
            logging.info('no data in the asked timeframe')
            return h,data
            
        # -- handle datetime
        if self.start_date is not None:
            min_day=self.start_date.date()
            max_day=self.end_date.date()
        elif algo_data.start_date is not None:
            min_day=algo_data.start_date.date()
            max_day=algo_data.end_date.date()
        else:          
            min_day=min([x.to_datetime().date() for x in data.index])
            max_day=max([x.to_datetime().date() for x in data.index])
        
        #-----------------------------------
        # PLOT
        #-----------------------------------   
        # -- plot infos
        title='From ' + dt.datetime.strftime(min_day,'%d/%m/%Y') + ' To '+ dt.datetime.strftime(max_day,'%d/%m/%Y')
        cmap=cm.spectral
        xlabel=''
        
        if var == 'mturnover_euro':
            title+='\nTotal Turnover: ' + str(round(np.sum(data['mturnover_euro']),1)) + ' MEuros'
            xlabel='Turnover (,000,000 Euros)'
            if gvar_vals == 'is_dma':
                title+='\n DMA Turnover: ' + str(round(np.sum(data['mturnover_euro'][data['is_dma']=='Algo DMA']),1)) + ' MEuros'
                cmap=[kc_main_colors()['blue_1'],kc_main_colors()['blue_2']] 
                
        elif var == 'nb_occurrence':
            xlabel='Number of orders'
                    
        h=dfplots.stackbar(data,var=var,gvar=gvar,gvar_vals=gvar_vals,
                           is_horizontal=True,
                           title=title,
                           ecart_bucket=0.2,
                           add_gvar_txt=True,
                           xlabel=xlabel,
                           show=False,
                           cmap=cmap,
                           color=kc_main_colors()['blue_1'],
                           legend_loc='lower right',
                           FIG_SIZE=DEFAULT_FIGSIZE)
        
        return h,data 
    
    
    def plot_intraday_exec_curve(self,algo_data=None,group_var='strategy_name_mapped'):
        
        """
        intraday_exec_curve : 
        Plot the daily exec curve in turnover cross by group_var
        """
        data=pd.DataFrame()
        h=None
        #-----------------------------------
        # INPUT
        #-----------------------------------
        if algo_data is None:
            raise ValueError('algo_data is not a proper algostatsprocessor class')
        
        #-----------------------------------
        # GET / COMPUTE DATA
        #-----------------------------------
        data=algo_data.data_intraday_agg_deals
        
        # -- check  
        if data.shape[0]==0:
            logging.info('no deals data')
            return h,data
        
        # -- config
        stats_config={'nb_day': lambda df : len(df.mturnover_euro),
                      'mturnover_euro': lambda df : np.sum(df.mturnover_euro)}
          
        # -- filter
        if data.shape[0]>0 and self.start_date is not None and self.end_date is not None:
            # handle timezone
            if self.start_date.tzinfo is None and data.index[0].to_datetime().tzinfo is not None:
                self.start_date=pytz.UTC.localize(self.start_date)
                self.end_date=pytz.UTC.localize(self.end_date)
            # do
            data=data[map(lambda x : x.to_datetime()>=self.start_date and x.to_datetime()<=self.end_date,data.index)]
          
        # -- check  
        if data.shape[0]==0:
            logging.info('no data in the asked timeframe')
            return h,data
        
        # -- needed 4 aggregate
        if self.start_date is not None:
            min_day=self.start_date.date()
            max_day=self.end_date.date()
        elif algo_data.start_date is not None:
            min_day=algo_data.start_date.date()
            max_day=algo_data.end_date.date()
        else:          
            min_day=min([x.to_datetime().date() for x in data.index])
            max_day=max([x.to_datetime().date() for x in data.index])
         
        data['end_slice']=[dt.datetime.combine(max_day,x.timetz()) for x in data.index]
        # data['begin_slice']=[dt.datetime.strftime(dt.datetime.combine(max_day,x.time()),fmt) for x in data['begin_slice']]
          
        # -- aggregate
        data = dftools.agg(data,group_vars=['end_slice','strategy_name_mapped'],stats=stats_config)
        
        #-----------------------------------
        # PLOT
        #-----------------------------------   
        tzname='Europe/London'
        h=dfplots.stackbar(data,var='mturnover_euro',gvar='end_slice',gvar_vals='strategy_name_mapped',
                           gvar_tzname=tzname,
                           title='Intraday Execution Curve \n From ' + dt.datetime.strftime(min_day,'%d/%m/%Y') + ' To '+ dt.datetime.strftime(max_day,'%d/%m/%Y'),
                           ylabel='Turnover/Algo (,000,000 Euros)',
                           xlabel='Hours ('+tzname+')',
                           show=False,
                           legend_loc='upper center',
                           cmap = algo_colors(),
                           FIG_SIZE=DEFAULT_FIGSIZE)
        
        return h,data
        
    def plot_piechart(self, level = None, algo_data = None, var='mturnover_euro', gvar = 'strategy_name_mapped'):
        
        stats_config = statsformula.get_formula(level=level, key=var)
        if stats_config is None:
            raise ValueError('Formula is not defined for %s'%var)
        data = algo_data.agg_stats(level=level,stats_config=stats_config,gvar=gvar)
        labels = np.unique(data[gvar])            
        sizes = data[var]
        pct_size = 1.0*sizes/sum(sizes)
        idx_other = pct_size < 0.05
        idx_keep = pct_size >= 0.05
        size_other = sum(sizes[idx_other])
        labels = labels[idx_keep]
        sizes = sizes[idx_keep]
        if size_other > 0:
            sizes = np.append(sizes, size_other)
            labels = np.append(labels, 'Others')
        plt.pie(sizes, labels=labels,
                      autopct='%1.1f%%', startangle=90)
        return data

if __name__=='__main__':
    
    from lib.data.ui.Explorer import Explorer
    from lib.tca.algostats import AlgoStatsProcessor
    
    #-----  TEST plot_intraday_exec_curve
    sdate=dt.datetime(2013,12,03)
    edate=dt.datetime(2013,12,04)
    algo_data = AlgoStatsProcessor(start_date = sdate, end_date = edate)
    algo_data.get_db_data(level='sequence',force_colnames_only=['strategy_name_mapped','rate_to_euro','turnover','TargetSubID','ExDestination'])
    algo_data.get_db_data(level='deal')
    algo_data.get_intraday_agg_deals_data(group_var='strategy_name_mapped',step_sec=60*30)
    
    #------------------
    #-- all days
    testp=PlotEngine()
    
    #- BY DMA
    testp.plot_algo_hbar(algo_data=algo_data,gvar='strategy_name_mapped',gvar_vals='is_dma')
    plt.show()
    
#     
#     #- BY PLACE
#     testp.plot_algo_hbar(algo_data=algo_data,gvar='place',gvar_vals='is_dma')
#     plt.show()  
#      
#     testp.plot_algo_evolution(algo_data=algo_data,level='sequence',var='mturnover_euro',gvar='is_dma')
#     plt.show() 
    
    #- EXEC CURVE
    testp.plot_intraday_exec_curve(algo_data=algo_data)
    plt.show()
    
#     #------------------
#     #-- last day  
#     sdate_tmp=edate - dt.timedelta(seconds=60*60*24)
#     testp=PlotEngine(start_date = sdate_tmp, end_date = edate)  
#     
#     
#     #- BY DMA
#     testp.plot_algo_hbar(algo_data=algo_data,gvar='strategy_name_mapped',gvar_vals='is_dma')
#     plt.show()
#     
#     #- BY PLACE
#     testp.plot_algo_hbar(algo_data=algo_data,gvar='place',gvar_vals='is_dma')
#     plt.show()  
#             
#     #-- EXEC CURVE
#     testp.plot_intraday_exec_curve(algo_data=algo_data)
#     plt.show()    
    
    
    
    
    