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
import lib.data.matlabutils as mutils
import matplotlib.pyplot as plt
import matplotlib.cm as cm

DEFAULT_FIGSIZE = (13,7)

class PlotEngine(object):
    ###########################################################################
    # INIT : NOTHING FOR NOW
    ###########################################################################
    
    
    
    ###########################################################################
    # PLOT whithout self infos
    ###########################################################################
    def plot_intraday_exec_curve(self,algo_data=None,start_date=None,end_date=None):
        """
        intraday_exec_curve : 
        Plot the daily exec curve in turnover cross by group_var
        """
        #-----------------------------------
        # INPUT
        #-----------------------------------
        if algo_data is None:
            raise ValueError('algo_data is not a proper algostatsprocessor class')
        
        #-----------------------------------
        # GET / COMPUTE DATA
        #-----------------------------------
        group_var='strategy_name_mapped'
        data=algo_data.data_intraday_agg_deals
        
        # -- config
        stats_config={'nb_day': lambda df : len(df.mturnover_euro),
                      'mturnover_euro': lambda df : np.sum(df.mturnover_euro)}
        
        # -- filter
        if data.shape[0]>0 and start_date is not None and end_date is not None:
            # handle timezone
            if start_date.tzinfo is None and data.index[0].to_datetime().tzinfo is not None:
                start_date=pytz.UTC.localize(start_date)
                end_date=pytz.UTC.localize(end_date)
            # do
            data=data[map(lambda x : x.to_datetime()>=start_date and x.to_datetime()<=end_date,data.index)]
            
        # -- check  
        if data.shape[0]==0:
            logging.info('no data')
            return
        
        # -- needed 4 aggregate
        tmp=np.array([[x.to_datetime().time(),x.to_datetime().date()] for x in data.index])
        data['tmp_date']=tmp[:,0]
        max_day=max(tmp[:,1])
        max_time=max(tmp[:,0])
        data['begin_slice']=[x.time() for x in data['begin_slice']]
        
        # -- aggregate
        data = dftools.agg(data,group_vars=['begin_slice','tmp_date',group_var],stats=stats_config)
        
        if data.shape[0]>0:
            data = data.sort_index(by=['tmp_date','begin_slice',group_var])
            data = data.set_index('tmp_date')
        else:
            logging.info('no data')
            return
            
        #-----------------------------------
        # PLOT
        #-----------------------------------   
        # ----- NEEDED
        min_time=min(data['begin_slice'])
        uni_strat = np.sort(np.unique(data[group_var].values).tolist())
        colors_strat = cm.spectral(np.linspace(0, 1.0, len(uni_strat)))
        uni_strat_islabeled = np.array([False]*len(uni_strat))
        # ----- PLOT
        h = plt.figure(figsize = DEFAULT_FIGSIZE)
        axes = plt.gca()
        axes.grid(True)
        
        plt.hold(True)
        prev_date=''
        prev_date_cum=0
        for i in range(data.shape[0]):
            #---
            # date=data.index[i].to_datetime()
            date=dt.datetime.combine(max_day,data.index[i])
            beg_date=dt.datetime.combine(max_day,data.iloc[i]['begin_slice'])
            idx_uni_strat=np.nonzero(uni_strat==data[group_var].ix[i])[0][0]
            #--
            args=[]
            if (not date==prev_date):
                args.append([beg_date,date,date,beg_date])
                args.append([0,0,data['mturnover_euro'].ix[i],data['mturnover_euro'].ix[i]])
                prev_date_cum=data['mturnover_euro'].ix[i]
            else:
                args.append([beg_date,date,date,beg_date])
                args.append([prev_date_cum,prev_date_cum,prev_date_cum+data['mturnover_euro'].ix[i],prev_date_cum+data['mturnover_euro'].ix[i]])
                prev_date_cum=prev_date_cum+data['mturnover_euro'].ix[i] 
            #--
            kwargs={'facecolor':colors_strat[idx_uni_strat],'alpha':0.85}
            if not uni_strat_islabeled[idx_uni_strat]:
                kwargs.update({'label':uni_strat[idx_uni_strat]})
                uni_strat_islabeled[idx_uni_strat]=True
            #--
            plt.gca().fill(*args,**kwargs)
            prev_date=date
            
        plt.hold(False)
        #-- text
        plt.ylabel('Turnover (,000,000) euros')
        plt.title('Intraday traded curve', size = 'large')
        #-- bounds
        xy_limit=axes.axis()
        axes.axis((np.round(xy_limit[0])+float(min_time.hour*60*60+min_time.minute*60+min_time.second)/(24*60*60),
                   np.round(xy_limit[0])+float(max_time.hour*60*60+max_time.minute*60+max_time.second)/(24*60*60), xy_limit[2], xy_limit[3]))
        
        #-- legend
        plt.legend(loc='upper center')   
        
        return h
    

if __name__=='__main__':
    
    from lib.data.ui.Explorer import Explorer
    from lib.tca.algostats import AlgoStatsProcessor
    
    #-----  TEST plot_intraday_exec_curve
    sdate=dt.datetime(2013,8,12)
    edate=dt.datetime(2013,8,15)
    algo_data = AlgoStatsProcessor(start_date = sdate, end_date = edate)
    algo_data.get_db_data(level='deal')
    algo_data.get_intraday_agg_deals_data()
    
    #-- all days
    testp=PlotEngine()
    testp.plot_intraday_exec_curve(algo_data=algo_data,start_date = sdate, end_date = edate)
    plt.show()
    
    #-- last days
    testp.plot_intraday_exec_curve(algo_data=algo_data,start_date = edate - dt.timedelta(seconds=60*60*24), end_date = edate)
    plt.show()    
    
    
    
    
    