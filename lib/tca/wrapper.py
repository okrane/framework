import lib.dbtools.get_algodata as get_algodata
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from lib.plots.color_schemes import kc_main_colors
from numpy import unique, arange, any
from lib.io.toolkit import last_business_day
import lib.data.st_data as st_data
import pandas as pd
import numpy as np
import scipy 
import matplotlib.cm as cm
from lib.dbtools import get_repository
import lib.tca.get_algostats as get_algostats
from lib.logger.custom_logger import *

class Singleton(object):
    _instance = None
    def __init__(self, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(Singleton, cls).__init__(
                                cls, *args, **kwargs)
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(Singleton, cls).__new__(
                                cls, *args, **kwargs)
        return cls._instance
    
DEFAULT_FIGSIZE = (13,7)

def autolabel_barh(rects):
    # attach some text labels
    for rect in rects:
        width = rect.get_width()
        if width > 0:
            plt.text('%d'%(int(width)*1.02+2 ), rect.get_y() + rect.get_height()/2.5, width,
                    ha='left', va='bottom', size = 12, weight = 750)
        
class DataProcessor(object):
    get_done = 0
    def __init__(self, start_date = None, end_date = None, filter = None):
        """start_date and end_date are datetime"""
        self.get_data(start_date, end_date, filter = filter)
        DataProcessor.get_done += 1
    
    def get_data(self, start_date = None, end_date = None, filter = None):
        if start_date is None and end_date is None:
            self.start_date = self.end_date = last_business_day(datetime.now())
        else:
            self.start_date = start_date
            self.end_date   = end_date
        
        self.filter = filter
            
        self.start_date_str = datetime.strftime(self.start_date, "%d/%m/%Y")
        self.end_date_str   = datetime.strftime(self.end_date, "%d/%m/%Y")
        
        self.data_seq       = get_algodata.sequence_info(start_date = self.start_date_str,
                                                         end_date   = self.end_date_str, 
                                                         filter     = filter)
        self.data_occ       = get_algodata.occurrence_info(start_date = self.start_date_str,
                                                           end_date   = self.end_date_str, 
                                                           filter     = filter)            
    def get_deals(self):

        self.data_deals     = get_algodata.deal(start_date  = self.start_date_str,
                                                end_date    = self.end_date_str, 
                                                filter      = self.filter)         
    def get_agg_deals(self,step_sec=60*30):
        self.data_agg_deals     = get_algostats.agg_daily_deal(start_date=self.start_date_str,
                                                               end_date=self.end_date_str,
                                                               step_sec=step_sec,
                                                               filter=self.filter)                                              

class Statistic(DataProcessor):
    def get_stat_turnover_euro(self):
        self.data_seq['turnover_euro'] = self.data_seq['turnover'] * self.data_seq['rate_to_euro']
        
        
    def get_stat_dma(self):
        if 'turnover_euro' not in self.data_seq:
            self.get_stat_turnover_euro()
        f                = lambda x: x.loc['TargetSubID' ] == 'ON1' or x.loc['TargetSubID' ] == 'ON2' or x.loc['TargetSubID' ] == 'ON3'
        self.ind_dma     = self.data_seq.apply(func = f, axis = 1, raw=False )

        self.data_seq['is_dma'] = self.ind_dma.values
        
class PlotEngine(Statistic):
    def plot_algo_dma_vs_all(self, title, value_dma, value_all, index_all, xlabel ):  
        # Strategy Name
        fig = plt.figure(figsize = DEFAULT_FIGSIZE)
        axes = plt.gca()
        axes.grid(True)
        plt.hold(True)
        maxi                    = max(value_all)

        height                  = 0.8
        
        ind                     = arange(len(index_all)) 
        
        p_all                   = plt.barh( ind, value_all, height = height, color=kc_main_colors()['blue_2'], alpha=0.85)
        p_dma                   = plt.barh( ind, value_dma, height = height, color=kc_main_colors()['blue_1'], alpha=0.95)
        
        
        plt.xlabel(xlabel)
        plt.title(title, size = 'large')
        plt.yticks(height/2 + ind, map(str, index_all) )
        
        autolabel_barh(p_all)
        
        plt.legend([p_all, p_dma], ['OTHER', 'ALGO DMA'], loc=4)
        plt.hold(False)  
        return fig

    #------------------------------------------------------------------------------
    #  Intraday executed curve
    #------------------------------------------------------------------------------
    def plot_intraday_exec_curve(self, duration = "", step_sec=60*30, group_var='strategy_name_mapped'):
        """
        intraday_exec_curve : 
        Plot the daily exec curve in turnover cross by group_var
        """
        self.get_agg_deals(step_sec=step_sec)
        
        ##############################################################
        # plot
        ##############################################################  
        # ----- NEEDED    
        uni_strat = np.sort(np.unique(self.data_agg_deals[group_var].values).tolist())
        colors_strat = cm.spectral(np.linspace(0, 1.0, len(uni_strat)))
        uni_strat_islabeled = np.array([False]*len(uni_strat))
        # ----- PLOT
        h = plt.figure(figsize = DEFAULT_FIGSIZE)
        axes = plt.gca()
        axes.grid(True)
        
        plt.hold(True)
        prev_date=''
        prev_date_cum=0
        for i in range(self.data_agg_deals.shape[0]):
            #---
            date=self.data_agg_deals.index[i].to_datetime()
            idx_uni_strat=np.nonzero(uni_strat==self.data_agg_deals[group_var].ix[i])[0][0]
            #--
            args=[]
            if (not date==prev_date):
                args.append([date-timedelta(seconds=step_sec),date,date,date-timedelta(seconds=step_sec)])
                args.append([0,0,self.data_agg_deals['mturnover_euro'].ix[i],self.data_agg_deals['mturnover_euro'].ix[i]])
                prev_date_cum=self.data_agg_deals['mturnover_euro'].ix[i]
            else:
                args.append([date-timedelta(seconds=step_sec),date,date,date-timedelta(seconds=step_sec)])
                args.append([prev_date_cum,prev_date_cum,prev_date_cum+self.data_agg_deals['mturnover_euro'].ix[i],prev_date_cum+self.data_agg_deals['mturnover_euro'].ix[i]])
                prev_date_cum=prev_date_cum+self.data_agg_deals['mturnover_euro'].ix[i] 
            #--
            kwargs={'facecolor':colors_strat[idx_uni_strat],'alpha':0.85}
            if not uni_strat_islabeled[idx_uni_strat]:
                kwargs.update({'label':uni_strat[idx_uni_strat]})
                uni_strat_islabeled[idx_uni_strat]=True
            #--
            plt.gca().fill(*args,**kwargs)
            prev_date=date
            
        plt.hold(False)
        plt.ylabel('Turnover (,000,000) euros')
        plt.title('Intraday traded curve: ' + duration, size = 'large')
        plt.legend()   

        return h

    def plot_basic_stats(self, path = ['vol.jpg', 'occ.png', 'place.png']):
        self.plot_algo_volume().savefig(path[0])
        self.plot_algo_occ().savefig(path[1])
        self.plot_algo_place().savefig(path[2])   
        
        
    def plot_algo_volume(self):
        self.get_stat_dma()
        
        value_per_algo_dma2     = self.data_seq[self.ind_dma].groupby('strategy_name_mapped')['turnover_euro'].sum().order()
        value_per_algo_all      = self.data_seq.groupby('strategy_name_mapped')['turnover_euro'].sum().order()
        
        value_per_algo_dma      = pd.Series()
        for index in value_per_algo_all.index:
            if index not in value_per_algo_dma2.index:
                value_per_algo_dma = value_per_algo_dma.set_value(index,  0.0)
            else:
                value_per_algo_dma = value_per_algo_dma.set_value(index,  value_per_algo_dma2[index])
        
        
        
        value_per_algo_values_d = [round(p/1000000,2) for p in value_per_algo_dma.values]
        value_per_algo_values_a = [round(p/1000000,2) for p in value_per_algo_all.values]
        
        xlabel                  = 'Volume/Algo (,000,000 Euros)'
        total_all               = sum(value_per_algo_values_a)
        total_dma               = sum(value_per_algo_values_d)
        title  = 'From ' + self.start_date_str + ' To '+ self.end_date_str + '\nTotal Turnover: ' + str(round(total_all,1)) + ' MEuros\n DMA Turnover: ' + str(round(total_dma,1)) + ' MEuros'
        return self.plot_algo_dma_vs_all(title, value_per_algo_values_d, value_per_algo_values_a, value_per_algo_all.index, xlabel)
    
    def plot_algo_place(self):
        places = get_repository.tag100_to_place_name()
        
        self.get_stat_dma()
        
        
        # Get out the nan in ExDestination
        ind_seq_dma = self.data_seq[self.ind_dma]['ExDestination'].apply(lambda x: isinstance(x, basestring))
        ind_seq     = self.data_seq['ExDestination'].apply(lambda x: isinstance(x, basestring))
        
        seq = self.data_seq[ind_seq]
        seq_dma = self.data_seq[self.ind_dma][ind_seq_dma]
        
        # Get out the nan in turnover_euro
        ind_seq_dma_no_nan = seq_dma['turnover_euro'].apply(lambda x: np.isfinite(x))
        ind_seq_no_nan     = seq['turnover_euro'].apply(lambda x: np.isfinite(x))
        
        seq     = seq[ind_seq_no_nan]
        seq_dma = seq_dma[ind_seq_dma_no_nan]
        
        place_per_algo_dma      = pd.Series()
        place_per_algo_dma2     = seq_dma.groupby('ExDestination')['turnover_euro'].sum().order()
        place_per_algo_all      = seq.groupby('ExDestination')['turnover_euro'].sum().order()
        
        for index in place_per_algo_all.index:
            if index not in place_per_algo_dma2.index:
                place_per_algo_dma = place_per_algo_dma.set_value(index,  0)
            else:
                place_per_algo_dma = place_per_algo_dma.set_value(index,  place_per_algo_dma2[index])
        
        value_per_place_values_d = [round(p/1000000,2) for p in place_per_algo_dma.values]
        value_per_place_values_a = [round(p/1000000,2) for p in place_per_algo_all.values]
        
        xlabel                  = 'Volume/Algo (,000,000 Euros)'
        total_all               = sum(value_per_place_values_a)
        total_dma               = sum(value_per_place_values_d)
        title  = 'From ' + self.start_date_str + ' To '+ self.end_date_str + '\nTotal Turnover: ' + str(round(total_all,1)) + ' MEuros\n DMA Turnover: ' + str(round(total_dma,1)) + ' MEuros'
        
        index = []
        for suffix in place_per_algo_all.index:
            try:
                index.append(places[places['suffix'] == suffix]['name'].iloc[0])
            except:
                index.append('Unknown (%s)' %suffix)    
        return self.plot_algo_dma_vs_all(title, value_per_place_values_d, value_per_place_values_a, index, xlabel)
        
        
    
    def plot_algo_occ(self):
        self.get_stat_dma()
        
        nbr_dma_occ      = pd.Series()
        nbr_dma_occ2     = self.data_seq[self.ind_dma].groupby('strategy_name_mapped')['p_occ_id'].apply(lambda x: len(np.unique(x))).order()
        nbr_all_occ      = self.data_seq.groupby('strategy_name_mapped')['p_occ_id'].apply(lambda x: len(np.unique(x))).order()
        
        for index in nbr_all_occ.index:
            if index not in nbr_dma_occ2.index:
                nbr_dma_occ = nbr_dma_occ.set_value(index,  0)
            else:
                nbr_dma_occ = nbr_dma_occ.set_value(index,  nbr_dma_occ2[index])

        
        xlabel                  = 'Nbr Occ/Algo'
        total_all               = sum(nbr_all_occ)
        total_dma               = sum(nbr_dma_occ)
        title  = 'From ' + self.start_date_str + ' To '+ self.end_date_str + '\nTotal Occurrence: ' + str(int(total_all))  + ' \n DMA Occurrence: ' + str(int(total_dma))
        return self.plot_algo_dma_vs_all(title, nbr_dma_occ, nbr_all_occ, nbr_all_occ.index, xlabel)
    
    
class AdvancedPlotEngine(PlotEngine):
    pass 

      
if __name__=='__main__':
    from lib.dbtools.connections import Connections
    Connections.change_connections("production")
    
    
#     day = datetime(year=2013, month=8, day=21)
    day = datetime.now() - timedelta(days=1)
    
#     daily = PlotEngine(start_date = day - timedelta(days=56), end_date = day  , filter = {'Account': {'$regex' : 'AKO.*'}})
#     
#     # One DAY
#     daily = PlotEngine(start_date = day, end_date = day)
#     daily.plot_basic_stats()
#     daily.plot_intraday_exec_curve()
#     plt.show()
#     
#     # Weekly
#     weekly = PlotEngine(start_date = day - timedelta(days=7), end_date = day )
#     weekly.plot_basic_stats()
#     plt.show()
     
    # Monthly
    monthly = PlotEngine(start_date = day - timedelta(days=0), end_date = day )
    monthly.plot_basic_stats()
    monthly.plot_intraday_exec_curve()
    plt.show()
    