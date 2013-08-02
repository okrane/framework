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
        plt.text('%d'%(int(width)*1.02+2 ), rect.get_y() + rect.get_height()/2.5, width,
                ha='left', va='bottom', size = 12, weight = 750)
        
class DataProcessor(object):
    get_done = 0
    def __init__(self, start_date = None, end_date = None):
        """start_date and end_date are datetime"""
        self.get_data(start_date, end_date)
        DataProcessor.get_done += 1
    
    def get_data(self, start_date = None, end_date = None):
        if start_date is None and end_date is None:
            self.start_date = self.end_date = last_business_day(datetime.now())
        else:
            self.start_date = start_date
            self.end_date   = end_date
            
        self.start_date_str = datetime.strftime(self.start_date, "%d/%m/%Y")
        self.end_date_str   = datetime.strftime(self.end_date, "%d/%m/%Y")
        
        self.data_seq       = get_algodata.sequence_info(start_date = self.start_date_str,
                                                         end_date   = self.end_date_str)
        self.data_occ       = get_algodata.occurence_info(start_date = self.start_date_str,
                                                          end_date   = self.end_date_str)            
    def get_deals(self, merge_order_colnames):
        self.data_deals     = get_algodata.deal(db_name, sequence_id, merge_order_colnames = merge_order_colnames, start_date = self.start_date_str,
                                                          end_date   = self.end_date_str)         
    def get_stat_turnover_euro(self):
        self.data_seq['turnover_euro'] = self.data_seq['turnover'] * self.data_seq['rate_to_euro']
        
    def get_stat_dma(self):
        if 'turnover_euro' not in self.data_seq:
            self.get_stat_turnover_euro()
        f               = lambda x: x.loc['TargetSubID' ] == 'ON1' or x.loc['TargetSubID' ] == 'ON2' or x.loc['TargetSubID' ] == 'ON3'
        self.ind_dma    = self.data_seq.apply(func = f, axis = 1, raw=False )
        
        self.data_seq['is_dma'] = self.ind_dma.values
        
    def plot_algo(self):  
        # Strategy Name
        self.get_stat_dma()
        fig = plt.figure(figsize = DEFAULT_FIGSIZE)
        axes = plt.gca()
        axes.grid(True)

        value_per_algo_dma      = self.data_seq[self.ind_dma].groupby('strategy_name_mapped')['turnover_euro'].sum().order()
        value_per_algo_all      = self.data_seq.groupby('strategy_name_mapped')['turnover_euro'].sum().order()
        
        for index in value_per_algo_all.index:
            if index not in value_per_algo_dma.index:
                value_per_algo_dma = value_per_algo_dma.set_value(index,  0.0)
                
        value_per_algo_dma = value_per_algo_dma.order()
        
        ind                     = arange(len(value_per_algo_all.index)) 
        
        value_per_algo_values_d = [round(p/1000000,2) for p in value_per_algo_dma.values]
        value_per_algo_values_a = [round(p/1000000,2) for p in value_per_algo_all.values]
        
        maxi                    = max(value_per_algo_values_a)
        total_all               = sum(value_per_algo_values_a)
        total_dma               = sum(value_per_algo_values_d)
        height                  = 0.8
        
        p_all                   = plt.barh( ind, value_per_algo_values_a, height = height, color=kc_main_colors()['blue_2'], alpha=0.85)
        p_dma                   = plt.barh( ind, value_per_algo_values_d, height = height, color=kc_main_colors()['blue_1'], alpha=0.95)
        
        
        plt.xlabel('Volume/Algo (,000,000 Euros)')
        plt.title('From ' + self.start_date_str + ' To '+ self.start_date_str + '\nTotal Turnover: ' + str(round(total_all,1)) + ' MEuros\n DMA Turnover: ' + str(round(total_dma,1)) + ' MEuros', size = 'large')
        plt.yticks(height/2 + ind, map(str, value_per_algo_all.index) )
        
       
        
        plt.xticks(
                   arange(start = 0, 
                          stop = maxi, 
                          step = round(maxi, 3-len(str(round(maxi))))/10
                          )
                   )
        
        autolabel_barh(p_all)
        
        plt.legend([p_all, p_dma], ['OTHER', 'ALGO DMA'], loc=4)
        return plt

    #------------------------------------------------------------------------------
    #  Intraday executed curve
    #------------------------------------------------------------------------------
    def plot_intraday_exec_curve(self, step_sec=60*30, group_var='strategy_name_mapped'):
        """
        intraday_exec_curve : 
        Plot the daily exec curve in turnover cross by group_var
        """
        self.data_deals = get_algodata.deal(merge_order_colnames = ['rate_to_euro', 'strategy_name_mapped', 'ExDestination'], start_date = self.start_date_str,
                                                          end_date   = self.end_date_str)
        
        ##############################################################
        # aggregate data
        ##############################################################  
        grouped=self.data_deals.groupby( [st_data.gridTime(date=self.data_deals.index, step_sec=step_sec, out_mode='ceil'), group_var] )
        grouped_data=pd.DataFrame([{'date':k[0],group_var:k[1],
                              'mturnover_euro': np.sum(v.rate_to_euro*v.price*v.volume)*1e-6} for k,v in grouped])
        grouped_data=grouped_data.set_index('date')
        # on passe en string parce que ca ne sorte pas sinon !!   
        grouped_data['tmpindex']=[datetime.strftime(x.to_datetime(),'%Y%m%d-%H:%M:%S.%f') for x in grouped_data.index]
        grouped_data=grouped_data.sort_index(by=['tmpindex',group_var]).drop(['tmpindex'],axis=1)
    
        ##############################################################
        # plot
        ##############################################################  
        # ----- NEEDED    
        uni_strat=np.sort(np.unique(grouped_data[group_var].values).tolist())
        colors_strat=cm.spectral(np.linspace(0, 1.0, len(uni_strat)))
        # ----- PLOT 
        plt.figure()
        plt.hold(True)
        prev_date=''
        prev_date_cum=0
        for i in range(grouped_data.shape[0]):
        #for i in range(20):
            date=grouped_data.index[i].to_datetime()
            idx_uni_strat=np.nonzero(uni_strat==grouped_data[group_var].ix[i])[0][0]
            if (not date==prev_date):
                plt.gca().fill([date-timedelta(seconds=step_sec),date,date,date-timedelta(seconds=step_sec)],
                       [0,0,grouped_data['mturnover_euro'].ix[i],grouped_data['mturnover_euro'].ix[i]],
                       facecolor=colors_strat[idx_uni_strat],alpha = 0.5)
                       
                prev_date_cum=grouped_data['mturnover_euro'].ix[i]
                # ,edgecolor='none'
            else:
                plt.gca().fill([date-timedelta(seconds=step_sec),date,date,date-timedelta(seconds=step_sec)],
                       [prev_date_cum,prev_date_cum,prev_date_cum+grouped_data['mturnover_euro'].ix[i],prev_date_cum+grouped_data['mturnover_euro'].ix[i]],
                       facecolor=colors_strat[idx_uni_strat],alpha = 0.5)
                       
                prev_date_cum=prev_date_cum+grouped_data['mturnover_euro'].ix[i]     
            prev_date=date
        
        plt.hold(False)
        plt.legend(uni_strat)
        
        return plt
    
    
if __name__=='__main__':
    from lib.dbtools.connections import Connections
    Connections.change_connections("dev")
    day = datetime.now() - timedelta(days=6)
    p = DataProcessor(start_date = day, end_date = day)
#     p.plot_algo().show()
    p.plot_intraday_exec_curve(step_sec=60*60, group_var='ExDestination').show()
    