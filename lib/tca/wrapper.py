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
        self.data_occ       = get_algodata.occurrence_info(start_date = self.start_date_str,
                                                          end_date   = self.end_date_str)            
    def get_deals(self, merge_order_colnames):
        self.data_deals     = get_algodata.deal(start_date = self.start_date_str,
                                                          end_date   = self.end_date_str)         
    def get_stat_turnover_euro(self):
        self.data_seq['turnover_euro'] = self.data_seq['turnover'] * self.data_seq['rate_to_euro']
        
        
    def get_stat_dma(self):
        if 'turnover_euro' not in self.data_seq:
            self.get_stat_turnover_euro()
        f                = lambda x: x.loc['TargetSubID' ] == 'ON1' or x.loc['TargetSubID' ] == 'ON2' or x.loc['TargetSubID' ] == 'ON3'
        self.ind_dma     = self.data_seq.apply(func = f, axis = 1, raw=False )

        self.data_seq['is_dma'] = self.ind_dma.values
        
    def plot_algo_volume(self):
        self.get_stat_dma()
        
        value_per_algo_dma      = self.data_seq[self.ind_dma].groupby('strategy_name_mapped')['turnover_euro'].sum().order()
        value_per_algo_all      = self.data_seq.groupby('strategy_name_mapped')['turnover_euro'].sum().order()
        
        for index in value_per_algo_all.index:
            if index not in value_per_algo_dma.index:
                value_per_algo_dma = value_per_algo_dma.set_value(index,  0.0)
                
        value_per_algo_dma = value_per_algo_dma.order()
        
        
        
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
        
        place_per_algo_dma      = self.data_seq[self.ind_dma].groupby('ExDestination')['turnover_euro'].sum().order()
        place_per_algo_all      = self.data_seq.groupby('ExDestination')['turnover_euro'].sum().order()
        
        for index in place_per_algo_all.index:
            if index not in place_per_algo_dma.index:
                place_per_algo_dma = place_per_algo_dma.set_value(index,  0)
                
        place_per_algo_dma = place_per_algo_dma.order()
        
        value_per_place_values_d = [round(p/1000000,2) for p in place_per_algo_dma.values]
        value_per_place_values_a = [round(p/1000000,2) for p in place_per_algo_all.values]
        
        xlabel                  = 'Volume/Algo (,000,000 Euros)'
        total_all               = sum(value_per_place_values_a)
        total_dma               = sum(value_per_place_values_d)
        title  = 'From ' + self.start_date_str + ' To '+ self.end_date_str + '\nTotal Turnover: ' + str(round(total_all,1)) + ' MEuros\n DMA Turnover: ' + str(round(total_dma,1)) + ' MEuros'
        
        index = []
        for suffix in place_per_algo_all.index:
            index.append(places[places['suffix'] == suffix]['name'].iloc[0])
        return self.plot_algo_dma_vs_all(title, value_per_place_values_d, value_per_place_values_a, index, xlabel)
        
        
    
    def plot_algo_occ(self):
        self.get_stat_dma()
        
        nbr_dma_occ      = self.data_seq[self.ind_dma].groupby('strategy_name_mapped')['p_occ_id'].apply(lambda x: len(np.unique(x))).order()
        nbr_all_occ      = self.data_seq.groupby('strategy_name_mapped')['p_occ_id'].apply(lambda x: len(np.unique(x))).order()

        
        for index in nbr_all_occ.index:
            if index not in nbr_dma_occ.index:
                nbr_dma_occ = nbr_dma_occ.set_value(index,  0)
                
        nbr_dma_occ = nbr_dma_occ.order()

        
        xlabel                  = 'Nbr Occ/Algo'
        total_all               = sum(nbr_all_occ)
        total_dma               = sum(nbr_dma_occ)
        title  = 'From ' + self.start_date_str + ' To '+ self.end_date_str + '\nTotal Occurrence: ' + str(int(total_all))  + ' \n DMA Occurrence: ' + str(int(total_dma))
        return self.plot_algo_dma_vs_all(title, nbr_dma_occ, nbr_all_occ, nbr_all_occ.index, xlabel)        
      
    def plot_algo_dma_vs_all(self, title, value_dma, value_all, index_all, xlabel ):  
        # Strategy Name
        fig = plt.figure(figsize = DEFAULT_FIGSIZE)
        axes = plt.gca()
        axes.grid(True)

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
          
        return fig

    #------------------------------------------------------------------------------
    #  Intraday executed curve
    #------------------------------------------------------------------------------
    def plot_intraday_exec_curve(self, step_sec=60*30, group_var='strategy_name_mapped'):
        """
        intraday_exec_curve : 
        Plot the daily exec curve in turnover cross by group_var
        """
        self.data_deals = get_algodata.deal(start_date = self.start_date_str,
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
        uni_strat_islabeled=np.array([False]*len(uni_strat))
        # ----- PLOT
        h = plt.figure()
        plt.hold(True)
        prev_date=''
        prev_date_cum=0
        for i in range(grouped_data.shape[0]):
            #---
            date=grouped_data.index[i].to_datetime()
            idx_uni_strat=np.nonzero(uni_strat==grouped_data[group_var].ix[i])[0][0]
            #--
            args=[]
            if (not date==prev_date):
                args.append([date-timedelta(seconds=step_sec),date,date,date-timedelta(seconds=step_sec)])
                args.append([0,0,grouped_data['mturnover_euro'].ix[i],grouped_data['mturnover_euro'].ix[i]])
                prev_date_cum=grouped_data['mturnover_euro'].ix[i]
            else:
                args.append([date-timedelta(seconds=step_sec),date,date,date-timedelta(seconds=step_sec)])
                args.append([prev_date_cum,prev_date_cum,prev_date_cum+grouped_data['mturnover_euro'].ix[i],prev_date_cum+grouped_data['mturnover_euro'].ix[i]])
                prev_date_cum=prev_date_cum+grouped_data['mturnover_euro'].ix[i] 
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
        plt.title('Intraday traded curve', size = 'large')
        plt.legend()   

        return h

    def plot_basic_stats(self, path = ['vol.jpg', 'occ.png', 'place.png', 'detailed_vol']):
        daily.plot_algo_volume().savefig(path[0])
        daily.plot_algo_occ().savefig(path[1])
        daily.plot_algo_place().savefig(path[2])
    
if __name__=='__main__':
    from lib.dbtools.connections import Connections
    Connections.change_connections("dev")
    
    
    day = datetime(year=2013, month=7, day=23)
    day = datetime.now() - timedelta(days=1)
    # One DAY
    daily = DataProcessor(start_date = day, end_date = day)
    daily.plot_basic_stats()
    plt.show()
    
    # Weekly
    weekly = DataProcessor(start_date = day, end_date = day + timedelta(days=7))
    weekly.plot_basic_stats()
    plt.show()
     
    # Monthly
    monthly = DataProcessor(start_date = day - timedelta(days=21), end_date = day + timedelta(days=7))
    monthly.plot_basic_stats()
    plt.show()
      
    p.plot_intraday_exec_curve(step_sec=60*60, group_var='ExDestination')
    plt.show()
    