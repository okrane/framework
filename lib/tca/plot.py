import numpy as np
import scipy 
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from datetime import *
import pytz
import pandas as pd
import lib.data.st_data as st_data


#------------------------------------------------------------------------------
#  Intraday executed curve
#------------------------------------------------------------------------------
def intraday_exec_curve(data=None,step_sec=60*30,group_var='strategy_name_mapped'):
    """
    intraday_exec_curve : 
    Plot the daily exec curve in turnover cross by group_var
    """
    ##############################################################
    # input handling
    ##############################################################
    if (data is None):
        raise NameError('plot:intraday_exec_curve - data is missing')
    
    ##############################################################
    # aggregate data
    ##############################################################  
    grouped=data.groupby([st_data.gridTime(date=data.index,step_sec=step_sec,out_mode='ceil'),group_var])
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
    plt.show()
    


#------------------------------------------------------------------------------
# evolution_exec
#------------------------------------------------------------------------------
def evolution_exec(data=None,x_var='day',y_var='strategy_name_mapped'):
    """
    evolution_exec : 
    Plot the evolution  in turnover by x_var and cross y_var
    """
    ##############################################################
    # input handling
    ##############################################################
    if (data is None):
        raise NameError('plot:evolution_exec - data is missing')
    
    if (x_var=='day'):
        data[x_var]=[datetime.strptime(datetime.strftime(x.to_datetime(),'%Y%m%d'),'%Y%m%d')  for x in data.index]
        step_sec=60*60*24
        
    ##############################################################
    # aggregate data
    ##############################################################    
    grouped=data.groupby([x_var,y_var])
    grouped_data=pd.DataFrame([{x_var:k[0],y_var:k[1],
                          'mturnover_euro': np.sum(v.rate_to_euro*v.turnover)*1e-6} for k,v in grouped])
    grouped_data=grouped_data.set_index(x_var) 
    # on passe en string parce que ca ne sorte pas sinon !!   
    grouped_data['tmpindex']=[datetime.strftime(x.to_datetime(),'%Y%m%d') for x in grouped_data.index]
    grouped_data=grouped_data.sort_index(by=['tmpindex',y_var]).drop(['tmpindex'],axis=1)

    ##############################################################
    # plot
    ##############################################################  
    # ----- NEEDED    
    uni_strat=np.sort(np.unique(grouped_data[y_var].values).tolist())
    colors_strat=cm.spectral(np.linspace(0, 1.0, len(uni_strat)))
    # ----- PLOT
    plt.figure()
    plt.hold(True)
    prev_date=''
    prev_date_cum=0
    for i in range(grouped_data.shape[0]):
        #for i in range(20):
        date=grouped_data.index[i].to_datetime()
        idx_uni_strat=np.nonzero(uni_strat==grouped_data[y_var].ix[i])[0][0]
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
    plt.show()
    
    
    
    
    
    