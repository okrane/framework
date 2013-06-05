# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import datetime
import pytz
from lib.plots.color_schemes import *
import lib.data.st_data as st_data

def kcintraday(data, start_time = None, end_time = None, exlude_auction = False, exclude_dark = False):
    """
    Two subplots:
        - upper plots the deals with their price, and bid-ask spread
        - lower plots volumes in volume bars of 5 minutes
    """
    # filter the data - TODO: should probably be done in an outside function
    #idx_filter = #* data["ask"] > 0 * data["price"] > 0
    
    data_no_auction = data[data["auction"]==0]    
    fig, axes = plt.subplots(nrows=1, ncols=1)
    axes_twin = plt.twinx(axes)
    
    grouped=data_no_auction.groupby(pd.TimeGrouper(freq='5Min')) 
    cumvolumes = dict( (k, np.sum(v["volume"])) for k, v in grouped )    
    color_bar = color_grayscale(kc_main_colors()["light_blue"], 10)[8]    
    axes_twin.bar([x.to_datetime() for x in cumvolumes.keys()], cumvolumes.values(), width = 0.002, color = color_bar, alpha = 0.3, edgecolor = "b")    
    
    axes.step(data_no_auction.index, data_no_auction['bid'], color = kc_main_colors()["blue_1"], linewidth = 2)
    axes.step(data_no_auction.index, data_no_auction['ask'], color = kc_main_colors()["blue_2"], linewidth = 2)
    
    axes.fill_between(data_no_auction.index, data_no_auction['bid'], data_no_auction['ask'], color = kc_main_colors()["light_blue"])    
    data_no_auction['price'].plot(marker="^", ax = axes, color = kc_main_colors()["dark_blue"], linestyle = 'None')        
    axes.grid(True)
    #axes[0].title = "Intraday View"
    #axes[0].ylabel("price")    
   
    # Viewport definition
    inflation_coefficient = 1.75       
    max_volumes = max(cumvolumes.values())     
    min_price, max_price, mean_price = min(data_no_auction["price"]), max(data_no_auction["price"]), (min(data_no_auction["price"])+max(data["price"]))/2
    
    ymin_price, ymax_price = mean_price - inflation_coefficient * (mean_price - min_price), mean_price + inflation_coefficient * (max_price - mean_price)
    price_limits = axes.axis()
    axes.axis((price_limits[0], price_limits[1], ymin_price, ymax_price))    
        
    
    twin_limit = axes_twin.axis()
    alpha = (mean_price - min_price) / (max_price - min_price)    
    axes_twin.axis((twin_limit[0],twin_limit[1], twin_limit[2], 1/alpha * max_volumes))    
    
    plt.show()
    

def plot_intraday(data,start_datetime=None,end_datetime=None,exclude_auction=[0,0,0,0],exclude_dark=False,step_sec=60):
    """
    Two subplots:
        - upper plots the deals with their price, and bid-ask spread
        - lower plots volumes in volume bars of 5 minutes
    """
    #--------------------------------------------------------------------------
    # filter the data
    #--------------------------------------------------------------------------
    # -- filter time
    if (start_datetime is not None) and (end_datetime is not None):
        # -- handle timezone
        # end_datetime.replace(tzinfo=pytz.timezone('Europe/London'))
        if not start_datetime.tzinfo:
            start_datetime=start_datetime.replace(tzinfo=data.index.tz)
            end_datetime=end_datetime.replace(tzinfo=data.index.tz)
        # -- filter
        data=data.ix[nonzero(map(lambda x : x>=start_datetime and x<=end_datetime,[x.to_datetime() for x in data.index]))[0]]
    # -- filter auction
    if any(np.array(exclude_auction)==1):
        if exclude_auction[0]==1:
           data=data[data['opening_auction']==0]
        if exclude_auction[1]==1:
           data=data[data['intraday_auction']==0]
        if exclude_auction[2]==1:
           data=data[data['closing_auction']==0]
        if exclude_auction[3]==1:
           data=data[~((data['auction']==1) & (data['opening_auction']==0) & (data['intraday_auction']==0) & (data['closing_auction']==0))]
    # -- filter time
    if exclude_dark:
       data=data[data['dark']==0]
    #--------------------------------------------------------------------------
    # compute aggregation
    #--------------------------------------------------------------------------    
    grouped=data.groupby([st_data.gridTime(date=data.index,step_sec=step_sec,out_mode='ceil'),'auction'])
    grouped_data=pd.DataFrame([{'date': k[0], 'auction': k[1],
                            'time_close' :v.index.max(),
                            'volume': v.volume.sum()}
                            for k,v in grouped])
    grouped_data=grouped_data.set_index('date')
    
    #---- filter data and data_acution
    max_volumes=max(grouped_data['volume'])
    grouped_data_auction=grouped_data[grouped_data['auction']==1]  
    data_auction=data[data['auction']==1]  
    grouped_data=grouped_data[grouped_data['auction']==0] 
    data=data[data['auction']==0] 
    
    #--------------------------------------------------------------------------
    #  plot
    #--------------------------------------------------------------------------    
    fig, axes = plt.subplots(nrows=1, ncols=1)
    axes_twin = plt.twinx(axes)
    
    #----- continuous                 
    xaxis=[x.to_datetime() for x in grouped_data.index]
    color_bar_continuous = color_grayscale(kc_main_colors()["light_blue"], 10)[8]    

    for i in range(len(xaxis)-1):
        axes_twin.fill([xaxis[i]-datetime.timedelta(seconds=step_sec),xaxis[i],xaxis[i],xaxis[i]-datetime.timedelta(seconds=step_sec)],
               [0,0,grouped_data['volume'].ix[i],grouped_data['volume'].ix[i]],facecolor=color_bar_continuous,alpha = 0.5,edgecolor='none')
    
    axes.step(data.index, data['bid'], color = kc_main_colors()["blue_1"], linewidth = 2)
    axes.step(data.index, data['ask'], color = kc_main_colors()["blue_2"], linewidth = 2)
    
    axes.fill_between(data.index, data['bid'], data['ask'], color = kc_main_colors()["light_blue"])    
    data['price'].plot(marker="^", ax = axes, color = kc_main_colors()["dark_blue"], linestyle = 'None') 
    
    #----- auction 
    if data_auction.values.size>0:
        axes_twin.stem(grouped_data_auction['time_close'].values, grouped_data_auction['volume'].values)
        data_auction['price'].plot(marker="o", ax = axes, color = kc_main_colors()["dark_blue"], linestyle = 'None')

    #----- renormalize x-y axis
    inflation_coefficient = 1.75           
    min_price, max_price, mean_price = min(data["price"]), max(data["price"]), (min(data["price"])+max(data["price"]))/2
    
    ymin_price, ymax_price = mean_price - inflation_coefficient * (mean_price - min_price), mean_price + inflation_coefficient * (max_price - mean_price)
    price_limits = axes.axis()
    twin_limit = axes_twin.axis()
    axes.axis((min(price_limits[0],twin_limit[0])-step_sec/(24*60*60), max(price_limits[1],twin_limit[1])+step_sec/(24*60*60), ymin_price, ymax_price))    
        
    
    alpha = (mean_price - min_price) / (max_price - min_price)    
    axes_twin.axis((min(price_limits[0],twin_limit[0])-step_sec/(24*60*60), max(price_limits[1],twin_limit[1])+step_sec/(24*60*60), twin_limit[2], 1/alpha * max_volumes))    
    
    axes.grid(True)
    plt.show()  
    
if __name__ == "__main__":
    from lib.data.st_data import *
    data = from_mat_file("Q:/dev_repository/get_tick/ft/FTE.PA/2013_05_02.mat")
    kcintraday(data) 
    
    import lib.dbtools.read_dataset as read_dataset
    data=read_dataset.ftickdb(security_id=110,date='13/03/2013')
    plot_intraday(data,exclude_auction=[0,0,0,0],step_sec=5*60)
    