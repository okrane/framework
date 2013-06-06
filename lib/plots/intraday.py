# -*- coding: utf-8 -*-
"""
Created on Fri May 24 16:04:51 2013

@author: silviu
"""
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import datetime
from lib.plots.color_schemes import *

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
    

  
    
if __name__ == "__main__":
    from lib.data.st_data import *
    data=read_dataset.ftickdb(security_id=110,date='17/05/2013')

    
   # data = from_mat_file("Q:/dev_repository/get_tick/ft/FTE.PA/2013_05_02.mat")
   
    kcintraday(data) 