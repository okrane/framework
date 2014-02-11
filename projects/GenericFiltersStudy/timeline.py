# -*- coding: utf-8 -*-
"""
Created on Thu Sep 12 10:47:36 2013

@author: svlasceanu
"""
from lib.dbtools.connections import Connections
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pandas.io.parsers import read_csv
from datetime import datetime, timedelta
from lib.plots.color_schemes import *

def stoxx_timeline():
    snp = read_csv('C:/st_sim/repository/table_snp.csv', sep = ',')        
    snp.index = [datetime.strptime(x, '%Y-%m-%d') for x in snp['Date']]
    print snp
    
    query = """select DATE, VALUE from Market_data..HISTOINDEXTIMESERIES where INDEXID = 'IND_37' and ATTRIBUTEID = 43 and DATE > '20100101' order by DATE"""
    eurostoxx = Connections.exec_sql('MARKET_DATA', query, as_dataframe = True)
    eurostoxx.index = eurostoxx['DATE']
    return eurostoxx, snp

e, s = stoxx_timeline()

fig, ax = plt.subplots(nrows=1, ncols=1)
ax_twin = plt.twinx(ax)
#print s.index[0], s.index[-1], np.arange(s.index[-1], s.index[0], timedelta(days=30))
#ax.tick_params(rotation = 90)
p1 = s['Close'].plot(ax = ax, color = kc_main_colors()['dark_blue'], linewidth = 2)
p2 = e['VALUE'].plot(ax = ax_twin, color = nice_colors()['dark_red'], linewidth = 2)
plt.setp(ax.xaxis.get_majorticklabels(), rotation = 90)
#plt.xticks(np.array(range(0,len(s.index)))+0.5, matplotlib.dates.date2num(s.index) )
lns1 = ax.get_legend_handles_labels()
lns2 = ax_twin.get_legend_handles_labels()
lns = lns1[0]+lns2[0]
labs = ["EUROSTOXX600", "S&P500"]
ax.legend(lns, labs, loc=0)
ax.set_ylabel("Price")
ax.set_title("S&P500 and EUROSTOXX600 evolution and major market incidents.", fontdict = {'fontsize': 20} )
plt.show()

def vol_gk(o, h, l, c):
    return (h-l)**2 / 2 + (2*np.log(2) -1) * (c-o) ** 2
    

def trade_frequency():
    query = """select security_id, EXCHANGE from KGR..INDEXCOMPONENT where INDEXID = 'IND_1' and DATE = '20130905' """    
    result = Connections.exec_sql('KGR', query, as_dataframe = True)    
    eurostoxx_components = result[np.isfinite(result['security_id'])]['security_id']   
    
    #print eurostoxx_components, len(eurostoxx_components)
    frame = pd.DataFrame() 
    frame_turnover = pd.DataFrame()
    for stock in eurostoxx_components:        
        query = """select date, nb_deal, open_prc, high_prc, low_prc, close_prc, turnover from trading_daily where security_id = %s and trading_destination_id is NULL and DATE >= '20080101' """ % stock            
        trading_data = Connections.exec_sql('MARKET_DATA', query, as_dataframe = True)    
        trading_data.index = trading_data['date']
        
        if( len(trading_data[np.isfinite(trading_data['nb_deal'])]) < 900):
            continue
        
        trading_data['vol'] = vol_gk(trading_data['open_prc'], trading_data['high_prc'], trading_data['low_prc'],trading_data['close_prc'], )
        trading_data['ratio'] = np.sqrt(trading_data['nb_deal']) / trading_data['vol']
                
        #trading_data['ratio'] = trading_data['nb_deal'] / trading_data['turnover']                
        #frame = frame.join(trading_data['nb_deal'], how = "outer", rsuffix = "%s_" % int(stock))
        frame = frame.join(trading_data['ratio'], how = "outer", rsuffix = "%s_" % int(stock))
        frame_turnover = frame_turnover.join(trading_data['turnover'], how = "outer", rsuffix = "%s_" % int(stock))
    frame['sum'] = frame.sum(axis = 1) 
    frame_turnover['sum'] = frame_turnover.sum(axis = 1)
    return frame, frame_turnover
    
#ratio, turnover = trade_frequency()
#figure, (ax1, ax2) = plt.subplots(nrows = 2, ncols = 1)
#ratio['sum'].plot(ax = ax2)
#turnover['sum'].plot(ax = ax1)
#plt.show()
