# -*- coding: utf-8 -*-
"""
Created on Fri Sep 20 10:57:31 2013

@author: njoseph
"""

import matplotlib.pyplot as plt
import matplotlib.cm as cm
import pandas as pd
import datetime as dt
import time as time
import numpy as np
import pytz
# import lib.data.matlabutils as matlabutils
from lib.dbtools.connections import Connections
from lib.tca.referentialdata import ReferentialDataProcessor
from lib.tca.marketdata import MarketDataProcessor
from lib.tca.algostats import AlgoStatsProcessor
from lib.tca.algodata import AlgoDataProcessor
import lib.data.dataframe_tools as dftools
import lib.data.dataframe_plots as dfplots
from lib.data.ui.Explorer import Explorer
from lib.tca.algoplot import PlotEngine



#----------
# params
client_prefix='BLKUK'
start_date=dt.datetime(2013,1,1)
end_date=dt.datetime(2013,9,19)



#----------
# get data
# -- 
algo_occ_fe_data=AlgoStatsProcessor(start_date = start_date, end_date = end_date,
                             filter = {'Account': {'$regex' : client_prefix+'.*'}})
# algo_occ_fe_data=AlgoStatsProcessor(start_date = start_date, end_date = end_date,
#                              filter = {'Account': {'$regex' : client_prefix+'.*'}})
algo_occ_fe_data.get_occ_fe_data()

algo_occ_fe_data.data_occurrence.ix[(np.where(map(lambda x : x is None or not np.isfinite(x) , algo_occ_fe_data.data_occurrence['rate_to_euro']))[0])]
         
# -- 
# algo_deal_data=AlgoStatsProcessor(start_date = start_date, end_date = end_date,
#                              filter = {'Account': {'$regex' : client_prefix+'.*'}})
# algo_deal_data.get_db_data(level='sequence',force_colnames_only=['Account'])
# algo_deal_data.get_db_data(level='deal')
# algo_deal_data.get_intraday_agg_deals_data()
# algo_data.get_xls_occ_fe_data()
# 


#----------
# PLot on all the period
plot_allp=PlotEngine()

#-- evolution / turnover by algo
tmp=plot_allp.plot_algo_evolution(algo_data=algo_occ_fe_data,level='occ_fe',var='mturnover_euro',gvar='occ_fe_strategy_name_mapped')
plt.show()

#-- algo usage in nb occurence
# plot_allp.plot_intraday_exec_curve(algo_data=algo_deal_data)
# plt.show()  

#-- algo usage in turnover
tmp=plot_allp.plot_algo_hbar(algo_data=algo_occ_fe_data,level='occ_fe',var='mturnover_euro',gvar='occ_fe_strategy_name_mapped')
plt.show()     
 
#-- algo usage in nb occurence
tmp=plot_allp.plot_algo_hbar(algo_data=algo_occ_fe_data,level='occ_fe',var='nb_occurrence',gvar='occ_fe_strategy_name_mapped')
plt.show()   






          





