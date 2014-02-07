# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 08:52:56 2013

@author: njoseph
"""

import pandas as pd
import datetime as dt
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
#from lib.tca.algoplot import PlotEngine
import lib.stats.slicer as slicer
import xlrd
import lib.data.matlabutils as mutils
import lib.dbtools.get_repository as get_repository

########################################
# data
########################################

start_date = dt.datetime(2013,8,1,0,0,1)
end_date = dt.datetime(2013,11,1,0,0,1)

algo_data = AlgoDataProcessor(start_date = start_date,end_date = end_date)
algo_data.get_db_data(level='sequence',force_colnames_only=['strategy_name_mapped','cheuvreux_secid','ProgramName','TargetSubID','Account','rate_to_euro','turnover'])

data = algo_data.data_sequence.copy()

#data['tmp_month_date_end'] = [dt.datetime.combine(x.date().replace(month=x.to_datetime().date().month % 12 + 1, day = 1,year = x.to_datetime().date().year if x.to_datetime().date().month < 12 else x.to_datetime().date().year + 1) - dt.timedelta(days=1),dt.time(0,0,0)) for x in data.index]
data['start_month'] = [dt.datetime.combine(x.to_datetime().date().replace(day=1),dt.time(0,0,0)) for x in data.index]
data['is_dma']=data['TargetSubID' ].apply(lambda x: 'Algo DMA' if x in  ['ON1','ON2','ON3'] else 'Other')


########################################
# excel
########################################
writer = pd.ExcelWriter('C:/stats_algo.xls')

#----------
# by dma 
agg_ = dftools.agg(data,
                       group_vars = ['start_month','is_dma'],
                       stats = {'turnover(euro)': lambda df : np.sum(df.rate_to_euro*df.turnover)})
agg_.to_excel(writer , 'turnover_service')


#----------
# by dma / algo
agg_ = dftools.agg(data,
                       group_vars = ['start_month','strategy_name_mapped','is_dma'],
                       stats = {'turnover(euro)': lambda df : np.sum(df.rate_to_euro*df.turnover)})
agg_.to_excel(writer , 'turnover_algo_service')

#----------
# by stock / algo
top_2keep = 10
gcolnames = ['start_month','strategy_name_mapped']
agg_ = dftools.agg(data,
                       group_vars = gcolnames + ['cheuvreux_secid'],
                       stats = {'turnover(euro)': lambda df : np.sum(df.rate_to_euro*df.turnover)})
agg_ = agg_.sort(gcolnames + ['turnover(euro)'], ascending=[1, 1, 0])
agg_2 = pd.DataFrame()               
uni_agg_ = mutils.uniqueext(agg_[gcolnames].values,rows = True)
for uni_ in uni_agg_:
    for igcol in range(0,len(gcolnames)):
        if igcol == 0:
            id_ = (agg_[gcolnames[igcol]] == uni_[igcol])
        else:
            id_ = id_ & (agg_[gcolnames[igcol]] == uni_[igcol])
        
    agg_2 = agg_2.append(agg_[id_].head(top_2keep))

agg_ = agg_2
agg_['security_name'] = get_repository.symbol6toname(agg_['cheuvreux_secid'].apply(lambda x : int(x)).values)
del agg_['cheuvreux_secid']

agg_.to_excel(writer , 'turnover_algo_top10stock')

##----------
## by stock / account
#top_2keep = 10
#gcolnames = ['start_month','Account']
#agg_ = dftools.agg(data,
#                       group_vars = gcolnames + ['Account'],
#                       stats = {'turnover(euro)': lambda df : np.sum(df.rate_to_euro*df.turnover)})
#agg_ = agg_.sort(gcolnames + ['turnover(euro)'], ascending=[1, 1, 0])
#agg_2 = pd.DataFrame()               
#uni_agg_ = mutils.uniqueext(agg_[gcolnames].values,rows = True)
#for uni_ in uni_agg_:
#    for igcol in range(0,len(gcolnames)):
#        if igcol == 0:
#            id_ = (agg_[gcolnames[igcol]] == uni_[igcol])
#        else:
#            id_ = id_ & (agg_[gcolnames[igcol]] == uni_[igcol])
#        
#    agg_2 = agg_2.append(agg_[id_].head(top_2keep))
#    
#agg_ = agg_2
#agg_['security_name'] = get_repository.symbol6toname(agg_['cheuvreux_secid'].apply(lambda x : int(x)).values)
#del agg_['cheuvreux_secid']           
#
#agg_.to_excel(writer , 'turnover_algo_top10account')

writer.save()



# 