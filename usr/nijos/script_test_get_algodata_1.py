# -*- coding: utf-8 -*-
"""
Created on Thu May 16 14:02:13 2013

@author: njoseph
"""
import numpy as np
import scipy 
import matplotlib.pyplot as plt
from datetime import *
import pytz
from scipy.io  import matlab
import pandas as pd
from lib.data.matlabutils import *
import lib.data.st_data as st_data
import lib.dbtools.get_repository as get_repository
import lib.dbtools.read_dataset as read_dataset
from lib.data.ui.Explorer import Explorer
import lib.dbtools.get_algodata as get_algodata
import lib.tca.get_algostats as get_algostats
import lib.tca.compute_stats as compute_stats
from lib.dbtools.connections import Connections



data_market=read_dataset.ftickdb(security_id=110,date='01/07/2013')






# data_seq=get_algodata.sequence_info(sequence_id=['FYBL2SUT00008L0001'])
# data_seq = get_algodata.sequence_info(start_date="13/06/2013",end_date="13/06/2013")
# Explorer(data_seq)
# 
# data_occ=get_algodata.occurence_info(occurence_id=['FYLCoAA0016'])


# data_deal=get_algodata.deal(sequence_id=['FYBL2SUT00008L0001'])
# print data_deal
Connections.change_connections('dev')
#data_deal=get_algodata.deal(start_date='09/07/2013',end_date='09/07/2013')
data_deal=get_algodata.deal(start_date='09/07/2013',end_date='09/07/2013',merge_order_colnames=['cheuvreux_secid','strategy_name_mapped'])
Explorer(data_deal)
uniqueext(data_deal['MIC'].values)

# 
# data=data_deal
# data_seq=get_algodata.sequence_info(sequence_id=uniqueext(data_deal['ClOrdID'].values).tolist())
# merge_order_colnames=['cheuvreux_secid','strategy_name_mapped']
# all([x in data_seq.columns.tolist() for x in merge_order_colnames])
# # initialize columns
# for x in merge_order_colnames:
#     data[x]=None 
# # initialize columns
# if data_seq.shape[0]>0:
#     for idx in range(0,data_seq.shape[0]):
#         idx_in=np.where(data['ClOrdID'].values==data_seq.ix[idx]['ClOrdID'])[0]        
#         for x in merge_order_colnames:
#             data[x][idx_in]=data_seq.ix[idx][x]        
# 
# 
# 
# # data_deal=get_algodata.deal(sequence_id=['FYBL2SUT00008L0001'])
# Explorer(data_deal)



# 
# # test
# data_order=get_algodata.sequence_info(sequence_id=['FY000015064401'])
# data_deal=get_algodata.deal(sequence_id=['FY2000007221801'])
# a=compute_stats.aggexec(data_order=data_order,data_deal=data_deal)
# b=compute_stats.aggexec(data_order=data_order,data_deal=data_deal)
# 
# 
# #data=get_algostats.sequence(occurence_id='FY2000007382301')
# data_occ=get_algodata.sequence_info(occurence_id=['FY2000007221801','FY2000007089101'])
# Connections.change_connections('dev')
# data_occ=get_algostats.sequence_info(occurence_id=['FY2000007221801','FY2000007089101'])

#data_occ=get_algodata.occurence_info(occurence_id=['FY2000007221801','FY2000007089101'])
#data_occ=get_algostats.sequence_info(occurence_id=['FY2000007221801','FY2000007089101'])
#
#
#data_occ=get_algodata.sequence_info(start_date='17/06/2013',end_date='17/06/2013')
#
#
#data_occ.index.min()
## test on market data
#data=read_dataset.ftickdb(security_id=110,date='17/05/2013')
#stats=compute_stats.aggmarket(data=data,exchange_id_main=np.min(data['exchange_id']),start_datetime=data.index[1].to_datetime(),end_datetime=data.index[5000].to_datetime(),limit_price=8.23,side=-1)
#
#
#data_occ=get_algostats.sequence_info(occurence_id=['FY2000007221801','FY2000007089101'])
#
#
## mode "sequenceinfo"
##data=get_algodata("sequence_info",sequence_id="FY2000007381401")
##data=get_algodata("sequence_info",sequence_id=["FY2000007382301","FY2000007414521"])
##data=get_algodata("sequence_info",occurence_id="FY2000007383101")
#data=get_algodata.sequence_info(start_date="28/05/2013",end_date="28/05/2013")
## mode "occurenceinfo"
#data=get_algodata.occurence_info(occurence_id=["FY2000007383101"])
#data=get_algodata.occurence_info(start_date="28/05/2013",end_date="28/05/2013")
## mode deal
#data=get_algodata.deal(sequence_id="FY2000004393001")
#
#req=(" SELECT top 1 * from Market_data..trading_daily ")
#vals=Connections.exec_sql('MARKET_DATA',req,schema = True)
#


#
#



