'%d.%m.%Y %H:%M'# -*- coding: utf-8 -*-
"""
Created on Thu May 16 14:02:13 2013

@author: njoseph
"""
import os
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
from lib.data.ui.Explorer import Explorer
from lib.tca.algodata import AlgoDataProcessor
from lib.tca.algostats import AlgoStatsProcessor

import lib.data.dataframe_tools as dftools
import lib.data.matlabutils as matlabutils
import lib.tca.mapping as mapping



#------ OCC DATA
occ_data=AlgoStatsProcessor(start_date=datetime(2013,8,1),end_date=datetime(2013,8,5))
occ_data.get_occ_fe_stats()

#                 occ_nb_replace, Side, Account, occ_fe_prv_turnover, occ_duration,
#        occ_fe_period_low, occ_fe_prv_wexec, rate_to_euro,
#        occ_fe_prv_volume, ExDestination, Symbol, occ_ID, ClientID,
#        occ_fe_inmkt_volume, TargetSubID, occ_fe_exec_shares,
#        occ_fe_avg_sprd, occ_fe_inmkt_turnover, occ_fe_period_high,
#        occ_fe_arrival_price, MsgType, cheuvreux_secid, occ_prev_exec_qty,
#        occ_fe_final_price, occ_prev_turnover, p_occ_id, Currency,
#        occ_prev_exec_vwap, _id, occ_fe_avg_price, occ_fe_order_perc,
#        SendingTime
# 
# occ_data.data_occurrence[['occ_fe_inmkt_turnover','occ_fe_prv_turnover',
#                           'occ_fe_avg_price','occ_fe_prv_wexec',
#                           'occ_fe_exec_shares','occ_exec_qty']].values
# 
# #------ LOAD HISTO DATA OCC FE DATA
# path_data='H:\\data'
# filename='Export_EOD_Flex_2013_extract.csv'
# data=pd.read_csv(os.path.join(path_data,filename),sep=';')
# 
# # NORMALIZE like the db              
# # rate_to_euro, cheuvreux_secid, Side, occ_strategy_name_mapped
# dict_rename={
#     'STARTTIME':'SendingTime',
#     'ENDTIME':'eff_endtime', 
#     'STRATEGY':'occ_strategy_name_mapped',
#     'CURRENCY':'Currency',
#     'TICKERFLEX':'Symbol',
#     'USRID':'TargetSubID',
#     'OMSREF':'Account',
#     'EXECQTY':'occ_fe_exec_shares',
#     'EXECPRICE' : 'occ_fe_avg_price',
#     'MARKETTURNOVER':'occ_fe_inmkt_turnover',
#     'MARKETVOLUME':'occ_fe_inmkt_volume',
#     'ARRIVALPRICE':'occ_fe_arrival_price',
#     'HIGH':'occ_fe_period_high',
#     'LOW':'occ_fe_period_low',
#     'FINALPRICE':'occ_fe_final_price',
#     'TICKSIZE':'occ_fe_ticksize',
#     'AVGSPREAD':'occ_fe_avg_sprd',
#     'SIDE':'Side',
#     'PORTFOLIO':'p_occ_id',
#     'PARTVOL':'occ_fe_order_perc'}
# 
# #---- drop non needed colnames
# data=data.drop(np.setdiff1d(data.columns.values.tolist(),dict_rename.keys()).tolist(),axis=1)
# 
# #---- rename colnames
# data=data.rename(columns=dict_rename)
# 
# #--- add needed basic colnames
# data['occ_fe_prv_turnover']=0
# data['occ_fe_prv_volume']=0
# data['occ_fe_prv_wexec']=0
# 
# #---- basic formula
# data['SendingTime']=map(lambda x : datetime.strptime(x,'%d.%m.%Y %H:%M'),data['SendingTime'].values)
# data['eff_endtime']=map(lambda x : datetime.strptime(x,'%d.%m.%Y %H:%M'),data['eff_endtime'].values)
# data['Side']=data['Side'].apply(lambda x : 1 if x=='B' else -1)
# 
# #----- cheuvreux_secid
# uni_,idx_in_uni_=matlabutils.uniqueext(data['Symbol'].values,return_inverse=True)
# uni_vals=map(lambda x : get_repository.get_symbol6_from_ticker(x),uni_)
# vals=[np.nan]*len(idx_in_uni_)
# for i in range(0,len(vals)):
#     vals[i]=uni_vals[idx_in_uni_[i]]
# data['cheuvreux_secid']=vals
# 
# #----- strategy name
# uni_,idx_in_uni_=matlabutils.uniqueext(data['occ_strategy_name_mapped'].values,return_inverse=True)
# uni_vals=map(lambda x : str(mapping.StrategyName(x)),uni_)
# vals=['']*len(idx_in_uni_)
# for i in range(0,len(vals)):
#     vals[i]=uni_vals[idx_in_uni_[i]]
# data['occ_strategy_name_mapped']=vals
# 
# #----- rate to euro 
# uni_curr=np.unique(data['Currency'].values).tolist()
# data_rte=read_dataset.histocurrencypair(start_date = datetime.strftime(np.min(data['SendingTime']),'%Y%m%d'), 
#                                         end_date = datetime.strftime(np.max(data['eff_endtime']),'%Y%m%d'),currency = uni_curr)
# data_rte['tmpmergetime']=map(lambda x : datetime.strftime(x.to_datetime(),'%Y-%m-%d'),data_rte.index)
# data['tmpmergetime']=map(lambda x : datetime.strftime(x,'%Y-%m-%d'),data['SendingTime'])
# data_rte=data_rte.rename(columns={'ccy':'Currency'}).drop('ccyref',axis=1)
# data=pd.merge(left=data,right=data_rte,how='left',on=['tmpmergetime','Currency'])
# data=data.rename(columns={'rate':'rate_to_euro'}).drop('tmpmergetime',axis=1)
# 
# #----- index
# data=data.set_index('SendingTime')










#------------------------ AKO Dark study
# 
# ako_data=AlgoDataProcessor(start_date=datetime(2013,8,1),end_date=datetime(2013,9,10),
#                            filter = {'Account': {'$regex' : 'AKO.*'}})
# ako_data.get_db_data(level='sequence')
# ako_data.get_db_data(level='deal')
# 
# 
# config_stats={'Mturnover_euro': {'default': 0.0 ,'slicer' : lambda df : np.sum(map(lambda x,y,z : x*y*z*1e-6,df.volume.values,df.price.values,df.rate_to_euro.values))}}                       
# agg_data=dftools.agg(ako_data.data_deal,
#                      group_vars='MIC',
#                      stats=dict([(x,config_stats[x]['slicer']) for x in config_stats.keys()]))
# def isdark(x):
#     res=0
#     if x in ['BATD','CHID','BLNK','SGMX','TRQM','XUBS','XPOS']:
#         res=1
#     return res
# 
# agg_data['isdark']=agg_data['MIC'].apply(lambda x : isdark(x))
# agg_data=agg_data.sort_index(by=['Mturnover_euro'],ascending=False)
# agg_data.to_csv('C:/test_dark.csv')
# 
# np.sum(agg_data['Mturnover_euro'][agg_data['isdark']==1])/np.sum(agg_data['Mturnover_euro'])










#[PORTFOLIO, EXECDATE, TICKERFLEX, SIDE, LIMIT, STARTTIME,
#       ENDTIME, , PARTVOL, MARKETVOLUME, MARKETTURNOVER, USRID,
#       MODIFIED, AGGRESSIVITY, ARRIVALPRICE, TISCKSIZE, STRATEGY,
#       AVGSPREAD, WOULDLEVEL, FINALPRICE, LOW, HIGH, STRICTVOL, PGTNAME,
#       OMSREF, CURRENCY, WOULDEXEC, WOD]




#------ LOAD OCC DATA FROM
# data_occ_db=AlgoDataProcessor(start_date=datetime(2013,7,1,0,0,0),
#                               end_date=datetime(2013,9,30,0,0,0))
# data_occ_db.get_db_data(level='occurrence')
# 
# 
# data_seq=get_algostats.sequence_info(sequence_id='FYLCoAA0009')
# 
# 
# 
# Connections.change_connections('dev')
# data_occ=get_algodata.occurence_info(start_date='01/07/2013',end_date='22/07/2013')
# 
# 
# Connections.change_connections('dev')
# data_seq=get_algostats.sequence_info(start_date='22/07/2013',end_date='22/07/2013')
# data_seq.to_csv('C:/test.csv')
# data_seq=get_algostats.sequence_info(sequence_id=['FY00000142856ESLO1'])

# data_seq=get_algodata.sequence_info(sequence_id=['FY00000142856ESLO1'])
# data_seq = get_algodata.sequence_info(start_date="13/06/2013",end_date="13/06/2013")
# Explorer(data_seq)
# 
# data_occ=get_algodata.occurence_info(occurence_id=['FYLCoAA0016'])


# data_deal=get_algodata.deal(sequence_id=['FYBL2SUT00008L0001'])
# print data_deal
# Connections.change_connections('dev')
# #data_deal=get_algodata.deal(start_date='09/07/2013',end_date='09/07/2013')
# data_deal=get_algodata.deal(start_date='09/07/2013',end_date='09/07/2013',merge_order_colnames=['cheuvreux_secid','strategy_name_mapped'])
# Explorer(data_deal)
# uniqueext(data_deal['MIC'].values)

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



