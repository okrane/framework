# -*- coding: utf-8 -*-
"""
Created on Thu May 16 14:02:13 2013

@author: njoseph
"""
import numpy as np
import scipy 
import matplotlib.pyplot as plt
import datetime as dt
import time
from scipy.io  import matlab
import pandas as pd
from lib.data.matlabutils import *
import lib.data.st_data as st_data
import lib.dbtools.get_repository as get_repository
import lib.dbtools.read_dataset as read_dataset
from lib.data.ui.Explorer import Explorer
import lib.dbtools.get_algodata as get_algodata
import lib.tca.compute_stats as compute_stats


# test on market data
data=read_dataset.ftickdb(security_id=110,date='17/05/2013')
stats=compute_stats.aggmarket(data=data,exchange_id_main=np.min(data['exchange_id']),start_datetime=data.index[1].to_datetime(),end_datetime=data.index[5000].to_datetime(),limit_price=8.23,side=-1)

##Explorer(stats)
##------------ occurence ID
##-- algodata
#occ_id='FY2000007382301'
#data_occ=get_algodata.sequence_info(occurence_id=occ_id)
## data_occ=get_algodata.occurence_info(occurence_id=occ_id)
##-- market data
#sec_id=data_occ.ix[0]['cheuvreux_secid']
#datestr=datetime.strftime(data_occ.index[0].to_datetime(), '%d/%m/%Y')
#datatick=pd.DataFrame()
#if sec_id>0:
#    datatick=read_dataset.ftickdb(security_id=sec_id,date=datestr)
##-- agg market data on sequence
#dataggseq=pd.DataFrame()
#for i in range(0,data_occ.shape[0]-1):
#    #-- needed
#    starttime=data_occ.index[i].to_datetime()
#    endtime=data_occ.ix[i]['eff_endtime']
#    if isinstance(endtime,datetime):
#        endtime=endtime.replace(tzinfo=pytz.UTC)
#    exchange_id_main=np.min(datatick['exchange_id'])
#    #-- compute
#    if isinstance(endtime,datetime):
#        tmp=aggmarket(data=datatick,exchange_id_main=exchange_id_main,
#                      start_datetime=starttime,end_datetime=endtime,
#                      limit_price=data_occ.ix[i]['Price'],side=data_occ.ix[i]['Side'])
#        tmp=tmp.join(pd.DataFrame([data_occ.ix[i][['ClOrdID','occ_ID']].to_dict()]))                 
#        dataggseq=dataggseq.append(tmp)           
#                          
#    Explorer(data_occ)
#
#
#
#
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







