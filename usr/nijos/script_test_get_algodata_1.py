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

#data=get_algostats.sequence(occurence_id='FY2000007382301')
data_occ=get_algodata.sequence_info(occurence_id=['FY2000007221801','FY2000007089101'])
data_occ=get_algostats.sequence_info(occurence_id=['FY2000007221801','FY2000007089101'])

data_occ=get_algodata.occurence_info(occurence_id=['FY2000007221801','FY2000007089101'])
data_occ=get_algostats.sequence_info(occurence_id=['FY2000007221801','FY2000007089101'])


data_occ=get_algodata.sequence_info(start_date='17/06/2013',end_date='17/06/2013')


data_occ.index.min()
# test on market data
data=read_dataset.ftickdb(security_id=110,date='17/05/2013')
stats=compute_stats.aggmarket(data=data,exchange_id_main=np.min(data['exchange_id']),start_datetime=data.index[1].to_datetime(),end_datetime=data.index[5000].to_datetime(),limit_price=8.23,side=-1)


data_occ=get_algostats.sequence_info(occurence_id=['FY2000007221801','FY2000007089101'])


# mode "sequenceinfo"
#data=get_algodata("sequence_info",sequence_id="FY2000007381401")
#data=get_algodata("sequence_info",sequence_id=["FY2000007382301","FY2000007414521"])
#data=get_algodata("sequence_info",occurence_id="FY2000007383101")
data=get_algodata.sequence_info(start_date="28/05/2013",end_date="28/05/2013")
# mode "occurenceinfo"
data=get_algodata.occurence_info(occurence_id=["FY2000007383101"])
data=get_algodata.occurence_info(start_date="28/05/2013",end_date="28/05/2013")
# mode deal
data=get_algodata.deal(sequence_id="FY2000004393001")

req=(" SELECT top 1 * from Market_data..trading_daily ")
vals=Connections.exec_sql('MARKET_DATA',req,schema = True)




