# -*- coding: utf-8 -*-
"""
Created on Fri Jun 07 09:18:30 2013

@author: njoseph
"""

import pandas as pd
import numpy as np
from datetime import *
#import os as os
#from lib.dbtools.connections import Connections
#import lib.dbtools.get_repository as get_repository
#from lib.data.matlabutils import *
#import lib.data.st_data as st_data




#------------------------------------------------------------------------------
# agg
#------------------------------------------------------------------------------
def agg(data=pd.DataFrame(),start_datetime=None,end_datetime=None,exclude_auction=[0,0,0,0],exclude_dark=False):
    

    ##############################################################
    # check input
    ##############################################################
    default_indicators={'volume' : 0,
                        'vwap' : np.nan}

    ##############################################################
    # compute stats
    ##############################################################
    if data.shape[0]>0:
        # -- filter market data
        data=data.ix[np.nonzero(map(lambda x : x>=start_datetime and x<end_datetime,[x.to_datetime() for x in data.index]))[0]]
        if any(np.array(exclude_auction)==1):
            if exclude_auction[0]==1:
               data=data[data['opening_auction']==0]
            if exclude_auction[1]==1:
               data=data[data['intraday_auction']==0]
            if exclude_auction[2]==1:
               data=data[data['closing_auction']==0]
            if exclude_auction[3]==1:
               data=data[~((data['auction']==1) & (data['opening_auction']==0) & (data['intraday_auction']==0) & (data['closing_auction']==0))]
        if exclude_dark:
           data=data[data['dark']==0]
        # -- compute
        if data.shape[0]>0:
            indicators={'volume' : np.sum(data['volume']), 
                    'vwap' : np.sum(data['volume']*data['price'])/np.sum(data['volume'])}
        else:
            indicators=default_indicators
        indicators.update({'data' : 1,'start_time':start_datetime,'end_time':end_datetime})
        out=pd.DataFrame([indicators])
    else:
        default_indicators=default_indicators.update({'data' : 0,'start_time':start_datetime,'end_time':end_datetime})
        out=pd.DataFrame([default_indicators])        
    
    return out

if __name__ == "__main__":
    from lib.data.st_data import *