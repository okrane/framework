# -*- coding: utf-8 -*-
"""
Created on Mon Jun 24 14:21:03 2013

@author: njoseph
"""
import pandas as pd
import numpy as np
import lib.tca.mapping as mapping
from datetime import *
import pytz

#--------------------------------------------------------------------------
# GLOBAL needed
#--------------------------------------------------------------------------    
utc=pytz.UTC

#------------------------------------------------------------------------------
# extract_benchtime
#------------------------------------------------------------------------------
def extract_benchtime(data=None,lasttick_datetime=None,tdhours=pd.DataFrame()):
    #######################
    ## test 
    #######################
    # extract start time and end time from an algo sequence information
    if not isinstance(data,pd.Series):
        raise NameError('extract_benchtime:Input - Bad data input')
    # needed columns
    needed_columns=['ExcludeAuction','StrategyName','SweepLit','eff_endtime','EndTime','exec_qty','OrderQty','occ_prev_exec_qty']
    
    #######################
    ## needed 
    #######################    
    excl_auction=mapping.ExcludeAuction(data['ExcludeAuction'])
    strategy_name=mapping.StrategyName(data['StrategyName'],data['SweepLit'])
    
    #######################
    ## compute
    #######################    
    bench_starttime=data.name.to_datetime()  
    bench_endtime=data['eff_endtime']

    if ((strategy_name=="VWAP") and 
            (data['exec_qty']==(data['OrderQty']-data['occ_prev_exec_qty']))):
                # fullfiled vwap end time is the one set by the client if it is set or the end of trading
                if isinstance(data['EndTime'],datetime):
                    bench_endtime=max(bench_endtime,utc.localize(data['EndTime']))
                elif isinstance(lasttick_datetime,datetime):
                    bench_endtime=max(bench_endtime,lasttick_datetime+timedelta(seconds=5))
                    
    return bench_starttime,bench_endtime

    
    