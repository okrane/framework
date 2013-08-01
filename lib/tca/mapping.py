# -*- coding: utf-8 -*-
"""
Created on Thu Jun 13 10:49:01 2013

@author: njoseph
"""
import pandas as pd
import numpy as np
from datetime import datetime
from lib.dbtools.connections import Connections
#import pytz

#------------------------------------------------------------------------------
# ExcludeAuction
#------------------------------------------------------------------------------
def ExcludeAuction(x):
    if x==0:
        out=[0,0,0,1]
    elif x==1:
        out=[1,1,1,1]
    elif x==2:
        out=[1,0,0,1]    
    elif x==3:
        out=[0,0,1,1]   
    else:
        raise NameError('mapping:ExcludeAuction - Bad inputs')
    return out
    
#------------------------------------------------------------------------------
# StrategyName
#------------------------------------------------------------------------------
def StrategyName(id, sweep_lit = None, database = 'Mars'):
    client     = Connections.getClient("MARS")
    collection = client[database]["map_tagFIX"]
    req        = {"tag_name" : "StrategyName", "tag_value" : str(id)}
    
    if sweep_lit is not None:
        req["SweepLit"] = sweep_lit
        
    result     = collection.find(req).sort([("validity_date",-1)])
    l_result   = list(result)
    if len(l_result) == 0:
        raise NameError('mapping:StrategyName - Bad inputs')
    return l_result[0]["short_name"]

if __name__ == "__main__":
    print StrategyName(id = 5)
    print StrategyName(id = 9, sweep_lit='BL')
    print StrategyName(id = 9, sweep_lit='yes')
    print StrategyName(id = 9, sweep_lit='CF')