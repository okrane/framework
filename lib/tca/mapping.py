# -*- coding: utf-8 -*-
"""
Created on Thu Jun 13 10:49:01 2013

@author: njoseph
"""
import pandas as pd
import numpy as np
#from datetime import *
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
def StrategyName(id, sweepLit = None, database = 'Mars'):
    file_orders = open(self.server['ip_addr'] + '.orders.json', 'r')
    u_orders = simplejson.loads(file_orders.read(), object_hook = serialize.as_datetime)
    file_orders.close()
    
    database = self.client[database]
    collection = database["map_tagFIX"]
    result   = collection.find({"tag_name" : "StrategyName"})
    l_result = list(result)
    for el in l_result:
    self.strategy_name[int(el["tag_value"])] = el["strategy_name"]
    
    if id==1:
        out="VWAP"
    elif id==2:
        out="TWAP"
    elif id==3:
        out="VOL"    
    elif id==4:
        out="ICEBERG"   
    elif id==5:
        out="DYNVOL"   
    elif id==6:
        out="IS"   
    elif id==7:
        out="CROSSFIRE"   
    elif id==8:
        out="CLOSE"   
    elif id==10:
        out="BLINK"    
    elif id==9:
        if sweepLit=="CF":
            out="CROSSFIRE" 
        elif sweepLit=="BF":
            out="BLINK" 
        elif sweepLit=="yes":
            out="HUNT" 
        else:
            raise NameError('mapping:StrategyName - Bad inputs')
    else:
        raise NameError('mapping:ExcludeAuction - Bad inputs')
    return out
