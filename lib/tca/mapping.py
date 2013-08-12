# -*- coding: utf-8 -*-
"""
Created on Thu Jun 13 10:49:01 2013

@author: njoseph
"""
import pandas as pd
import numpy as np
from datetime import datetime
from lib.dbtools.connections import Connections
import simplejson
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

def OrdStatus(char):
    global ord_status_dict
    
    if 'ord_status_dict' in globals():
        if isinstance(ord_status_dict, dict) and len(ord_status_dict) > 0:
            return ord_status_dict[char]
        
    
    import os
    full_path           = os.path.realpath(__file__)    
    path, f             = os.path.split(full_path)
    
    file = open(path + '/../io/fix_types.json', 'r')
    input = file.read()
    file.close()

    list_of_dict = simplejson.loads(input)["fix"]["fields"]["field"]
    ord_status_dict = {}
    
    for el in list_of_dict:
        if el['number'] == '39':
            for e in el['value']:
                ord_status_dict[e['enum']] = e['description']
    
    return ord_status_dict[char]
    
    
#------------------------------------------------------------------------------
# StrategyName
#------------------------------------------------------------------------------
def StrategyName(id, sweep_lit = None, database = 'Mars'):
    client     = Connections.getClient(database)
    collection = client[database]["map_tagFIX"]
    req        = {"tag_name" : "StrategyName", "tag_value" : str(id)}
    
    if isinstance(sweep_lit,basestring):
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
    print OrdStatus('0')
    print OrdStatus('1')
    print OrdStatus('3')
    print OrdStatus('5')
    print OrdStatus('A')
    print OrdStatus('E')
