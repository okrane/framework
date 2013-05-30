# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
"""
Created on Fri May 24 16:04:51 2013

@author: silviu
"""
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import pymongo
from datetime import datetime

def plot_fillrates(ax = None, start = None, end = None, sampling = 150):
    collection = pymongo.MongoClient("PARFLTLAB02", 27017).Mercure.crossfire_indicators
    d = {}
    dates = {}
    for doc in collection.find({"fillrate_buy" : {"$exists" : True}}):
        if not d.has_key(doc["symbol"]) : d[doc["symbol"]] = []
        if not dates.has_key(doc["symbol"]) : dates[doc["symbol"]] = []
        d[doc["symbol"]].append(doc["fillrate_buy"])
        dates[doc["symbol"]].append(datetime.strptime(doc["date"], '%Y%m%d-%H:%M:%S'))
        
        
    df = {}
    dd = pd.DataFrame()
    print dd
    del d["FTEp.AG"]
    for k, v in d.iteritems():
        if k[-3:] != ".AG":            
            df[k] = pd.DataFrame(d[k], columns = ['fillrate_%s' % k], index = dates[k])
            df[k]["index"] = df[k].index
            df[k] = df[k].drop_duplicates(cols='index', take_last=True)            
            del df[k]["index"] 
            dd = pd.merge(dd, df[k], how = "outer", left_index = True, right_index = True, sort = True )
    dd = dd.fillna(method = "pad")
    sample=dd.groupby(pd.TimeGrouper(freq = '%dS' % sampling)).agg(lambda x: np.nan if len(x) == 0 else x[-1]).fillna(method = "pad")
    print sample.head(10)    
    
    sample.plot(kind = "bar", stacked = True)
    plt.show()
    #cumvolumes = dict( (k, np.sum(v["volume"])) for k, v in grouped )   
        
    
            
    
        
        
plot_fillrates()
    
    
    
    