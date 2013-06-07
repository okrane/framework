# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def rolling_aggregate_vwap(data, sampling, delta_t):
    data_sampling = data.groupby(pd.TimeGrouper(freq = '%dS' % sampling)).agg(lambda x:np.nan if len(x) == 0 else x[-1])    
    data_sampling = data_sampling[~np.isnan(data_sampling['volume'])]       
    datevals=np.array([x.to_datetime() for x in data.index])    
    value = []
    for i in range(len(data_sampling)):        
        d = data_sampling.index[i].to_datetime()
        idx = (datevals>d) & (datevals<=(d + timedelta(seconds=delta_t)))
        #if i%100 == 0: print sum(idx == True)
        data_temp = data[idx]        
        value.append(np.sum(data_temp['volume'] * data_temp['price']) / (np.sum(data_temp['volume'])) if np.sum(data_temp['volume']) > 0 else np.nan )
        
    return pd.Series(value, index = data_sampling.index), data_sampling

