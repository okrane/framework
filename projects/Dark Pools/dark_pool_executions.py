# -*- coding: utf-8 -*-

import lib.dbtools.read_dataset as read_dataset
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from lib.data.dataframe_tools import *
import matplotlib.pyplot as plt
from lib.plots.intraday import kcintraday

def projected_vwap(data, sample_dates, delta_t):
    datevals=np.array([x.to_datetime() for x in data.index])    
    frame = {}
    frame['value'] = []
    frame['price'] = []
    for d in sample_dates:
        idx = (datevals>d) & (datevals<=(d + timedelta(seconds=delta_t)))        
        data_sample = data[idx]
        frame['value'].append(np.sum(data_sample['volume'] * data_sample['price']) / (np.sum(data_sample['volume'])) if np.sum(data_sample['volume']) > 0 else np.nan )
        frame['price'].append(data_sample['price'][0] if len(data_sample['price'])>0 else np.NaN)
    return pd.DataFrame(frame, index = sample_dates)
       
security = 110
data = read_dataset.ftickdb(security_id = security, date = '02/05/2013')
kcintraday(data)
data_dark = data[data['dark'] == 1]
sample_dates = np.array([x.to_datetime() for x in data_dark.index])
timescale = [5, 10, 20, 30, 60, 120, 180, 240, 300]

full_frame = {}
median_distrib = []

for i in range(len(timescale)):
    vwap = projected_vwap(data, sample_dates, timescale[i])
    frame = {'vwap_agg_%s' % timescale[i] : vwap['value'], 'price': vwap["price"], 'returns_%d' % timescale[i]: 10000 * (vwap["price"] - vwap['value']) / vwap['price'] }
    full_frame['returns_%d' % timescale[i]] =  frame['returns_%d' % timescale[i]]
        
print full_frame
data_agg = pd.DataFrame(full_frame, index = data_dark.index )
data_agg = data_agg.reindex_axis(sorted(data_agg.columns, cmp = lambda x, y : int(x.split("_")[1]) - int(y.split("_")[1])), axis=1)
print data_agg
fig, axes = plt.subplots(nrows=1, ncols=1)
data_agg.boxplot(ax = axes)
plt.show()

print data_agg.median(axis = 0)

fig, axes = plt.subplots(nrows=1, ncols=1)
data_agg.median(axis = 0).plot()
plt.show()


#sample=dd.groupby(pd.TimeGrouper(freq = '%dS' % sampling)).agg(lambda x: np.nan if len(x) == 0 else x[-1]).fillna(method = "pad")

#datevals=np.array([x.to_datetime() for x in data.index])
#vals_rolling_mean=[np.mean(data['volume'].values[(datevals>=x) & (datevals<(x + timedelta(seconds=60)))]) for x in datevals]



