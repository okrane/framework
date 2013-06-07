# -*- coding: utf-8 -*-

from lib.dbtools.read_dataset import read_dataset
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from lib.data.dataframe_tools import *
import matplotlib.pyplot as plt
from lib.plots.intraday import kcintraday

security = 110
data = read_dataset('ft', security_id = security, date = '02/05/2013')
kcintraday(data)
data_dark = data[data['dark'] == 1]


timescale = [5, 10, 20, 30, 60, 120, 180, 240, 300]
sampling = 45
full_frame = {}
median_distrib = []

for i in range(len(timescale)):
    vwap, df_agg = rolling_aggregate_vwap(data, sampling, timescale[i])
    frame = {'vwap_agg_%s' % timescale[i] : vwap, 'price': df_agg["price"], 'returns_%d' % timescale[i]: 10000 * (df_agg["price"] - vwap) / df_agg["price"] }
    full_frame['returns_%d' % timescale[i]] =  10000.0 * (vwap - df_agg["price"]) / df_agg["price"]
        
print full_frame
data_agg = pd.DataFrame(full_frame, index = df_agg.index )
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


