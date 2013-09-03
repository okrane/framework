# -*- coding: utf-8 -*-

from lib.dbtools.read_dataset import *
from datetime import datetime, timedelta
from lib.dbtools.connections import Connections
import matplotlib.pyplot as plt
import numpy as np
from numpy import arange,array,ones,linalg
import pandas as pd

start_date = datetime(2013, 01, 10)
end_date = datetime(2013, 06, 01)

"""
Connections.change_connections('production')

query = "select * from trading_daily where security_id = 110 and trading_destination_id = 1 and date > CONVERT(datetime, '%s')" % (datetime.strftime(start_date, '%Y-%m-%d'))
print query

data = Connections.exec_sql('MKTDATA', query, as_dict = True)
rand = []
volume = []

for i in range(1, len(data)):
    rand.append(np.abs(data[i]['close_prc'] - data[i-1]['close_prc']))
    volume.append(data[i]['close_volume'])

print volume
    
plt.plot(rand, volume, '.', color = 'r')
plt.show()
    
"""
security = 2

data0 = ft(date = datetime.strftime(start_date, '%d/%m/%Y') , security_id = security)
print data0
close = []

close.append(data0[data0['closing_auction'] == 1]['price'].ix[-1])

frame = {'close_returns': [], 'close_volume': [], 'intraday_vol': [], 'close_median': [], 'open': [], 'high': [], 'low': [], 'close': [], 'total_volume': [], 'date': []}
print close
for i in range(1, 120):
    data = ft(date = datetime.strftime(start_date + timedelta(days = i), '%d/%m/%Y') , security_id = security)
    if len(data.index) > 0:
        frame['close_returns'].append(10000 * np.abs((data[data['closing_auction'] == 1]['price'].ix[-1] - close[-1]) / close[-1]))
        close.append(data[data['closing_auction'] == 1]['price'].ix[-1])
        frame['close_volume'].append(sum(data[data['closing_auction'] == 1]['volume']))
        high, low, o, c = max(data['price']), min(data['price']), data['price'][0], data['price'][-1]        
        frame['open'].append(o)
        frame['high'].append(high)
        frame['low'].append(low)
        frame['close'].append(c)
        frame['date'].append(start_date + timedelta(days = i))
        frame['total_volume'].append(sum(data['volume']))
        frame['intraday_vol'].append(16*np.sqrt((high-low)**2/2 - (2*np.log(2)- 1)*(c-o)**2))
        #print frame['close_volume'], max(-20, -len(frame['close_volume']))
        frame['close_median'].append(np.mean(array(frame['close_volume'][:max(-20, -len(frame['close_volume']) +1)])))
        #print frame['close_median']
        
    
print frame
df = pd.DataFrame(frame, index = frame['date'])
df.to_csv('C:\st_sim\projects\TargetClose\data.csv')
print 'done'

A = np.array([frame['close_returns'], frame["intraday_vol"]])
y = np.array(frame['close_volume'])
w = np.linalg.lstsq(A.T,y)[0]

tracking_error = y -  w.dot(A)
print np.median(tracking_error)
print np.std(tracking_error)

#############
A = np.array([frame['close_returns']])
y = np.array(frame['close_volume'])
w = np.linalg.lstsq(A.T,y)[0]
print np.median(tracking_error)
tracking_error = y -  w.dot(A)
print np.std(tracking_error)

fig, ax = plt.subplots(nrows=1, ncols=1)
ax.plot(frame['close_returns'], array(frame['close_volume'])-array(frame['close_median']), 'x', color = 'r')
ax.set_xlabel('Absolute Close-Close returns (bp)')
ax.set_ylabel('Close Volume')
plt.show()
