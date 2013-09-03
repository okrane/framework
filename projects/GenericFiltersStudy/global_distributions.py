# -*- coding: utf-8 -*-

from lib.dbtools.connections import Connections
from pandas import DataFrame, isnull
from pandas.io.parsers import read_csv
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats.mstats import mquantiles
from lib.plots.matplotlib_tools import FixedOrderFormatter
from lib.plots.color_schemes import *
from lib.data.ui.Explorer import *
from datetime import datetime, timedelta

def global_street_order_count():
    data = read_csv('C:/st_sim/repository/ULPROD20130902O.csv', sep = ';', na_values=["nan"], keep_default_na = False, index_col = False)    
    data = data[data['ClOrdID'] != '']
    print np.unique(data['MsgType'])
    
        
    data.index = [datetime.strptime(d, '%Y-%m-%d %H:%M:%S') for d in data['SendingTime']]
    return data

def number_of_orders_distribution():
    
    data = global_street_order_count()[['ClOrdID', 'ParentClientID']]
    parents = np.unique(data['ParentClientID'])
    
    frequency = []
    print len(parents)
    i = 0
    for o in parents:
        #print i
        i += 1
        order_list =data[data['ParentClientID'] == o]             
        group_list = order_list.groupby(pd.TimeGrouper(freq = '%dS' % 60))   
        duration = order_list.index[-1] -  order_list.index[0]        
        order_count = len(order_list.index)
        if order_count > 10 and duration.total_seconds() > 10 * 60:
            for g in group_list:
                frequency.append((len(g[1]), o))
        #print order_count, duration, duration.total_seconds() / 60
        
    return frequency 

d = number_of_orders_distribution()

frequency = [f[0] for f in d]
fig = plt.figure()
ax = fig.add_subplot(111)
print min(frequency), max(frequency)
for a in d:
    if a[0] == max(frequency): print a[1]
        
count,division = np.histogram(frequency, bins = np.arange(min(frequency), max(frequency), 1))    
plt.hist(frequency, bins = division)
plt.show()
# -----------------------------------------------------------------------------

def global_parent_order_size():
    collection = Connections.getClient('Mars')['Mars']['AlgoOrders']
    deals = collection.aggregate([{'$match': {'MsgType' : "D"}},{'$project': {'SendingTime': 1, 'OrderQty': 1, 'cheuvreux_secid': 1, "_id": 0, 'strategy_name': 1, 'turnover': 1, 'rate_to_euro': 1}}])
    #result = collection.find(fields = {'SendingTime': 1, 'LastShares': 1, 'OrderQty': 1, 'SecurityID': 1, 'LastMkt': 1, "_id": 0 })
    return DataFrame(list(deals['result']))
    
def security_stats(sec_list):    
    query = "select security_id, indicator_value from Market_data..ci_security_indicator where " \
            " security_id in (%s) and trading_destination_id is NULL and indicator_id = 14 " % ','.join([str(int(v)) if v != np.NaN else '0' for v in sec_list])    
    return Connections.exec_sql('MARKET_DATA', query, as_dataframe = True)

def get_statistics():   
    data = global_parent_order_size()
    data = data[np.isfinite(data['cheuvreux_secid'])]    
    advs = security_stats(np.unique(data['cheuvreux_secid']))      
    data['adv'] = data['cheuvreux_secid'].apply(lambda x: advs[advs['security_id'] == x]['indicator_value'].values[0] if len(advs[advs['security_id'] == x]['indicator_value'].values) > 0 else np.nan )
    data = data[np.isfinite(data['adv'])]    
    return data
    
def distribution():
    data = get_statistics()        
    ratio = data['OrderQty'] / data['adv']
    print data
    filter_ratio = ratio[ratio > 0.05]
    count,division = np.histogram(filter_ratio, 0.025 * np.arange(80))
    
    fig1 = plt.figure()
    ax1 = fig1.add_subplot(111)
    filter_ratio.hist(ax = ax1, bins = division)
    
    fig2 = plt.figure()
    ax2 = fig2.add_subplot(111)
    ax2.set_title('Repartition of Order Turnover (greater than 1M)')
    ax2.set_xlabel('Order Turnover (Millions of Euros)')
    ax2.set_ylabel('Number of Orders')
    ax2.xaxis.set_major_formatter(FixedOrderFormatter(6))
    filtered_turnover = data[np.logical_and(np.isfinite(data['turnover']), data['turnover']>0)]      
    turnover = filtered_turnover['turnover'] * filtered_turnover['rate_to_euro']      
    count,division = np.histogram(turnover, bins = np.arange(1e6, max(turnover), 2.5e5))    
    turnover.hist(ax = ax2, bins = division, color = kc_main_colors()['dark_blue'])
 
    print mquantiles(turnover.values, [0.99, 0.995, 0.999])
    
    plt.show()



#distribution()