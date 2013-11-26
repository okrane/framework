# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 09:50:46 2013

@author: svlasceanu
"""

import pandas as pd
from lib.dbtools.connections import Connections
from lib.dbtools.read_dataset import ft
from datetime import datetime, timedelta
import numpy as np

data = pd.read_csv("C:/st_sim/repository/TCA GE AM - v2.csv", sep = ';')
data = data.ix[:len(data)-2]
data_processed = data
print data_processed
"""
data_processed.drop(['AP', 
                     'Slippage AP (bps)', 
                     'Slippage AP (Eur)',
                    'VWAP',
                    'Slippage VWAP (bps)',
                    'Slippage VWAP (Eur)', 
                    'Volume',
                    '% Vol'] , axis = 1)
                    """
                    
dico = {}                    
dico['Arrival Price(mid)'] = []
dico['Arrival Price(best)'] = []
dico['Arrival Price(opposite)'] = []
dico['Arrival Price(last)'] = []
dico['Slippage Arrival (bp)'] = []
dico['Slippage Arrival (Eur)'] = [] 

dico['VWAP (no dark)'] = []
dico['VWAP (with dark)'] = []
dico['Slippage VWAP(bp)'] = []
dico['Slippage VWAP(Eur)'] = []

dico['Volume (no dark)'] = []
dico['Volume (with dark)'] = []

dico['%Volume (no dark)'] = []
dico['%Volume (with dark)'] =[]

for i in range(len(data)):
    row = data.ix[i]    
    print row
    security_id_query = "select SYMBOL6 from SECURITY where SYMBOL5 ='%s'" % row['SECID']
    result = Connections.exec_sql('KGR', security_id_query)
    if not result :
        continue
    security_id = int(result[0][0])
    date  = datetime.strptime(row['CREATETIME'], '%d/%m/%Y %H:%M') + timedelta(hours = 2)
    start = datetime.strptime(row['CREATETIME'], '%d/%m/%Y %H:%M') + timedelta(hours = 2)
    end   = datetime.strptime(row['LASTFILLTIME'], '%d/%m/%Y %H:%M') + timedelta(hours = 2)
    print date
    print security_id
    market = ft(security_id = security_id, date = datetime.strftime(date, "%d/%m/%Y"))    
    print market    
    market = market[np.logical_and(market.index >= start, market.index <= end)]
    market_dark = market    
    market = market[market["dark"] == 0]    
    market = market[market["cross"] == 0]        
    print market
    
    arrival_mid      = 0.5* (market["bid"][0]+market["ask"][0])
    arrival_best     = market["bid"][0] if row["SIDE"] == "Buy" else market["ask"][0]
    arrival_opposite = market["bid"][0] if row["SIDE"] == "Sell" else market["ask"][0]
    arrival_last     = market["price"][0]
    vwap_no_dark          = np.sum(market["price"] * market["volume"]) / np.sum(market["volume"])
    volume_no_dark        = np.sum(market["volume"])
    print arrival_mid, arrival_best, arrival_opposite, arrival_last
    print vwap_no_dark, volume_no_dark
    
    vwap_with_dark          = np.sum(market_dark["price"] * market_dark["volume"]) / np.sum(market_dark["volume"])
    volume_with_dark        = np.sum(market_dark["volume"])
    print vwap_with_dark, volume_with_dark
    
    side = 1 if row["SIDE"] == "Buy" else -1
    
    dico['Arrival Price(mid)'].append(arrival_mid)
    dico['Arrival Price(best)'].append( arrival_best)
    dico['Arrival Price(opposite)'].append(arrival_opposite)
    dico['Arrival Price(last)'].append(arrival_last)
    dico['Slippage Arrival (bp)'].append(side * (arrival_last - row["AVG PRICE"]) / arrival_last * 10000)
    dico['Slippage Arrival (Eur)'].append(side * (arrival_last - row["AVG PRICE"]) * int(row["FILLEDQTY"]))
    
    dico['VWAP (no dark)'].append(vwap_no_dark)
    dico['VWAP (with dark)'].append(vwap_with_dark)
    dico['Slippage VWAP(bp)'].append(1.0 * side * (vwap_no_dark - row["AVG PRICE"]) / vwap_no_dark * 10000   )
    dico['Slippage VWAP(Eur)'].append(1.0 * side * (vwap_no_dark - row["AVG PRICE"]) * int(row["FILLEDQTY"]))
    
    dico['Volume (no dark)'].append( volume_no_dark)
    dico['Volume (with dark)'].append(volume_with_dark)
        
    dico['%Volume (no dark)'].append(1.0 * int(row["FILLEDQTY"]) / volume_no_dark * 100)
    dico['%Volume (with dark)'].append( 1.0 * int(row["FILLEDQTY"]) / volume_with_dark * 100)    
    
    
print dico['Arrival Price(mid)']
print len(dico['Arrival Price(mid)'])
print len(data_processed)

data_processed['Arrival Price(mid)'] = dico['Arrival Price(mid)']
data_processed['Arrival Price(best)'] = dico['Arrival Price(best)']
data_processed['Arrival Price(opposite)'] = dico['Arrival Price(opposite)']
data_processed['Arrival Price(last)'] = dico['Arrival Price(last)']
data_processed['Slippage Arrival (bp)'] = dico['Slippage Arrival (bp)']
data_processed['Slippage Arrival (Eur)'] = dico['Slippage Arrival (Eur)']   

data_processed['VWAP (no dark)'] = dico['VWAP (no dark)']
data_processed['VWAP (with dark)'] = dico['VWAP (with dark)']
data_processed['Slippage VWAP(bp)'] = dico['Slippage VWAP(bp)'] 
data_processed['Slippage VWAP(Eur)'] = dico['Slippage VWAP(Eur)']  

data_processed['Volume (no dark)'] = dico['Volume (no dark)']
data_processed['Volume (with dark)'] = dico['Volume (with dark)']

data_processed['%Volume (no dark)'] = dico['%Volume (no dark)']
data_processed['%Volume (with dark)'] = dico['%Volume (with dark)']

data_processed.to_csv("C:/st_sim/repository/TCA GE AM v2 - fixed.csv", sep=';')
    
    
