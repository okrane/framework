# -*- coding: utf-8 -*-
"""
Created on Tue Nov 12 11:57:28 2013

@author: svlasceanu
"""

from lib.dbtools.connections import Connections
import pandas as pd
from lib.dbtools.read_dataset import ft
from datetime import datetime, timedelta
import numpy as np
import re

def get_data():
    Connections.change_connections("production")
    
    query = """ select occ.IdOccurence,
        occ.SecurityRateToEuro,
        occ.SecurityId,
        occ.Side,     
        seq.StartTime, 
        seq.EndTime, 
        seq.AutomatonName,         
        occ.OrderDate,
        seq.AutomatonStatTime, 
        seq.AutomatonEndTime, 
        seq.ExecutedAmount, 
        seq.ExecutedQuantity
            
        from dm_algo..Algo_Sequence seq, dm_algo..Algo_Occurence occ
        where occ.OrderDate > '20130101' and occ.OrderDate < '20130201' 
        and occ.IdOccurence = seq.IdOccurence 
        and seq.ExecutedQuantity is not NULL"""
    
    #query = """select top 10 * from dm_algo..Algo_Occurence        """
    
    result = Connections.exec_sql("VEGA", query, as_dict = True)
    print "Fetched", len(result), "lines"
    keys = result[0].keys()
    f = open("C:/st_sim/repository/export_cheuvreux_orders.csv", "w")
    f.writelines(";".join(keys)+"\n")
    for k in result:
        f.writelines(";".join([str(x) for x in k.values()]) + "\n")
    f.close()
    #print result["AutomatonStatTime"]
    #result.to_csv("C:/st_sim/repository/export_cheuvreux_orders.xls", sep = ";")

def enrich_with_market_data():
    data = pd.read_csv("C:/st_sim/repository/export_cheuvreux_orders.csv", sep = ';')
    data = data[data["AutomatonName"].isin(["EVP", "Vwap", "Twap", "ImplementationShortFall", "TargetClose", "EVPMarshallWace", ])]
    #
   # data["EndTime"] = datetime.strptime(data["EndTime"], "%Y-%m-%d %H:%M:%S")
    data["ExecutedAmount"]= [int(x) for x in data["ExecutedAmount"]]
    data["SecurityRateToEuro"] = [float(x) if x != "None" else 0 for x in data["SecurityRateToEuro"] ]    
    data["TurnoverEuro"] = data["ExecutedAmount"] * data["SecurityRateToEuro"]
    
    data["StartTime"] = [datetime.strptime(x, "%Y-%m-%d %H:%M:%S") for x in data["StartTime"]]
    data["EndTime"] = [datetime.strptime(x, "%Y-%m-%d %H:%M:%S") for x in data["EndTime"]]
    
    data = data[data["TurnoverEuro"] > 10000]
    data.index = range(len(data))
    
    no_steps = 100
    step = len(data) / no_steps
    for i in range(46, no_steps):
        try:
            data_x = data.ix[i*step:(i+1)*step]
            enrich(data_x, i, step)
        except e:
            print "=====================Error============================", i
            continue
    
def enrich(data, index, step):
    extras = {"ArrivalPrice": [], 
              "EndPrice": [], 
              "VWAP": [],
              "MktTurnover": [],
                "MktVolume": [],
              "Spread": [],
              "Low": [],
              "High": [],}     
    for i in range(index*step, (index+1)*step+1):
        print i, "/" , step, index*step, (index+1) * step, len(data)
               
        date = datetime.strptime(data.ix[i]["OrderDate"], "%Y-%m-%d %H:%M:%S")
        start = data.ix[i]["StartTime"]
        end = data.ix[i]["EndTime"]
        market = ft(security_id = data.ix[i]["SecurityId"], date = datetime.strftime(date, "%d/%m/%Y"))
        market = market[np.logical_and(market.index >= start, market.index <= end)]
        
        arrival_last     = market["price"][0] if len(market) > 0 else 0
        end_price        = market["price"][-1] if len(market) > 0 else 0
        vwap_no_dark     = np.sum(market["price"] * market["volume"]) / np.sum(market["volume"]) if len(market) > 0 else 0
        mkt_volume       = np.sum(market["volume"]) if len(market) > 0 else 0
        mkt_turnover     = np.sum(market["price"] * market["volume"]) if len(market) > 0 else 0
        spread_bp        = (market["ask"][0] - market["bid"][0]) / arrival_last * 10000 if len(market) > 0 else 0
        low              = np.min(market["price"]) if len(market) > 0 else 0
        high             = np.max(market["price"]) if len(market) > 0 else 0
        
        extras["ArrivalPrice"].append(arrival_last)
        extras["EndPrice"].append(end_price)
        extras["VWAP"].append(vwap_no_dark)
        extras["MktTurnover"].append(mkt_turnover)
        extras["MktVolume"].append(mkt_volume)
        extras["Spread"].append(spread_bp)
        extras["Low"].append(low)
        extras["High"].append(high)
    
    data["ArrivalPrice"] = extras["ArrivalPrice"]
    data["EndPrice"] = extras["EndPrice"]
    data["VWAP"] = extras["VWAP"]
    data["MktTurnover"] = extras["MktTurnover"]
    data["Spread"] = extras["Spread"]
    data["Low"] = extras["Low"]
    data["High"] = extras["High"]
    data["MktVolume"] = extras["MktVolume"]
        
    data.to_csv("C:/st_sim/repository/enriched_cheuvreux_orders_%d.csv" % index, sep = ";")    

def merge_data():
    data = pd.read_csv("C:/st_sim/repository/enriched_cheuvreux_orders_0.csv", sep = ";")
    for i in range(1, 45):
        d = pd.read_csv("C:/st_sim/repository/enriched_cheuvreux_orders_%d.csv" % i, sep = ";")
        data = data.merge(d, how = "outer")
        print data
    data = data[data["VWAP"] != 0]
    data.index = range(len(data))
        
    #data.to_csv("C:/st_sim/repository/order_data_cheuvreux.csv", sep = ";")
    spread = []
    securities = data["SecurityId"]
    print securities
    for i in range(len(securities)):
        security_id = securities[i]
        d = ft(security_id = security_id, date = "06/11/2013")
        if len(d)< 51:
            spread.append(0)
        else:
            spread.append(10000.0 * (d.ix[50]["ask"] - d.ix[50]["bid"]) / d.ix[50]["price"])
    
    data["spread_alt"] = spread
    data.to_csv("C:/st_sim/repository/order_data_cheuvreux222.csv", sep = ";")
merge_data()
