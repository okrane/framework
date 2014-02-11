# -*- coding: utf-8 -*-
from lib.dbtools.connections import Connections
from pandas import DataFrame
import matplotlib.pyplot as plt
from datetime import datetime

def turnover_over_time():
    cac40_query = "select distinct security_id from indice_component where INDEXID = 'IND_1'"
    cac40 =[str(x[0]) for x in Connections.exec_sql('KGR', cac40_query)]
        
    query = """select security_id, date, turnover, close_turnover from Market_data..trading_daily where
            trading_destination_id is NULL and
            security_id in (%s) and
            date > '20120101'""" % (",".join(cac40))    
    df = Connections.exec_sql("Market_data", query, as_dataframe = True)
    
    dates = []
    turnover = []
    auction = []
    for date, group in df.groupby('date'):
        dates.append(datetime.strptime(date,  '%Y-%m-%d' ))
        turnover.append(sum(group["turnover"]))
        auction.append(sum(group["close_turnover"]))
        
    aggregate = DataFrame({"date": dates, "turnover": turnover, "auction": auction})
    aggregate.index = aggregate["date"]
    
    plt.bar(aggregate["date"], 100.0 * aggregate["auction"] / aggregate["turnover"])
    plt.show()

def daily_vs_auction():
    query = """select security_id, date, turnover, close_turnover from MARKET_DATA..trading_daily where
            trading_destination_id is NULL and
            security_id=110 and
            date > '20120101'"""
    df = Connections.exec_sql("MARKET_DATA", query, as_dataframe = True)   
    
    plt.plot(df["close_turnover"], df["turnover"]-df["close_turnover"], ".")


daily_vs_auction()