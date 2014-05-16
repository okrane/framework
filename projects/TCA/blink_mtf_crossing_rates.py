# -*- coding: utf-8 -*-
"""
Created on Mon Oct 21 13:38:37 2013

@author: svlasceanu
"""
from lib.dbtools.connections import Connections
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
from lib.plots.color_schemes import kc_main_colors

def daily_buy_sell(security_id, start, end):
    db = Connections.getClient("Mars")["Mars"]["AlgoOrders"]
    deals = Connections.getClient("Mars")["Mars"]["OrderDeals"]
    #result_deals = deals.aggregate([{"$match": {"cheuvreux_secid": security_id, "TransactTime":{"$gte": start, "$lte": end}, "LastMkt": "BLNK"}}, {"$project": {"LastMkt": 1, "OrderQty": 1, "_id": 0}} ])        
    
    #deal_list = deals.find({"cheuvreux_secid": security_id, "TransactTime":{"$gte": start, "$lte": end}, "LastMkt": "BLNK"})        
        
    results = []
    
    day = start
    while day <= end:
        result_orders = db.aggregate([{"$match": {"cheuvreux_secid": security_id, "TransactTime":{"$gte": day, "$lt": day + timedelta(days=1)}}}, {"$project": {"Side": 1, "OrderQty": 1, "_id": 0}}])    
        #print result_orders        
        buy_volume = sum([x["OrderQty"] if x["Side"] == "1" else 0 for x in result_orders["result"]])
        sell_volume = sum([x["OrderQty"] if x["Side"] == "2" else 0 for x in result_orders["result"]])
        result_deals = deals.aggregate([{"$match": {"cheuvreux_secid": security_id, "TransactTime":{"$gte": day, "$lte": day + timedelta(days=1)}, "LastMkt": "BLNK"}}, {"$project": {"LastMkt": 1, "LastShares": 1, "_id": 0}} ])    
        blink_volume = sum([x["LastShares"] for x in result_deals["result"]])        
        print day        
        print blink_volume     
        print buy_volume, sell_volume        
        
        results.append({"date": day, "buy_volume": buy_volume, "sell_volume": sell_volume, "blink_volume": blink_volume})
        day = day + timedelta(days = 1)
    
    df = {"date": [k["date"] for k in results], 
          "buy_volume" : [x["buy_volume"] for x in results], 
          "sell_volume" : [x["sell_volume"] for x in results], 
          "blink_volume" : [x["blink_volume"] for x in results]}    
    return pd.DataFrame(df, index = df["date"])
    
    
def build_graph(filename):
    data = pd.read_csv(filename, sep=";")
    print data["date"]    
    dates = [datetime.strptime(x, "%Y-%m-%d %H:%M:%S") for x in data["date"]]
    dates_f = [datetime.strftime(y, "%d/%m") for y in dates]
    #print dates_f
    x = np.array(range(len(dates)))
    #x = dates_f
    fig, ax = plt.subplots(2, sharex=True)
    plt.setp(ax[1].xaxis.get_majorticklabels(), rotation = 90)
    ax[0].set_title("Kepler-Cheuvreux Vodafone Traded Volumes")   
    ax[0].set_ylabel("Volume")    
    ax[0].xaxis.set_ticks(x)
    ax[0].set_xticklabels(dates_f)
    width = 0.55 
    bar1 = ax[0].bar(x, data["sell_volume"], width,  color = kc_main_colors()["light_orange"])
    
    bar2 = ax[0].bar(x, data["buy_volume"], width, bottom = data["sell_volume"], color = kc_main_colors()["dark_blue"])
    #bar3 = ax[0].bar(x+2*+width, data["blink_volume"], width, color = "g")
    ax[0].legend( (bar1[0], bar2[0]), ('Sell', 'Buy',) )
    
    crossing_rates = [k if k <= 100 else 0 for k in (100.0 * data["blink_volume"] / (1+ np.minimum(data["buy_volume"], data["sell_volume"])))]
    
    ax_twin = plt.twinx(ax[1])    
    ax_twin.bar(x, data["blink_volume"], width,  color = kc_main_colors()["light_blue"], alpha = 0.4, edgecolor = "b")     
    ax_twin.set_ylabel("Algo Blink Volume")     
    
    ax[1].set_title("BLINK CROSSING RATES (VODAFONE)")    
    ax[1].set_ylabel("Crossing Rate")    
    ax[1].plot(x, crossing_rates, "o", color = "g", linewidth = 2)
    ax[1].plot(x, crossing_rates, color = "g", linewidth = 2)
    
      
      
    
    plt.show()
    
    
    
    
if __name__=="__main__":
    """
    start = datetime(2013, 8, 01)
    end   = datetime(2013, 10, 18)
    results = daily_buy_sell(107509, start, end)
    print results
    results.to_csv("C:/st_sim/repository/blink_cross_rate_cagr.csv", sep=";")
    """
    build_graph("C:/st_sim/repository/blink_cross_rate_cagr.csv")