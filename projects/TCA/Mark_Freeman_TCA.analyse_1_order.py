# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 15:01:53 2013

@author: svlasceanu
"""

from lib.dbtools.connections import Connections
from datetime import datetime, timedelta
from lib.dbtools.get_repository import convert_symbol
from lib.dbtools.read_dataset import ft


def algos_on_a_day(bloom_code, date):

    db = Connections.getClient("Mars")["Mars"]["AlgoOrders"]
    mapping = Connections.getClient("Mars")["Mars"]["map_tagFIX"]    
    strategy_name = lambda x : mapping.aggregate([{"$match":{"tag_name": "StrategyName", "tag_value": x}}])["result"][0]["strategy_name"]
    
    
    security_id = int(convert_symbol(source = "bloomberg", dest = "security_id", value = bloom_code))
    print security_id
    date = datetime.strptime(date, '%d/%m/%Y')
    date_end = date + timedelta(days = 1)
    print date
    
    result = db.aggregate([{'$match': {"cheuvreux_secid": security_id, 'SendingTime': {'$gte': date, "$lte": date_end}, 'occ_nb_replace': 0}}])
    print len(result["result"])
    for r in result["result"]:
        #print r["StrategyName"]
        print "Algo:", r["strategy_name_mapped"], "Id", r["p_cl_ord_id"], "Size:", r["OrderQty"], "StartTime:" , r["TransactTime"], "Trader:", r["TraderName"] if r.has_key("TraderName") else "-"
    
    return result["result"]

def get_one_order(bloom_code, date):
     #db = Connections.getClient("Mars")["Mars"]["AlgoOrders"]
     r = algos_on_a_day(bloom_code, date)
     child = Connections.getClient("Mars")["Mars"]["OrderDeals"]
     
     security_id = int(convert_symbol(source = "bloomberg", dest = "security_id", value = bloom_code))
     
     order_list = [x["p_cl_ord_id"] for x in r]
     result = child.aggregate([{"$match": {"p_cl_ord_id": {"$in": order_list}}}, {"$project": {"LastPx":1, "LastShares": 1, "_id": 0, "TransactTime": 1}}])
     for order in result["result"]:
         print order["TransactTime"], order["LastShares"], order["LastPx"]
     
     intraday = ft(security_id = security_id, date = date)
     print intraday
     
if __name__ == "__main__":
    #algos_on_a_day("SAFT FP", "24/10/2013")
    #algos_on_a_day("ORA PA", "24/10/2013")
    get_one_order("SAFT FP", "24/10/2013")
    