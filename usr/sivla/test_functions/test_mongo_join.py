# -*- coding: utf-8 -*-

from lib.dbtools.connections import Connections
db = Connections.getClient("HPP").DB_test

# I want to keep only StrategyName and OrderQty
#a = dict([(y["tag_value"], y["strategy_name"]) for y in db.map_tagFIX.find({"tag_name" : "StrategyName"})])
#print a
#b = [(a[x["StrategyName"]], x["OrderQty"]) for x in db.AlgoOrders.find({"StrategyName": {"$exists": True}, "OrderQty": {"$exists": True}})]
#print b
    
    
#import usr.sivla.funcs.DBTools.manycollection_query as mcq

#query_sql = "select AlgoOrders.OrderQty from AlgoOrders"
#print mcq.runQuery(query_sql, "HPP", "DB_test")

#query_sql = "select map_tagFIX.strategy_name, AlgoOrders.OrderQty from AlgoOrders, map_tagFIX where map_tagFIX.tag_value = AlgoOrders.StrategyName"
#print mcq.runQuery(query_sql, "HPP", "DB_test")
from lib.dbtools.connections import Connections
db = Connections.getClient("HPP").DB_test
from datetime import *
#result = db.AlgoOrders.aggregate([{"$project": {"ClOrdID": 1, "_id": 0}}])
result = db.AlgoOrders.aggregate([{"$match": {"SendingTime" : {"$gt" : datetime(2013, 05, 23), "$lte": datetime(2013, 05, 24)}, "OrderQty" : {"$gt": "9"}}}, {"$project": {"ClOrdID": 1, "SendingTime":1, "_id": 0}}])
print [k["SendingTime"] for k in result["result"]]
deals = db.OrderDeals.aggregate([{"$match" : {"ClOrdID" : {"$in": [k["ClOrdID"] for k in result["result"]]} }}, {"$project": {"Symbol": 1, "OrderQty": 1, "_id": 0}}])
#print deals["result"]
