# -*- coding: utf-8 -*-

from pymongo import MongoClient
client = MongoClient("172.29.0.32", 27017)
db = client.DB_test

# I want to keep only StrategyName and OrderQty
a = dict([(y["tag_value"], y["strategy_name"]) for y in db.map_tagFIX.find({"tag_name" : "StrategyName"})])
print a
b = [(a[x["StrategyName"]], x["OrderQty"]) for x in db.AlgoOrders.find({"StrategyName": {"$exists": True}, "OrderQty": {"$exists": True}})]
print b
    