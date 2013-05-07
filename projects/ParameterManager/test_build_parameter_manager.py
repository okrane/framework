# -*- coding: utf-8 -*-

from pymongo import MongoClient
client = MongoClient("PARFLTLAB02", 27017)
collection = client.Mercure.parameter_manager

collection.remove()
documents = []

documents.append({'strategy': "CROSS", 'key' : {}, "static_parameters": {'MinATS': 2, 'MaxATS': 5, 'MinDarkATS': 3, "MaxDarkATS": 6, 'IndicatorRefreshTimerPeriod': 60000}})
documents.append({'strategy': "CROSS", 'key' : {'client_id': 11}, "static_parameters": {'MinATS': 0}})
documents.append({'strategy': "CROSS", 'key' : {'trader_id': 1, 'place_id': "rome"}, "static_parameters": {'MinATS': 1, 'MaxATS': 1}})
documents.append({'strategy': "CROSS", 'key' : {'place_id': "paris", 'parent_algo_id': "VWAP"}, "static_parameters": {}})
documents.append({'strategy': "VWAP", 'key' : {'place_id': "paris", 'parent_algo_id': "VWAP"}, "static_parameters": {}})
documents.append({'strategy': "VWAP", 'key' : {}, "static_parameters": {}})
###########################################################

for doc in documents:
    collection.insert(doc)


    
print "--------------------------------------------"
for doc in collection.find({"key.place_id" : {"$exists": True}}):
    print doc
    print " "