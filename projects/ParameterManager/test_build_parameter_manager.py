# -*- coding: utf-8 -*-

from pymongo import MongoClient
client = MongoClient("PARFLTLAB02", 27017)
collection = client.Mercure.parameter_manager

collection.remove()
documents = []

documents.append({'strategy': "CROSS", 'key' : {}, "static_parameters": {'MinATS': 2, 'MaxATS': 5, 'dt': 100, 'blabla': 5 }})
documents.append({'strategy': "CROSS", 'key' : {'client_id': 11}, "static_parameters": {'MinATS': 0, 'blabla': 11 }})
documents.append({'strategy': "CROSS", 'key' : {'trader_id': 1, 'place_id': "rome"}, "static_parameters": {'MinATS': 1, 'MaxATS': 1, 'dt': 1, 'blabla': 1 }})
documents.append({'strategy': "CROSS", 'key' : {'place_id': "paris", 'parent_algo_id': "VWAP"}, "static_parameters": {'blabla': 46 }})
documents.append({'strategy': "VWAP", 'key' : {'place_id': "paris", 'parent_algo_id': "VWAP"}, "static_parameters": {'blabla': 1111 }})
documents.append({'strategy': "VWAP", 'key' : {}, "static_parameters": {'blabla': 0 }})
###########################################################

for doc in documents:
    collection.insert(doc)
    
print "--------------------------------------------"
for doc in collection.find({"key.place_id" : {"$exists": True}}):
    print doc
    print " "