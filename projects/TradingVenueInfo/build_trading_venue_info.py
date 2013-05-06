# -*- coding: utf-8 -*-
"""
Created on Mon Apr 22 16:44:18 2013

@author: svlasceanu
"""

from pymongo import MongoClient
client = MongoClient("PARFLTLAB02", 27017)
collection = client.Mercure.trading_venue_info

collection.remove()
documents = []

documents.append({"trading_venue_id": 1, "exchange_id": 1, 'name' : 'NYSE EURONEXT PARIS', 'trading_venue_type': "L", 'is_primary': 1, 'code': "PA", 'destination':"TEST", 'feed': "PA", 'ranking': 5, "internal" : 0})
documents.append({"trading_venue_id": 12, "exchange_id": 1, 'name' : 'NYSE EURONEXT LISBON', 'trading_venue_type': "L", 'is_primary': 1, 'code': "LI", 'destination':"TEST", 'feed': "LI", 'ranking': 5, "internal" : 0})
documents.append({"trading_venue_id": 13, "exchange_id": 1, 'name' : 'NYSE EURONEXT AMSTERDAM', 'trading_venue_type': "L", 'is_primary': 1, 'code': "AS", 'destination':"TEST", 'feed': "AS", 'ranking': 5, "internal" : 0})
documents.append({"trading_venue_id": 14, "exchange_id": 1, 'name' : 'NYSE EURONEXT BRUSSELS', 'trading_venue_type': "L", 'is_primary': 1, 'code': "BE", 'destination':"TEST", 'feed': "BE", 'ranking': 5, "internal" : 0})

documents.append({"trading_venue_id": 16, "exchange_id": 3, 'name' : 'XETRA', 'trading_venue_type': "L", 'is_primary': 1, 'code': "DE", 'destination':"TEST", 'feed': "DE", 'ranking': 5, "internal" : 0})

documents.append({"trading_venue_id": 2, "exchange_id": 6, 'name' : 'CHIX', 'trading_venue_type': "L", 'is_primary':0, 'code': "Chi", 'destination':"TEST", 'feed': "CHIX", 'ranking': 1, "internal" : 0})
documents.append({"trading_venue_id": 3, "exchange_id": 7, 'name' : 'BATS', 'trading_venue_type': "L", 'is_primary':0, 'code': "Bats", 'destination':"TEST" , 'feed': "BATS", 'ranking': 2, "internal" : 0})
documents.append({"trading_venue_id": 4, "exchange_id": 8, 'name' : 'TURQOISE', 'trading_venue_type': "L", 'is_primary':0, 'code': "Tqse", 'destination':"TEST", 'feed': "TRQ", 'ranking': 3, "internal" : 0})

documents.append({"trading_venue_id": 5, "exchange_id": 6, 'name' : 'CHIX DARK', 'trading_venue_type': "D", 'is_primary':0, 'code': "ChiD", 'destination':"TEST",'feed': "", 'ranking': 6, "internal" : 0})
documents.append({"trading_venue_id": 6, "exchange_id": 7, 'name' : 'BATS DARK', 'trading_venue_type': "D", 'is_primary':0, 'code': "BatsD", 'destination':"TEST",'feed': "", 'ranking': 7, "internal" : 0})
documents.append({"trading_venue_id": 7, "exchange_id": 8, 'name' : 'TURQOISE DARK', 'trading_venue_type': "D", 'is_primary':0, 'code': "TqseD", 'destination':"TEST",  'feed': "", 'ranking': 8, "internal" : 0})

documents.append({"trading_venue_id": 8, "exchange_id": 9, 'name' : 'BLINK DARK', 'trading_venue_type': "D", 'is_primary':0, 'code': "BLNKD", 'destination':"TEST", 'feed': "", 'ranking': 4, "internal" : 1})
documents.append({"trading_venue_id": 9, "exchange_id": 9, 'name' : 'BLINK MID', 'trading_venue_type': "M", 'is_primary':0, 'code': "BLNKM", 'destination':"TEST", 'feed': "", 'ranking': 4, "internal" : 1})
documents.append({"trading_venue_id": 10, "exchange_id": 10, 'name' : 'SIGMA X', 'trading_venue_type': "M", 'is_primary':0, 'code': "SGMXM", 'destination':"TEST", 'feed': "", 'ranking': 3, "internal" : 0})
documents.append({"trading_venue_id": 10, "exchange_id": 11, 'name' : 'UBS MTF', 'trading_venue_type': "M", 'is_primary':0, 'code': "UBSM", 'destination':"TEST", 'feed': "", 'ranking': 2, "internal" : 0})
documents.append({"trading_venue_id": 11, "exchange_id": 12, 'name' : 'ITG POSIT', 'trading_venue_type': "M", 'is_primary':0, 'code': "ITGM", 'destination':"TEST",  'feed': "", 'ranking': 9, "internal" : 0})
documents.append({"trading_venue_id": 17, "exchange_id": 3, 'name' : 'XETRA MID', 'trading_venue_type': "M", 'is_primary': 0, 'code': "DEM", 'destination':"TEST", 'feed': "", 'ranking': 5, "internal" : 0})

for doc in documents:
    collection.insert(doc)