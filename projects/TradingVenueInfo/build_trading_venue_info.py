# -*- coding: utf-8 -*-
"""
Created on Mon Apr 22 16:44:18 2013

@author: svlasceanu
"""

from pymongo import MongoClient
from lib.dbtools.connections import Connections
client = MongoClient("PARFLTLAB02", 27017)
collection = client.Mercure.trading_venue_info

collection.remove()
documents = []

flex_code = {1 : "PA", 
             2 : "DE", 
             5 : "MI",
             22: "MC", 
             38: "AS",
             55: "CO",
             26: "BR",
             51: "VX",
             18: "S",
             112: "HX",
             17: "SX",
             34: "LN",
             7: "VN",
             72: "NO",
             85: "PO",
             122: "AG",
             80: "GA",
             39: "ID",
             40: "PW",
             15: "US",
             102: "AM",
             159: "CHIX",
             189: "BATE",
             183: "TRQ"}


"""
documents.append({"trading_venue_id": 1, "exchange_id": 1, 'name' : 'NYSE EURONEXT PARIS', 'type': "L", 'is_primary': 1, 'code': "PA", 'destination':"TEST", 'feed': "PA", 'ranking': 5, "internal" : 0})
documents.append({"trading_venue_id": 12, "exchange_id": 1, 'name' : 'NYSE EURONEXT LISBON', 'type': "L", 'is_primary': 1, 'code': "LI", 'destination':"TEST", 'feed': "LI", 'ranking': 5, "internal" : 0})
documents.append({"trading_venue_id": 13, "exchange_id": 1, 'name' : 'NYSE EURONEXT AMSTERDAM', 'type': "L", 'is_primary': 1, 'code': "AS", 'destination':"TEST", 'feed': "AS", 'ranking': 5, "internal" : 0})
documents.append({"trading_venue_id": 14, "exchange_id": 1, 'name' : 'NYSE EURONEXT BRUSSELS', 'type': "L", 'is_primary': 1, 'code': "BE", 'destination':"TEST", 'feed': "BE", 'ranking': 5, "internal" : 0})

documents.append({"trading_venue_id": 16, "exchange_id": 3, 'name' : 'XETRA', 'type': "L", 'is_primary': 1, 'code': "DE", 'destination':"TEST", 'feed': "DE", 'ranking': 5, "internal" : 0})

documents.append({"trading_venue_id": 2, "exchange_id": 6, 'name' : 'CHIX', 'type': "L", 'is_primary':0, 'code': "Chi", 'destination':"TEST", 'feed': "CHIX", 'ranking': 1, "internal" : 0})
documents.append({"trading_venue_id": 3, "exchange_id": 7, 'name' : 'BATS', 'type': "L", 'is_primary':0, 'code': "Bats", 'destination':"TEST" , 'feed': "BATE", 'ranking': 2, "internal" : 0})
documents.append({"trading_venue_id": 4, "exchange_id": 8, 'name' : 'TURQOISE', 'type': "L", 'is_primary':0, 'code': "Tqse", 'destination':"TEST", 'feed': "TRQ", 'ranking': 3, "internal" : 0})

documents.append({"trading_venue_id": 5, "exchange_id": 6, 'name' : 'CHIX DARK', 'type': "D", 'is_primary':0, 'code': "ChiD", 'destination':"TEST",'feed': "", 'ranking': 6, "internal" : 0})
documents.append({"trading_venue_id": 6, "exchange_id": 7, 'name' : 'BATS DARK', 'type': "D", 'is_primary':0, 'code': "BatsD", 'destination':"TEST",'feed': "", 'ranking': 7, "internal" : 0})
documents.append({"trading_venue_id": 7, "exchange_id": 8, 'name' : 'TURQOISE DARK', 'type': "D", 'is_primary':0, 'code': "TqseD", 'destination':"TEST",  'feed': "", 'ranking': 8, "internal" : 0})

documents.append({"trading_venue_id": 8, "exchange_id": 9, 'name' : 'BLINK DARK', 'type': "D", 'is_primary':0, 'code': "BLNKD", 'destination':"TEST", 'feed': "", 'ranking': 4, "internal" : 1})
documents.append({"trading_venue_id": 9, "exchange_id": 9, 'name' : 'BLINK MID', 'type': "M", 'is_primary':0, 'code': "BLNKM", 'destination':"TEST", 'feed': "", 'ranking': 4, "internal" : 1})
documents.append({"trading_venue_id": 10, "exchange_id": 10, 'name' : 'SIGMA X', 'type': "M", 'is_primary':0, 'code': "SGMXM", 'destination':"TEST", 'feed': "", 'ranking': 3, "internal" : 0})
documents.append({"trading_venue_id": 10, "exchange_id": 11, 'name' : 'UBS MTF', 'type': "M", 'is_primary':0, 'code': "UBSM", 'destination':"TEST", 'feed': "", 'ranking': 2, "internal" : 0})
documents.append({"trading_venue_id": 11, "exchange_id": 12, 'name' : 'ITG POSIT', 'type': "M", 'is_primary':0, 'code': "ITGM", 'destination':"TEST",  'feed': "", 'ranking': 9, "internal" : 0})
documents.append({"trading_venue_id": 17, "exchange_id": 3, 'name' : 'XETRA MID', 'type': "M", 'is_primary': 0, 'code': "DEM", 'destination':"TEST", 'feed': "", 'ranking': 5, "internal" : 0})

for doc in documents:
    collection.insert(doc)

"""
Connections.change_connections("production")
# first we add the lit primary markets
query = """select exc.MARKET, exc.COUNTRY, exc.EXCHGID, exc.CDBEXCHGID, ref.MIC, ref.GLOBALZONEID, ref.TIMEZONE, ref.PLATFORM, ref.EXCHANGETYPE, tz.OFFSET from KGR..EXCHANGE exc, KGR..EXCHANGEREFCOMPL ref, KGR..TIMEZONE tz
           where 
           ref.EXCHANGE = exc.MARKET and
           ref.TIMEZONE = tz.TIMEZONE and
           ref.GLOBALZONEID = 1 and
           ref.EXCHANGETYPE = 'M' and
           GETDATE() < tz.ENDDATE and
           GETDATE() >= tz.BEGINDATE
           
           """
entries = Connections.exec_sql("KGR", query, as_dict = True)

trading_venue_id = 1

for e in entries:  
    documents.append({"trading_venue_id": trading_venue_id, 
                      "exchange_id": e['MARKET'], 
                      "exchange": e['EXCHGID'] , 
                      'name' : e["PLATFORM"], 
                      'type': "L", 
                      'is_primary': 1, 
                      'code': e["MIC"],                       
                      'destination':"TEST",                       
                      "feed": flex_code[int(e["MARKET"])] if flex_code.has_key(int(e["MARKET"])) else "",
                      "flex_code": flex_code[int(e["MARKET"])] if flex_code.has_key(int(e["MARKET"])) else "",
                      'ranking': 5, 
                      "internal" : 0, 
                      "timezone" : e["TIMEZONE"], 
                      "gmt_offset" : e["OFFSET"]
                      })
    trading_venue_id += 1
    
# then we add the MTFs
query = """select exc.MARKET, exc.COUNTRY, exc.EXCHGID, exc.CDBEXCHGID, ref.MIC, ref.GLOBALZONEID, ref.TIMEZONE, ref.PLATFORM, ref.EXCHANGETYPE, tz.OFFSET from KGR..EXCHANGE exc, KGR..EXCHANGEREFCOMPL ref, KGR..TIMEZONE tz
           where 
           ref.EXCHANGE = exc.MARKET and
           ref.TIMEZONE = tz.TIMEZONE and
           ref.GLOBALZONEID = 1 and
           ref.EXCHANGETYPE = 'F' and
           GETDATE() < tz.ENDDATE and
           GETDATE() >= tz.BEGINDATE           
           """
entries = Connections.exec_sql("KGR", query, as_dict = True)
for e in entries:  
    documents.append({"trading_venue_id": trading_venue_id, 
                      "exchange_id": e['MARKET'], 
                      "exchange": e['EXCHGID'] , 
                      'name' : e["PLATFORM"], 
                      'type': "L", 
                      'is_primary': 0, 
                      'code': e["MIC"],                      
                      "flex_code": flex_code[int(e["MARKET"])] if flex_code.has_key(int(e["MARKET"])) else "",
                      'destination':"TEST", 
                      'feed': flex_code[int(e["MARKET"])] if flex_code.has_key(int(e["MARKET"])) else "",                      
                      'ranking': 5, 
                      "internal" : 0, 
                      "timezone" : e["TIMEZONE"], 
                      "gmt_offset" : e["OFFSET"]
                      })
    trading_venue_id += 1
    
# next we add other dark pools
query = """select exc.MARKET, exc.COUNTRY, exc.EXCHGID, exc.CDBEXCHGID, ref.MIC, ref.GLOBALZONEID, ref.TIMEZONE, ref.PLATFORM, ref.EXCHANGETYPE, tz.OFFSET from KGR..EXCHANGE exc, KGR..EXCHANGEREFCOMPL ref, KGR..TIMEZONE tz
           where 
           ref.EXCHANGE = exc.MARKET and
           ref.TIMEZONE = tz.TIMEZONE and
           ref.GLOBALZONEID = 1 and
           ref.EXCHANGETYPE = 'D' and
           GETDATE() < tz.ENDDATE and
           GETDATE() >= tz.BEGINDATE           
           """
entries = Connections.exec_sql("KGR", query, as_dict = True)
for e in entries:  
    documents.append({"trading_venue_id": trading_venue_id, 
                      "exchange_id": e['MARKET'], 
                      "exchange": e['EXCHGID'] , 
                      'name' : e["PLATFORM"], 
                      'type': "M", 
                      'is_primary': 0, 
                      'code': e["MIC"],                      
                      'destination':"TEST", 
                      'feed': "",
                      "flex_code": e["MIC"],
                      'ranking': 5, 
                      "internal" : 0, 
                      "timezone" : e["TIMEZONE"], 
                      "gmt_offset" : e["OFFSET"]
                      })
    trading_venue_id += 1

# add blink
documents.append({"trading_venue_id": trading_venue_id, 
                      "exchange_id": 161, 
                      "exchange": "BLINK" , 
                      'name' : "BLINK-D", 
                      'type': "D", 
                      'is_primary': 0, 
                      'code': "BLNK-D", 
                      'destination':"TEST", 
                      "flex_code": "BLNK-D",
                      'feed': "", 
                      'ranking': 5, 
                      "internal" : 1, 
                      "timezone" : 'Europe/Paris', 
                      "gmt_offset" : 3600
                      })
documents.append({"trading_venue_id": trading_venue_id, 
                      "exchange_id": 161, 
                      "exchange": "BLINK" , 
                      'name' : "BLINK-M", 
                      'type': "M", 
                      'is_primary': 0, 
                      'code': "BLNK-M",
                      "flex_code": "BLNK-D",
                      'destination':"TEST", 
                      'feed': "", 
                      'ranking': 5, 
                      "internal" : 1, 
                      "timezone" : 'Europe/Paris', 
                      "gmt_offset" : 3600
                      })
                      
# add other mid points
query = """select exc.MARKET, exc.COUNTRY, exc.EXCHGID, exc.CDBEXCHGID, ref.MIC, ref.GLOBALZONEID, ref.TIMEZONE, ref.PLATFORM, ref.EXCHANGETYPE, tz.OFFSET from KGR..EXCHANGE exc, KGR..EXCHANGEREFCOMPL ref, KGR..TIMEZONE tz
           where 
           ref.EXCHANGE = exc.MARKET and
           ref.TIMEZONE = tz.TIMEZONE and
           ref.GLOBALZONEID = 1 and
           ref.EXCHANGETYPE = 'F' and
           GETDATE() < tz.ENDDATE and
           GETDATE() >= tz.BEGINDATE           
           """
entries = Connections.exec_sql("KGR", query, as_dict = True)
for e in entries:  
    documents.append({"trading_venue_id": trading_venue_id, 
                      "exchange_id": e['MARKET'], 
                      "exchange": e['EXCHGID'] , 
                      'name' : e["PLATFORM"], 
                      'type': "M", 
                      'is_primary': 0, 
                      'code': "%s-M" % e["MIC"], 
                      'flex_code': "%s-M" % e["MIC"],
                      'destination':"TEST", 
                      'feed': "", 
                      'ranking': 5, 
                      "internal" : 0, 
                      "timezone" : e["TIMEZONE"], 
                      "gmt_offset" : e["OFFSET"]
                      })
    trading_venue_id += 1


for doc in documents:
    collection.insert(doc)
