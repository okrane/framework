# -*- coding: utf-8 -*-
"""
Created on Fri Apr 05 08:49:12 2013

@author: nijos
"""

""" 
-------------------------------------------------------------------------------
IMPORT
-------------------------------------------------------------------------------
""" 
from pymongo import MongoClient
import numpy as np
import pandas as pd

""" 
-------------------------------------------------------------------------------
FUNCS
-------------------------------------------------------------------------------
""" 
def add_dfts_to_collection(df,db_name,collection_name,server="localhost",port=27017):
    client = MongoClient(server,port)
    # creat databse
    db = client[db_name]
    # create collection
    collection = db[collection_name]
    # drop datetime if in the dataset
    if 'datetime' in df.columns.tolist():
        df=df.drop(['datetime'],axis=1)
    
    for i in range(0,len(df)-1):
        doc=df.ix[i].to_dict()
        doc.update({'datetime':df.index[i].to_datetime()})
        collection.insert(doc)
        
    client.close();


""" 
-------------------------------------------------------------------------------
EX (2)
-------------------------------------------------------------------------------
""" 
# CREATE DFTS
timedata = pd.date_range('17/3/2013 10:00', '17/3/2013 11:00', freq='60s')
tmp=np.random.rand(len(timedata))
data=pd.DataFrame({'bid' : 10+tmp, 'ask' : 10+tmp+np.abs(tmp)},index=timedata)

# SAVE in a collection
db_name='testdb1'
collection_name='testcollect1'
server="localhost"
port=27017
add_dfts_to_collection(data,db_name,collection_name,server=server,port=port)

# Extract from a collection
extract_colnames=['bid','ask']
client = MongoClient(server,port)

# find all sauf _id !
documents = []
for v in client[db_name][collection_name].find({},{'datetime':1,'ask':1,'bid':1,'_id':0}):
    documents.append(v)
    
data2 = pd.DataFrame.from_records(documents, columns=['datetime', 'ask', 'bid'], index='datetime')

client.close()
   
""" 
-------------------------------------------------------------------------------
EX (1)
-------------------------------------------------------------------------------
""" 
client = MongoClient("localhost", 27017)

# creat databse
db = client['test-database']
# create collection
collection = db['test-collection']
# insert documents in the collection
documents = []
documents.append({'strategy': "CROSS", 'key' : {}, "static_parameters": {'MinATS': 2, 'MaxATS': 5, 'dt': 100, 'blabla': 5 }})
documents.append({'strategy': "CROSS", 'key' : {'client_id': 11}, "static_parameters": {'MinATS': 0, 'blabla': 11 }})
documents.append({'strategy': "CROSS", 'key' : {'trader_id': 1, 'place_id': "rome"}, "static_parameters": {'MinATS': 1, 'MaxATS': 1, 'dt': 1, 'blabla': 1 }})
documents.append({'strategy': "CROSS", 'key' : {'place_id': "paris", 'parent_algo_id': "VWAP"}, "static_parameters": {'blabla': 46 }})
documents.append({'strategy': "VWAP", 'key' : {'place_id': "paris", 'parent_algo_id': "VWAP"}, "static_parameters": {'blabla': 1111 }})
documents.append({'strategy': "VWAP", 'key' : {}, "static_parameters": {'blabla': 0 }})
documents.append({'strategy': "VWAP", "static_parameters": {'blabla': 0 }})

for doc in documents:
    collection.insert(doc)
    
for i in client[db_name][collection_name].find({},{'ask':1,'bid':1,'_id':0}):
    print i
    print " "
 # delete the collection
# collection.remove()
   
# request the collection
for i in collection.find({ "strategy": "VWAP" },{"strategy": 1,"key": 1, "_id": 0}):    
    print i

# drop the collection 
db.drop_collection('test-collection')
    
# drop the databse 
client.drop_database('test-database')