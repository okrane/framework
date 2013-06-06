# -*- coding: utf-8 -*-
"""
Created on Wed May 15 10:07:05 2013

@author: njoseph
"""

from pymongo import *
import pandas as pd
import datetime as dt
#from lib.dbtools.connections import Connections

#--------------------------------------------------------------------------
# sequence_info
#--------------------------------------------------------------------------
def sequence_info(**kwargs):
     
    #### CONFIG and CONNECT
    connect_info="mongodb://python_script:pythonpass@172.29.0.32:27017/DB_test"
    
    #### DEFAULT OUTPUT    
    data=[]
    
    #### CONNECTIONS and DB 
    db_name="DB_test"
    order_cname="AlgoOrders"
    client = MongoClient(connect_info)
    occ_db = client[db_name][order_cname]
    
    #### Build the request
    # get all the sequences from sequence ids
    if "sequence_id" in kwargs.keys():
        ids=kwargs["sequence_id"]
        if isinstance(ids,str):
            ids=[ids] 
        req_=occ_db.find({"ClOrdID": {"$in" : ids}})
    # get all the sequences from occurence ids
    elif "occurence_id" in kwargs.keys():  
        ids=kwargs["occurence_id"]
        if isinstance(ids,str):
            ids=[ids]
        req_=occ_db.find({"occ_ID": {"$in" : ids}})
    # get all the sequences from date start end
    elif all(x in ["start_date","end_date"] for x in kwargs.keys()):
        sday=dt.datetime.strptime(kwargs["start_date"], '%d/%m/%Y')
        eday=dt.datetime.strptime(kwargs["end_date"], '%d/%m/%Y')+dt.timedelta(days=1)
        req_=occ_db.find({"SendingTime": {"$gte":sday },"SendingTime": {"$lt":eday }})  
    else:
        raise NameError('get_algodata:sequence_info - Bad input data')
    
    #### Create the data
    documents=[]
    columns=[]
    for v in req_:
            documents.append(v)
            columns.extend(v.keys())
            columns=list(set(columns))
            
    if not documents:
        return data
    data=pd.DataFrame.from_records(documents, columns=columns,index='SendingTime')
    data=data.tz_localize('GMT')
    
    #### CONNECTIONS
    client.close();
    
    return data
#from lib.dbtools.connections import Connections
#db = Connections.getClient("HPP").DB_test
#from datetime import *
##result = db.AlgoOrders.aggregate([{"$project": {"ClOrdID": 1, "_id": 0}}])
#result = db.AlgoOrders.aggregate([{"$match": {"SendingTime" : {"$gt" : datetime(2013, 05, 22), "$lte": datetime(2013, 05, 23)}, "OrderQty" : {"$gt": "9"}}}, {"$project": {"ClOrdID": 1, "_id": 0}}])
##print [k["ClOrdID"] for k in result["result"]]
#deals = db.OrderDeals.aggregate([{"$match" : {"ClOrdID" : {"$in": [k["ClOrdID"] for k in result["result"]]} }}, {"$project": {"Symbol": 1, "OrderQty": 1, "_id": 0}}])
#print deals["result"]
#--------------------------------------------------------------------------
# occurence_info
#--------------------------------------------------------------------------        
def occurence_info(**kwargs): 
    
    #### CONFIG and CONNECT
    connect_info="mongodb://python_script:pythonpass@172.29.0.32:27017/DB_test"
    
    #### DEFAULT OUTPUT    
    data=[]
    
    #### CONNECTIONS and DB
    db_name="DB_test"
    order_cname="AlgoOrders"
    client = MongoClient(connect_info)
    occ_db = client[db_name][order_cname]
    
    #### Build the request
    # if list of sequence_id then        
    if "occurence_id" in kwargs.keys():  
        ids=kwargs["occurence_id"]
        if isinstance(ids,str):
            ids=[ids] 
        req_=occ_db.find({"MsgType":"D","occ_ID": {"$in" : ids}})
    elif all(x in ["start_date","end_date"] for x in kwargs.keys()):
        sday=dt.datetime.strptime(kwargs["start_date"], '%d/%m/%Y')
        eday=dt.datetime.strptime(kwargs["end_date"], '%d/%m/%Y')+dt.timedelta(days=1)
        req_=occ_db.find({"MsgType":"D","SendingTime": {"$gte":sday},"SendingTime": {"$lt":eday}})  
    else:
        raise NameError('get_algodata:occurence_info - Bad input data')
    
    #### Create the data
    documents=[]
    columns=[]
    for v in req_:
            documents.append(v)
            columns.extend(v.keys())
            columns=list(set(columns))
    if not documents:
        return data
    data=pd.DataFrame.from_records(documents, columns=columns,index='SendingTime')
    data=data.tz_localize('GMT')
    
    #### CONNECTIONS
    client.close();
    
    return data       
    
#--------------------------------------------------------------------------
# deal
#--------------------------------------------------------------------------        
def deal(**kwargs): 
    
    #### CONFIG and CONNECT
    connect_info="mongodb://python_script:pythonpass@172.29.0.32:27017/DB_test"
    
    #### DEFAULT OUTPUT    
    data=[]
    
    #### CONNECTIONS and DB
    db_name="DB_test"
    deal_cname="OrderDeals"
    client = MongoClient(connect_info)
    deal_db = client[db_name][deal_cname]            
        
    #### Build the request
    # if list of sequence_id then        
    if "sequence_id" in kwargs.keys():  
        ids=kwargs["sequence_id"]
        if isinstance(ids,str):
            ids=[ids] 
        req_=deal_db.find({"ClOrdID": {"$in" : ids}})
    else:
        raise NameError('get_algodata:deal - Bad input data')            
                    
    #### Create the data
    documents=[]
    columns=[]
    for v in req_:
            documents.append(v)
            columns.extend(v.keys())
            columns=list(set(columns))
            
    if not documents:
        return data
    data=pd.DataFrame.from_records(documents, columns=columns,index='TransactTime')
    data=data.tz_localize('GMT')
    
    #### CONNECTIONS
    client.close();
    return data

