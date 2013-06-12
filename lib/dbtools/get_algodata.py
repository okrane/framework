# -*- coding: utf-8 -*-
"""
Created on Wed May 15 10:07:05 2013

@author: njoseph
"""

from pymongo import *
import pandas as pd
import datetime as dt
import numpy as np
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
        client.close();
        raise NameError('get_algodata:sequence_info - Bad input data')
    
    #### Create the data
    documents=[]
    columns=[]
    for v in req_:
            documents.append(v)
            columns.extend(v.keys())
            columns=list(set(columns))
            
    if not documents:
        client.close();
        return data
    data=pd.DataFrame.from_records(documents, columns=columns,index='SendingTime')
    #data=pd.DataFrame.from_records(documents, columns=columns)
    data=data.tz_localize('GMT')
    data=data.sort_index()
    
    #### Transform the data
    if (('nb_replace' in data.columns.tolist()) and (any(np.isnan(data['nb_replace'].values)))):
        tmp=data['nb_replace']
        tmp[np.nonzero(np.isnan(data['nb_replace'].values))[0]]=0
        data['nb_replace']=tmp
    if ('Side' in data.columns.tolist()):
        tmp=np.array([np.NaN]*data.shape[0])
        tmp[np.nonzero([x in [1,3] for x in data['Side']])[0]]=1
        tmp[np.nonzero([x in [2,4] for x in data['Side']])[0]]=-1
        if np.any(np.isnan(tmp)):
            raise NameError('get_algodata:sequence_info - Side : strange values')
        data['Side']=tmp
        
    #### CONNECTIONS
    client.close();
    
    #### HANDLING COLNAMES
    needed_colnames=[ # - id/order infos
    u'_id',u'occ_ID', u'ClOrdID',u'OrigClOrdID',
    # - user/client infos
    u'ClientID',u'TargetSubID',u'Account', u'MsgType',
    #- security symbol
    u'Symbol',u'cheuvreux_secid',u'TickerHisto',u'SecurityID',u'ExDestination',u'SecurityType',u'Currency',
    #- info at occurence level
    u'occ_prev_exec_qty',u'occ_prev_turnover',   
    #- exec info
    u'exec_qty',u'turnover',u'volume_at_would',u'nb_replace',u'nb_exec',
    #- time infos
    u'eff_starttime',u'eff_endtime',u'duration',
    #- parameter info
    u'Side',u'OrderQty',u'StrategyName',
    u'StartTime',u'EndTime',u'ExcludeAuction',
    u'Price',u'WouldLevel',
    u'MinPctVolume',u'MaxPctVolume',u'AuctionPct',
    u'AggreggatedStyle',u'WouldDark', u'MinSize',u'MaxFloor',u'BenchPrice',u'ExecutionStyle',  u'OBType', u'SweepLit', u'MinQty',
    # - others
    u'OrdStatus',u'reason']
    
    # - drop colnames
    for x in data.columns.tolist():
        if x not in needed_colnames:
            data=data.drop([x],axis=1)
    # - add colnames
    for x in needed_colnames:
        if x not in data.columns.tolist():
            data[x]=np.NaN 
    
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
    
    #### HANDLING COLNAMES
    needed_colnames=[ # - id/order infos
    u'_id',u'occ_ID', u'ClOrdID',u'OrigClOrdID',
    # - user/client infos
    u'ClientID',u'TargetSubID',u'Account', u'MsgType',
    #- security symbol
    u'Symbol',u'cheuvreux_secid',u'TickerHisto',u'SecurityID',u'ExDestination',u'SecurityType',u'Currency',
    #- info at occurence level
    u'occ_nb_replace']
    
    # - drop colnames
    for x in data.columns.tolist():
        if x not in needed_colnames:
            data=data.drop([x],axis=1)
    # - add colnames
    for x in needed_colnames:
        if x not in data.columns.tolist():
            data[x]=np.NaN 
    
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


#--------------------------------------------------------------------------
# fieldList
#--------------------------------------------------------------------------        
def fieldList(cname=None):
    
    #### CONFIG and CONNECT
    connect_info="mongodb://python_script:pythonpass@172.29.0.32:27017/DB_test"
    
    #### CONNECTIONS and DB
    db_name="DB_test"
    map_name="field_map"
    client = MongoClient(connect_info)
    req_=client[db_name][map_name].find({"collection_name":cname},{"list_columns":1,"_id":0}) 
    
    #### Create the data
    out=[]
    for v in req_:
        out.append(v)
    
    return out[0]['list_columns']




if __name__ == "__main__":
    from lib.data.ui.Explorer import Explorer
    #------------ occurence ID
    #-- algodata
    occ_id='FY2000007382301'
    data_occ=sequence_info(occurence_id=occ_id)
    Explorer(data_occ)
    
    