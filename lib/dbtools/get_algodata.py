# -*- coding: utf-8 -*-
"""
Created on Wed May 15 10:07:05 2013

@author: njoseph
"""

from pymongo import *
import pandas as pd
import datetime as dt
import time as time
import numpy as np
import lib.dbtools.read_dataset as read_dataset
import lib.dbtools.get_repository as get_repository
import lib.data.matlabutils as matlabutils
from lib.dbtools.connections import Connections

#--------------------------------------------------------------------------
# sequence_info
#--------------------------------------------------------------------------
def sequence_info(db_name = "Mars", **kwargs):
     
    #### DEFAULT OUTPUT    
    data=pd.DataFrame()
    
    #client = MongoClient(connect_info)
    client = Connections.getClient(db_name.upper())
    occ_db = client[db_name]["AlgoOrders"]
    
    #### Build the request
    t0=time.clock()
    
    # get all the sequences from sequence ids
    if "sequence_id" in kwargs.keys():
        ids=kwargs["sequence_id"]
        if isinstance(ids, basestring):
            ids=[ids] 
        req_=occ_db.find({"p_cl_ord_id": {"$in" : ids}})
    # get all the sequences from occurence ids
    elif "occurence_id" in kwargs.keys():  
        ids=kwargs["occurence_id"]
        if isinstance(ids,basestring):
            ids=[ids]
        req_=occ_db.find({"p_occ_id": {"$in" : ids}})
    # get all the sequences from date start end
    elif all(x in ["start_date","end_date"] for x in kwargs.keys()):
        sday=dt.datetime.strptime(kwargs["start_date"]+'-00:00:01', '%d/%m/%Y-%H:%M:%S')
        eday=dt.datetime.strptime(kwargs["end_date"]+'-23:59:59', '%d/%m/%Y-%H:%M:%S')
        req_=occ_db.find({"SendingTime": {"$gte":sday , "$lt":eday }})  
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
           
    #### CONNECTIONS
    client.close();
    
    if not documents:
        return data
    
    data=pd.DataFrame.from_records(documents, columns=columns,index='SendingTime')
    #data=pd.DataFrame.from_records(documents, columns=columns)
    #data=data.tz_localize('GMT')
    data=data.sort_index()  

    print 'sequence_info time : Mongo + insert into dataframe <%3.2f> secs ' %(time.clock()-t0)
    
    #### HANDLING COLNAMES
    needed_colnames=[ # - id/order infos
    u'_id',u'p_cl_ord_id',u'p_occ_id',
    # - user/client infos
    u'ClientID',u'TargetSubID',u'Account', u'MsgType',
    #- security symbol
    u'Symbol',u'cheuvreux_secid',u'ExDestination',u'Currency',u'rate_to_euro',
    #- info at occurence level
    u'occ_prev_exec_qty',u'occ_prev_turnover',   
    #- exec info
    u'exec_qty',u'turnover',u'volume_at_would',u'nb_replace',u'nb_exec',
    #- time infos
    u'eff_starttime',u'eff_endtime',u'duration',
    #- parameter info
    u'Side',u'OrderQty',u'StrategyName',u'strategy_name_mapped',
    u'StartTime',u'EndTime',u'ExcludeAuction',
    u'Price',u'WouldLevel',
    u'MinPctVolume',u'MaxPctVolume',u'AuctionPct',
    u'AggreggatedStyle',u'WouldDark', u'MinSize',u'MaxFloor',u'BenchPrice',u'ExecutionStyle',u'OBType', u'SweepLit', u'MinQty',
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
    
    #### Transform the data
    if (('nb_replace' in data.columns.tolist()) and (any(np.isnan(data['nb_replace'].values)))):
        tmp=data['nb_replace']
        tmp[np.nonzero(np.isnan(data['nb_replace'].values))[0]]=0
        data['nb_replace']=tmp
    if ('Side' in data.columns.tolist()):
        tmp=np.array([np.NaN]*data.shape[0])
        tmp[np.nonzero([x in ['1','3'] for x in data['Side']])[0]]=1
        tmp[np.nonzero([x in ['2','4'] for x in data['Side']])[0]]=-1
        if np.any(np.isnan(tmp)):
            raise NameError('get_algodata:sequence_info - Side : strange values')
        data['Side']=tmp
    
    return data

#--------------------------------------------------------------------------
# occurence_info
#--------------------------------------------------------------------------        
def occurrence_info(db_name = "Mars", **kwargs): 
    
    #### DEFAULT OUTPUT    
    data=pd.DataFrame()
    
    #client = MongoClient(connect_info)
    client = Connections.getClient(db_name.upper())
    occ_db = client[db_name]['AlgoOrders']
    
    #### Build the request
    # if list of sequence_id then        
    if "occurence_id" in kwargs.keys():  
        ids=kwargs["occurence_id"]
        if isinstance(ids,basestring):
            ids=[ids] 
        req_=occ_db.find({"MsgType":"D","p_occ_id": {"$in" : ids}})
    elif all(x in ["start_date","end_date"] for x in kwargs.keys()):
        sday=dt.datetime.strptime(kwargs["start_date"]+'-00:00:01', '%d/%m/%Y-%H:%M:%S')
        eday=dt.datetime.strptime(kwargs["end_date"]+'-23:59:59', '%d/%m/%Y-%H:%M:%S')
        #req_=occ_db.find({"SendingTime": {"$gte":sday , "$lt":eday }})  
        req_=occ_db.find({"MsgType":"D","SendingTime": {"$gte":sday , "$lt":eday}})  
    else:
        raise NameError('get_algodata:occurence_info - Bad input data')
    
    #### CONNECTIONS
    client.close();
    
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
    
    #### HANDLING COLNAMES
    fields_algoorders=fieldList(cname='AlgoOrders')
    
    needed_colnames=[ # - id/order infos
    u'_id',u'p_occ_id',
    # - user/client infos
    u'ClientID',u'TargetSubID',u'Account',
    #- security symbol
    u'Symbol',u'cheuvreux_secid',u'ExDestination',u'Currency',u'rate_to_euro',
    #- info at occurence level
    u'Side',u'occ_nb_replace',u'occ_duration']
    # add occ_fe_    
    add_occ_fe=fields_algoorders[np.nonzero([x[:min(7,len(x))]=='occ_fe_' for x in fields_algoorders])[0]]
    needed_colnames=needed_colnames+add_occ_fe.tolist()
    
    # - drop colnames
    for x in data.columns.tolist():
        if x not in needed_colnames:
            data=data.drop([x],axis=1)
    # - add colnames
    for x in needed_colnames:
        if x not in data.columns.tolist():
            data[x]=np.NaN 
            
    #### Transform the data        
    if ('Side' in data.columns.tolist()):
        tmp=np.array([np.NaN]*data.shape[0])
        tmp[np.nonzero([x in ['1','3'] for x in data['Side']])[0]]=1
        tmp[np.nonzero([x in ['2','4'] for x in data['Side']])[0]]=-1
        if np.any(np.isnan(tmp)):
            raise NameError('get_algodata:occurence_info - Side : strange values')
        data['Side']=tmp   

    return data
      
    
#--------------------------------------------------------------------------
# deal
#--------------------------------------------------------------------------        
def deal(db_name="Mars", sequence_id=None, start_date=None, end_date=None, merge_order_colnames=None): 
    
    #### DEFAULT OUTPUT    
    data=pd.DataFrame()
    
    #### CONNECTIONS and DB
    client = Connections.getClient(db_name.upper())
    deal_db = client[db_name]["OrderDeals"]            
    
    #################################################    
    #### Input parsing
    #################################################
    # if list of sequence_id then        
    if sequence_id is not None:  
        ids=sequence_id
        if isinstance(ids,basestring):
            ids=[ids] 
        req_=deal_db.find({"p_cl_ord_id": {"$in" : ids}}).sort([("TransactTime",ASCENDING), ("p_exec_id",ASCENDING)])
    elif (start_date is not None) and (end_date is not None):
        sday=dt.datetime.strptime(start_date+'-00:00:01', '%d/%m/%Y-%H:%M:%S')
        eday=dt.datetime.strptime(end_date+'-23:59:59', '%d/%m/%Y-%H:%M:%S')
        # req_=deal_db.find({"TransactTime": {"$gte":sday , "$lt":eday }}).sort([("TransactTime",ASCENDING), ("ExecID",ASCENDING)]) 
        req_=deal_db.find({"TransactTime": {"$gte":sday , "$lt":eday }}).sort([("TransactTime",ASCENDING), ("p_exec_id",ASCENDING)])    
    else:
        raise NameError('get_algodata:deal - Bad input data')  
    
    #### CONNECTIONS
    client.close();
    
    ################################################    
    #### Request/Extract 
    ################################################
    documents=[]
    columns=[]
    for v in req_:
        documents.append(v)
        columns.extend(v.keys())
        columns=list(set(columns))
        
    if not documents:
        return data
    data=pd.DataFrame.from_records(documents, columns=columns,index='TransactTime')
    
    ################################################    
    #### HANDLING COLNAMES
    ################################################    
    
    # TODO: rajouter les infos de rate_to_euro etc une fosi integrer
    needed_colnames=[ # - id/order infos
    "p_exec_id","p_cl_ord_id",
     # - deal infos
    "Side","Symbol","LastPx","LastShares","LastMkt","ExecType","Currency",
    "rate_to_euro","cheuvreux_secid","strategy_name_mapped"]
    # - drop colnames
    for x in data.columns.tolist():
        if x not in needed_colnames:
            data=data.drop([x],axis=1)
    # - add colnames
    for x in needed_colnames:
        if x not in data.columns.tolist():
            data[x]=np.NaN 
    # - rename 
    data = data.rename(columns={'LastPx': 'price','LastShares': 'volume', 'LastMkt' :'MIC'})
    
    #### Side
    if ('Side' in data.columns.tolist()):
        tmp=np.array([np.NaN]*data.shape[0])
        tmp[np.nonzero([int(x) in [1,3] for x in data['Side']])[0]]=1
        tmp[np.nonzero([int(x) in [2,4] for x in data['Side'].values])[0]]=-1
        if np.any(np.isnan(tmp)):
            raise NameError('get_algodata:deal - Side : strange values')
        data['Side']=tmp
    
    ####  exchange_id
    if not ('exchange_id' in data.columns.tolist()):
        data['exchange_id']=get_repository.mic2exchangeid(mic=data['MIC'].values)
        
    ################################################    
    #### HANDLING COLNAMES
    ################################################    
    if (merge_order_colnames is not None):
        data_seq=sequence_info(sequence_id=matlabutils.uniqueext(data['p_cl_ord_id'].values).tolist())
        if not all([x in data_seq.columns.tolist() for x in merge_order_colnames]):
            raise NameError('get_algodata:deal - bad merge_order_colnames')    
        # initialize columns
        for x in merge_order_colnames:
            data[x]=None 
        # add
        if data_seq.shape[0]>0:
            for idx in range(0,data_seq.shape[0]):
                idx_in=np.nonzero(data['p_cl_ord_id'].values==data_seq.ix[idx]['p_cl_ord_id'])[0]        
                for x in merge_order_colnames:
                    data[x][idx_in]=data_seq.ix[idx][x]
                    
    return data

#--------------------------------------------------------------------------
# fieldList
#--------------------------------------------------------------------------        
def fieldList(cname=None, db_name="Mars", **kwargs):
    
    #### CONNECTIONS and DB
    map_name="field_map"
    #client = MongoClient(connect_info)
    client = Connections.getClient(db_name.upper())
    req_=client[db_name][map_name].find({"collection_name":cname},{"list_columns":1,"_id":0}) 
    
    #### Create the data
    out=[]
    for v in req_:
        out.append(v)
    
    return np.array(out[0]['list_columns'])
