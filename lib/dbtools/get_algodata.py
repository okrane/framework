# -*- coding: utf-8 -*-
"""
Created on Wed May 15 10:07:05 2013

@author: njoseph
"""

from pymongo import *
import pandas as pd
import datetime as dt

def get_algodata(mode, **kwargs):
    
    #### CONFIG and CONNECT
    server="172.29.0.32"
    port=27017
    #### DEFAULT OUTPUT    
    data=[]
    
    if (mode=="sequence_info"):
        
        #### CONNECTIONS and DB
        db_name="DB_test"
        order_cname="ClientOrders"
        client = MongoClient(server,port)
        occ_db = client[db_name][order_cname]
        
        #### Build the request
        # if list of sequence_id then        
        if "sequence_id" in kwargs.keys():  
            seqids=kwargs["sequence_id"]
            if isinstance(seqids,str):
                seqids=[seqids] 
            req_=occ_db.find({"ClOrdID": {"$in" : seqids}})
        elif all(x in ["start_date","end_date"] for x in kwargs.keys()):
            sday=dt.datetime.strptime(kwargs["start_date"], '%d/%m/%Y')
            eday=dt.datetime.strptime(kwargs["end_date"], '%d/%m/%Y')+dt.timedelta(days=1)
            req_=occ_db.find({"SendingTime": {"$gt":sday },"SendingTime": {"$lt":eday }})  
        else:
            a=1
            
        #### Create the data
        documents=[]
        columns=[]
        for v in req_:
                documents.append(v)
                columns.extend(v.keys())
                columns=list(set(columns))
                
        data=pd.DataFrame.from_records(documents, columns=columns)
        
        #### CONNECTIONS
        client.close();
        
    elif (mode=="occurence_info"): 
        
        
    else:
        a=1
    
    return data



""" 
-------------------------------------------------------------------------------
MAIN
-------------------------------------------------------------------------------
""" 

if __name__=='__main__':
    
    test2print=True
    if test2print:
        # mode "sequenceinfo"
        data=get_algodata("sequence_info",sequence_id="FY2000007382301")
        data=get_algodata("sequence_info",sequence_id=["FY2000007382301","FY2000007414521"])
        #data=get_algodata("sequence_info",start_date="14/05/2013",end_date="14/05/2013")
        
    
    
    
    