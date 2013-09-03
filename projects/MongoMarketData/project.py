# -*- coding: utf-8 -*-

from lib.dbtools.read_dataset import *
from lib.dbtools.connections import Connections
from datetime import datetime
import pandas as pd

def upload(security_id, date):
    data = ft(security_id = security_id, date = date)
    client = Connections.getClient('PARFLTLAB02')
    collection = client['MarketData']['Tick']
    collection.remove()    
    for i in range(len(data.index)):
        document = {}
        document['date'] = data.index[i].to_datetime()
        document.update(data.ix[i])
        document['security_id'] = security_id
        
        collection.insert(document)
    
    collection.ensure_index("security_id")
    
    
def download(security_id, date):
    client = Connections.getClient('PARFLTLAB02')
    collection = client['MarketData']['Tick']    
    #d = datetime.strptime(date, '%d/%m/%Y')

    start = datetime.now()    
    #result = collection.aggregate([{'$match': {'security_id': 110}}])     
    result = collection.find({'security_id': 110})     
    print datetime.now() - start    
    
    frame = {}
    for r in result:
        for k, v in r.iteritems():
            if k not in frame.keys(): frame[k] = []
            frame[k].append(v)
    df = pd.DataFrame(frame, index = frame['date'])
    
    return df
    
if __name__ ==  '__main__':
    #upload(110, '20/03/2013')
    
    date = '20/03/2013'
    start = datetime.now()
    data_file = ft(security_id = 110, date = date)
    duration_file = datetime.now() - start
    print data_file    
    
    start = datetime.now()    
    data_mongo = download(110, '20/03/2013')
    duration_mongo = datetime.now() - start
    print data_mongo
    
    print "Duration File: ", duration_file
    print "Duration Mongo: ", duration_mongo
    