# -*- coding: utf-8 -*-
"""
Created on Mon Apr 22 17:11:09 2013

@author: svlasceanu
"""
from pymongo import MongoClient

def export_trading_venue_info(server, port, target_file):
    client = MongoClient(server, port);
    collection = client.Mercure.trading_venue_info
    
    f = open(target_file, "w")
    
    first_parameter = collection.find_one()
    line = ';'.join(first_parameter.keys())
    f.write(line + '\n')    
    print line
    
    for elem in collection.find():
        line = ';'.join([str(k) for k in elem.values()])       
        f.write(line + '\n')
        print line
    f.close()
    
    
print "Starting Run"
export_trading_venue_info('PARFLTLAB02', 27017, 'trading_venue_info.kcdb')
print "Ended Run"