# -*- coding: utf-8 -*-

from lib.data.pyData import convertStr
from pymongo import MongoClient

def upload(filename):
    collection = MongoClient("PARFLTLAB02", 27017).Mercure.crossfire_indicators
    collection.remove()
    documents = []    
    
    f = open(filename, 'r')
    for line in f.readlines():
        pairs = line.split("|")
        d = dict()
        for p in pairs:
            key = p.split("=")[0]
            value = convertStr(p.split("=")[1])
            d[key] = value
        documents.append(d)
    collection.insert(documents)
    
if __name__ ==  "__main__":
    import sys       
    if len(sys.argv) > 1:
        upload(sys.argv[1])
    else:
        print "Usage: upload_mongo_for_crossfire.py source_filename"


