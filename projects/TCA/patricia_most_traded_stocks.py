# -*- coding: utf-8 -*-
"""
Created on Tue Nov 12 09:56:59 2013

@author: svlasceanu
"""

from bson.code import Code
from lib.dbtools.connections import Connections
import operator
from lib.dbtools.get_repository import convert_symbol
import matplotlib.pyplot as plt
#

def top_traded():
    map_func = Code ("""function() {
                    if (this.occ_nb_replace == 0){
                        emit(this.cheuvreux_secid, this.turnover * this.rate_to_euro)
                        }
                    }""")
                    
    reduce_func = Code (""" function(sec_id, quantity) {
                        return Array.sum(quantity)
                        }
                        """)
    Connections.change_connections("production")
    db = Connections.getClient("MARS")["Mars"]["AlgoOrders"]
    result = db.map_reduce(map_func, reduce_func, "my_result")
    
    turnover_executed = {}
    for doc in result.find():    
        if doc[u"_id"] is not None and doc[u"value"] > 0: 
            turnover_executed[int(doc[u"_id"])] = int(doc[u"value"])
    
    sorted_x = sorted(turnover_executed.iteritems(), key= lambda x: x[1], reverse = True)
    
    sec_ids = []
    keys = []
    values = []
    for i in range(20):
        sec_ids.append(sorted_x[i][0])
        keys.append(convert_symbol(source = "security_id", dest = "security_name", value = sorted_x[i][0])[0][0])
        values.append(sorted_x[i][1])
        print convert_symbol(source = "security_id", dest = "security_name", value = sorted_x[i][0])
    for i in range(20):
        print sorted_x[i][1]    
    figure, ax = plt.subplots(1, 1)
    ax.bar(range(20), values)
    plt.show()

def top_crossed():
    map_func = Code ("""function() {
                    if (this.LastMkt == "BLNK"){
                        emit(this.cheuvreux_secid, this.LastPx * this.LastShares * this.rate_to_euro)
                        }
                    }""")
                    
    reduce_func = Code (""" function(sec_id, quantity) {
                        return Array.sum(quantity)
                        }
                        """)
    Connections.change_connections("production")
    db = Connections.getClient("MARS")["Mars"]["OrderDeals"]
    result = db.map_reduce(map_func, reduce_func, "my_result")
    
    turnover_executed = {}
    for doc in result.find():    
        print doc
        if doc[u"_id"] is not None and doc[u"value"] > 0: 
            turnover_executed[int(doc[u"_id"])] = int(doc[u"value"])
    
    sorted_x = sorted(turnover_executed.iteritems(), key= lambda x: x[1], reverse = True)
    
    sec_ids = []
    keys = []
    values = []
    for i in range(20):
        sec_ids.append(sorted_x[i][0])
        keys.append(convert_symbol(source = "security_id", dest = "security_name", value = sorted_x[i][0])[0][0])
        values.append(sorted_x[i][1])
        print convert_symbol(source = "security_id", dest = "security_name", value = sorted_x[i][0])
    for i in range(20):
        print sorted_x[i][1]    
    figure, ax = plt.subplots(1, 1)
    ax.bar(range(20), values)
    plt.show()
    
top_crossed()