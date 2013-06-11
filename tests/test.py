import pyodbc
import pymongo as mongo
from lib.dbtools import connections
import datetime
import paramiko
from projects.DMAlgo.src import import_FIX as fix
from lib.data.pyData import convertStr
import pytz

#
# from lib.dbtools.connections import Connections
# db = Connections.getClient("HPP").DB_test
# from datetime import *
# #result = db.AlgoOrders.aggregate([{"$project": {"ClOrdID": 1, "_id": 0}}])
# result = db.AlgoOrders.aggregate([{"$match": {"SendingTime" : {"$gt" : datetime(2013, 05, 22), "$lte": datetime(2013, 05, 23)}, "OrderQty" : {"$gt": "9"}}}, {"$project": {"ClOrdID": 1, "_id": 0}}])
# #print [k["ClOrdID"] for k in result["result"]]
# deals = db.OrderDeals.aggregate([{"$match" : {"ClOrdID" : {"$in": [k["ClOrdID"] for k in result["result"]]} }}, {"$project": {"Symbol": 1, "OrderQty": 1, "_id": 0}}])
# print deals["result"]


# cnxn = pyodbc.connect('DSN=PARSRV21;UID=python_ro;PWD=python4ever!!')
# cursor = cnxn.cursor()
#  
#  
# cursor.execute("select top 10 * from SECURITY")
# row = cursor.fetchall()
# for r in row:
#     print r
#      
# cnxn.close()

# - Open MONGODB connection
# Client = mongo.MongoClient('172.29.0.32', 27017)
Client = mongo.MongoClient("mongodb://python_script:pythonpass@172.29.0.32:27017/DB_test",tz_aware=True)
#"mongodb://python_script:pythonpass@172.29.0.32:27017/DB_test"


db = Client['DB_test']
collec = db['AlgoOrders']

job_id = 'AO20130510'

time_fields = ['TransactTime','SendingTime','eff_starttime', 'eff_endtime', 'EndTime', 'StartTime', 'ExpireTime']
docs = collec.find({'job_id':job_id})

for doc in docs:
    print doc['_id']
    for u, v in doc.iteritems():
        if u in time_fields:
            print v
            
            if v.tzinfo == pytz.utc:
                print "conversion passed !"
            else:
                doc[u] = v.replace(tzinfo=pytz.timezone('UTC'))
                
    collec.save(doc)
            
            
# print db['AlgoOrders'].find({'job_id':'AO20130521'}).count()
# db['AlgoOrders'].remove({'job_id':'AO20130521'})

# collection = db['map_tagFIX']

# temp_dico_trader = collection.find({'tagFIX':'9249'})
# 
# dico_trader = []
# for trd in temp_dico_trader:
#     dico_trader.append(trd)
# 
# 
# def match_trader(ip, trader_name, dico_trader):
#     r_trader_name = ""
#     for docs in dico_trader:
#         if trader_name == docs['tag_value'] and ip == docs['ip_server']:
#             r_trader_name = docs['trader_name']
#             return r_trader_name
#     return trader_name
# 
# collec = db['AlgoOrders']
# orders = collec.find()
# 
# for order in orders:
#     order['TraderName'] = match_trader('172.25.152.73',order['TraderName'], dico_trader)
#     collec.save(order, manipulate = False, safe=True)


# dico_side = [{'tagFIX':'54', 'tag_name':'Side', 'tag_value':'1', 'label':'Buy'},
#              {'tagFIX':'54', 'tag_name':'Side', 'tag_value':'2', 'label':'Sell'},
#              {'tagFIX':'54', 'tag_name':'Side', 'tag_value':'3', 'label':'Buy minus'},
#              {'tagFIX':'54', 'tag_name':'Side', 'tag_value':'4', 'label':'Sell plus'},
#              {'tagFIX':'54', 'tag_name':'Side', 'tag_value':'5', 'label':'Sell short'},
#              {'tagFIX':'54', 'tag_name':'Side', 'tag_value':'6', 'label':'sell short exempt'},
#              {'tagFIX':'54', 'tag_name':'Side', 'tag_value':'7', 'label':'Undisclosed'},
#              {'tagFIX':'54', 'tag_name':'Side', 'tag_value':'8', 'label':'Cross'},
#              {'tagFIX':'54', 'tag_name':'Side', 'tag_value':'9', 'label':'Cross short'},
#              {'tagFIX':'54', 'tag_name':'Side', 'tag_value':'A', 'label':'Cross short exempt'},
#              {'tagFIX':'54', 'tag_name':'Side', 'tag_value':'B', 'label':'As defined'},
#              {'tagFIX':'54', 'tag_name':'Side', 'tag_value':'C', 'label':'Opposite'},
#              {'tagFIX':'54', 'tag_name':'Side', 'tag_value':'D', 'label':'Subscribe'},
#              {'tagFIX':'54', 'tag_name':'Side', 'tag_value':'E', 'label':'Redeem'},
#              {'tagFIX':'54', 'tag_name':'Side', 'tag_value':'F', 'label':'Lend'},
#              {'tagFIX':'54', 'tag_name':'Side', 'tag_value':'G', 'label':'Borrow'}]
# 
# collection.insert(dico_side, manipulate=False)
# collection.options()

## - Test ref SQL Kepler
# l_kep_sec_ids = ['B1YXBJ7','4821100', 'B9G6L89']
#  
# query = "select SYMBOL6 from SECURITY where SYMBOL3 in ('%s')" % "','".join(map(str,l_kep_sec_ids))
# print query
# result = connections.Connections.exec_sql('KGR', query, as_dataframe = True)
# 
# print result.SYMBOL6[1]

# day = '20130523'
# collec = db['AlgoOrders']
# s_date = datetime.datetime(2013,05,23)
# e_date = datetime.datetime(2013,05,23)
# docs = collec.find({"SendingTime": {"$gt" : s_date}})
# print docs.count()
# collec.remove({"SendingTime": {"$gt" : s_date}})

# collec = db['AlgoOrders']
# collec.remove()

# collec.remove({"SendingTime": {"$gte" : s_date}})

# universe_file = '../projects/DMAlgo/cfg/KC_universe.xml'
# dico_FIX = '../cfg/FIX42.xml'
# 
# conf = fix.get_conf('preprod', universe_file)
# server = conf['WATFLT01']
# day = '20130521'
# 
# ssh = paramiko.SSHClient()
# ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
# ssh.connect(server['ip_addr'], username=server['list_users']['flexapp']['username'], password=server['list_users']['flexapp']['passwd'])
# cmd = 'grep SYM /home/flexapp/ushare/exportprodAV1%s' %day
# (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
# 
# for line in stdout_grep:
#     print line