import pyodbc
import pymongo as mongo
from lib.dbtools import connections
import datetime
import time
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
# Client1 = mongo.MongoClient("mongodb://python_script:pythonpass@172.29.1.32:27017/DB_test",tz_aware=True)
# Client1 = mongo.MongoClient("172.29.100.27", 27017)
#"mongodb://python_script:pythonpass@172.29.0.32:27017/DB_test"


# db = Client1['Mars']


# job_ids = ['AO20130521','AO20130522','AO20130523','AO20130524','AO20130527','AO20130528','AO20130529','AO20130530', 'AO20130531','AO20130603']
# 
# time_fields = ['TransactTime','SendingTime','eff_starttime', 'eff_endtime', 'EndTime', 'StartTime', 'ExpireTime']
# mkt_data_fields = ['EXEC_SHARES' ,'ORDER_PERC','INMKT_VOLUME','INMKT_TURNOVER','PRV_VOLUME','PRV_TURNOVER','AVG_SPRD','IN_VWAS','ARRIVAL_PRICE','FINAL_PRICE','PERIOD_LOW','PERIOD_HIGH','PRV_WEXEC']
# 
# for job_id in job_ids:
#     
#     docs = collec.find({'job_id':job_id})
#     for doc in docs:
#         print doc['_id']
#         for u, v in doc.iteritems():
#             if u in time_fields:
#                 doc[u] = v.replace(tzinfo=pytz.timezone('UTC'))
#             
#             if u[4:].upper() in mkt_data_fields:
#                 doc['occ_fe_%s'%u[4:]] = doc[u]
#                 del doc[u]
#             
#         if 'eff_starttime' in doc.keys():
#             doc['first_deal_time'] = doc['eff_starttime']
#             del doc['eff_starttime']
#         
#         if 'duration' in doc.keys() and 'eff_endtime' in doc.keys():
#             duration = time.mktime(doc['eff_endtime'].timetuple()) - time.mktime(doc['SendingTime'].timetuple())
#             doc['duration'] = duration
#             print 'seq_duration : %d' %duration
#             
#         if 'occ_duration' in doc.keys() and 'eff_endtime' in doc.keys():
#             sub_ords = collec.find({'occ_ID':doc['occ_ID']}).sort('SendingTime',1)
#             last_ord = sub_ords[sub_ords.count()-1] 
#             
#             if 'eff_endtime' in last_ord.keys():
#                 duration = time.mktime(last_ord['eff_endtime'].timetuple()) - time.mktime(doc['SendingTime'].timetuple())
#                 doc['occ_duration'] = duration
#                 print 'occ_duration : %d' %duration
#         
#         collec.save(doc)
            
            
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

 
# day = '20130614'
# order = {'HandlInst': 1, 'AggreggatedStyle': 'yes', 'IDSource': 2, 'OrderQty': 10000, 'OnBehalfOfSubID': 'ALGO', 'OrdType': 2, 'StrategyName': 9, 'TargetSubID': 'ON1', 'SweepLit': 'CF', 'ExDestination': 'LN', 'Symbol': 'COLTl.AG', 'ClientID': 'PMUR', 'BeginString': 'FIX.4.2', 'TransactTime': datetime.datetime(2013, 6, 14, 10, 18, 6), 'SecurityID': 'B138NB9', 'ClOrdID': 'FY00000138083ESLO1', 'Rule80A': 'P', 'WouldDark': 'no', 'BodyLength': 335, 'SenderCompID': 'CLNT1', 'TargetCompID': 'FLINKI', 'MinSize': 1, 'MsgSeqNum': 1319, 'TickerHisto': 'COLT.LN', 'MsgType': 'D', 'ExcludeAuction': 1, 'Side': 2, 'CheckSum': 69, 'TimeInForce': 0, 'Currency': 'GBX', 'OnBehalfOfCompID': 'PMUR', 'SendingTime': datetime.datetime(2013, 6, 14, 10, 18, 6), 'Price': 102}
# dico_trader = {}
# dico_tags = {}
# ignore_tags = []
# dico_fix = '../projects/DMAlgo/cfg/FIX42.xml'    
# environment = 'prod'
# universe_file = '../projects/DMAlgo/cfg/KC_universe.xml'
# 
# conf = fix.get_conf(environment, universe_file)
# res = fix.OrderLife(order, dico_fix, day, ignore_tags, dico_tags, conf['LUIFLT01'], dico_trader, source="CLNT1", import_type='Client')
# 
# 
# for order in res[0]:
#     print order

# universe_file = '../projects/DMAlgo/cfg/KC_universe.xml'
# dico_FIX = "../projects/DMAlgo/cfg/FIX42.xml"
# 
# conf = fix.get_conf('prod', universe_file)
# 
# l_server = ['LUIFLT01', 'WATFLT01']
# day = '20130625'
# outfile = open("./Parent_%s.txt" %day,'w')
# portfolio = 'BLCLNT1_122'
# 
# # ER_file = '/home/flexsys/logs/trades/%s/FLEX_ULPROD%sI.fix' %(day, day)
# # SEND_file = '/home/flexsys/logs/trades/%s/FLEX_ULPROD%sO.fix' %(day, day)
# 
# ER_file = '/home/flexsys/logs/trades/%s/FLINKI_CLNT1%sO.fix' %(day, day)
# SEND_file = '/home/flexsys/logs/trades/%s/FLINKI_CLNT1%sI.fix' %(day, day)
# 
# 
# colnames = ['ID', 'Ticker', 'Side', 'Qty', 'ExecQty', 'lastExecPx', 'NbExec']
# line = ''
# for name in colnames:
#     line = '%s%s;' %(line, name)
# 
# line = "%s\n" %line
# 
# outfile.write(line)
# 
# for server_name in l_server:
#     print 'Start loading orders for server : %s' %server_name
#     
#     server = conf[server_name]
#     ssh = paramiko.SSHClient()
#     ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#     ssh.connect(server['ip_addr'], username=server['list_users']['flexapp']['username'], password=server['list_users']['flexapp']['passwd'])
#     cmd = "prt_fxlog %s 3 | egrep '35=D'" %(SEND_file)
#     (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
#     
#     print cmd
#     
#     dico_tags = {}
#     ignore_tags= []
#     
#     NOS_dico = {}
#


#     for line in stdout_grep:
#         line = line.replace('|\n','')
#         res_trans = fix.line_translator(line, dico_FIX, dico_tags, ignore_tags)
#         dico_tags = res_trans[1]
#         raw_order = res_trans[0]
#         order = {'ID':raw_order['ClOrdID'], 'Ticker':raw_order['Symbol'], 'Side':raw_order['Side'], 'Qty':raw_order['OrderQty'], 'ExecQty':0, 'lastExecPx':0, 'NbExec':0}
#         
#         NOS_dico[order['ID']] = order
#         
#     cmd = "prt_fxlog %s 3 | egrep '35=8.*150=(1|2)'" %(ER_file)
#     (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
#     
#     for line in stdout_grep:
#         line = line.replace('|\n','')
#         res_trans = fix.line_translator(line, dico_FIX, dico_tags, ignore_tags)
#         dico_tags = res_trans[1]
#         er = res_trans[0]
#         
#         if er['ClOrdID'] in NOS_dico.keys():
#             org_order = NOS_dico[er['ClOrdID']]
#             org_order['ExecQty'] = org_order['ExecQty'] + er['LastShares']
#             org_order['lastExecPx'] = er['LastPx']
#             org_order['NbExec'] += 1
#             
#             NOS_dico[er['ClOrdID']] = org_order
#     
#     # - Writting
#     print "Start Wrting results..."
#     outfile.write("Order ON %s\n" %server_name)
#     for u, v in NOS_dico.iteritems():
#         line = "%s;%s;%s;%s;%s;%s;%s;\n" %(v['ID'], v['Ticker'], str(v['Side']), str(v['Qty']), str(v['ExecQty']), str(v['lastExecPx']), str(v['NbExec']))
#         outfile.write(line)
#     
#     
# outfile.close()

print datetime.datetime.today().strftime('%Y%m%d')
print 'bienvenue sur Python'