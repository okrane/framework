#!python2.7
'''
Created on 23 Apr 2013

@author: flexsys
'''

import xml.etree.ElementTree as ET
import paramiko
import pymongo as mongo
import time
from datetime import datetime
from lib.dbtools import connections
from pandas import *
from lib.data.pyData import convertStr
import pytz

def get_last_seq(order, l_orders):
    
    primID = order['occ_ID']
    
    res_ord = order
    
    nb_seq = 0
    for ord in l_orders:
        if ord['occ_ID'] == primID and ord['MsgType'] == 'G':
            if nb_seq < ord['nb_replace']:
                nb_seq = ord['nb_replace']
                res_ord = ord
    
    return res_ord
    
def match_trader(trader_name, dico_trader):
    r_trader_name = ""
    if trader_name in dico_trader.keys():
        r_trader_name = dico_trader[trader_name]
        return r_trader_name
    return trader_name

def get_FIX(dico_fix, fix_number):
    struct = ET.parse(dico_fix)
    raw_data = struct.getroot()
    i=1
    for elt in raw_data.find('fields'):
        if int(elt.get('number')) == fix_number:
            return (elt.get('name'), elt.get('type'))
    
    return

def update_colmap(l_field, client, collection):
    
    database = client['DB_test']
    map_coll = database['field_map']
    
    old_line = map_coll.find({"collection_name":collection})
    
    if old_line.count() == 1:
        old_line = old_line[0]
        old_list = old_line['list_columns']
        new_list = list(set(old_list) | set(l_field))
        old_line['list_columns'] = new_list
        map_coll.save(old_line, safe=True, manipulate=False)
        
    elif old_line.count() == 0:
        map_coll.insert({'collection_name':collection, 'list_columns':l_field})
        
    else:
        print '##### Conflict while updating map collection : 2 rows for same collection !! #####'
        
    return 0
    
def get_conf(referential, dico_universe):
    
    conf ={}
    
    struct = ET.parse(dico_universe)
    raw_data = struct.getroot()
    
    servs_dico = {}
    for elt in raw_data.findall('env'):
        if elt.get('name') == referential:
            servs_dico = elt
    
    if servs_dico == {}:
        pass
    else:
        for attr in servs_dico.findall('server'):
            serv_name = attr.get('name')
            dict_server = {'ip_addr': attr.get('ip_addr')}
            l_users = {}
            for u_attr in attr.findall('user'):
                l_users[u_attr.get('username')] = u_attr.attrib
            dict_server['list_users'] = l_users
            conf[serv_name] = dict_server
    return conf

def import_FIXmsg(dico_FIX, server, day, type, IO, dico_tags={}, trader='', ignore_tags=[], source = "CLNT1"):
    
    # - IO :
    #    I for parent order (incoming orders)
    #    O for child(street) orders (outgoing orders from Execution Report)
     
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(server['ip_addr'], username=server['list_users']['flexsys']['username'], password=server['list_users']['flexsys']['passwd'])
    
    path = './logs/trades/%s/FLINKI_%s%s%s.fix' %(day, source, day, IO)
    l_orders = []
    
    if IO == 'I':
        if type != '':
            
            if type not in ['D', 'G', 'F']:
                print "error no valid type order ! (only D, G or F are allowed)"
            else:
                if trader =='':
                    cmd = 'prt_fxlog %s 3 | grep 35=%s' %(path, type)
                else:
                    cmd = "prt_fxlog %s 3 | egrep '35=%s.*57=%s'" %(path, type, trader)
            
                (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
                
                for line in stdout_grep:
                    line = line.replace('|\n','')
                    res_trans = line_translator(line, dico_FIX, dico_tags, ignore_tags)
                    dico_tags = res_trans[1]
                    
                    if res_trans[0] != {} and len(res_trans[0]) > 1:
                        l_orders.append(res_trans[0])
        else:
            l_FIXtype = ['D', 'G', 'F']
            for tp in l_FIXtype:
                if trader == '':
                    cmd = "prt_fxlog %s 3 | grep 35=%s" %(path, tp)
                else:
                    cmd = "prt_fxlog %s 3 | egrep '35=%s.*57=%s'" %(path, tp, trader)
                
                (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
            
                for line in stdout_grep:
                    line = line.replace('|\n','')
                    res_trans = line_translator(line, dico_FIX, dico_tags, ignore_tags)
                    dico_tags = res_trans[1]
                    
                    if res_trans[0] != {} and len(res_trans[0]) > 1:
                        l_orders.append(res_trans[0])
    elif IO == 'O':
        if str(type) != '':
            
            if str(type) not in ['1', '2'] :
                print "error no valid type ! (only 1 or 2 are allowed)"
            else:
                if trader =='':
                    cmd = "prt_fxlog %s 3 | egrep '35=8.*150=%s'" %(path, str(type))
                else:
                    cmd = "prt_fxlog %s 3 | egrep '35=8.*50=%s.*150=%s'" %(path, trader, str(type))
                
                (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
                
                for line in stdout_grep:
                    line = line.replace('|\n','')
                    res_trans = line_translator(line, dico_FIX, dico_tags, ignore_tags)
                    dico_tags = res_trans[1]
                    l_orders.append(res_trans[0])
                    
                    if res_trans[0] != {} and len(res_trans[0]) > 1:
                        l_orders.append(res_trans[0])
        else:
            l_FIXtype = ['1', '2']
            for tp in l_FIXtype:
                if trader == '':
                    cmd = "prt_fxlog %s 3 | egrep '35=8.*150=%s'" %(path, str(tp))
                else:
                    cmd = "prt_fxlog %s 3 | egrep '35=8.*57=%s.*150=%s'" %(path, trader, str(tp))
                
                (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
            
                for line in stdout_grep:
                    line = line.replace('|\n','')
                    res_trans = line_translator(line, dico_FIX, dico_tags, ignore_tags)
                    dico_tags = res_trans[1]
                    
                    if res_trans[0] != {} and len(res_trans[0]) > 1:
                        l_orders.append(res_trans[0])
    else:
        print "wrong connection IO only 'I' or 'O' accepted !"
    
    return [l_orders, dico_tags]


def line_translator(line, dico_fix, dico_tags, ignore_tags):
    line = line.rsplit('|')
    dict_order = {}
    
    for item in line:
        item = item.rsplit('=')
        if len(item) == 2:
            if item[1] != ' ' and item[1] != '':
                if int(item[0]) not in ignore_tags:
                    if int(item[0]) not in dico_tags.keys():
                        dico_info = get_FIX(dico_fix, int(item[0]))
                        dico_tags[int(item[0])] = dico_info
                    
                    name = dico_tags[int(item[0])][0]
                    type = dico_tags[int(item[0])][1]
                    if type != 'UTCTIMESTAMP':
                        dict_order[name] = convertStr(item[1])
                    else:
                        data = datetime.strptime(item[1], '%Y%m%d-%H:%M:%S')
                        dict_order[name] = data.replace(tzinfo=pytz.timezone('UTC'))
    return (dict_order, dico_tags)

def storeDB(l_orders, Collection, client, jobID, mode='insert'):
    
    database = client['DB_test']
    collection = database[Collection]
    
    if mode == 'insert':
        print "insert %s Orders into database" %str(len(l_orders))
    
    for order in l_orders:
        
        if mode == 'insert':    
            order.update({'job_id': jobID})
            collection.insert(order, manipulate=False)
        elif mode == 'update':
            collection.save(order)
            
        list_columns = order.keys()
        update_colmap(list_columns, client, Collection)

def check_EoL(d_msg, reason, day, socket, dico_fix, dico_tags, ignore_tags, import_type='Client'):
    
    done = False
    reason = ''
    
    OrigOrderID = d_msg['ClOrdID']
    print "Looking for other reason for order : %s" %OrigOrderID
    
    if import_type == 'Client':
        IN_file = '/home/flexsys/logs/trades/%s/FLINKI_CLNT1%sI.fix' %(day, day)
        ER_file = '/home/flexsys/logs/trades/%s/FLINKI_CLNT1%sO.fix' %(day, day)
    elif import_type == 'Street':
        IN_file = '/home/flexsys/logs/trades/%s/FLEX_ULPROD%sI.fix' %(day, day)
        ER_file = '/home/flexsys/logs/trades/%s/FLEX_ULPROD%sO.fix' %(day, day)
    
    cmd = "prt_fxlog %s 3 | egrep '41=%s'" %(IN_file, OrigOrderID)
    (stdin, stdout_grep, stderr) = socket.exec_command(cmd)
    

    if stdout_grep.readlines() == 1:
        
        line = stdout_grep.readlines()[0]
        line = line.replace('|\n','')
        [line, dico_tags] = line_translator(line, dico_fix, dico_tags, ignore_tags)
        
        if line == {}:
            return [reason, done]
        
        if line['MsgType'] == 'G':
            reason = 'replaced not acked '
            cmd = "prt_fxlog %s 3 | egrep '11=%s'" %(ER_file, line['ClOrdID'])
            
            (stdin, stdout_grep, stderr) = socket.exec_command(cmd)
            
            for er_line in stdout_grep:
                er_line = er_line.replace('|\n','')
            
            [er_line, dico_tags] = line_translator(er_line, dico_fix, dico_tags, ignore_tags)
            
            if 'ExecType' not in er_line.keys():
                pass
            
            if er_line['ExecType'] == '3':
                reason = '%s and Done for day' %reason
                done = True
                
    return [reason, done]

def OrderLife(order, dico_fix, day, ignore_tags, dico_tags, server, dico_trader, source="CLNT1", import_type='Client'):
    
    # - Open SSH connection 
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(server['ip_addr'], username=server['list_users']['flexsys']['username'], password=server['list_users']['flexsys']['passwd'])
    
    ClOrdID = order['ClOrdID']
    Trader = order['TargetSubID']
    Type = order['MsgType']
    
    order_life = []
    p_nb_exec = 0
    nb_replace = 0
    p_exec_qty = 0
    p_real_duration = 0
    global_duration = 0
    p_reason = 'unknown'
    p_eff_starttime = 'none'
    
    p_eff_endtime = 'none'
    g_eff_endtime = 'none'
    
    prev_exec = 0
    num_exec_vwap = 0
    prev_num_exec_vwap = 0
    p_num_exec_vwap = 0
    p_volume_at_would = 0
    
    p_ClOrdID = ClOrdID
    
    if Type == 'D':
        
        # - Looking for Execution report sequence
        if import_type == 'Client':
            ER_file = './logs/trades/%s/FLINKI_%s%sO.fix' %(day, source, day)
            IN_file = './logs/trades/%s/FLINKI_%s%sI.fix' %(day, source, day)
            cmd = "prt_fxlog %s 3 | egrep '35=8.*50=%s.*%s'" %(ER_file, Trader, ClOrdID)
        elif import_type == 'Street':
            ER_file = './logs/trades/%s/FLEX_ULPROD%sI.fix' %(day, day)
            IN_file = './logs/trades/%s/FLEX_ULPROD%sO.fix' %(day, day)
            cmd = "prt_fxlog %s 3 | egrep '35=8.*%s'" %(ER_file, ClOrdID)
        
        
        (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
        
        done = False
        replaced = False
        
        WouldLevel = 0
        Side = 2 * (float(order['Side']) - 1.5)
        
        if "WouldLevel" in order.keys():
            WouldLevel = order['WouldLevel']
        
        for str_msg in stdout_grep:
            
            str_msg = str_msg.replace('|\n','')
            res_trans = line_translator(str_msg, dico_fix, dico_tags, ignore_tags)
            
            if res_trans[0] != {}:
                p_msg = res_trans[0]
            else:
                continue
            
            dico_tags = res_trans[1]
            
            if 'ExecType' not in p_msg.keys():
                continue
            
            if str(p_msg['ExecType']) == '0':
                p_reason = 'new acked'
                
            elif str(p_msg['ExecType']) == '1':
                p_exec_qty += int(p_msg['LastShares'])
                num_exec_vwap += float(p_msg['LastShares']) * float(p_msg['LastPx'])
                p_nb_exec += 1
                if p_eff_starttime == "none":
                    p_eff_starttime = p_msg['TransactTime']
                
                if Side * float(p_msg['LastPx']) < WouldLevel and WouldLevel != 0 and (Side == 1 or Side == -1):
                    p_volume_at_would += int(p_msg['LastShares'])
                
            elif str(p_msg['ExecType']) == '2':
                p_nb_exec += 1
                p_exec_qty += int(p_msg['LastShares'])
                num_exec_vwap += float(p_msg['LastShares']) * float(p_msg['LastPx'])
                
                if Side * float(p_msg['LastPx']) < WouldLevel and WouldLevel != 0 and (Side == 1 or Side == -1):
                    p_volume_at_would += int(p_msg['LastShares'])
                
                p_reason = 'filled'
                if p_eff_starttime == "none":
                    p_eff_starttime = p_msg['TransactTime']
                    
                done = True
                
            elif str(p_msg['ExecType']) == '3':
                p_reason = 'done for day'
                done = True
                
            elif str(p_msg['ExecType']) == '4':
                p_reason = 'cancelled'
                done = True
                
            elif str(p_msg['ExecType']) == '5':
                p_reason = 'replaced'
                nb_replace += 1
                replaced = True
                
            elif str(p_msg['ExecType']) == '8':
                p_reason = 'rejected'
                done = True
                
            elif str(p_msg['ExecType']) == 'A':
                p_reason = 'pending new'
                
            elif str(p_msg['ExecType']) == 'E':
                p_reason = 'pending replace'
            
            p_eff_endtime = p_msg['SendingTime']
            g_eff_endtime = p_eff_endtime
            
        if not done and not replaced :
            [p_reason, done] = check_EoL(order, p_reason, day, ssh, dico_fix, dico_tags, ignore_tags, import_type)
            
            if not done:
                p_reason = 'Front End handling'
            
            
        prev_exec += p_exec_qty
        prev_num_exec_vwap +=  num_exec_vwap
        p_num_exec_vwap = num_exec_vwap
        
        if p_eff_endtime != "none":
            p_real_duration = time.mktime(p_eff_endtime.timetuple()) - time.mktime(order['SendingTime'].timetuple())
        
        if replaced:
            
            loop_count = 0
            while not done and loop_count<100 and replaced == True:
                
                replaced = False
                loop_count += 1
                
                cmd = "prt_fxlog %s 3 | egrep '35=G.*41=%s'" %(IN_file, ClOrdID)
                (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
                
                for str_msg in stdout_grep:
                    str_msg = str_msg.replace('|\n','')
                
                res_trans = line_translator(str_msg, dico_fix, dico_tags, ignore_tags)
                c_msg = res_trans[0]
                dico_tags = res_trans[1]
                
                nb_exec = 0
                exec_qty = 0
                reason = 'unknown'
                eff_starttime = "none"
                real_duration = 0
                num_exec_vwap = 0
                volume_at_would = 0
                
                ClOrdID = c_msg['ClOrdID']
                Trader = c_msg['TargetSubID']
                
                WouldLevel = 0
                if "WouldLevel" in c_msg.keys():
                    WouldLevel = c_msg['WouldLevel']
                
                if import_type == 'Client':
                    cmd = "prt_fxlog %s 3 | egrep '35=8.*50=%s.*%s'" %(ER_file, Trader, ClOrdID)
                elif import_type == 'Street':
                    cmd = "prt_fxlog %s 3 | egrep '35=8.*%s'" %(ER_file, ClOrdID)
    
                (stdin, stdout_grep, stderr)= ssh.exec_command(cmd)
                rpl_checked = False
                
                for str_msg in stdout_grep:
                    str_msg = str_msg.replace('|\n','')
                    res_trans = line_translator(str_msg, dico_fix, dico_tags, ignore_tags)
                    
                    if res_trans[0] != {}:
                        cc_msg = res_trans[0]
                    else:
                        continue
                    
                    dico_tags = res_trans[1]
                    
                    if str(cc_msg['ExecType']) == '0':
                        reason = 'new acked'
                        
                    elif str(cc_msg['ExecType']) == '1':
                        exec_qty += int(cc_msg['LastShares'])
                        num_exec_vwap += float(cc_msg['LastShares']) * float(cc_msg['LastPx'])
                        nb_exec += 1
                        
                        if Side * float(cc_msg['LastPx']) < WouldLevel and WouldLevel != 0 and (Side == 1 or Side == -1):
                            volume_at_would += int(cc_msg['LastShares'])
                        
                        if eff_starttime == "none":
                            eff_starttime = cc_msg['TransactTime']
                        
                    elif str(cc_msg['ExecType']) == '2':
                        nb_exec += 1
                        exec_qty += int(cc_msg['LastShares'])
                        num_exec_vwap += float(cc_msg['LastShares']) * float(cc_msg['LastPx'])
                        
                        if Side * float(cc_msg['LastPx']) < WouldLevel and WouldLevel != 0 and (Side == 1 or Side == -1):
                            volume_at_would += int(cc_msg['LastShares'])
                            
                        reason = 'filled'
                        done = True
                        if eff_starttime == "none":
                            eff_starttime = cc_msg['TransactTime']
                        
                    elif str(cc_msg['ExecType']) == '3':
                        reason = 'done for day'
                        done = True
                        
                    elif str(cc_msg['ExecType']) == '4':
                        reason = 'cancelled'
                        done = True
                        
                    elif str(cc_msg['ExecType']) == '5':
                        if rpl_checked == False:
                            rpl_checked =True
                        else:
                            reason = 'replaced'
                            nb_replace += 1
                            replaced = True
                        
                    elif str(cc_msg['ExecType']) == '8':
                        reason = 'rejected'
                        done = True
                        
                    elif str(cc_msg['ExecType']) == 'A':
                        reason = 'pending new'
                        
                    elif str(cc_msg['ExecType']) == 'E':
                        reason = 'pending replace'
                
                if not done and not replaced :
                    [reason, done] = check_EoL(c_msg, reason, day, ssh, dico_fix, dico_tags, ignore_tags, import_type)
                    
                    if not done:
                        reason = '%s Front End handling' %reason
                
                eff_endtime = cc_msg['SendingTime']
                g_eff_endtime = eff_endtime
                
                if eff_endtime != 'none':
                    real_duration = time.mktime(eff_endtime.timetuple()) - time.mktime(c_msg['SendingTime'].timetuple())
                
                vwap = 0
                prev_vwap = 0
                
                if exec_qty != 0:
                    vwap = num_exec_vwap / float(exec_qty)
                
                if prev_exec != 0:
                    prev_vwap = prev_num_exec_vwap / float(prev_exec)
                
                if 'TraderName' in c_msg.keys():
                    c_msg['TraderName'] = match_trader(c_msg['TraderName'], dico_trader)
                
                
                enrichment = {'reason': reason, 'nb_exec' : nb_exec, 'exec_qty': exec_qty, 'duration': real_duration, 'occ_ID': p_ClOrdID, 'occ_prev_exec_qty': prev_exec, 'exec_vwap': vwap, 'occ_prev_exec_vwap': prev_vwap, 'turnover': num_exec_vwap, 'occ_prev_turnover': prev_num_exec_vwap, 'nb_replace': loop_count}
                
                if eff_starttime != 'none':
                    enrichment.update({'first_deal_time': eff_starttime})
                
                if eff_endtime != 'none':
                    enrichment.update({'eff_endtime': eff_endtime})
                
                if volume_at_would != 0:
                    enrichment.update({'volume_at_would': volume_at_would})
                    
                c_msg.update(enrichment)
                order_life.append(c_msg)
                
                prev_exec += exec_qty
                prev_num_exec_vwap += num_exec_vwap
                
        if g_eff_endtime != 'none':
            global_duration = time.mktime(g_eff_endtime.timetuple()) - time.mktime(order['SendingTime'].timetuple())
        
        p_vwap = 0
        if p_exec_qty != 0:
            p_vwap = p_num_exec_vwap / float(p_exec_qty)
        
        enrichment = {'reason': p_reason, 'nb_exec' : p_nb_exec, 'occ_nb_replace' : nb_replace, 'exec_qty': p_exec_qty, 'duration': p_real_duration, 'occ_duration': global_duration, 'occ_ID': p_ClOrdID, 'occ_prev_exec_qty': 0, 'exec_vwap': p_vwap, 'occ_prev_exec_vwap': 0, 'turnover': p_num_exec_vwap, 'occ_prev_turnover': 0}
        
        if p_eff_starttime != 'none':
            enrichment.update({'first_deal_time': p_eff_starttime})
        
        if p_eff_endtime != 'none':
            enrichment.update({'eff_endtime': p_eff_endtime})
        
        if p_volume_at_would != 0:
            enrichment.update({'volume_at_would': p_volume_at_would})
            
        order.update(enrichment)
        
        # - append parent order to order_life list
        if 'TraderName' in order.keys():
            order['TraderName'] = match_trader(order['TraderName'], dico_trader)
        
        order_life.append(order)
        
    else:
        print 'invalid order type : only NewOrderSingle (D) are allowed !'
    
    ssh.close()
    return [order_life, dico_tags]


def export(database, server_flex, environment, io, source, dates):
    import os
    full_path = os.path.realpath(__file__)    
    path, f = os.path.split(full_path)        
    from lib.dbtools.connections import Connections
    
    # - Parameters
        
    universe_file = path +'/../cfg/KC_universe.xml'
    dico_FIX = path + '/../cfg/FIX42.xml'    
    conf = get_conf(environment, universe_file)
    
    ignore_tags = [8, 21, 22, 9, 34, 49, 56, 58, 10, 47, 369]
    
    trader = ''
    IO = io
    dico_tags = {}
    
    # - Open MONGODB connection
    #Client = mongo.MongoClient("mongodb://python_script:pythonpass@172.29.0.32:27017/DB_test")
    print database
    Client = connections.Connections.getClient(database)
    
    # - Trader dico generation for matching alias
    if IO == 'I':
        print 'Generating dico for Trader matching'
        temp_dico_trader = Client['DB_test']['map_tagFIX'].find({'tagFIX':'9249', 'ip_server':conf[server_flex]['ip_addr']})
        dico_trader = {}
        for trd in temp_dico_trader:
            dico_trader[trd['tag_value']] = trd['trader_name']
            
    mkt_data_fields = ['EXEC_SHARES' ,'ORDER_PERC','INMKT_VOLUME','INMKT_TURNOVER','PRV_VOLUME','PRV_TURNOVER','AVG_SPRD','IN_VWAS','ARRIVAL_PRICE','FINAL_PRICE','PERIOD_LOW','PERIOD_HIGH','PRV_WEXEC']
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(conf[server_flex]['ip_addr'], username=conf[server_flex]['list_users']['flexapp']['username'], password=conf[server_flex]['list_users']['flexapp']['passwd'])
    
    for day in dates:
        print "-----> Start import for : %s" %day
        
        if IO == 'O':
            
            job_id = 'OD%s' %day
            
            Client['DB_test']['OrderDeals'].remove({'job_id':job_id})
            
            type = ''
            res_import = import_FIXmsg(dico_FIX, conf[server_flex], day, type, IO, dico_tags, trader, ignore_tags, source)
            c_orders = res_import[0]
            dico_tags = res_import[1]
            
            storeDB(c_orders, 'OrderDeals', Client, job_id)
            
        elif IO == 'I':
            
            type = 'D'
            # - Single Day import
            res_import = import_FIXmsg(dico_FIX, conf[server_flex], day, type, IO, dico_tags, trader, ignore_tags, source)
            d_orders = res_import[0]
            dico_tags = res_import[1]
             
            l_kep_secids = []
            job_id = 'AO%s' %day
            
            Client['DB_test']['AlgoOrders'].remove({'job_id':job_id})
            
            for order in d_orders:
                print 'Order ID : %s' %order['ClOrdID']
                l_events = OrderLife(order, dico_FIX, day, ignore_tags, dico_tags, conf[server_flex], dico_trader, source)
                u_order = l_events[0]
                dico_tags = l_events[1]
                storeDB(u_order, 'AlgoOrders', Client, job_id)
             
                if 'SecurityID' in u_order[0].keys():
                    l_kep_secids.append(u_order[0]['SecurityID'])  
            
            l_kec_secids = set(l_kep_secids)
             
            # - UPDATE CHEUVREUX SECURITY IDS
            print "UPDATE CHEUVREUX SECURITY IDS AND MARKET DATA"
            query = "select SYMBOL3, SYMBOL6 from SECURITY where SYMBOL3 in ('%s')" % "','".join(map(str,l_kep_secids))
            
            result = connections.Connections.exec_sql('KGR', query, as_dict = True)
            dict_secs = {}
            for sec in result:
                dict_secs[str(sec['SYMBOL3'])] = str(sec['SYMBOL6'])
            
            s_date = datetime.strptime('%s-00:01:00'%day, '%Y%m%d-%H:%M:%S')
            e_date = datetime.strptime('%s-23:59:00'%day, '%Y%m%d-%H:%M:%S')
            
            new_docs = Client['DB_test']['AlgoOrders'].find({"SendingTime": {"$gte":s_date, "$lt":e_date}})
            
            # - GET COLUMN NUMBER IN MKT
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(conf[server_flex]['ip_addr'], username=conf[server_flex]['list_users']['flexapp']['username'], password=conf[server_flex]['list_users']['flexapp']['passwd'])
            cmd = 'grep SYM /home/flexapp/ushare/exportprodAV1%s' %day
            (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
            
            header = ""
            for line in stdout_grep:
                header = line
            
            dico_header = {}
            f_mkt_data = False
            if header != "":
                header = header[:-1]
                header  = header.rsplit(',')
                f_mkt_data = True
                    
                for col in mkt_data_fields:
                    if col in header:
                        dico_header[col.lower()] = header.index(col)
            
            l_docs = []
            for doc in new_docs:
                l_docs.append(doc)
                
            for order in l_docs:
                # Enrichment Market Data 
                if order['MsgType'] == 'D' and f_mkt_data:
                    last_seq = get_last_seq(order, l_docs)
                    Trader_code = last_seq['TargetSubID']
                     
                     
                    ClOrdID = '%sCLNT1' %last_seq['ClOrdID']
                    Ticker = order['Symbol']
                     
                    cmd = "egrep '%s.*%s' /home/flexapp/ushare/exportprod%s%s" %(Ticker, ClOrdID, Trader_code,day)
                     
                    (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
                     
                    mkt_data = None
                    mkt_data = stdout_grep.readlines()[0]
                    
                    if mkt_data is not None:
                        mkt_data = mkt_data[:-1]
                        mkt_data = mkt_data.rsplit(',')
                         
                        for u, v in dico_header.iteritems():
                            if mkt_data[v] != '':
                                order['occ_fe_%s'%u] = convertStr(mkt_data[v])
                            
                if "SecurityID" in order.keys() and str(order['SecurityID']) in dict_secs.keys():
                    order['cheuvreux_secid'] = dict_secs[str(order['SecurityID'])]
        
                storeDB([order], 'AlgoOrders', Client, '','update')
        else:
            print "wrong type of FIX connection : only 'I' or 'O' accepted !"
            
        print "----> END OF IMPORT FOR : %s" %day
    
    if Client.alive():
        Client.close()

if __name__ == '__main__':
    import sys    
    if len(sys.argv) == 1:
        print "Usage Examples: "
        print "python2.7 import_FIX.py PARFLTLAB02 PARFLTLAB02 dev I"
        print "python2.7 import_FIX.py PARFLTLAB02 PARFLTLAB02 dev O"
        print "python2.7 import_FIX.py HPP PARFLTLAB02 dev I CLNT1"
        print "python2.7 import_FIX.py HPP PARFLTLAB02 dev O CLNT1"
        print "python2.7 import_FIX.py HPP WATFLT01 preprod I CLNT1"
        print "python2.7 import_FIX.py HPP WATFLT01 preprod O CLNT1"
        
        database    = 'HPP'
        server_flex = 'WATFLT01'
        environment = 'preprod'
        io          = 'I'
        source      = 'CLNT1'
        date        = '20130604'
        
    else:
    
        database    = sys.argv[1]
        server_flex = sys.argv[2]
        environment = sys.argv[3]
        io          = sys.argv[4]
        source      = sys.argv[5]
        date        = sys.argv[6]
        
    export(database, server_flex, environment, io, source, [date])
    
    
