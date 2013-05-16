'''
Created on 23 Apr 2013

@author: flexsys
'''

import xml.etree.ElementTree as ET
import paramiko
import pymongo as mongo
import time
from datetime import datetime

def get_FIX(dico_fix, fix_number):
    struct = ET.parse(dico_fix)
    raw_data = struct.getroot()
    i=1
    for elt in raw_data.find('fields'):
        if int(elt.get('number')) == fix_number:
            return (elt.get('name'), elt.get('type'))
    
    return
    
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

def import_FIXmsg(dico_FIX, server, day, type, IO, dico_tags={}, trader='', ignore_tags=[]):
    
    # - IO :
    #    I for parent order (incoming orders)
    #    O for child(street) orders (outgoing orders from Execution Report)
     
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(server['ip_addr'], username=server['list_users']['flexsys']['username'], password=server['list_users']['flexsys']['passwd'])
    
    path = './logs/trades/%s/FLINKI_CLNT1%s%s.fix' %(day, day, IO)
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
                    l_orders.append(res_trans[0])
    else:
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
                    l_orders.append(res_trans[0])
                    
    return [l_orders, dico_tags]


def line_translator(line, dico_fix, dico_tags, ignore_tags):
    line = line.rsplit('|')
    dict_order = {}
    for item in line:
        item = item.rsplit('=')
        if item[1] != ' ' and item[1] != '':
            if int(item[0]) not in ignore_tags:
                if int(item[0]) not in dico_tags.keys():
                    dico_info = get_FIX(dico_FIX, int(item[0]))
                    dico_tags[int(item[0])] = dico_info
                
                name = dico_tags[int(item[0])][0]
                type = dico_tags[int(item[0])][1]
                if type != 'UTCTIMESTAMP':
                    dict_order[name] = item[1]
                else:
                    dict_order[name] = datetime.strptime(item[1], '%Y%m%d-%H:%M:%S')
    
    return (dict_order, dico_tags)

def storeDB(l_orders, Collection, client, jobID):
    
    database = client['DB_test']
    collection = database[Collection]
    
    print "insert ClientOrders into database"
    
    for order in l_orders:
        order.update({'job_id': jobID})
        collection.insert(order, manipulate=False)

def OrderLife(order, dico_fix, day, ignore_tags, dico_tags, server):
    
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
    p_eff_starttime = "none"
    prev_exec = 0
    num_exec_vwap = 0
    prev_num_exec_vwap = 0
    p_num_exec_vwap = 0
    p_volume_at_would = 0
    
    p_ClOrdID = ClOrdID
    
    if Type == 'D':
        
        # - Looking for Execution report sequence
        ER_file = './logs/trades/%s/FLINKI_CLNT1%sO.fix' %(day, day)
        IN_file = './logs/trades/%s/FLINKI_CLNT1%sI.fix' %(day, day)
        
        cmd = "prt_fxlog %s 3 | egrep '35=8.*50=%s.*%s'" %(ER_file, Trader, ClOrdID)
        
        (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
        
        done = False
        replaced = False
        
        WouldLevel = 0
        Side = 2 * (float(order['Side']) - 1.5)
        order['Side'] = Side
        
        if "WouldLevel" in order.keys():
            WouldLevel = order['WouldLevel']
        
        for str_msg in stdout_grep:
            str_msg = str_msg.replace('|\n','')
            res_trans = line_translator(str_msg, dico_fix, dico_tags, ignore_tags)
            p_msg = res_trans[0]
            dico_tags = res_trans[1]
                
            if str(p_msg['ExecType']) == '0':
                p_reason = 'new acked'
                
            elif str(p_msg['ExecType']) == '1':
                p_exec_qty += int(p_msg['LastShares'])
                num_exec_vwap += float(p_msg['LastShares']) * float(p_msg['LastPx'])
                p_nb_exec += 1
                if p_eff_starttime == "none":
                    p_eff_starttime = p_msg['TransactTime']
                
                if order['Side'] * float(p_msg['LastPx']) < WouldLevel and WouldLevel != 0:
                    p_volume_at_would += int(p_msg['LastShares'])
                
            elif str(p_msg['ExecType']) == '2':
                p_nb_exec += 1
                p_exec_qty += int(p_msg['LastShares'])
                num_exec_vwap += float(p_msg['LastShares']) * float(p_msg['LastPx'])
                
                if Side * float(p_msg['LastPx']) < WouldLevel and WouldLevel != 0:
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
        
        if not done and not replaced :
            p_reason = 'Front End handling'
        
        p_eff_endtime = p_msg['SendingTime']
        g_eff_endtime = p_eff_endtime
        prev_exec += p_exec_qty
        prev_num_exec_vwap +=  num_exec_vwap
        p_num_exec_vwap = num_exec_vwap
        
        if p_eff_starttime != "none":
            p_real_duration = time.mktime(p_eff_endtime.timetuple()) - time.mktime(p_eff_starttime.timetuple())
        
        if replaced:
            
            loop_count = 0
            while not done and loop_count<100 and replaced == True:
                
                replaced = False
                loop_count += 1
                
                cmd = "prt_fxlog %s 3 | egrep '35=G.*57=%s.*41=%s'" %(IN_file, Trader, ClOrdID)
                (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
                
                for str_msg in stdout_grep:
                    str_msg = str_msg.replace('|\n','')
                
                nb_exec = 0
                exec_qty = 0
                reason = 'unknown'
                eff_starttime = "none"
                real_duration = 0
                num_exec_vwap = 0
                volume_at_would = 0
                
                res_trans = line_translator(str_msg, dico_fix, dico_tags, ignore_tags)
                c_msg = res_trans[0]
                dico_tags = res_trans[1]
                
                ClOrdID = c_msg['ClOrdID']
                
                WouldLevel = 0
                if "WouldLevel" in c_msg.keys():
                    WouldLevel = c_msg['WouldLevel']
            
                cmd = "prt_fxlog %s 3 | egrep '35=8.*50=%s.*%s'" %(ER_file, Trader, ClOrdID)
    
                (stdin, stdout_grep, stderr)= ssh.exec_command(cmd)
                rpl_checked = False
                
                for str_msg in stdout_grep:
                    str_msg = str_msg.replace('|\n','')
                    res_trans = line_translator(str_msg, dico_fix, dico_tags, ignore_tags)
                    cc_msg = res_trans[0]
                    dico_tags = res_trans[1]
                    
                    if str(cc_msg['ExecType']) == '0':
                        reason = 'new acked'
                        
                    elif str(cc_msg['ExecType']) == '1':
                        exec_qty += int(cc_msg['LastShares'])
                        num_exec_vwap += float(cc_msg['LastShares']) * float(cc_msg['LastPx'])
                        nb_exec += 1
                        
                        if order['Side'] * float(cc_msg['LastPx']) < WouldLevel and WouldLevel != 0:
                            volume_at_would += int(cc_msg['LastShares'])
                        
                        if eff_starttime == "none":
                            eff_starttime = cc_msg['TransactTime']
                        
                    elif str(cc_msg['ExecType']) == '2':
                        nb_exec += 1
                        exec_qty += int(cc_msg['LastShares'])
                        num_exec_vwap += float(cc_msg['LastShares']) * float(cc_msg['LastPx'])
                        
                        if order['Side'] * float(cc_msg['LastPx']) < WouldLevel and WouldLevel != 0:
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
                    reason = 'Front End handling'
                
                eff_endtime = cc_msg['SendingTime']
                g_eff_endtime = eff_endtime
                
                if eff_starttime != 'none':
                    real_duration = time.mktime(eff_endtime.timetuple()) - time.mktime(eff_starttime.timetuple())
                
                vwap = 0
                prev_vwap = 0
                
                if exec_qty != 0:
                    vwap = num_exec_vwap / float(exec_qty)
                
                if prev_exec != 0:
                    prev_vwap = prev_num_exec_vwap / float(prev_exec)
                    
                c_msg.update({'reason': reason, 'nb_exec' : nb_exec, 'exec_qty': exec_qty, 'duration': real_duration, 'occ_ID': p_ClOrdID, 'eff_starttime': eff_starttime, 'eff_endtime': eff_endtime, 'occ_prev_exec_qty': prev_exec, 'exec_vwap': vwap, 'occ_prev_exec_vwap': prev_vwap, 'turnover': num_exec_vwap, 'occ_prev_turnover': prev_num_exec_vwap, 'volume_at_would': volume_at_would, 'nb_replace': loop_count})
                order_life.append(c_msg)
                
                prev_exec += exec_qty
                prev_num_exec_vwap += num_exec_vwap
                
        if g_eff_endtime != 'none' and p_eff_starttime != 'none':
            global_duration = time.mktime(g_eff_endtime.timetuple()) - time.mktime(p_eff_starttime.timetuple())
        
        p_vwap = 0
        if p_exec_qty != 0:
            p_vwap = p_num_exec_vwap / float(p_exec_qty)
            
        order.update({'reason': p_reason, 'nb_exec' : p_nb_exec, 'occ_nb_replace' : nb_replace, 'exec_qty': p_exec_qty, 'duration': p_real_duration, 'occ_duration': global_duration, 'occ_ID': p_ClOrdID, 'eff_starttime': p_eff_starttime, 'eff_endtime': p_eff_endtime, 'occ_prev_exec_qty': 0, 'exec_vwap': p_vwap, 'occ_prev_exec_vwap': 0, 'turnover': p_num_exec_vwap, 'occ_prev_turnover': 0, 'volume_at_would': p_volume_at_would})
        
        # - append parent order to order_life list
        order_life.append(order)
        
    else:
        print 'invalid order type : only NewOrderSingle (D) are allowed !'
    
    return [order_life, dico_tags]


if __name__ == '__main__':
    
    # - Parameters
    universe_file = '../cfg/KC_universe.xml'
    dico_FIX = '../cfg/FIX42.xml'
    
    conf = get_conf('preprod', universe_file)
    
    ignore_tags = [8, 21, 22, 9, 34, 49, 56, 10, 47, 369]
    
    day = '20130514'
    type = 'D'
    trader = ''
    IO = 'I'
    dico_tags = {}
    
    # - Open MONGODB connection
    Client = mongo.MongoClient('172.29.0.32', 27017)
    
#     # - Empty Collection : ClientOrders
#     database = Client['DB_test']
#     collection = database['ClientOrders']
#     collection.remove()
     
    # - Store Client Orders for all days
#     ssh = paramiko.SSHClient()
#     ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#     ssh.connect(conf['WATFLT01']['ip_addr'], username=conf['WATFLT01']['list_users']['flexsys']['username'], password=conf['WATFLT01']['list_users']['flexsys']['passwd'])
#       
#     cmd = 'ls ./logs/trades'
#     (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
#       
#     for folder in stdout_grep:
#         folder = folder.replace('\n','')
#         print 'Processing orders for day : %s' %folder
#           
#         res_import = import_FIXmsg(dico_FIX, conf['WATFLT01'], folder, type, IO, dico_tags, trader, ignore_tags)
#         d_orders = res_import[0]
#         dico_tags = res_import[1]
#           
# #         if d_orders != []:
# #             storeDB(d_orders, 'OrderDeals', Client)
#           
#         for order in d_orders:
#             print 'Order ID : %s' %order['ClOrdID']
#             l_events = OrderLife(order, dico_FIX, folder, ignore_tags, dico_tags, conf['WATFLT01'])
#             u_order = l_events[0]
#             dico_tags = l_events[1]
#             storeDB(u_order, 'ClientOrders', Client)
    
    # - Single Day import
    res_import = import_FIXmsg(dico_FIX, conf['WATFLT01'], day, type, IO, dico_tags, trader, ignore_tags)
    d_orders = res_import[0]
    dico_tags = res_import[1]
     
    for order in d_orders:
        print 'Order ID : %s' %order['ClOrdID']
        l_events = OrderLife(order, dico_FIX, day, ignore_tags, dico_tags, conf['WATFLT01'])
        u_order = l_events[0]
        dico_tags = l_events[1]
        job_id = 'AO%s' %day
        storeDB(u_order, 'ClientOrders', Client, job_id)
        
    # - Example with single message
#     str_msg = '8=FIX.4.2|9=301|35=D|49=CLNT1|56=FLINKI|34=125|369=151|52=20130214-08:15:33|50=SLEFIX1|57=AV1|1=EUR1576248|11=FY2000005066301|15=CHF|21=1|22=4|38=3000|40=1|48=CH0127480363|54=1|55=AUTNz.AG|58=EUR1576248|59=0|60=20130214-08:15:33|100=S|9249=AV1|9252=5|9253=29.000000|9254=31.000000|9256=2|9261=no|9265=yes|9266=AUTN.S|10=131'
#     res_trans = line_translator(str_msg, dico_FIX, dico_tags, ignore_tags)
#     order = res_trans[0]
#     dico_tags = res_trans[1]
#     l_events = OrderLife(order, dico_FIX, folder, ignore_tags, dico_tags, conf['WATFLT01'])
#     u_order = l_events[0]
#     dico_tags = l_events[1]
#     storeDB(u_order, 'ClientOrders', Client)
    
#     IO = 'O'
#     type = ''
#     trader = ''
#     c_orders = import_FIXmsg(dico_FIX, conf['WATFLT01'], day, type, IO, trader, ignore_tags)
#     storeDB(c_orders, 'StreetOrders', Client)
    
    
#     # - Test volumetrie
#     
#     line = '8=FIX.4.2|9=314|35=D|49=CLNT1|56=FLINKI|34=1092|52=20130422-14:43:44|57=TU4|115=PWARE|1=A1|11=FYAAA211|15=EUR|21=2|22=2|38=150|40=2|44=154.3|48=5972643|54=1|55=ERFp.AG|59=0|60=20130422-14:43:44|100=PA|9249=TU4|9250= |9251= |9252=9|9253= |9254= |9255= |9256= |9257=0|9258= |9259= |9260= |9261=no|9262= |9264= |9265=yes|9266=ERF.PA|10=208'
#     dico_tags = {}
#     t_line = line_translator(line, dico_FIX, dico_tags, ignore_tags)
#     t_line = t_line[0]
#     nb_line = 765038
#     
#     rec_file = open('./stats_file.txt','w')
#     
#     header = 'nb_lines;duration;data_size;total_size;store_size;\n'
#     rec_file.write(header)
#     
#     frac = int(float(nb_line)/365)
#     lines = 0
#     duration = 0
#     for i in range(365):
#         
#         l_orders = []
#         for i in range(frac):
#             l_orders.append(t_line)
#         
#         t0 = time.time()
#         storeDB(l_orders, Client)
#         
#         duration = duration + time.time() - t0
#         
#         
#         
#         stats = database.command({'collStats' : 'ClientOrders', 'scale' : 1024})
#         data_size = stats['size']
#         store_size = stats['totalIndexSize']
#         total_size = stats['size'] + stats['totalIndexSize']
#         lines = lines + frac
#         stats = '%s;%s;%s;%s;%s;\n' %(str(lines), str(duration), str(data_size), str(total_size), str(store_size))
#         rec_file.write(stats)
#     
#     rec_file.close()


    # - Close session
    if Client.alive():
        Client.close()
    