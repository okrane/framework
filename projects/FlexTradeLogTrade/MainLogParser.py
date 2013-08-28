from datetime import datetime
import paramiko
import csv
import sys
import xml.etree.ElementTree as ET
from lib.io.smart_converter import convert_str
import pytz

def get_FIX(fix_number, dico_FIX):
    struct = ET.parse(dico_FIX)
    raw_data = struct.getroot()
    i=1
    for elt in raw_data.find('fields'):
        if int(elt.get('number')) == fix_number:
            return (elt.get('name'), elt.get('type'))

def line_translator(line, dico_tags, dico_FIX, ignore_tags = []):
    line                = line.rsplit('|')
    dict_order          = {}
    last_correct_date   = None
    for item in line:
        item = item.rsplit('=')
        if len(item) == 2:
            if item[1] != ' ' and item[1] != '':
                if int(item[0]) not in ignore_tags:
                    if int(item[0]) not in dico_tags.keys():
                        dico_info = get_FIX(int(item[0]), dico_FIX)
                        dico_tags[int(item[0])] = dico_info
                    
                    nameD = dico_tags[int(item[0])][0]
                    typeD = dico_tags[int(item[0])][1]
                    if typeD != 'UTCTIMESTAMP':
                        
                        temp = convert_str(item[1])
                        if temp is None:
                            logging.warning("This variable is equal to None : "+ str(nameD))
                        else:
                            dict_order[nameD] = temp
                    else:
                        try:
                            data = datetime.strptime(item[1], '%Y%m%d-%H:%M:%S')
                            dict_order[nameD] = data.replace(tzinfo=pytz.timezone('UTC'))
                            last_correct_date = data
                        except ValueError, e:
                            try :
                                #sometimes instead of getting a date at this format, we get only the time : '%H:%M:%S'
                                data = datetime.strptime(last_correct_date.strftime("%Y%m%d") + "-" +item[1], '%Y%m%d-%H:%M:%S')
                                dict_order[nameD] = data.replace(tzinfo=pytz.timezone('UTC'))
                                logging.warning("This datetime: "+str(item[1]) +"has been found to UTC ")
                            except:
                                logging.warning("An order has been removed")
                                continue  
    return (dict_order, dico_tags)      
    
def get_conf(referential, dico_universe):

    conf        = {}
    struct      = ET.parse(dico_universe)
    raw_data    = struct.getroot()
    servs_dico  = {}
    
    for elt in raw_data.findall('env'):
        if elt.get('name') == referential:
            servs_dico = elt
            
    if servs_dico != {}:
        for attr in servs_dico.findall('server'):
            serv_name   = attr.get('name')
            dict_server = {'ip_addr': attr.get('ip_addr')}
            l_users     = {}
            
            for u_attr in attr.findall('user'):
                l_users[u_attr.get('username')] = u_attr.attrib
                
            dict_server['list_users']   = l_users
            conf[serv_name]             = dict_server
    return conf

def getOrderList(mode, day, OrderID, ssh, dicoTags, dicoFIX, ignore_tags):
    
    if mode == 'client':
        path = '/home/flexsys/logs/trades/%s/FLINKI_CLNT1%sI.fix' %(day, day)
    elif mode == 'street':
        path = '/home/flexsys/logs/trades/%s/FLEX_ULPROD%sO.fix' %(day, day)
    else:
        raise NameError('bad mode assigned')
        
    cmd = "prt_fxlog %s 3 | egrep '35=(G|D|F).*11=%s'" %(path, OrderID)
    
    (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
    stdout_grep = stdout_grep.readlines()
    
    l_ids = []
    OrderDico = {}
    
    if len(stdout_grep) == 0:
        raise NameError('No Order for this specified ID !!!')
    
    elif len(stdout_grep) == 1:
        
        o_str_line = stdout_grep[0].replace('|\n','')
        o_line = o_str_line.rsplit('|')
        
        bwd = True
        fwd = True
        
        # - Looking Backward
        str_line = o_str_line
        line     = o_line
        while bwd == True:
            bwd = False
            for elt in line:
                elt = elt.rsplit('=')
                if elt[0] == '41':
                    bwd = True
                    NewOrderID = elt[1]
                    if NewOrderID not in l_ids:
                        l_ids.insert(0, NewOrderID)
                        res = line_translator(str_line, dicoTags, dicoFIX, ignore_tags)
                        OrderDico[NewOrderID] = res[0]
                        dicoTags = res[1]
                    
            if bwd == True:
                cmd = "prt_fxlog %s 3 | egrep '35=(G|D|F).*11=%s'" %(path, NewOrderID)
                (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
                stdout_grep = stdout_grep.readlines()
                line = stdout_grep[0].replace('|\n','')
                str_line = line
                line = line.rsplit('|')
        
        # - Looking Forward
        str_line = o_str_line
        line     = o_line
        while fwd == True:
            fwd = False
            for elt in line:
                
                elt = elt.rsplit('=')
                if elt[0] == '11':
                    fwd = True
                    NewOrderID = elt[1]
                    if NewOrderID not in l_ids:
                        l_ids.append(NewOrderID)
                        res = line_translator(str_line, dicoTags, dicoFIX, ignore_tags)
                        OrderDico[NewOrderID] = res[0]
                        dicoTags = res[1]
                    
            if fwd == True:
                cmd = "prt_fxlog %s 3 | egrep '35=(G|D|F).*41=%s'" %(path, NewOrderID)
                (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
                stdout_grep = stdout_grep.readlines()
                if len(stdout_grep) == 0:
                    fwd = False
                else:
                    line = stdout_grep[0].replace('|\n','')
                    str_line = line
                    line = line.rsplit('|')
            
    return [dicoTags, l_ids, OrderDico]

def write_to_csv(log_file, dico_tags, orders, title=''):
    
    header = ''
    keys = []
    
    for u, v in dico_tags.iteritems():
        header = '%s%s,' %(header,v[0])
        keys.append(v[0])
    header = '%s\n' %header
    
    if title != '':
        log_file.write('%s,\n'%title.upper())
    
    log_file.write(header)
    
    for order in orders:
        line = ''
        for key in keys:
            if key in order.keys():
                line = '%s%s,' %(line, order[key])
            else:
                line = '%s ,' %line
        line = '%s\n' %line
        log_file.write(line)

def allindices(string, sub, listindex=[], offset=0):
    i = string.find(sub, offset)
    while i >= 0:
        listindex.append(i)
        i = string.find(sub, i + 1)
    return listindex

def getFIXmsg(ssh, conf_srv, day, ClOrdID, dico_tags, dico_FIX):
    
    ignore_tags = [8, 21, 22, 9, 34, 49, 56, 10, 47, 369]
    
    ssh.connect(conf_srv['ip_addr'], username=conf_srv['list_users']['flexsys']['username'], password=conf_srv['list_users']['flexsys']['passwd'])
    log_file = open('./Report-%s-%s' %(day, ClOrdID),'w')
    l_res = getOrderList('client', day, ClOrdID, ssh, dico_tags, dico_FIX, ignore_tags)
    
    dico_tags = l_res[0]
    l_clordID = l_res[1]
    prt_orders = l_res[2]
    
    print len(l_clordID)
    
    l_orders = []
    for u, v in prt_orders.iteritems():
        print v
        l_orders.append(v)
        
    title = 'parent order sequences'
    write_to_csv(log_file, dico_tags, l_orders, title)
    
    if g_st == True:
        
        # - Street orders
        prtIDs = []
        trader = []
        for clID in l_clordID:
            order = prt_orders[clID]
            prtIDs.append(order['ClOrdID'])
    #         trader.append(order['TargetSubID'])
        
        for prtID in l_clordID:
            print prtID
            
            cmd = "prt_fxlog /home/flexsys/logs/trades/%s/FLEX_ULPROD%sO.fix 3 | egrep '35=(D|G|F).*9271=%sCLNT1\|'" %(day, day, prtID)
            (stdin, s_stdout_grep, stderr) = ssh.exec_command(cmd)
            
            ld_StOrd = []
            for line in s_stdout_grep:
                res_trans = line_translator(line, dico_tags, dico_FIX, ignore_tags)
                dico_tags = res_trans[1]
                order = res_trans[0]
                ld_StOrd.append(order)
            
            title = 'Street orders for parent : %s' %prtID
            write_to_csv(log_file, dico_tags, ld_StOrd, title)
    log_file.close()
    
def getLogMsg(ssh, conf_srv, day, portfolio, ticker):
    
    ssh.connect(conf_srv['ip_addr'], username=conf_srv['list_users']['flexapp']['username'], password=conf_srv['list_users']['flexapp']['passwd'])
    log_file = open('./FLEXLOGS-%s-%s' %(day, portfolio),'w')
    dict_logs = {}
    
    cmd = "egrep -r '%s[^0-9].*%s' /home/flexapp/logs/trdlog/%s/" %(portfolio, ticker, day)
    (stdin, s_stdout_grep, stderr) = ssh.exec_command(cmd)
    
    for line in s_stdout_grep.readlines():
        s_line = line.rsplit('[')
        id = s_line[0]
        id = id.rsplit('/')
        id = id[len(id)-1].rsplit(':')
        id = id[0]
        
        if id in dict_logs.keys():
            l_log = dict_logs[id]
        else:
            l_log = []
        
        l_log.append(line)
        dict_logs[id] = l_log
        
    for u, l_logs in dict_logs.iteritems():
        log_file.write('================================================================\n')
        log_file.write('%s\n' %u)
        log_file.write('================================================================\n')
        for line in l_logs:
            line = line.rsplit(':')
            for elt in line[1:]:
                log_file.write(elt)
            
        log_file.write('\n')
    log_file.close()
        
if __name__ == '__main__':
    
    universe_file = '../DMAlgo/cfg/KC_universe.xml'
    dico_FIX = '../DMAlgo/cfg/FIX42.xml'
    
    args = sys.argv
    environment = 'prod'
    
    #--------------------------------------
    #              Parameters :
    #--------------------------------------
    
    if len(args) < 1:
        mode = 'LOG'
        server_flex = 'WATFLT01'
#         server_flex = 'LUIFLT01'
#         server_flex = 'PARFLTLAB'
        
        ClOrdID = 'FY200010731672ESLO0'
        g_st = True
        day = '20130813'
    
        portfolio = 'SLCLNT1_15'    
        ticker = 'PUBp.AG'
    else:
        
        #path_param = args[1]
        path_param = "./params.txt"
        param_file = open(path_param, 'r')
        
        content = param_file.readlines()
        for line in content:
            
            if line[-1] == '\n':
                line = line[:-1]
            
            line = line.rsplit('=')
            
            if line[0] == 'mode':
                mode = line[1]
            elif line[0] == 'server_flex':
                if line[1] == "prod.73":
                    server_flex = "WATFLT01"
                else:
                    server_flex = "LUIFLT01"
            elif line[0] == 'ClOrdID':
                ClOrdID = line[1]
            elif line[0] == 'get_street':
                if line[1] == 'yes':
                    g_st = True
                else:
                    g_st = False
            elif line[0] == 'day':
                day = line[1]
            elif line[0] == 'portfolio':
                portfolio = line[1]
            elif line[0] == 'ticker':
                ticker = line[1]
    
    dico_tags = {}
    
    conf = get_conf(environment, universe_file)
    conf_srv = conf[server_flex]
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    if mode == 'FIX':
        getFIXmsg(ssh, conf_srv, day, ClOrdID, dico_tags, dico_FIX)
            
    elif mode == 'LOG':
        getLogMsg(ssh, conf_srv, day, portfolio, ticker)
        
    
    print "####### Finished scanning logs #######"
    ssh.close()
    