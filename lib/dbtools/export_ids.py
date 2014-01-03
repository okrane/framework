#!python2.7
'''
Created on 23 Apr 2013

@author: flexsys
'''

import xml.etree.ElementTree as ET
import paramiko
from datetime import datetime, timedelta
from lib.dbtools import connections
import pytz
import simplejson
from lib.logger import *
from lib.io.toolkit import *
logging.getLogger("paramiko").setLevel(logging.WARNING)
from lib.tca import mapping
import os

def generate_file(day, all=False, export_path=os.path.realpath(__file__), last_day = False , export_name=None, with_none = False, send2flexapp = True, export2json = True):
    new_dict = {}
    gl_list  = [] 
    
    conf    = get_conf('prod')
    server  = conf['WATFLT01']
    ssh     = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(server['ip_addr'], 
                username = server['list_users']['flexsys']['username'], 
                password = server['list_users']['flexsys']['passwd'])
    
    # -- find the last day before 'day' in directory
    if last_day:
        (stdin, stdout_grep, stderr) = ssh.exec_command('ls -d /home/flexsys/logs/rtp/*/')
        lines                        = stdout_grep.readlines()
        
        dates = []
        for line in lines:
            tmp_dir = line.split('/')[-2]
            if tmp_dir[0] == '2' and len(tmp_dir) == 8:
                dates.append(tmp_dir)
        dates.sort(reverse = True)
        
        for d in dates:
            if d < day:
                day = d
                break
            
    # -- get flex dictionnary
    path_flex_dico = '/home/flexsys/logs/rtp/%s/gl_flex_dictionary' %day
    cmd = 'cat ' + path_flex_dico 
    
    (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
    lines                        = stdout_grep.readlines()
    
    for line in lines:
        l_line              = line.split('\t')
        new_dict[l_line[4]] = l_line[0]
        gl_list.append(l_line[4])
            
    ssh.close()
    
    logging.info(path_flex_dico + ' has been used as flex dico')
    
    unique_ids  = set(gl_list) 
    len_ids     = len(unique_ids)
    
    result      = []
    subdivision = 100
    l = []
    i = 0
    for e in unique_ids:
        l.append(e)
        if i % subdivision == 0 or i == len_ids -1 :
            try:
                query   = "select SYMBOL1, SYMBOL2, SYMBOL3, SYMBOL4, SYMBOL5, SYMBOL6, SECID, PARENTCODE, CREATETIME from SECURITY where STATUS = 'A'  and SECID in ('%s')" % "','".join(map(str, l))
                result.extend(connections.Connections.exec_sql('KGR', query, as_dict = True))
            except:
                print get_traceback()
                print query
            l       = []
        i += 1
    
    dict_secs       = {}
    dict_s6         = {}
    dict_s6['None'] = {}
    dict_s6['doublon'] = {}
    kgr_list        = []
    
    for sec in result:
        temp = str(sec['SECID'])
        ticker = new_dict[temp]
        if sec['PARENTCODE'] is not None:
            ag = str(sec['PARENTCODE']) + '.AG'
        else:
            ag = ''
        dict_secs[temp] = {
                              "gl_secid"            : str(sec['SYMBOL1']),
                              "isin"                : str(sec['SYMBOL2']),
                              "sedol_secid"         : str(sec['SYMBOL3']),
                              "reuters_secid"       : str(sec['SYMBOL4']),
                              "bloomberg_secid"     : str(sec['SYMBOL5']),
                              "cheuvreux_secid"     : str(sec['SYMBOL6']),
                              "tickerAG"            : ag,
                              'PARENTCODE'          : str(sec['PARENTCODE']),
                              'SECID'               : str(sec['SECID'])   
                       }
        kgr_list.append(sec['SECID'])
        if ticker[:-3].upper() != '.AG':
            dict_secs[temp]['ticker'] = ticker
            
        if sec['SYMBOL6'] is not None:
            if (sec['SYMBOL6'] in dict_s6.keys() and
                dict_secs[temp]['ticker'] != dict_s6[sec['SYMBOL6']]['ticker']):
                
                dict_s6['doublon'][temp] = dict_secs[temp]
            else:
                dict_s6[sec['SYMBOL6']] = dict_secs[temp]
        else:
            dict_s6['None'][temp]   = dict_secs[temp]
    l = []
    
    optional = ''
    
    if all:
        optional =  'gl_secid;isin;sedol_secid;reuters_secid;bloomberg_secid;SECID;'
    
    l.append('cheuvreux_secid;ticker;tickerAG;' + optional + '\n')
    
    
    def line_to_append(my_dict):
        
        line = '%s;%s;%s;' %(  my_dict['cheuvreux_secid'],
                                  my_dict['ticker'],
                                  my_dict['tickerAG']
                               )
        if all:
            line += '%s;%s;%s;%s;%s;%s;' %(   my_dict['gl_secid'],
                                                   my_dict['isin'],
                                                   my_dict['sedol_secid'],
                                                   my_dict['reuters_secid'],
                                                   my_dict['bloomberg_secid'],
                                                   my_dict['SECID']
                                                 )
        line += '\n'
        line.replace("|", "")
        return line
    
    for cheuvreux_secids, dict in dict_s6.iteritems():
        if cheuvreux_secids == 'None':
            if with_none:
                dict_none = dict_s6['None']
                for key, val in dict_none.iteritems():
                    try:
                        l.append(line_to_append(val))
                    except Exception, e:
                        logging.error(str(val))
                        logging.error(str(key))
                        logging.error(get_traceback())
                        
        elif cheuvreux_secids== 'doublon':
            dict_doublon = dict_s6[cheuvreux_secids]
            for key, val in dict_doublon.iteritems():
                l.append(line_to_append(val))
                
        else:
            l.append(line_to_append(dict))
            

    
    if all:
        for el in gl_list:
            if el in kgr_list:
                continue
            ticker = new_dict[el]
            if ticker[:-3].upper() == '.AG':
                ticker_ag   = ticker
                ticker      = ''
            else:
                ticker_ag   = ''
            l.append(';%s;%s;;;;;;%s;\n' % (ticker, ticker_ag, el))
            
    if export_name is not None:
        csv_path = os.path.join(export_path, export_name)
    else:
        csv_path = os.path.join(export_path, day + '-export.csv')
        
    file = open(csv_path, 'w')
    file.writelines(l)
    file.close()
    
    if export2json:
        to_json = simplejson.dumps(dict_secs, separators=(',',':'), indent='\t')
        
        json_path = os.path.join(export_path, day + '-export.json')
        
        file_orders = open(json_path, 'w')
        file_orders.write(to_json)
        file_orders.close()
    
    if send2flexapp:
        send(csv_path, '/home/flexapp/logs/volume_curves/'+ day + '-export.csv', server_remote = 'PARFLTLAB', env = 'dev')
        send(csv_path, '/home/flexapp/logs/volume_curves/TRANSCOSYMBOLCHEUVREUX.csv', server_remote = 'PARFLTLAB' , env = 'dev')
        
    return new_dict


   
if __name__ == '__main__':
    day = datetime.strftime(datetime.now(), format= '%Y%m%d')
    generate_file(day)
    #generate_file(day, export_path='C:\\', export_name='champion.csv',send2flexapp = False, export2json = False)
    