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
import lib.dbtools.read_dataset as read_dataset
import simplejson
from lib.logger import *
from lib.io.toolkit import *
logging.getLogger("paramiko").setLevel(logging.WARNING)
from lib.tca import mapping

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

def generate_file(day, all=False, export_path='C:\\'):
    new_dict = {}
    gl_list  = [] 
    
    import os
    full_path           = os.path.realpath(__file__)    
    path, f             = os.path.split(full_path) 
    
    universe_file  = path + '/KC_universe.xml'  
    conf           = get_conf('prod', universe_file)
    
    server  = conf['WATFLT01']
    ssh     = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(server['ip_addr'], 
                username = server['list_users']['flexsys']['username'], 
                password = server['list_users']['flexsys']['passwd'])
    
    
    
    cmd = 'cat /home/flexsys/logs/rtp/%s/gl_flex_dictionary' %day
    
    (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
    lines                        = stdout_grep.readlines()
    
    for line in lines:
        l_line              = line.split('\t')
        new_dict[l_line[4]] = l_line[0]
        gl_list.append(l_line[4])
            
    ssh.close()
    logging.info( "UPDATE CHEUVREUX SECURITY IDS AND MARKET DATA")
    query   = "select SYMBOL1, SYMBOL2, SYMBOL3, SYMBOL4, SYMBOL5, SYMBOL6, SECID, PARENTCODE from SECURITY where STATUS = 'A'  and SECID in ('%s')" % "','".join(map(str,set(gl_list)))
    result  = connections.Connections.exec_sql('KGR', query, as_dict = True)
    
    dict_secs       = {}
    dict_s6         = {}
    dict_s6['None'] = {}
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
            dict_s6[sec['SYMBOL6']] = dict_secs[temp]
        else:
            dict_s6['None'][temp]   = dict_secs[temp]
    l = []
    
    optional = ''
    
    if all:
        optional =  'gl_secid; isin; sedol_secid; reuters_secid; bloomberg_secid; SECID;'
    
    l.append('cheuvreux_secid; ticker; tickerAG;' + optional + '\n')
    
    
    def line_to_append(my_dict):
        line = '%s; %s; %s; ' %(  my_dict['cheuvreux_secid'],
                                  my_dict['ticker'],
                                  my_dict['tickerAG']
                               )
        if all:
            line += '%s; %s; %s; %s; %s; %s;' %(   my_dict['gl_secid'],
                                                   my_dict['isin'],
                                                   my_dict['sedol_secid'],
                                                   my_dict['reuters_secid'],
                                                   my_dict['bloomberg_secid'],
                                                   my_dict['SECID']
                                                 )
        line += '\n'
        return line

    for cheuvreux_secids, dict in dict_s6.iteritems():
        if cheuvreux_secids == 'None':
            if all:
                for key, val in dict.iteritems():
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
            l.append('; %s; %s; ; ; ; ; ; %s;\n' % (ticker, ticker_ag, el))
            
    file = open(export_path + day +'-export.csv', 'w')
    file.writelines(l)
    file.close()
    
    to_json = simplejson.dumps(dict_secs, separators=(',',':'), indent='\t')
    file_orders = open(export_path + day + '-export.json', 'w')
    file_orders.write(to_json)
    file_orders.close()
    return new_dict

if __name__ == '__main__':
    day = datetime.strftime(datetime.now(), format= '%Y%m%d')
    generate_file(day, export_path = 'C:\\')
    