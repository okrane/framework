'''
Created on 15 May 2013

@author: flexsys
'''

import xml.etree.ElementTree as ET
import paramiko
import pymongo as mongo
import time
from datetime import datetime

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

def check_lines(job_id, day, collection, conf_DB, conf_FT, log_file):
    
    # count lines in DATABASE
    # - Open MONGODB connection
    Client = mongo.MongoClient("mongodb://%s:%s@%s:27017/DB_test" %(conf_DB['list_users']['python_script']['username'], conf_DB['list_users']['python_script']['passwd'], conf_DB['ip_addr']))
    
    file = open(log_file, 'a')
    day = job_id[2:]
    if collection == 'AlgoOrders':
        IN_file = '/home/flexsys/logs/trades/%s/FLINKI_CLNT1%sI.fix' %(day, day)
        
    elif collection == 'OrderDeals':
        IN_file = '/home/flexsys/logs/trades/%s/FLINKI_CLNT1%sO.fix' %(day, day)
    
    
    # - GET NB LINES FOR THE JOB ID IN DB
    database = Client['DB_test']
    collec = database[collection]
    
    db_nb_lines = collec.find({'job_id': job_id}).count()
    
    # - GET NB LINES IN LOG FILE
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(conf_FT['ip_addr'], username=conf_FT['list_users']['flexsys']['username'], password=conf_FT['list_users']['flexsys']['passwd'])
    
    if collection == 'AlgoOrders':
        cmd =  "prt_fxlog %s 3 | egrep '35=(D|G)'" %(IN_file)
    elif collection == 'OrderDeals':
        cmd =  "prt_fxlog %s 3 | egrep '35=8.*150=(1|2)'" %(IN_file)
    
    (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
    file_nb_lines = len(stdout_grep.readlines())
    
    if db_nb_lines == file_nb_lines:
        check =  'CHECK PASSED no difference'
    else:
        check = 'CHECK FAILED %d lines difference' %(file_nb_lines - db_nb_lines) 
#         collection.remove({'job_id':job_id})
    
    msg = "DAY : %s - JOB_ID : %s - %s lines stored in Database - %s lines in file ====> %s \n" %(day, job_id, str(db_nb_lines), str(file_nb_lines), check)
    file.write(msg)
    print msg
    
    Client.close()
    ssh.close()
    file.close()
    
    return 0

if __name__ == '__main__':
    
    ref = 'preprod'
    universe_file = '../cfg/KC_universe.xml'
    collection = 'AlgoOrders'
    conf_FT = get_conf(ref, universe_file)
    conf_db = get_conf('HPP', universe_file)
    log_file = '../cfg/log_import.txt'
    
    l_day = ['20130510','20130513','20130514','20130515','20130516','20130517','20130520','20130521','20130522']
#     l_day = ['20130521']
    for day in l_day:
        collection = 'AlgoOrders'
        job_id = 'AO%s' %day
        check_lines(job_id, day, collection, conf_db['PARFLT02'], conf_FT['WATFLT01'], log_file)
        
        collection = 'OrderDeals'
        job_id = 'OD%s' %day
        check_lines(job_id, day, collection, conf_db['PARFLT02'], conf_FT['WATFLT01'], log_file)
        