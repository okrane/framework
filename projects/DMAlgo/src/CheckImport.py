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

def check_lines(job_id, collection, conf_DB, conf_FT):
    
    # count lines in DATABASE
    # - Open MONGODB connection
    Client = mongo.MongoClient(conf_DB['ip_addr'], 27017)
    
    day = job_id[2:]
    if collection == 'ClientOrders':
        IN_file = '/home/flexsys/logs/trades/%s/FLINKI_CLNT1%sI.fix' %(day, day)
    
    
    # - GET NB LINES FOR THE JOB ID IN DB
    database = Client['DB_test']
    collec = database[collection]
    
    docs = collec.find({'job_id': job_id})
    db_nb_lines = docs.count()
    
    # - GET NB LINES IN LOG FILE
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(conf_FT['ip_addr'], username=conf_FT['list_users']['flexsys']['username'], password=conf_FT['list_users']['flexsys']['passwd'])
    
    cmd =  "prt_fxlog %s 3 | egrep '35=(D|G)'" %(IN_file)
    print cmd
    (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
    
    file_nb_lines = 0
    for line in stdout_grep:
        file_nb_lines += 1
    
    if db_nb_lines == file_nb_lines:
        print 'test ok for job_id : %s' %job_id
    else:
        collection.remove({'job_id':job_id})
    
    return 0

if __name__ == '__main__':
    
    ref = 'preprod'
    universe_file = '../cfg/KC_universe.xml'
    collection = 'ClientOrders'
    conf_FT = get_conf('preprod', universe_file)
    job_id = 'AO20130514'
    conf_db = get_conf('HPP', universe_file)
    
    check_lines(job_id, collection, conf_db['PARFLT02'], conf_FT['WATFLT01'])
    