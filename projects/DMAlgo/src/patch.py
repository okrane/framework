#!python2.7
'''
Created on 23 Apr 2013

@author: flexsys
'''

import xml.etree.ElementTree as ET
import paramiko
import pymongo as mongo
import time
from datetime import datetime, timedelta
from lib.dbtools import connections
from pandas import *
import pandas as pd
from lib.data.pyData import convertStr
import pytz
import lib.dbtools.read_dataset as read_dataset
from lib.logger import *
from lib.io.toolkit import *
from bson.json_util import default
from lib.dbtools.get_repository import get_symbol6_from_ticker
from lib.io.smart_converter import *
from paramiko import ssh_exception
from pickle import TRUE
from django.utils.datetime_safe import strftime
logging.getLogger("paramiko").setLevel(logging.WARNING)
from lib.tca import mapping
from lib.io.fix import FixTranslator  
from import_FIX import DatabasePlug

class DatabaseUpdate(DatabasePlug):
    def update_exclude_auction(self):
        to_return   = []
        self.io     = "I"
        
        logging.info('---------------------------------') 
        logging.info('-------- Update Algo Orders -------')
        logging.info('---------------------------------')
        for s in self.conf:
            logging.info('Get data from server: ' + str(s))
            self.server = self.conf[s]
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self.server['ip_addr'], 
                        username = self.server['list_users']['flexsys']['username'], 
                        password = self.server['list_users']['flexsys']['passwd'])
            for day in self.dates:
                logging.info('-------- %s -------' % day)
                
                self.logPath    = './logs/trades/%s/FLINKI_%s%s%s.fix' %(day, self.source, day, self.io)
                
                
                
                cmd = "prt_fxlog %s 3" % self.logPath
                (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
                i = 0
                for line in stdout_grep:
                    try:
                        d = self.fix_translator.line_translator(line)
                        p_cl_ord_id = day + d["ClOrdID"] + s.replace('02','01')
                        #print "Avant = " + str(list(self.client[self.database]['AlgoOrders'].find({"p_cl_ord_id" : p_cl_ord_id}))[0]["ExcludeAuction"]) + " - Apres = " + str(d["ExcludeAuction"])
                        self.client[self.database]['AlgoOrders'].update({"p_cl_ord_id" : p_cl_ord_id}, 
                                                                        {"$set" : {"ExcludeAuction" : d["ExcludeAuction"]}})
                        i = i + 1
                    except IndexError:
                        pass
                    except KeyError:
                        pass
                    except :
                        get_traceback()
                logging.info('Update %d data '% i)
            ssh.close()
                
    
if __name__ == '__main__':
    from lib.dbtools.connections import Connections
    Connections.change_connections("dev")    
    
    database_server     = 'MARS'
    database            = 'Mars'
    environment         = 'dr'
    source              = 'CLNT1'
    date                = last_business_day(datetime.now())
    limit               = datetime(year = 2013, month = 10, day = 8)
    dates               = []
    l_date              = []
    
    while date > limit:
        dates.append(datetime.strftime(date, "%Y%m%d"))
        date = last_business_day(date)

#    dates               = [date]

    DatabaseUpdate(database_server    = database_server, 
                 database           = database,
                 environment        = environment, 
                 source             = source, 
                 dates              = dates,
                 mode               = "write").update_exclude_auction()
    