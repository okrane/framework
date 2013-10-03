# -*- coding: utf-8 -*-
"""
Created on Wed Oct 02 09:33:52 2013

@author: njoseph
"""

import os
import datetime as dt
from lib.dbtools.connections import Connections
from lib.data.ui.Explorer import Explorer
from lib.logger import *
import lib.io.toolkit as toolkit
from lib.io.toolkit import send_email
from lib.io.toolkit import get_traceback

def send_report(dict_file,email_list):
    
    html_message = ''
    # ---       
    for x in dict_file.keys():
        tmp_= '<br/> SERVER: ' + dict_file[x]['server'] + ' USER: ' + dict_file[x]['user'] + ' FILE: ' + dict_file[x]['path'] + '/' +  dict_file[x]['file'] + '<br/>'
        if dict_file[x]['send_status']:
            tmp_ += ' Successfully sent <br/>'
        else:
            tmp_ += ' ERROR in sent <br/>'
            
        html_message += tmp_
    # ---
    send_email(_to = email_list, 
               _from = 'njoseph@keplercheuvreux.com',
               _subject = "[SymServer] Files sending ",
               _message = html_message)
    
    
    
    
if __name__ == "__main__":
    
    logging.info("SEND FILES SYMSERVER : START")
    
    #----------------
    # -- enviroonement
    ENV = 'dev'    
    
    #----------------
    # -- PATH
    day = dt.datetime.strftime(dt.datetime.now(), format= '%Y%m%d')
    
    if os.name == 'nt':
        GPATH = 'C:\\export_sym'
    else:
        GPATH = '/home/quant/export_sym'
    
    PATH_FILE = os.path.join(GPATH, day)
    
    if not os.path.exists(PATH_FILE):
        raise ValueError('No Data at all')
        
    REPORT_MAILING_LIST=['njoseph@keplercheuvreux.com' , 'alababidi@keplercheuvreux.com']
    
    #----------------
    # -- Send files
    
    # -- 
    if ENV == 'dev':
        
        dict_to_send = {'indicator' : {'server' : 'PARFLTLAB', 'env' : 'dev', 'user' : 'flexsys' ,
                                       'path' : '/home/flexsys/flex/data' , 'file' : 'symdata',
                                       'send_status' : False},
                         
                        'volume curve specific' : {'server' : 'PARFLTLAB', 'env' : 'dev', 'user' : 'flexapp' ,
                                       'path' : '/home/flexapp' , 'file' : 'VWAP_Profile_0',
                                       'send_status' : False},
                         
                        'volume curve generic' : {'server' : 'PARFLTLAB', 'env' : 'dev', 'user' : 'flexapp' ,
                                       'path' : '/home/flexapp/usrs' , 'file' : 'USR.vwap.opts',
                                       'send_status' : False}}
                                        
    elif ENV == 'prod' and not os.name == 'nt':
        print 'TO DO'
        
    else:
        raise ValueError()
        
    # -- TEST FILE EXISTS
    for x in dict_to_send.keys():
        
        if not os.path.exists(os.path.join(PATH_FILE , dict_to_send[x]['file'])):
            raise ValueError(x + ' :No data')
            
    # -- SEND
    for x in dict_to_send.keys():
        
        try:
            is_send = toolkit.send(os.path.join(PATH_FILE , dict_to_send[x]['file']), 
                         dict_to_send[x]['path'] + '/' + dict_to_send[x]['file'],
                         server_remote = dict_to_send[x]['server'], 
                         env = dict_to_send[x]['env'],
                         user = dict_to_send[x]['user'])
                                
            dict_to_send[x]['send_status'] = is_send
        except:
            get_traceback()
            logging.error('')
        
        
    #----------------
    # -- Send report        
    send_report(dict_to_send , REPORT_MAILING_LIST)
    
        
    logging.info("SEND FILES SYMSERVER : END")
 
    