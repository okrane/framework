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
               _subject = "[SymServer] Files sending Report ",
               _message = html_message)
    
    
    
    
if __name__ == "__main__":
    
    logging.info("SEND FILES SYMSERVER : START")
    
    #----------------
    # -- enviroonement
    ENV = 'prod'    
    
    #----------------
    # -- PATH
    day = dt.datetime.strftime(dt.datetime.now(), format= '%Y%m%d')
    
    if os.name == 'nt':
        GPATH = 'W:\\Global_Research\\Quant_research\\projets\\export_sym'
    else:
        GPATH = '/home/quant/export_sym'
    
    PATH_FILE = os.path.join(GPATH, day)
    
    if not os.path.exists(PATH_FILE):
        raise ValueError('No Data at all')
        
    #-- maing list
    if os.name == 'nt':
        REPORT_MAILING_LIST=['njoseph@keplercheuvreux.com']
        
    else:
        # REPORT_MAILING_LIST=['njoseph@keplercheuvreux.com' , 'alababidi@keplercheuvreux.com' , 'sreydellet@keplercheuvreux.com']
        REPORT_MAILING_LIST=['algoquant@keplercheuvreux.com']
        
    #----------------
    # -- Send files
    
    # -- 
    if ENV == 'dev':
        
        dict_to_send = {'indicator' : {'server' : 'PARFLTLAB', 'env' : ENV, 'user' : 'flexsys' ,
                                       'path' : '/home/flexsys/flex/data' , 'file' : 'symdata',
                                       'send_status' : False},
                         
                        'volume curve specific' : {'server' : 'PARFLTLAB', 'env' : ENV, 'user' : 'flexapp' ,
                                       'path' : '/home/flexapp' , 'file' : 'VWAP_Profile_0',
                                       'send_status' : False},
                         
                        'volume curve generic' : {'server' : 'PARFLTLAB', 'env' : ENV, 'user' : 'flexapp' ,
                                       'path' : '/home/flexapp/usrs' , 'file' : 'USR.vwap.opts',
                                       'send_status' : False}}
                        
    elif ENV == 'prod' and not os.name == 'nt':
        
        dict_to_send = {#-- LUIFLT01 (Prod)
                        'indicator (LUIFLT01)' : {'server' : 'LUIFLT01', 'env' : ENV, 'user' : 'flexsys' ,
                                       'path' : '/home/flexsys/flex/data' , 'file' : 'symdata',
                                       'send_status' : False},
                        
                        'volume curve specific (LUIFLT01)' : {'server' : 'LUIFLT01', 'env' : ENV, 'user' : 'flexapp' ,
                                       'path' : '/home/flexapp' , 'file' : 'VWAP_Profile_0',
                                       'send_status' : False},
                        
                        'volume curve generic (LUIFLT01)' : {'server' : 'LUIFLT01', 'env' : ENV, 'user' : 'flexapp' ,
                                       'path' : '/home/flexapp/usrs' , 'file' : 'USR.vwap.opts',
                                       'send_status' : False},
                        
                        #-- LUIFLT02 (PrePRod DR)
                        'indicator (LUIFLT02)' : {'server' : 'LUIFLT02', 'env' : 'dr', 'user' : 'flexsys' ,
                                       'path' : '/home/flexsys/flex/data' , 'file' : 'symdata',
                                       'send_status' : False},
                        
                        'volume curve specific (LUIFLT02)' : {'server' : 'LUIFLT02', 'env' : 'dr', 'user' : 'flexapp' ,
                                       'path' : '/home/flexapp' , 'file' : 'VWAP_Profile_0',
                                       'send_status' : False},
                        
                        'volume curve generic (LUIFLT02)' : {'server' : 'LUIFLT02', 'env' : 'dr', 'user' : 'flexapp' ,
                                       'path' : '/home/flexapp/usrs' , 'file' : 'USR.vwap.opts',
                                       'send_status' : False},

                        #-- TELFLT01 (PreProd)
                        'indicator (TELFLT01)' : {'server' : 'TELFLT01', 'env' : ENV, 'user' : 'flexsys' ,
                                       'path' : '/home/flexsys/flex/data' , 'file' : 'symdata',
                                       'send_status' : False},

                        'volume curve specific (TELFLT01)' : {'server' : 'TELFLT01', 'env' : ENV, 'user' : 'flexapp' ,
                                       'path' : '/home/flexapp' , 'file' : 'VWAP_Profile_0',
                                       'send_status' : False},

                        'volume curve generic (TELFLT01)' : {'server' : 'TELFLT01', 'env' : ENV, 'user' : 'flexapp' ,
                                       'path' : '/home/flexapp/usrs' , 'file' : 'USR.vwap.opts',
                                       'send_status' : False},
 
                        #-- TELFLT02 (Prod DR)
                        'indicator (TELFLT02)' : {'server' : 'TELFLT02', 'env' : ENV, 'user' : 'flexsys' ,
                                       'path' : '/home/flexsys/flex/data' , 'file' : 'symdata',
                                       'send_status' : False},

                        'volume curve specific (TELFLT02)' : {'server' : 'TELFLT02', 'env' : ENV, 'user' : 'flexapp' ,
                                       'path' : '/home/flexapp' , 'file' : 'VWAP_Profile_0',
                                       'send_status' : False},

                        'volume curve generic (TELFLT02)' : {'server' : 'TELFLT02', 'env' : ENV, 'user' : 'flexapp' ,
                                       'path' : '/home/flexapp/usrs' , 'file' : 'USR.vwap.opts',
                                       'send_status' : False},
 
                        #-- PARALG01  (dev)
                        'indicator (PARALG01)' : {'server' : 'PARALG01', 'env' : 'dev', 'user' : 'flexsys' ,
                                       'path' : '/home/flexsys/flex/data' , 'file' : 'symdata',
                                       'send_status' : False},
                        
                        'volume curve specific (PARALG01)' : {'server' : 'PARALG01', 'env' : 'dev', 'user' : 'flexapp' ,
                                       'path' : '/home/flexapp' , 'file' : 'VWAP_Profile_0',
                                       'send_status' : False},
                        
                        'volume curve generic (PARALG01)' : {'server' : 'PARALG01', 'env' : 'dev', 'user' : 'flexapp' ,
                                       'path' : '/home/flexapp/usrs' , 'file' : 'USR.vwap.opts',
                                       'send_status' : False},

                        #-- PARALG03  (UAT AS)
                        'indicator (PARALG03)' : {'server' : 'PARALG03', 'env' : 'dev', 'user' : 'flexsys' ,
                                       'path' : '/home/flexsys/flex/data' , 'file' : 'symdata',
                                       'send_status' : False},

                        'volume curve specific (PARALG03)' : {'server' : 'PARALG03', 'env' : 'dev', 'user' : 'flexapp' ,
                                       'path' : '/home/flexapp' , 'file' : 'VWAP_Profile_0',
                                       'send_status' : False},

                        'volume curve generic (PARALG03)' : {'server' : 'PARALG03', 'env' : 'dev', 'user' : 'flexapp' ,
                                       'path' : '/home/flexapp/usrs' , 'file' : 'USR.vwap.opts',
                                       'send_status' : False}}
         
    else:
        raise ValueError('unknown environnement <' + ENV + '>')
        
    # -- TEST FILE EXISTS
    for x in dict_to_send.keys():
        
        if not os.path.exists(os.path.join(PATH_FILE , dict_to_send[x]['file'])):
            raise ValueError('At least one of the needed file is missing : ' + x)
            
    # -- SEND
    for x in dict_to_send.keys():
        is_send = False
        
        try:
            is_send = toolkit.send(os.path.join(PATH_FILE , dict_to_send[x]['file']), 
                         dict_to_send[x]['path'] + '/' + dict_to_send[x]['file'],
                         server_remote = dict_to_send[x]['server'], 
                         env = dict_to_send[x]['env'],
                         user = dict_to_send[x]['user'])
                        
        except:
            get_traceback()
            logging.error('error in send file')
            
        dict_to_send[x]['send_status'] = is_send
        
    #----------------
    # -- Send report        
    send_report(dict_to_send , REPORT_MAILING_LIST)
    
        
    logging.info("SEND FILES SYMSERVER : END")
 
    
