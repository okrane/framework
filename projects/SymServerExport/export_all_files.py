# -*- coding: utf-8 -*-
"""
Created on Tue Sep 24 08:26:35 2013

@author: njoseph
"""

import os
import shutil
import pandas as pd
import datetime as dt
import projects.SymServerExport.vc as vc
import projects.SymServerExport.indicator as indicator
import lib.dbtools.export_ids as export_ids
import lib.dbtools.get_repository as get_repository
from lib.dbtools.connections import Connections
import lib.dbtools.statistical_engine as se
from lib.data.ui.Explorer import Explorer
from lib.logger import *
from lib.io.toolkit import get_traceback
from lib.io.toolkit import send_email

#############################################################################
#- INFO
#############################################################################
# !!!!!!!!!! BACKUP DIRECTORY NEED TO BE FILLED WITh THE NEEDED DATA !!!!!!!!!!


def send_report(pathexport,copy_sec_bkp,security_ref,copy_exch_bkp,exchange_ref,info_indicator,info_vcs,info_vcg,email_list):
    
    #--------------
    # --- SUMMARY (message)
    
    html_message = ''
    html_message += '<h1> Summary </h1>'
    # ---
    html_message += '<h2> Security Referential </h2>'
    if copy_sec_bkp:
        html_message += 'Backup file has been loaded'
    html_message += 'contains (nb ' + str(security_ref.shape[0]) + ')<br/>'
    # ---
    html_message += '<h2> Exchange Referential </h2>'
    if copy_exch_bkp:
        html_message += 'Backup file has been loaded'
    html_message += 'contains (nb ' + str(exchange_ref.shape[0]) + ')<br/>'
    # ---
    html_message += '<h2> Volume curve generic </h2>'
    if info_vcg == []:
        html_message += 'Backup  file has been loaded'
    else:
        html_message += '<br/>curve OK (nb ' + str(len(info_vcg[0])) + ')<br/>'
        html_message += '<br/>curve BACKUP (nb ' + str(len(info_vcg[1])) + ')<br/>'
        html_message += '<br/>curve not in export file (nb ' + str(len(info_vcg[2])) + ')<br/>'
        html_message += '<br/>curve in error (nb ' + str(len(info_vcg[3])) + ')<br/>'
    # ---
    html_message += '<h2> Volume curve specific </h2>'
    if info_vcs == []:
        html_message += 'Backup file has been loaded'
    else:
        html_message += '<br/>Symbol with a curve (nb ' + str(len(info_vcs[0])) + ')<br/>'
        html_message += '<br/>Curve in error (nb ' + str(len(info_vcs[1])) + ')<br/>'
    # ---
    html_message += '<h2>Indicator</h2>'
    if info_indicator == []:
        html_message += 'Backup file has been loaded'
    else:
        html_message += '<br/>Symbol with indicators (nb ' + str(len(info_indicator[0])) + ')<br/>'
        html_message += '<br/>Symbol without indicators (nb ' + str(len(info_indicator[1])) + ')<br/>'      
        
    #--------------
    # ---  DETAILS (in attachment)
    txt_message = ''
    txt_message += '\n Volume curve generic \n'
    if info_vcg == []:
        txt_message += 'Backup  file has been loaded'
    else:
        txt_message += '\n curve OK (nb ' + str(len(info_vcg[0])) + ') \n'
        txt_message += ''.join([str(x) + ',' for x in info_vcg[0]])
        txt_message += '\n curve BACKUP (nb ' + str(len(info_vcg[1])) + ') \n'
        txt_message += ''.join([str(x) + ',' for x in info_vcg[1]])
        txt_message += '\n curve not in export file (nb ' + str(len(info_vcg[2])) + ') \n'
        txt_message += ''.join([str(x) + ',' for x in info_vcg[2]])
        txt_message += '\n curve in error (nb ' + str(len(info_vcg[3])) + ') \n'
        txt_message += ''.join([str(x) + ',' for x in info_vcg[3]])
    # ---
    txt_message += '\n Volume curve specific \n'
    if info_vcs == []:
        txt_message += 'Backup file has been loaded'
    else:
        txt_message += '\n Symbol with a curve (nb ' + str(len(info_vcs[0])) + ') \n'
        txt_message += ''.join([str(x) + ',' for x in info_vcs[0]])
        txt_message += '\n Curve in error (nb ' + str(len(info_vcs[1])) + ') \n'
        txt_message += ''.join([str(x) + ',' for x in info_vcs[1]])
    # ---
    txt_message += '\n Indicator \n'
    if info_indicator == []:
        txt_message += 'Backup file has been loaded'
    else:
        txt_message += '\n Symbol with indicators (nb ' + str(len(info_indicator[0])) + ') \n'
        txt_message += ''.join([str(x) + ',' for x in info_indicator[0]])
        txt_message += '\n Symbol without indicators (nb ' + str(len(info_indicator[1])) + ') \n' 
        txt_message += ''.join([str(x) + ',' for x in info_indicator[1]])
        
    out = open( os.path.join( pathexport , 'email_log.txt') ,'w')
    out.write(txt_message)
    out.close()
    
    # ---
    send_email(_to = email_list, 
               _from = 'njoseph@keplercheuvreux.com',
               _subject = "[SymServer] Export Files ",
               _message = html_message , 
               _files = [os.path.join( pathexport , 'email_log.txt')])





if __name__ == "__main__":
    
    logging.info("EXPORT SYMSERVER : START")
    
    Connections.change_connections('production')
    
    
    date = dt.datetime.now()
    day = dt.datetime.strftime(date, format= '%Y%m%d')
    
    #############################################################################
    #- GLOBAL VARS + TEST
    #############################################################################
    
    force_generate = True
    
    # -- path
    if os.name == 'nt':
        GPATH = 'W:\\Global_Research\\Quant_research\\projets\\export_sym'
    else:
        GPATH = '/home/quant/export_sym'
    
    PATH_EXPORT = os.path.join(GPATH, day)
    PATH_BACKUP = os.path.join(GPATH, 'backup')
    
    # --- check path
    if not os.path.exists(PATH_EXPORT):
        os.mkdir(PATH_EXPORT)
        
    elif not force_generate:
        raise ValueError('directory already exist')
        
    if not os.path.exists(PATH_BACKUP):
        raise ValueError('backup path does not exist')
        
    #-- security ref
    FNAME_SECURITY_REF = 'TRANSCOSYMBOLCHEUVREUX.csv'
    
    #-- exchange ref
    FNAME_EXCHANGE_REF = 'ref_trd_destination.csv' 
    
    #-- indicator
    FNAME_INDICATOR = 'symdata'
    
    #-- volume curves
    FNAME_VC_SPECIFIC = 'VWAP_Profile_0'
    FNAME_VC_GENERIC = 'USR.vwap.opts'
    
    #-- maing list
    if os.name == 'nt':
        REPORT_MAILING_LIST=['njoseph@keplercheuvreux.com']
        
    else:
        #REPORT_MAILING_LIST=['njoseph@keplercheuvreux.com' , 'alababidi@keplercheuvreux.com' , 'sreydellet@keplercheuvreux.com']
        REPORT_MAILING_LIST=['algoquant@keplercheuvreux.com']
    
    #############################################################################
    #- REFERENTIAL
    #############################################################################
    
    #----------------------------
    #- EXPORT SECURIY REF
    #-----------------------------
    copy_sec_bkp = False
    
    try:
        # TODO : check quand on peut le lancer ??
        export_ids.generate_file(day, export_path = PATH_EXPORT, export_name = FNAME_SECURITY_REF, last_day = True, send2flexapp = False, export2json = False)
        logging.info("security_ref has been created")
    except:
        get_traceback()
        logging.error("security_ref can't be created")
        copy_sec_bkp = True
               
    if copy_sec_bkp:
        if not os.path.exists(os.path.join(PATH_BACKUP, FNAME_SECURITY_REF)):
            raise ValueError('security_ref backup cant be found')
        shutil.copy2(os.path.join(PATH_BACKUP, FNAME_SECURITY_REF), os.path.join(PATH_EXPORT, FNAME_SECURITY_REF))
        logging.warning("security_ref backup has been copied")
        
    #----------------------------
    #- LOAD SECURIY REF
    #----------------------------
    security_ref = pd.read_csv(os.path.join(PATH_EXPORT, FNAME_SECURITY_REF),sep = ';')
    security_ref = security_ref[['cheuvreux_secid', 'ticker', 'tickerAG']]
    
    #----------------------------
    #- EXPORT EXCHANGE REF (needed in Flex code for VC)
    #----------------------------
    copy_exch_bkp = False
    
    try:
        exchange_ref = get_repository.get_flexexchangemapping()
        if exchange_ref.shape[0]==0:
            raise ValueError('no exchange')
        exchange_ref.to_csv(os.path.join(PATH_EXPORT, FNAME_EXCHANGE_REF), index = False)
        logging.info("exchange_ref has been created")
    except:
        get_traceback()
        logging.error("exchange_ref can't be created")
        copy_exch_bkp = True
        
    if copy_exch_bkp:
        if not os.path.exists(os.path.join(PATH_BACKUP, FNAME_EXCHANGE_REF)):
            raise ValueError('exchange_ref backup cant be found')
        shutil.copy2(os.path.join(PATH_BACKUP, FNAME_EXCHANGE_REF), os.path.join(PATH_EXPORT, FNAME_EXCHANGE_REF))
        logging.warning("exchange_ref backup has been copied")   
        
    #----------------------------
    #- LOAD EXCHANGE REF
    #----------------------------
    if copy_exch_bkp:
        exchange_ref = pd.read_csv(os.path.join(PATH_EXPORT, FNAME_EXCHANGE_REF))
        
    #############################################################################
    #- EXPORT INDICATORS
    #############################################################################
    copy_indicator_bkp = True
    info_indicator = []
    
    # -- check database
    db_indicator_ok = indicator.check_db_update(date)
    
    # -- extract
    if db_indicator_ok:
        
        try:
            info_indicator = indicator.export_symdata(data_security_referential = security_ref, 
                                     path_export = PATH_EXPORT, 
                                     filename_export = FNAME_INDICATOR)
            copy_indicator_bkp = False
        except:
            get_traceback()
            logging.error("indicator export can't be written")
            
    # -- backup        
    if copy_indicator_bkp:
        if not os.path.exists(os.path.join(PATH_BACKUP, FNAME_INDICATOR)):
            raise ValueError('indicator backup cant be found')
        shutil.copy2(os.path.join(PATH_BACKUP, FNAME_INDICATOR), os.path.join(PATH_EXPORT, FNAME_INDICATOR))
        logging.warning("indicator backup has been copied")
    
    #############################################################################
    #- EXPORT TRADING HOURS
    #############################################################################
    # NOT DONE AT THE MOMENT
    # WE KEEP exchangedata.txt in /config cause it is by country and not by cotation group

    
    #############################################################################
    #- EXPORT VOLUME CURVES FILES
    #############################################################################
    # -- default
    copy_specific_bkp = True
    info_vcs = []
    copy_generic_bkp = True
    info_vcg = []
    
    # -- check database
    db_curve_ok = se.check_db_update(date)
    
    # -- extract
    if db_curve_ok:
        
        #----------------------------
        #- SPECIFIC
        #----------------------------
    
        try:
            info_vcs = vc.export_vc_specific(data_security_referential = security_ref,
                       path_export = PATH_EXPORT, 
                       filename_export = FNAME_VC_SPECIFIC,
                       separator = '\t')
            
            copy_specific_bkp = False 
        except:
            get_traceback()
            logging.error("specific curve file can't be written")
            
            
        #----------------------------
        #- GENERIC
        #----------------------------
    
        try:
            info_vcg = vc.export_vc_generic(data_exchange_referential = exchange_ref,
                       path_export = PATH_EXPORT, 
                       filename_export = FNAME_VC_GENERIC,
                       separator = ':')
            
            copy_generic_bkp = False
        except:
            get_traceback()
            logging.error("generic curve file can't be written")
     
    # -- backup              
    if copy_specific_bkp:
        if not os.path.exists(os.path.join(PATH_BACKUP, FNAME_VC_SPECIFIC)):
            raise ValueError('specific curve backup cant be found')
        shutil.copy2(os.path.join(PATH_BACKUP, FNAME_VC_SPECIFIC), os.path.join(PATH_EXPORT, FNAME_VC_SPECIFIC))
        logging.warning("specific curve  backup has been copied")
        
    if copy_generic_bkp:
        if not os.path.exists(os.path.join(PATH_BACKUP, FNAME_VC_GENERIC)):
            raise ValueError('generic curve backup cant be found')
        shutil.copy2(os.path.join(PATH_BACKUP, FNAME_VC_GENERIC), os.path.join(PATH_EXPORT, FNAME_VC_GENERIC))
        logging.warning("generic curve  backup has been copied")
        
        
    #############################################################################
    #- SEND REPORT ON EXPORT INDICATOR AND VOLUME CURVES
    #############################################################################
    send_report(PATH_EXPORT,
                copy_sec_bkp,security_ref,
                copy_exch_bkp,exchange_ref,
                info_indicator,info_vcs,info_vcg,REPORT_MAILING_LIST)
    
    logging.info("EXPORT SYMSERVER : END")
    