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
import projects.SymServerExport.loadSymMapping as loadSymMapping
import projects.SymServerExport.loadExchange as loadExchange
import projects.SymServerExport.loadCurves as loadCurves
import projects.SymServerExport.loadGenericCurves as loadGenericCurves
import lib.dbtools.export_ids as export_ids
import lib.dbtools.get_repository as get_repository
from lib.dbtools.connections import Connections
from lib.data.ui.Explorer import Explorer
from lib.logger import *
from lib.io.toolkit import get_traceback

#############################################################################
#- INFO
#############################################################################
# !!!!!!!!!! BACKUP DIRECTORY NEED TO BE FILLED WITh THE NEEDED DATA !!!!!!!!!!


if __name__ == "__main__":
    
    logging.info("EXPORT SYMSERVER : START")
    
    Connections.change_connections('production')
    
    force_generate = True
    day = dt.datetime.strftime(dt.datetime.now(), format= '%Y%m%d')
    
    #############################################################################
    #- GLOBAL VARS + TEST
    #############################################################################
    # -- path
    if os.name == 'nt':
        GPATH = 'C:\\export_sym'
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
    
    
    #############################################################################
    #- REFERENTIAL
    #############################################################################
    
    #----------------------------
    #- EXPORT SECURIY REF
    #-----------------------------
    copy_bkp = False
    try:
        # TODO : check quand on peut le lancer ??
        export_ids.generate_file(day, export_path = PATH_EXPORT, export_name = FNAME_SECURITY_REF, send2flexapp = False, export2json = False)
        logging.info("security_ref has been created")
    except:
        get_traceback()
        logging.error("security_ref can't be created")
        copy_bkp = True
           
    if copy_bkp:
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
    copy_bkp = False
    try:
        exchange_ref = get_repository.get_flexexchangemapping()
        if exchange_ref.shape[0]==0:
            raise ValueError('no exchange')
        exchange_ref.to_csv(os.path.join(PATH_EXPORT, FNAME_EXCHANGE_REF), index = False)
        logging.info("exchange_ref has been created")
    except:
        get_traceback()
        logging.error("exchange_ref can't be created")
        copy_bkp = True
        
    if copy_bkp:
        shutil.copy2(os.path.join(PATH_BACKUP, FNAME_EXCHANGE_REF), os.path.join(PATH_EXPORT, FNAME_EXCHANGE_REF))
        logging.warning("exchange_ref backup has been copied")   
        
    #----------------------------
    #- LOAD EXCHANGE REF
    #----------------------------
    if copy_bkp:
        exchange_ref = pd.read_csv(os.path.join(PATH_EXPORT, FNAME_EXCHANGE_REF))
        
    #############################################################################
    #- EXPORT INDICATORS
    #############################################################################
    copy_indicator_bkp = False
    sec_ids_whithout_indicator = []
     
    try:
        sec_ids_whithout_indicator = indicator.export_symdata(data_security_referential = security_ref, 
                                 path_export = PATH_EXPORT, 
                                 filename_export = FNAME_INDICATOR)
    except:
        get_traceback()
        logging.error("indicator export can't be written")
        copy_indicator_bkp = True
      
    if copy_indicator_bkp:
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
    
    #----------------------------
    #- SPECIFIC
    #----------------------------
    copy_specific_bkp = False
    vcs_ids_not_pushed = []
    try:
        vcs_ids_not_pushed = vc.export_vc_specific(data_security_referential = security_ref,
                   path_export = PATH_EXPORT, 
                   filename_export = FNAME_VC_SPECIFIC,
                   separator = '\t')
    except:
        get_traceback()
        logging.error("specific curve file can't be written")
        copy_specific_bkp = True 
      
    if copy_specific_bkp:
        shutil.copy2(os.path.join(PATH_BACKUP, FNAME_VC_SPECIFIC), os.path.join(PATH_EXPORT, FNAME_VC_SPECIFIC))
        logging.warning("specific curve  backup has been copied")
        
    #----------------------------
    #- GENERIC
    #----------------------------
    copy_generic_bkp = False
    vcg_ids_not_pushed = []
    try:
        vcg_ids_not_pushed = vc.export_vc_generic(data_exchange_referential = exchange_ref,
                   path_export = PATH_EXPORT, 
                   filename_export = FNAME_VC_GENERIC,
                   separator = ':')
    except:
        get_traceback()
        logging.error("generic curve file can't be written")
        copy_generic_bkp = True
        
    if copy_generic_bkp:
        shutil.copy2(os.path.join(PATH_BACKUP, FNAME_VC_GENERIC), os.path.join(PATH_EXPORT, FNAME_VC_GENERIC))
        logging.warning("generic curve  backup has been copied")
        
    #############################################################################
    #- SEND REPORT ON EXPORT INDICATOR AND VOLUME CURVES
    #############################################################################
    # TO DO
    
    logging.info("EXPORT SYMSERVER : END")
    