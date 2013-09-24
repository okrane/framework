# -*- coding: utf-8 -*-
"""
Created on Tue Sep 24 08:26:35 2013

@author: njoseph
"""

import os
import shutil
import pandas as pd
import projects.SymServerExport.vc as vc
import projects.SymServerExport.indicator as indicator
import projects.SymServerExport.loadSymMapping as loadSymMapping
import projects.SymServerExport.loadExchange as loadExchange
import projects.SymServerExport.loadCurves as loadCurves
import projects.SymServerExport.loadGenericCurves as loadGenericCurves
from lib.dbtools.connections import Connections
from lib.data.ui.Explorer import Explorer
from lib.logger import *
from lib.io.toolkit import get_traceback


if __name__ == "__main__":
    
    Connections.change_connections('production_copy')
    
    #############################################################################
    #- GLOBAL VARS
    #############################################################################
    PATH_EXPORT = 'C:\\testexport'
    PATH_BACKUP = 'W:\\Global_Research\\Quant_research\\algo issue list'
    
    #-- security ref
    FNAME_SECURITY_REF = 'TRANSCOSYMBOLCHEUVREUX.csv'
    
    #-- security ref
    FNAME_EXCHANGE_REF = 'ref_trd_destination.csv' 
       
    #-- indicator
    FNAME_INDICATOR = 'symdata'
    
    #-- intermediary volume curves
    FNAME_INT_VC_SPECIFIC = 'vol_curves_specific.txt'
    FNAME_INT_VC_GENERIC = 'vol_curves_generic.txt'
    
    #-- intermediary volume curves
    FNAME_VC_GENERIC = 'USR.vwap.opts'
    
    #############################################################################
    #- REFERENTIAL
    #############################################################################
    
    #----------------------------
    #- EXPORT SECURIY REF
    #-----------------------------
    copy_bkp = False
    try:
        # TO DO
        raise ValueError('test')
    except:
        get_traceback()
        logging.error("security_ref can't be written")
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
        # TO DO
        raise ValueError('test')
    except:
        get_traceback()
        logging.error("exchange_ref can't be written")
        copy_bkp = True
        
    if copy_bkp:
        shutil.copy2(os.path.join(PATH_BACKUP, FNAME_EXCHANGE_REF), os.path.join(PATH_EXPORT, FNAME_EXCHANGE_REF))
        logging.warning("exchange_ref backup has been copied")   

    
    #############################################################################
    #- EXPORT INDICATORS
    #############################################################################
    copy_bkp = False
    try:
        indicator.export_symdata(data_security_referential = security_ref, 
                                 path_export = PATH_EXPORT, 
                                 filename_export = FNAME_INDICATOR)
    except:
        get_traceback()
        logging.error("indicator export can't be written")
        copy_bkp = True
    
    if copy_bkp:
        shutil.copy2(os.path.join(PATH_BACKUP, FNAME_INDICATOR), os.path.join(PATH_EXPORT, FNAME_INDICATOR))
        logging.warning("indicator backup has been copied")
        
    #############################################################################
    #- EXPORT TRADING HOURS
    #############################################################################
    # NOT DONE AT THE MOMENT
    # WE KEEP exchangedata.txt in /config cause it is by country and not by cotation group

    
    #############################################################################
    #- EXPORT INTERMEDIARY VOLUME CURVES FILES
    #############################################################################
    
    #----------------------------
    #- SPECIFIC
    #----------------------------
    load_vc_specific_bkp = False
    try:
        vc.export_vc(vc_level = 'specific', vc_estimator_id = 2, path = PATH_EXPORT, filename = FNAME_INT_VC_SPECIFIC)
    except:
        get_traceback()
        logging.error("specific curve file can't be written")
        load_vc_specific_bkp = True 
    
    #----------------------------
    #- GENERIC
    #----------------------------
    load_vc_generic_bkp = False
    try:
        vc.export_vc(vc_level = 'generic', vc_estimator_id = 2, path = PATH_EXPORT, filename = FNAME_INT_VC_GENERIC)
    except:
        get_traceback()
        logging.error("generic curve file can't be written")
        load_vc_generic_bkp = True    
    
#     #############################################################################
#     #- EXPORT  VOLUME CURVES FILES (FLEX CODE)
#     #############################################################################   
#     #----------------------------
#     #- NEEDED
#     #----------------------------
#     # security in class
#     mapping = loadSymMapping.extractSymbols( os.path.join(PATH_EXPORT,FNAME_SECURITY_REF) ) 
#     mapping.extract()
#     maps = mapping.symbols
#     
#     # exchange
#     exch = loadExchange.extractExchangeFromFile( os.path.join(PATH_EXPORT,FNAME_EXCHANGE_REF) )
#     exch.extract()
#     
#     #----------------------------
#     #- SPECIFIC
#     #----------------------------    
#     curves = loadCurves.extractProfiles( os.path.join(PATH_EXPORT, FNAME_INT_VC_SPECIFIC), PATH_EXPORT, "header.h" )
#     curves.extract()
#     curves.reconciliate( maps )
#     curves.toFile( '\t' )
#     
#     #----------------------------
#     #- GENERIC
#     #----------------------------    
#     genericCurves = loadGenericCurves.extractGenericProfile( os.path.join(PATH_EXPORT, FNAME_VC_GENERIC), PATH_EXPORT, exch.exchanges )
#     genericCurves.extract()
    
