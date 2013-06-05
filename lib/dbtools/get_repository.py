# -*- coding: utf-8 -*-
"""
Created on Mon May 20 13:52:46 2013

@author: njoseph
"""

import pandas as pd
import datetime as dt
import numpy as np
from lib.data.matlabutils import *
from lib.dbtools.connections import Connections


  
#------------------------------------------------------------------------------
# exchangeid2tz
#------------------------------------------------------------------------------
def exchangeid2tz(**kwargs):
    ##############################################################
    # input handling
    ##############################################################
    #---- exchange_id
    if "exchange_id" in kwargs.keys():
        lids=kwargs["exchange_id"]
        if isinstance(lids,list):
            lids=np.array(lids)
        elif not isinstance(lids,np.ndarray):
            lids=np.array([lids])
    else:
        raise NameError('get_repository:exchangeid2tz - Bad input : exchange_id is missing')
        
    out=np.array([None]*lids.size)    
    
    ##############################################################
    # request and format
    ##############################################################
    # ----------------
    # NEEDED
    # ----------------
    pref_ = "LUIDBC01_" if Connections.connections == "dev" else  ""   
    str_lids="("+"".join([str(x)+',' for x in uniqueext(lids)])
    str_lids=str_lids[:-1]+")"
    # ----------------
    # request
    # ----------------
    req=(" SELECT "
        " EXCHANGE,TIMEZONE " 
        " FROM " 
        " %sKGR..EXCHANGEREFCOMPL "
        " WHERE "
        " EXCHANGE in %s ") % (pref_,str_lids)
    vals=Connections.exec_sql('KGR',req)
    # ----------------
    # format
    # ----------------        
    if not (not vals):
        for x in vals:
            out[np.where(lids==int(x[0]))[0]]=x[1] 
    return out

#------------------------------------------------------------------------------
#  tdidch2exchangeid
#------------------------------------------------------------------------------
def tdidch2exchangeid(**kwargs):
    ##############################################################
    # input handling
    ##############################################################
    #---- sec_id
    if "security_id" in kwargs.keys():
        secid=kwargs["security_id"]
    else:
        secid=[]
    #---- td_id
    if "td_id" in kwargs.keys():
        lids=kwargs["td_id"]
        if isinstance(lids,list):
            lids=np.array(lids)
        elif not isinstance(lids,np.ndarray):
            lids=np.array([lids])
    else:
        raise NameError('get_repository:tdidch2exchangeid - Bad input : td_id is missing')
        
    out=np.array([None]*lids.size)    
    
    ##############################################################
    # request and format
    ##############################################################
    # ----------------
    # NEEDED
    # ----------------
    pref_ = "LUIDBC01_" if Connections.connections == "dev" else  ""
    str_lids="("+"".join([str(x)+',' for x in uniqueext(lids)])
    str_lids=str_lids[:-1]+")"
    # ----------------
    # PHASE 1 : request on destination
    # ----------------
    req=(" SELECT "
    " trading_destination_id,EXCHANGE "
    " FROM "
    " %sKGR..EXCHANGEMAPPING "
    " WHERE "
    " trading_destination_id in %s ") % (pref_,str_lids)

    vals=Connections.exec_sql('KGR',req)
    if not (not vals):
        for x in vals:
            out[np.where(lids==int(x[0]))[0]]=x[1]   
            
    # ----------------
    # PHASE 2 : if there is destination that we couldn't map
    # ----------------
    # Y a-t-il des None dans out ? 
    # Si oui vérifions qu'il s'agit de données US :
    # - oui + on a donné en entrée un security_id-> out = destination primaire
    # - non -> on place un signe '-' devant la trading destination.
    if any(x is None for x in out) and (not all(x is None for x in out)):
        idxnotnone=np.nonzero([x is not None for x in out])[0]
        
        str_exchnotnone="("+"".join([str(x)+',' for x in uniqueext(out[idxnotnone])])
        str_exchnotnone=str_exchnotnone[:-1]+")"
        
        req=(" SELECT "
        " EXCHANGE,TIMEZONE " 
        " FROM " 
        " %sKGR..EXCHANGEREFCOMPL "
        " WHERE "
        " EXCHANGE in %s ") % (pref_,str_exchnotnone)
        vals=Connections.exec_sql('KGR',req)
        
        if (any([x[1] is 'America/New_York' for x in vals])) and (not secid==[]):
            
            # get primary destination
            req=(" SELECT "
            " trading_destination_id " 
            " FROM " 
            " %sKGR..security_market "
            " WHERE "
            " security_id = %d "
            " and ranking=1 ") % (pref_,secid)
            vals=Connections.exec_sql('KGR',req)
            out=np.array([vals[0][0]]*lids.size) 
        else:
            idxnone=np.nonzero([x is None for x in out])[0]
            out[idxnone]=-1*lids[idxnone]
          
    return out

#------------------------------------------------------------------------------
# local_tz_from
#------------------------------------------------------------------------------
def local_tz_from(**kwargs):
    
    out=[]
    
    #### Build the request
    if "security_id" in kwargs.keys():  
        
        # get/transform inputs
        lids=kwargs["security_id"]
        if  isinstance(lids,list):
            lids=np.array(lids)
        elif not isinstance(lids,np.ndarray):
            lids=np.array([lids])
            
        out=np.array([None]*lids.size)

        # construct request
        str_lids="("+"".join([str(x)+',' for x in lids])
        str_lids=str_lids[:-1]+")"
        pref_ = "LUIDBC01_" if Connections.connections == "dev" else  ""
        
        req=(" select "
        " sec.SYMBOL6, exch.TIMEZONE "
        " from "
            " %sKGR..SECURITY sec ,"
            " %sKGR..EXCHANGEREFCOMPL exch "
        " where "
            " sec.SYMBOL6 in %s "
            " and sec.EXCHGID=exch.EXCHGID "
            " and exch.EXCHANGETYPE='M' "
            " and sec.STATUS='A' ") % (pref_,pref_,str_lids)
            
    else:
        raise NameError('get_repository:tz_from - Bad input data')
        
    #### EXECUTE REQUEST 
    vals=Connections.exec_sql('KGR',req)
     
    ####  OUTPUT 
    if not vals:
        return out   
        
    for x in vals:
        out[np.where(lids==int(x[0]))[0]]=x[1]
    
    return out