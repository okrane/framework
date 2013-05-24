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


def get_repository(mode, **kwargs):
    
    out=[]
    
    #--------------------------------------------------------------------------
    # MODE : local_tz_from
    #--------------------------------------------------------------------------
    if (mode=="local_tz_from"):

        #### Build the request
        if "security_id" in kwargs.keys():  
            
            # get/transform inputs
            lids=kwargs["security_id"]
            if isint(lids):
                lids=np.array([lids])  
            elif isinstance(lids,list):
                lids=np.array(lids)
                
            out=np.array([None]*lids.size)

            # construct request
            str_lids="("+"".join([str(x)+',' for x in lids])
            str_lids=str_lids[:-1]+")"
            pref_=""
            if Connections.connections=="dev":
                pref_="LUIDBC01_"
            
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
    #--------------------------------------------------------------------------
    # OTHERWISE
    #--------------------------------------------------------------------------
    else:
        raise NameError('get_repository:mode - Unknown mode <'+mode+'>')



if __name__=='__main__':
    get_repository("local_tz_from",security_id=[2,110,6069,0,110])
