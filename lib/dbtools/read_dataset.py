# -*- coding: utf-8 -*-
"""
Created on Fri May 24 16:04:51 2013

@author: njoseph
"""

import pandas as pd
import scipy.io
from datetime import *
import os as os
from lib.dbtools.connections import Connections
from lib.dbtools.get_repository import *
from lib.data.matlabutils import *
from lib.data.st_data import *


def read_dataset(mode, **kwargs):
    
    #### CONFIG and CONNECT
    # TODO: in xml file
    ft_root_path="Q:\\kc_repository"
    # a trouver dans Connections
    
    #### DEFAULT OUTPUT    
    out=[]
    
    #--------------------------------------------------------------------------
    #  FT : LOAD MATFILES OF STOCK TBT DATA
    #--------------------------------------------------------------------------
    if (mode=="ft"):
        ##############################################################
        # input handling
        ##############################################################
        #---- security_info
        if "security_id" in kwargs.keys():  
            ids=kwargs["security_id"]
        else:
            raise NameError('read_dataset:ft - Bad input')
        #---- date info
        if "date" in kwargs.keys():
            date=kwargs["date"]
            date_newf=datetime.strftime(datetime.strptime(date, '%d/%m/%Y'),'%Y_%m_%d') 
        else:
            raise NameError('read_dataset:ft - Bad input')        
        
        ##############################################################
        # load and format
        ##############################################################
        filename=os.path.join(ft_root_path,'get_tick','ft','%d'%(ids),'%s.mat'%(date_newf))
        try:
            mat = scipy.io.loadmat(filename, struct_as_record  = False)
        except:
            raise NameError('read_dataset:ft - This file does not exist <'+filename+'>')  
            
        out=to_dataframe(mat['data'],timezone=True)
    
    #--------------------------------------------------------------------------
    # MODE : histocurrency 
    #--------------------------------------------------------------------------
    elif (mode=="histocurrencypair"):
        ##############################################################
        # input handling
        ##############################################################
        #---- date info
        if "date" in kwargs.keys():
            from_newf=datetime.strftime(datetime.strptime(kwargs["date"], '%d/%m/%Y'),'%Y%m%d')
            to_newf=from_newf
        elif all([x in kwargs.keys() for x in ["start_date","end_date"]]):
            from_newf=datetime.strftime(datetime.strptime(kwargs["start_date"], '%d/%m/%Y'),'%Y%m%d')
            to_newf=datetime.strftime(datetime.strptime(kwargs["end_date"], '%d/%m/%Y'),'%Y%m%d')           
        else:
            raise NameError('read_dataset:histocurrencypair - Bad input for dates') 
            
        #---- currencyref info
        if "currency_ref" in kwargs.keys():
            raise NameError('read_dataset:histocurrencypair - currency_ref not available NOW') 
            # PAS POUR l'INSTANT !
#            curr_ref=kwargs["currency_ref"]   
#            if isinstance(curr_ref,basestring):
#                curr_ref=[curr_ref]
#            elif (not isinstance(curr_ref,list)) or (not isinstance(curr_ref,np.ndarray)):
#                raise NameError('read_dataset:histocurrencypair - Bad input for currency ref') 
        else:
            curr_ref=['EUR']
        str_curr_ref="("+"".join(["'"+x+"'," for x in curr_ref])
        str_curr_ref=str_curr_ref[:-1]+")"
        
        #---- currency info
        if "currency" in kwargs.keys():
            curr=kwargs["currency"]   
            if isinstance(curr,basestring):
                curr=[curr]
            elif (not isinstance(curr,list)) and (not isinstance(curr,np.ndarray)):
                raise NameError('read_dataset:histocurrencypair - Bad input for currency ref')
            str_curr="("+"".join(["'"+x+"'," for x in curr])
            str_curr=str_curr[:-1]+")" 
        else:
            curr=[] 
   
        ##############################################################
        # request and format
        ##############################################################
        pref_=""
        if Connections.connections=="dev":
            pref_="LUIDBC01_"
            
        ####  construct request    
        req=(" SELECT "
            " DATE,CCY,CCYREF,VALUE "
            " FROM "
                " %sKGR..HISTOCURRENCYTIMESERIES "
            " WHERE "
                " DATE>= '%s' "
                " and DATE<= '%s' "
                " and SOURCEID=1 "
                " and ATTRIBUTEID=43 "
                " and CCYREF in %s ") % (pref_,from_newf,to_newf,str_curr_ref)
        if not (not curr):
            req=req+((" and CCY in %s ") % (str_curr))
                
        #### EXECUTE REQUEST 
        vals=Connections.exec_sql('KGR',req)
         
        ####  OUTPUT 
        if not vals:
            return out   
            
        return pd.DataFrame.from_records(vals,columns=['date','ccy','ccyref','rate'],index=['date'])
                
       
    #--------------------------------------------------------------------------
    # OTHERWISE
    #--------------------------------------------------------------------------
    else:
        raise NameError('read_dataset:mode - Unknown mode <'+mode+'>')
        
    return out

if __name__=='__main__':
    # ft london stock
    data=read_dataset('ft',security_id=10735,date='11/03/2013')
    # ft french stock
    data=read_dataset('ft',security_id=110,date='11/03/2013')
    # currency rate
    data=read_dataset('histocurrencypair',start_date='01/05/2013',end_date='10/05/2013',currency=['GBX','SEK'])