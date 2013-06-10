# -*- coding: utf-8 -*-
"""
Created on Fri May 24 16:04:51 2013

@author: njoseph
"""

import pandas as pd
import numpy as np
import scipy.io
from datetime import *
import os as os
from lib.dbtools.connections import Connections
# import lib.dbtools.get_repository as get_repository
from lib.data.matlabutils import *
import lib.data.st_data as st_data
import lib.stats.slicer as slicer
import lib.stats.formula as formula

#--------------------------------------------------------------------------
#  FT : LOAD MATFILES OF STOCK TBT DATA
#--------------------------------------------------------------------------
def ft(**kwargs):
    #### CONFIG and CONNECT
    # TODO: in xml file
    if os.name=='nt':
        ft_root_path="Q:\\kc_repository"
    else:
        ft_root_path="/quant/kc_repository"
    
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
        
    return st_data.to_dataframe(mat['data'],timezone=True)

#--------------------------------------------------------------------------
#  ftickdb : filter and LOAD MATFILES OF STOCK TBT DATA
#--------------------------------------------------------------------------
def ftickdb(**kwargs):
    data=ft(**kwargs)
    if data.shape[0]>0:
        data=data[(data['trading_after_hours']==0) & (data['trading_at_last']==0) & (data['cross']==0)]
    return data

#--------------------------------------------------------------------------
# histocurrency 
#--------------------------------------------------------------------------
def histocurrencypair(**kwargs):
    
    out=[]
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
        # TODO:  autre que euro pour la ref
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
    pref_ = "LUIDBC01_" if Connections.connections == "dev" else  ""
        
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
# bic : basic indicatos compted
#--------------------------------------------------------------------------    
def bic(step_sec=300,exchange=False,**kwargs):
    
    ##############################################################
    # input handling
    ##############################################################
    #---- security_info
    if "security_id" in kwargs.keys():  
        ids=kwargs["security_id"]
    else:
        raise NameError('read_dataset:bic - Bad input')
    #---- date info
    if "date" in kwargs.keys():
        from_newf=datetime.strptime(kwargs["date"], '%d/%m/%Y')
        # from_newf=datetime.strftime(datetime.strptime(kwargs["date"], '%d/%m/%Y'),'%Y%m%d')
        to_newf=from_newf
    elif all([x in kwargs.keys() for x in ["start_date","end_date"]]):
        from_newf=datetime.strptime(kwargs["start_date"], '%d/%m/%Y')
        to_newf=datetime.strptime(kwargs["end_date"], '%d/%m/%Y')          
    else:
        raise NameError('read_dataset:bic - Bad input for dates') 
        
    ##############################################################
    # computation
    ##############################################################
    out=pd.DataFrame()
    curr_newf=from_newf
    while curr_newf<=to_newf:
        try:
            # --- get tick data
            datatick=ftickdb(security_id=ids,date=datetime.strftime(curr_newf, '%d/%m/%Y'))

            if datatick.shape[0]>0:
                # --- aggregate
                if not exchange:
                    #---- aggregation
                    grouped=datatick.groupby([st_data.gridTime(date=datatick.index,step_sec=step_sec,out_mode='ceil'),
                                          'opening_auction','intraday_auction','closing_auction'])
                    grouped_data=pd.DataFrame([{'date':k[0],
                                                'opening_auction':k[1],'intraday_auction':k[2],'closing_auction':k[3],
                    'time_open': v.index.max(),
                    'time_close': v.index.min(),
                    'nb_trades': np.size(v.volume),
                    'volume': np.sum(v.volume),
                    'vwap': slicer.vwap(v.price,v.volume),
                    'vwasbp': slicer.vwasbp(v.bid,v.ask,v.price,v.volume,v.auction),
                    'open': v.price[0],
                    'high': np.max(v.price),
                    'low': np.min(v.price),
                    'close': v.price[-1]} for k,v in grouped])
                    
                    grouped_data=grouped_data.set_index('date')
                    #---- after aggregation
                    grouped_data['vol_GK']=formula.vol_gk(grouped_data['open'].values,grouped_data['high'].values,grouped_data['low'].values,grouped_data['close'].values,grouped_data['nb_trades'].values,
                           np.tile(timedelta(seconds=step_sec),grouped_data.shape[0]))
                    #----- add
                    out=out.append(grouped_data)
                
        except Exception,e:
            print "%s" % e
        curr_newf=curr_newf+timedelta(days=1)
    
    return out       


if __name__=='__main__':
    # ft london stock
    data=read_dataset('ft',security_id=10735,date='11/03/2013')
    # ft french stock
    data=read_dataset('ft',security_id=110,date='11/03/2013')
    # currency rate
    data=read_dataset('histocurrencypair',start_date='01/05/2013',end_date='10/05/2013',currency=['GBX','SEK'])