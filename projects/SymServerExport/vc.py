# -*- coding: utf-8 -*-
"""
Created on Thu Sep 19 17:03:22 2013

@author: njoseph
"""

import os
import pandas as pd
import datetime as dt
import pytz
import numpy as np
import lib.dbtools.statistical_engine as se
from lib.dbtools.connections import Connections
from lib.logger import *
from lib.io.toolkit import get_traceback
from lib.data.ui.Explorer import Explorer

# format: 
#  * security_id 
#  * destination
#  * context
#  * varargin
#  * rank
#  * active
#  * id 'run_id:contex_id:domain_id:estimator_id'
#  * open_auction_end_time 
#  * closing_auction_end_time
#  * mid_auction_end_time
#  * first_continuous_slice_end_time
#  * auction_open_weight
#  * auction_close_weight
#  * auction_mid_weigh
#  * slices (max 120)

def mattime2pytime(x,out_mode='str'):
    if float(x)<0 or float(x)>1:
        raise ValueError('is not a matlab time')
    
    nb_hours=float(x)*24
    hours=int(np.floor(nb_hours))
    residu=nb_hours-hours
    minutes=0
    seconds=0
    if residu>0:
        minutes=int(np.floor(residu*60))
        residu=(residu*60-minutes)/(60)
        
    if residu>0:
        seconds=int(np.floor(residu*60*60))
        residu=(residu*60*60-seconds)/(60*60)
        
    if residu>0:
        mseconds=int(np.floor(residu*60*60*1000))
        
        if mseconds >= 500:
            if seconds == 59:
                seconds = 0
                if minutes == 59:
                    hours += 1
                    minutes = 0
                else:
                    minutes += 1
            else:
                seconds += 1
                
    out_time=dt.time(hours,minutes,seconds)
    if out_mode=='str':
        return dt.time.strftime(out_time,'%H:%M:%S')
    else:
        return out_time
        


def export_vc(vc_level=None,vc_estimator_id=2,path=None,filename=None):
    
    MAX_NB_SLICE=120
    #-------------------------------------------------------------------------
    # TEST INPUTS
    #-------------------------------------------------------------------------
    if path is None or filename is None:
        raise ValueError('bad input')
    
    if not os.path.exists(path):
        raise ValueError('path does not exist')
    
    logging.info('export_vc: START with level : %s and estimator_id %s' %(str(vc_level),str(vc_estimator_id))) 
    
    #-------------------------------------------------------------------------
    # GET and FORMAT ALL Volume Curves TO extract
    #-------------------------------------------------------------------------
    data=se.get_reference_run(estimator_id=vc_estimator_id,level=vc_level)
    
    #- 
    keep_columns=[]
    
    data['security_id']=data['security_id'].apply(lambda x : '' if (x is None or not np.isfinite(x)) else str(int(x)))
    keep_columns+=['security_id']
    
    data['EXCHANGE']=data['EXCHANGE'].apply(lambda x : 'all' if (x is None or not np.isfinite(x)) else str(int(x)))
    keep_columns+=['EXCHANGE']
    
    #- context to do in the loop
    data['context']=''
    keep_columns+=['context']
    
    data['varargin']=data['varargin'].apply(lambda x : 'null' if x is None else str(x))
    keep_columns+=['varargin']

    data['rank']=data['rank'].apply(lambda x : 'null' if x is None else str(x))
    keep_columns+=['rank']
        
    data['active']=data['active'].apply(lambda x : '1' if True else '0')
    keep_columns+=['active']
    
    data['id']=map(lambda x,y,z,a : str(int(x))+':'+str(int(y))+':'+str(int(z))+':'+str(int(a)),data['run_id'],data['context_id'],data['domain_id'],data['estimator_id'])
    keep_columns+=['id']
     
    #- add cols
    add_cols={'auction_open_closing':'',
              'auction_close_closing':'',
              'auction_mid_closing':'',
              'end_of_first_slice':'',
              'auction_open':'0',
              'auction_close':'0',
              'auction_mid':'0'}   
    
    for x in add_cols.keys():
        data[x]=add_cols[x]
        keep_columns+=[x]
        
    for x in range(1,MAX_NB_SLICE+1):
        if x<10:
            x='slice00'+str(int(x))
        elif x<100:
            x='slice0'+str(int(x))
        else:
            x='slice'+str(int(x))
        
        data[x]=''
        keep_columns+=[x]    
    
    data=data[keep_columns]
    
    #-------------------------------------------------------------------------
    # ADD CB PARAMS 
    #-------------------------------------------------------------------------    
    for i in range(0,data.shape[0]):
            
        #-- get data
        params,context=se.get_reference_param(data['context_id'].values[i],data['domain_id'].values[i],data['estimator_id'].values[i])
        
        if params.shape[0]==0:
            raise ValueError('params is empty !')
        
        #-- add context
        data['context'].values[i]=context
        
        #-- values datetime
        cols_dt=['auction_open_closing','auction_close_closing','auction_mid_closing','end_of_first_slice']
        for col in cols_dt:
            if col in params['parameter_name'].values:
                data[col].values[i]=mattime2pytime(params['value'][params['parameter_name']==col].values[0],out_mode='str')
                
        #-- other values
        for col in params['parameter_name']:
            if (col in ['auction_open','auction_close','auction_mid'] or col[0:min(len(col),5)]=='slice'):
                data[col].values[i]=str(params['value'][params['parameter_name']==col].values[0])   
       
    logging.info('export_vc : data ok in dataframe')
     
    #-------------------------------------------------------------------------
    # EXPORT IN CSV
    #--------------------------------------------------------------------------
    data.to_csv(os.path.join(path,filename+'.txt'),index=False,sep=';',header=False)
    
    logging.info('export_vc: successfull END')
    
    
if __name__ == "__main__":
    
    Connections.change_connections('production_copy')
    
    # export_vc(vc_level='generic',vc_estimator_id=2,path='C:\\',filename='test_generic')
    export_vc(vc_level='specific',vc_estimator_id=2,path='C:\\',filename='test_specific')
    