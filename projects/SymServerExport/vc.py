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
    data=data[keep_columns]
    data.to_csv(os.path.join(path,filename),index=False,sep=';',header=False)
    
    logging.info('export_vc: successfull END')
    
    
    
    
def export_vc_specific(data_security_referential = None,
                   path_export = None, filename_export = None,
                   separator = '\t'):

    #--------------------
    #- TEST INPUT
    #--------------------
    if data_security_referential is None or path_export is None or filename_export is None:
        raise ValueError('bad inputs')
        
    if not os.path.exists(path_export):
        raise ValueError('path not defined')
        
    if data_security_referential.shape[0]==0:
        raise ValueError('no security ref')
        
    logging.info('START export_vc_specific')
    
    
    #-------------------------------------------------------------------------
    # GET ALL Volume Curves + sec ids
    #-------------------------------------------------------------------------
    all_sec_ids = np.unique(data_security_referential['cheuvreux_secid'].values)
    data = se.get_reference_run(estimator_id=2,level='specific')
    data = data.sort(columns = ['security_id','EXCHANGE','rank'])
    data['id']=map(lambda x,y,z,a : str(int(x))+':'+str(int(y))+':'+str(int(z))+':'+str(int(a)),data['run_id'],data['context_id'],data['domain_id'],data['estimator_id'])
    
    DEFAULT_VAL= - 999
    NB_CONT_SLICE = 102
    
    #-------------------------------------------------------------------------
    # ADD CB PARAMS 
    #-------------------------------------------------------------------------  
    id_not_added = []  
    out = open( os.path.join(path_export, filename_export), 'w' )
    
    last_secid = DEFAULT_VAL
    last_tdid = DEFAULT_VAL
    
    for i in range(0,data.shape[0]):
        
        #-------------------
        #- input and needed
        #-------------------
        sec_id = int(data['security_id'].values[i])
        tdid = data['EXCHANGE'].values[i]
        if tdid is None or not np.isfinite(tdid):
            tdid = DEFAULT_VAL
            
        # -- restrict to known security_id
        if data['security_id'].values[i] not in all_sec_ids:
            continue
        
        # -- keep lowest rank (data is already sorted this way)
        if last_secid == sec_id and last_tdid == tdid:
            continue
        
        # -- needed
        this_sec_info = data_security_referential[ data_security_referential['cheuvreux_secid'] ==  sec_id]
        symbol = this_sec_info['ticker'].values[0]
        if tdid == DEFAULT_VAL:
            symbol = this_sec_info['tickerAG'].values[0]
            
        if symbol is None or not isinstance(symbol,basestring):
            continue
              
        #-- get  data
        params,context=se.get_reference_param(data['context_id'].values[i],data['domain_id'].values[i],data['estimator_id'].values[i])
        
        if params.shape[0]==0:
            raise ValueError('statistical engine issue, params is empty for ' + data['id'].values[i])
        
        #-------------------
        #- get curve values
        #-------------------
        
        #  format : symbol + opening auction + closing auction  + continuous bucket (103) + intraday auction 
        all_values= [0.0] * (NB_CONT_SLICE + 3)
        if 'auction_open' in params['parameter_name'].values:
            all_values[0] = params['value'][params['parameter_name'] == 'auction_open'].values[0]
            
        if 'auction_close' in params['parameter_name'].values:
            all_values[1] = params['value'][params['parameter_name'] == 'auction_close'].values[0]
            
        if 'auction_mid' in params['parameter_name'].values:
            all_values[-1] = params['value'][params['parameter_name'] == 'auction_mid'].values[0]
        
        max_num = -1
        for col in params['parameter_name']:
            if col[0:min(len(col),5)] == 'slice':
                num = int(col[5:len(col)])
                all_values[num + 1] = params['value'][params['parameter_name'] == col].values[0]
                max_num = np.max([max_num,num])
                
        #-- check  data : number of slice continuous
        if max_num > NB_CONT_SLICE:
            id_not_added.append(data['id'].values[i])
            logging.error('curve ' + data['id'].values[i] + 'has more than the required nb slice')
            continue
            
        #-- check  data : number of slice continuous
        if np.sum(all_values) < 0.999 or np.sum(all_values) > 1.001:
            id_not_added.append(data['id'].values[i])
            logging.error('curve ' + data['id'].values[i] + 'do not sum at 1')
            continue
                            
        #-------------------
        #-- write str
        #-------------------
        # default values
        add_str = symbol + separator
        add_str += "".join([str(x) + separator for x in all_values])[:-1]
        add_str += '\n'
        
        out.write(add_str)
        
        #-- for next loop
        last_tdid = tdid
        last_secid = sec_id
    
    logging.info('export_vc_specific: successfull END')
     
    return id_not_added
    
  

def export_vc_generic(data_exchange_referential = None,
                   path_export = None, filename_export = None,
                   separator = ':'):

    #--------------------
    #- TEST INPUT
    #--------------------
    if data_exchange_referential is None or path_export is None or filename_export is None:
        raise ValueError('bad inputs')
        
    if not os.path.exists(path_export):
        raise ValueError('path not defined')
        
    if data_exchange_referential.shape[0]==0:
        raise ValueError('no security ref')
        
    logging.info('START export_vc_generic')
    
    
    #-------------------------------------------------------------------------
    # GET ALL Volume Curves + sec ids
    #-------------------------------------------------------------------------
    data = se.get_reference_run(estimator_id=2,level='generic')
    data = data.sort(columns = ['varargin','EXCHANGE','rank'])
    data['id']=map(lambda x,y,z,a : str(int(x))+':'+str(int(y))+':'+str(int(z))+':'+str(int(a)),data['run_id'],data['context_id'],data['domain_id'],data['estimator_id'])
    
    
    NB_CONT_SLICE = 102
    
    #-------------------------------------------------------------------------
    # ADD CB PARAMS 
    #-------------------------------------------------------------------------  
    id_not_added = []  
    out = open( os.path.join(path_export, filename_export), 'w' )
    
    last_cotg = ''
    last_exchname = ''
    
    for i in range(0,data.shape[0]):
        
        #-------------------
        #- input and needed
        #-------------------
        # -- cotation group
        cotg = data['varargin'].values[i]
        if cotg is None or not isinstance(cotg,basestring):
            cotg='AG'
        
        # - td name
        exchname = ''
        exchid = data['EXCHANGE'].values[i]
        if exchid is not None and np.isfinite(exchid):
            if np.any(data_exchange_referential['EXCHANGE'] == exchid):
                exchname = data_exchange_referential[data_exchange_referential['EXCHANGE'] == exchid]['SUFFIX'].values[0]
            
        # -- restrict to known exchange
        if exchname == '':
            continue
        
        # -- keep lowest rank (data is already sorted this way)
        if last_exchname == exchname and last_cotg == cotg:
            continue
        
        #-- get  data
        params,context=se.get_reference_param(data['context_id'].values[i],data['domain_id'].values[i],data['estimator_id'].values[i])
        
        if params.shape[0] == 0:
            raise ValueError('statistical engine issue, params is empty for ' + data['id'].values[i])
        
        #-------------------
        #- get curve values
        #-------------------
        
        #  format : symbol + opening auction + closing auction  + continuous bucket (102) + intraday auction 
        all_values= [0] * (NB_CONT_SLICE + 3)
        if 'auction_open' in params['parameter_name'].values:
            all_values[0] = params['value'][params['parameter_name'] == 'auction_open'].values[0]
            
        if 'auction_close' in params['parameter_name'].values:
            all_values[1] = params['value'][params['parameter_name'] == 'auction_close'].values[0]
            
        if 'auction_mid' in params['parameter_name'].values:
            all_values[-1] = params['value'][params['parameter_name'] == 'auction_mid'].values[0]
        
        max_num = -1
        for col in params['parameter_name']:
            if col[0:min(len(col),5)] == 'slice':
                num = int(col[5:len(col)])
                all_values[num + 1] = params['value'][params['parameter_name'] == col].values[0]
                max_num = np.max([max_num,num])
                
        #-- check  data : number of slice continuous
        if max_num > NB_CONT_SLICE:
            id_not_added.append(data['id'].values[i])
            logging.error('curve ' + data['id'].values[i] + 'has more than the required nb slice')
            continue
            
        #-- check  data : number of slice continuous
        if np.sum(all_values) < 0.999 or np.sum(all_values) > 1.001:
            id_not_added.append(data['id'].values[i])
            logging.error('curve ' + data['id'].values[i] + 'do not sum at 1')
            continue
                            
        #-------------------
        #-- write str
        #-------------------
        add_str = exchname + '_' + cotg + ";0:0:0:0:103:0:" + str(all_values[0]) + separator + str(all_values[1]) + " "
        add_str += "".join([separator if x == 0.0 else str(x) + separator for x in all_values[2:-1]])
        add_str += str(all_values[-1]) + ": 0"  + '\n'
        
        out.write(add_str)
        
        #-- for next loop
        last_exchname = exchname
        last_cotg = cotg
    
    logging.info('export_vc_generic: successfull END')
     
    return id_not_added


 
if __name__ == "__main__":
    
    Connections.change_connections('production_copy')
    
    # export_vc(vc_level='generic',vc_estimator_id=2,path='C:\\',filename='test_generic')
    # export_vc(vc_level='specific',vc_estimator_id=2,path='C:\\',filename='test_specific')
    
#     #-- security test
#     
#     security_ref = pd.read_csv(os.path.join('C:\\export_sym\\extract', 'TRANSCOSYMBOLCHEUVREUX.csv'),sep = ';')
#     security_ref = security_ref[['cheuvreux_secid', 'ticker', 'tickerAG']]
#     
#     export_vc_specific(data_security_referential = security_ref,
#                    path_export = 'H:\\', 
#                    filename_export = 'test.txt',
#                    separator = '\t')
    
    #-- security test
    exchange_ref = pd.read_csv(os.path.join('C:\\export_sym\\extract', 'ref_trd_destination.csv'))
                    
    res = export_vc_generic(data_exchange_referential = exchange_ref,
                   path_export = 'H:\\', 
                   filename_export = 'test_specific.txt',
                   separator = ':')
    print res
    
    