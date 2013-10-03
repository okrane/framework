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
    NB_MIN_CURVES = 100
    
    #-------------------------------------------------------------------------
    # ADD CB PARAMS 
    #-------------------------------------------------------------------------  
    cb_id_error = []
    cb_added = []
    
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
            out.close()
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
            cb_id_error.append(symbol + ':' + data['id'].values[i])
            logging.error('curve ' + data['id'].values[i] + 'has more than the required nb slice')
            continue
            
        #-- check  data : number of slice continuous
        if np.sum(all_values) < 0.999 or np.sum(all_values) > 1.001:
            cb_id_error.append(symbol + ':' + data['id'].values[i])
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
        cb_added.append(symbol)
        
        #-- for next loop
        last_tdid = tdid
        last_secid = sec_id
    
    out.close()
    logging.info('export_vc_specific: successfull END')
    
    #-------------------------------------------------------------------------
    # CHECK ADDED CURVES
    #-------------------------------------------------------------------------
    if len(cb_added) < NB_MIN_CURVES:
        raise ValueError('generated file contain less than' + str(NB_MIN_CURVES) + ' curves')
    
    return cb_added , cb_id_error
    
  

def export_vc_generic(data_exchange_referential = None,
                   path_export = None, filename_export = None,
                   separator = ':'):
    
    #--------------------
    #- WRITE FORMATTER
    #--------------------
    def format_line(exchname,cotg,all_values,separator):
        add_str = exchname + '_' + cotg + ";0:0:0:0:103:0:" + str(all_values[0]) + separator + str(all_values[1]) + " "
        add_str += "".join([separator if x is None else str(x) + separator for x in all_values[2:-1]])
        add_str += str(all_values[-1]) + ": 0"  + '\n'
        return add_str
        
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
    NB_MIN_CURVES = 10
    
    #-------------------------------------------------------------------------
    # ADD CB PARAMS 
    #-------------------------------------------------------------------------  
    cb_id_error = [] 
    all_cb_added = {}
    cb_added = []
    cb_added_backup = []
    cb_not_added = []
    
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
            out.close()
            raise ValueError('statistical engine issue, params is empty for ' + data['id'].values[i])
        
        #-------------------
        #- get curve values
        #-------------------
        
        #  format : symbol + opening auction + closing auction  + continuous bucket (102) + intraday auction 
        all_values= [ None ] * (NB_CONT_SLICE + 2) + [0]
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
            cb_id_error.append(exchname + '_' + cotg + ':' + data['id'].values[i])
            logging.error('curve ' + exchname + '_' + cotg + ' ' +  data['id'].values[i] + 'has more than the required nb slice')
            continue
            
        #-- check  data : number of slice continuous
        if np.sum([ 0 if x is None else x for x in all_values]) < 0.999 or np.sum([ 0 if x is None else x for x in all_values]) > 1.001:
            cb_id_error.append(exchname + '_' + cotg + ':' + data['id'].values[i])
            logging.error('curve ' + exchname + '_' + cotg + ' ' + data['id'].values[i] + 'do not sum at 1')
            continue
                   
        #-------------------
        #-- write str
        #-------------------
        out.write( format_line( exchname, cotg, all_values, separator ) )
        
        all_cb_added.update({exchname + '_' + cotg : all_values})
        
        #-- for next loop
        last_exchname = exchname
        last_cotg = cotg
    
    out.close()
    
    #-------------------------------------------------------------------------
    # CHECK ADDED CURVES
    #-------------------------------------------------------------------------
    cb_added = all_cb_added.keys()
    if len(cb_added) < NB_MIN_CURVES:
        raise ValueError('generated file contain less than' + str(NB_MIN_CURVES) + ' curves')
        
    #-------------------------------------------------------------------------
    # BIDOUILLE : BACKUP CURVES
    #-------------------------------------------------------------------------
    # we need at least 1 '_AG' by market, we use some backup curves computed the 30 August 2013

    out = open( os.path.join(path_export, filename_export), 'a' )
    
    NEEDED_AG_CURVE = np.setdiff1d(np.unique(data_exchange_referential['SUFFIX'].tolist()) ,np.array(['AG','IX','TQ','ED','EB']))
    
    CB_BACKUP = { 'TI_AG' : [0.010126,0.067265,0.033511,0.025856,0.023893,0.015985,0.01671,0.013196,0.013484,0.009124,0.009952,0.010469,0.008524,0.008829,0.008845,0.008026,
                             0.006787,0.0075,0.006396,0.00654,0.006959,0.00498,0.00409,0.004217,0.005876,0.003626,0.005951,0.005141,0.003186,0.003529,0.003016,0.00689,
                             0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0.004575,0,0.0345,0.023137,0.021651,0.016497,0.01441,0.012755,0.012146,0.010932,0.009695,0.009135,
                             0.009112,0.008613,0.00785,0.005905,0.006293,0.010467,0.007399,0.009876,0.010512,0.010201,0.009415,0.011136,0.008703,0.010454,0.011598,
                             0.01074,0.010843,0.010681,0.009414,0.012388,0.012822,0.014224,0.016934,0.015702,0.019943,0.020746,0.022893,0.02527,0.049375,0.032279,
                             None,None,None,None,None,None,None,None,None,None,0.040302],
                 
                 'SJ_AG' : [0.001096,0.1773,0.003804,0.003576,0.004366,0.004575,0.004867,0.005136,0.005478,0.005398,0.005892,0.005247,0.00647,0.008158,0.014102,0.011561,
                            0.011149,0.010875,0.010431,0.011065,0.011332,0.010338,0.010382,0.010034,0.009915,0.010206,0.009884,0.010198,0.00975,0.009021,0.009355,0.009482,
                            0.010216,0.009058,0.008849,0.008596,0.008138,0.008344,0.008558,0.007723,0.008454,0.007857,0.007386,0.008316,0.007162,0.007003,0.007372,0.006671,
                            0.006954,0.007302,0.00719,0.006488,0.00702,0.006828,0.006639,0.005892,0.00619,0.006546,0.006642,0.006032,0.006148,0.006614,0.007304,0.006711,
                            0.006586,0.006964,0.007098,0.006754,0.007455,0.007649,0.007272,0.007423,0.008415,0.007919,0.008454,0.008685,0.007964,0.007953,0.008673,0.009748,
                            0.012295,0.012401,0.012507,0.012613,0.012719,0.012826,0.012921,0.013016,0.013111,0.013206,0.013302,0.013363,0.013424,0.013486,0.013547,0.013609,
                            0.013609,0.013609,None,None,None,None,None,None,0],
                 
                 'GA_AG' : [0.015599,0.060095,0.028998,0.032211,0.026341,0.023761,0.022322,0.021314,0.021141,0.015777,0.015232,0.013916,0.014612,0.015763,0.01432,0.01578,
                            0.013817,0.01233,0.012715,0.013027,0.011169,0.01022,0.009851,0.00914,0.008515,0.009333,0.008492,0.007894,0.008499,0.00757,0.009679,0.011266,
                            0.00871,0.007907,0.007753,0.008142,0.007625,0.008302,0.00894,0.007384,0.008908,0.009218,0.007488,0.009035,0.007048,0.007853,0.006905,0.007873,
                            0.007021,0.007601,0.006098,0.007879,0.008045,0.006067,0.008198,0.010684,0.007115,0.006759,0.008107,0.005952,0.006379,0.007196,0.008316,0.009467,
                            0.008217,0.009294,0.01078,0.009786,0.00987,0.010938,0.013424,0.012481,0.012781,0.011372,0.013053,0.015789,0.017512,0.018855,0.022068,0.033104,
                            None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,0] }

    
    for cb in NEEDED_AG_CURVE:
        if cb + '_AG' not in all_cb_added.keys():
            if cb in [x.split('_')[0] for x in cb_added]:
                # CASE 1 : there is a mono curve for this exchange, we take it
                for x in cb_added:
                    if x.split('_')[0] == cb:
                        replace_cb = x
                        break
                    
                out.write( format_line( replace_cb.split('_')[0], 'AG', all_cb_added[replace_cb], separator ) )
                cb_added_backup.append(cb + '_AG')
                
            elif cb + '_AG' in CB_BACKUP.keys():
                # CASE 2 : backup sale
                out.write( format_line( cb, 'AG', CB_BACKUP[cb + '_AG'], separator ) )
                cb_added_backup.append(cb + '_AG')
                
            else:
                cb_not_added.append(cb + '_AG')                
                
    out.close()
          
    logging.info('export_vc_generic: successfull END')
    
    return cb_added , cb_added_backup , cb_not_added , cb_id_error


 
if __name__ == "__main__":
    
    Connections.change_connections('production_copy')
    
    # export_vc(vc_level='generic',vc_estimator_id=2,path='C:\\',filename='test_generic')
    # export_vc(vc_level='specific',vc_estimator_id=2,path='C:\\',filename='test_specific')
    
    #-- security test
     
    security_ref = pd.read_csv(os.path.join('C:\\export_sym\\backup', 'TRANSCOSYMBOLCHEUVREUX.csv'),sep = ';')
    security_ref = security_ref[['cheuvreux_secid', 'ticker', 'tickerAG']]
     
    res = export_vc_specific(data_security_referential = security_ref,
                   path_export = 'H:\\', 
                   filename_export = 'test.txt',
                   separator = '\t')
    
    print res
    
    
#     #-- security test
#     exchange_ref = pd.read_csv(os.path.join('C:\\export_sym\\backup', 'ref_trd_destination.csv'))
#                     
#     res = export_vc_generic(data_exchange_referential = exchange_ref,
#                    path_export = 'H:\\', 
#                    filename_export = 'test_specific.txt',
#                    separator = ':')
#     print res
    
    