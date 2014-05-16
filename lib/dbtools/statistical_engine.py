# -*- coding: utf-8 -*-
"""
Created on Thu Sep 19 14:27:16 2013

@author: njoseph
"""

import pandas as pd
import datetime as dt
import pytz
import numpy as np
from lib.dbtools.connections import Connections
from lib.logger import *
from lib.io.toolkit import get_traceback


##------------------------------------------------------------------------------
# get_reference_run
#------------------------------------------------------------------------------
def get_reference_run(estimator_id=None,level=None):
    
    out=pd.DataFrame()
    
    if estimator_id == 2:
        
        #-- query
        if level is None:
            raise ValueError('level is mandatory')
            
        elif level == 'specific':
            query=""" SELECT context_id, domain_id, estimator_id, security_id, EXCHANGE, rank, active, default_context, run_id, varargin 
                 FROM QUANT..quant_reference 
                 WHERE security_id is not null and estimator_id = %d and active = 1
                 ORDER BY security_id, EXCHANGE, rank """  % (estimator_id)
                
        elif level == 'generic':
            query=""" SELECT context_id, domain_id, estimator_id, security_id, EXCHANGE, rank, active, default_context, run_id, varargin 
                 FROM QUANT..quant_reference 
                 WHERE security_id is null and estimator_id = %d and active = 1
                 ORDER BY varargin, EXCHANGE, rank """ % (estimator_id)
                     
        #-- query
        vals = Connections.exec_sql('QUANT',query,schema = True)
        if not vals[0]:
            logging.info('nothing in quant_reference')
            return out
            
        out = pd.DataFrame.from_records(vals[0],columns=vals[1])
        
    else:
        
        raise ValueError('bad input estimator_id')
    
    return out

##-----------------------------------------------------------------------------
# get_reference_param
#------------------------------------------------------------------------------
def get_reference_param(context_id,domain_id,estimator_id):
    
    params=pd.DataFrame()
    context_name=None
    
    #-- params
    query=""" SELECT parameter_name, value, x_value
    FROM QUANT..quant_param 
    WHERE context_id = %d and domain_id = %d and estimator_id = %d """ % (context_id,domain_id,estimator_id)
            
    vals = Connections.exec_sql('QUANT',query,schema = True)
    
    if not vals[0]:
        logging.info('nothing in quant_param')
    else:
        params = pd.DataFrame.from_records(vals[0],columns=vals[1])            
    
    #-- params
    query=""" SELECT context_name
            FROM QUANT..context 
            WHERE context_id = %d and estimator_id = %d """ % (context_id,estimator_id)
            
    vals = Connections.exec_sql('QUANT',query,schema = True)
    
    if not vals[0]:
        logging.info('nothing in context')
    else:
        context_name = vals[0][0][0]
    
    return params,context_name

##------------------------------------------------------------------------------
# check database update
#------------------------------------------------------------------------------       
def check_db_update(date):
    
    out = False
    
    try:
        
        date_s = dt.datetime.strftime(date.date(),'%Y%m%d')
        date_e = dt.datetime.strftime((date + dt.timedelta(days=1)).date(),'%Y%m%d')
        
        query=""" SELECT date , jobname ,status
                  FROM QUANT..upd_quant_data_report
                  WHERE date >= '%s' and date < '%s' """ % (date_s,date_e)
                  
        vals = Connections.exec_sql('QUANT',query,schema = True)
        if not vals[0]:
            logging.warning('No info of curve update for date : ' + date_s)
        else:
            data = pd.DataFrame.from_records(vals[0],columns=vals[1])
            #-- upd_quant_data
            if any(data['jobname'] == 'upd_quant_data'):
                out = data[data['jobname'] == 'upd_quant_data']['status'].values[0] == 'O'
                
            #--- quant_reference_update_context
            if out and any(data['jobname'] == 'quant_reference_update_context'):
                out = data[data['jobname'] == 'quant_reference_update_context']['status'].values[0] == 'O'
                
            if not out:
                logging.warning('Curve database has not been update properly for date : ' + date_s) 
               
    except:
        logging.error('error in check_db_update func')
    
    return out
   
   
##-----------------------------------------------------------------------------
# param curve 2 df
 #------------------------------------------------------------------------------   
def paramscurve_to_df(params_curve = None,slice_duration_min = 5):
    
    # TODO : handle timezone....
    
    #---------------
    #- TEST
    if params_curve is None:
        raise ValueError('bad inputs')
        
    if params_curve.shape[0] == 0:
        return
        
    out = pd.DataFrame()
    
    #-- end of first slice time
    nb_seconds = round((params_curve['value'][params_curve['parameter_name'] == 'end_of_first_slice'].values[0])*24*60*60)
    eofs = (dt.datetime(2014,1,1,0,0,0) + dt.timedelta(seconds = nb_seconds)).timetz()
    
    #-- slices
    slices = params_curve['parameter_name'].values
    slices = np.sort(slices[np.nonzero([x[:min(5,len(x))]=='slice' for x in slices])[0]]).tolist()
    
    for i in range(0,len(slices)):
        out = out.append(pd.DataFrame([{'begin_slice' : (dt.datetime.combine(dt.datetime(2014,1,1,0,0,0),eofs) + dt.timedelta(seconds = (i-1)*slice_duration_min*60)).timetz(),
                                  'end_slice' : (dt.datetime.combine(dt.datetime(2014,1,1,0,0,0),eofs) + dt.timedelta(seconds = (i)*slice_duration_min*60)).timetz(),
                                  'proportion' : params_curve['value'][params_curve['parameter_name'] == slices[i]].values[0]}]))
    
    
    
    
    #-- opening auction
    out['opening_auction'] = 0
    out['intraday_auction'] = 0
    out['closing_auction'] = 0
    
    auctions = ['open','mid','close']
    
    for _aa in  auctions:
        
        if 'auction_' + _aa + '_closing' in params_curve['parameter_name'].values.tolist():
            value = 0
            atime = round((params_curve['value'][params_curve['parameter_name'] == 'auction_' + _aa + '_closing'].values[0])*24*60*60)
            atime = (dt.datetime(2014,1,1,0,0,0) + dt.timedelta(seconds = atime)).timetz()
            
            if 'auction_' + _aa in params_curve['parameter_name'].values.tolist():
                value = params_curve['value'][params_curve['parameter_name'] == 'auction_' + _aa].values[0]
            
            if _aa == 'open':
                #-- first slice begin time to change
                out.begin_slice.values[0] = atime
                #-- update the dataframe
                out = out.append(pd.DataFrame([{'begin_slice' : atime,
                                                'end_slice' : atime,
                                                'proportion' : value,
                                                'opening_auction' : 1,
                                                'intraday_auction' : 0,
                                                'closing_auction' : 0}]))
            elif _aa == 'mid':
                #-- update the dataframe
                out = out.append(pd.DataFrame([{'begin_slice' : atime,
                                                'end_slice' : atime,
                                                'proportion' : value,
                                                'opening_auction' : 0,
                                                'intraday_auction' : 1,
                                                'closing_auction' : 0}]))
            elif _aa == 'close':
                #-- update the dataframe
                out = out.append(pd.DataFrame([{'begin_slice' : atime,
                                                'end_slice' : atime,
                                                'proportion' : value,
                                                'opening_auction' : 0,
                                                'intraday_auction' : 0,
                                                'closing_auction' : 1}]))
                                                
    
    out.set_index('end_slice', drop = False , inplace = True)
    out.sort_index(inplace = True)
    
    return out


                                        

 
                                        
        
         



if __name__ == "__main__":
    
    Connections.change_connections('production')
    # -- check
    #print check_db_update(dt.datetime(2013,10,8))
    
    print get_reference_run(estimator_id=2,level='generic')
    
#     all_runs=get_reference_run(estimator_id=2,level='specific')
#     print all_runs
#     
#     params,context_name=get_reference_param(all_runs.iloc[0]['context_id'],all_runs.iloc[0]['domain_id'],all_runs.iloc[0]['estimator_id'])
#     print context_name
    
    
      
    
            
        
    




