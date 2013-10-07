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

##------------------------------------------------------------------------------
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
# check update
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
            
            out = data[data['jobname'] == 'upd_quant_data']['status'].values[0] == 'O' and data[data['jobname'] == 'quant_reference_update_context']['status'].values[0] == 'O'
            
            if not out:
                logging.warning('Error in curve update for date : ' + date_s) 
               
    except:
        logging.error('error in check_db_update func')
    
    return out
   
if __name__ == "__main__":
    
    Connections.change_connections('production_copy')
    # -- check
    print check_db_update(dt.datetime(2013,10,07))
    
#     all_runs=get_reference_run(estimator_id=2,level='specific')
#     print all_runs
#     
#     params,context_name=get_reference_param(all_runs.iloc[0]['context_id'],all_runs.iloc[0]['domain_id'],all_runs.iloc[0]['estimator_id'])
#     print context_name
    
    
      
    
            
        
    




