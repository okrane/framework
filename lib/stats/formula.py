# -*- coding: utf-8 -*-
"""
Created on Fri Jun 07 11:39:39 2013

@author: njoseph
"""

import pandas as pd
import numpy as np
from datetime import *
import copy
#import os as os
#from lib.dbtools.connections import Connections
#import lib.dbtools.get_repository as get_repository
#from lib.data.matlabutils import *
#import lib.data.st_data as st_data


def isfinite(x_in , notfinite = False):
    if notfinite:
        return map(lambda x :  not ((x is not None) and (x == x) and not (x == np.Inf) and not (x == - np.Inf)) , x_in)
    else:
        return map(lambda x : (x is not None) and (x == x) and not (x == np.Inf) and not (x == - np.Inf) , x_in)

def none_modifier(x_in , inplace = False):
    if not isinstance(x_in, np.ndarray) and not isinstance(x_in,list):
        raise ValueError('input should be list or ndarray')
    
    if not inplace:
        x_in = copy.deepcopy(x_in)
    
    for i in range(0,len(x_in)):
        if x_in[i] is None:
            x_in[i] = np.nan
            
    return x_in
#--------------------------------------------------------------------------
# vol_gk
#-------------------------------------------------------------------------- 
def vol_gk(o, h, l, c, nb_trades, duration):
    duration_sec=[x.total_seconds() for x in duration]
    out=np.sqrt((0.5*np.power(h-l,2) - (2*np.log(2)-1)*np.power(c-o,2) )/
    (np.power(0.5*(o+c),2)* duration_sec/ 600)  ) * np.power(10,4)
    id_nan=map(lambda x : ((x[0]==0) and ((x[1]<20) or (x[2]<10*60))) or (x[1]==0) or (x[2]<5*60),
        np.vstack((out,nb_trades,duration_sec)).T)
    if any(id_nan):
        out[np.nonzero(id_nan)[0]]=np.nan
    return out
       


#
#    
#        
#    indicators={'data' : 0,
#                'volume_lit' : 0,
#                'volume_lit_main' : 0, # include in volume_lit
#                'volume_opening' : 0, # include in volume_lit
#                'volume_closing' : 0, # include in volume_lit
#                'volume_intraday' : 0, # include in volume_lit
#                'volume_other_auctions' : 0, # include in volume_lit
#                'volume_dark' : 0,
#                'volume_lit_constr' : 0,
#                'volume_lit_main_constr' : 0, # include in volume_lit_constr
#                'volume_dark_constr' : 0,
#                'turnover_lit' : 0,
#                'turnover_lit_main' : 0, # include in turnover_lit
#                'turnover_dark' : 0,
#                'turnover_lit_constr' : 0,
#                'turnover_lit_main_constr' : 0, # include in turnover_lit_constr
#                'turnover_dark_constr' : 0,
#                'nb_trades_lit_cont' : 0,
#                'nb_trades_lit_cont_main' : 0,
#                'open' : np.nan,
#                'high' : np.nan,
#                'low' : np.nan,
#                'close' : np.nan,
#                'vwas' : np.nan,
#                'vwas_main' : np.nan,
#                'vol_GK':np.nan}

    
      

if __name__ == "__main__":
    import lib.dbtools.read_dataset as read_dataset
    step_sec=300
    data=read_dataset.bic(security_id=110,step_sec=step_sec,date='03/05/2013')
    a=vol_gk(data['open'].values,data['high'].values,data['low'].values,data['close'].values,data['nb_trades'].values,
           np.tile(timedelta(seconds=step_sec),data.shape[0]))

    
    
    
    