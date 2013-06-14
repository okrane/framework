# -*- coding: utf-8 -*-
"""
Created on Fri Jun 07 11:39:39 2013

@author: njoseph
"""

import pandas as pd
import numpy as np
from datetime import *
#import os as os
#from lib.dbtools.connections import Connections
#import lib.dbtools.get_repository as get_repository
#from lib.data.matlabutils import *
#import lib.data.st_data as st_data



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
        

if __name__ == "__main__":
    import lib.dbtools.read_dataset as read_dataset
    step_sec=300
    data=read_dataset.bic(security_id=110,step_sec=step_sec,date='03/05/2013')
    a=vol_gk(data['open'].values,data['high'].values,data['low'].values,data['close'].values,data['nb_trades'].values,
           np.tile(timedelta(seconds=step_sec),data.shape[0]))

    
    
    
    