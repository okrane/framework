# -*- coding: utf-8 -*-
"""
Created on Fri Jun 07 15:00:06 2013

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
# vwap
#-------------------------------------------------------------------------- 
def vwap(price,size):
    return np.sum(price*size)/np.sum(size)

#--------------------------------------------------------------------------
# vwas
#-------------------------------------------------------------------------- 
def vwas(bid,ask,price,size,auction):
    #--- default
    out =np.nan
    #--- compute
    idx_ok=np.nonzero(map(lambda x : (x[2]==0) and (x[0]>0) and (x[1]>0) and (x[1]>x[0]),
        np.vstack((bid,ask,auction)).T))[0]
    if idx_ok.shape[0]>0:
        out=np.sum((ask[idx_ok]-bid[idx_ok])*size[idx_ok])/np.sum(size[idx_ok])
    #--- return
    return out

#--------------------------------------------------------------------------
# vwasbp
#-------------------------------------------------------------------------- 
def vwasbp(bid,ask,price,size,auction):
    return 10000*vwas(bid,ask,price,size,auction)/vwap(price,size)       








if __name__ == "__main__":
    import lib.dbtools.read_dataset as read_dataset
    data=read_dataset.ftickdb(security_id=110,date='03/05/2013')
    vwasbp(data['bid'].values,data['ask'].values,data['price'].values,data['volume'].values,data['auction'].values)


