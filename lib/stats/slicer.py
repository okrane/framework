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
# last finite
#-------------------------------------------------------------------------- 
def _isfinite(x):
    return (x is not None) and (x == x) and not (x == np.Inf)

def x_finite(x, mode = 'last'):
    if not (isinstance(x,np.ndarray) or isinstance(x,list)):
        raise ValueError('Input should be numpy array ior list')
    
    out = None
    irange = range(len(x))
    
    if mode == 'last':
        irange.reverse()
    
    for i in irange:
        if _isfinite(x[i]):
            out = x[i]
            break
    
    return out

def last_finite(x):
    return x_finite(x,mode='last')

def first_finite(x):
    return x_finite(x,mode='first')

#--------------------------------------------------------------------------
# weighted statistics
#-------------------------------------------------------------------------- 
def weighted_statistics(x,w,mode='mean',handle_nan=True):
    #----- test input
    if not isinstance(x,np.ndarray) or not isinstance(w,np.ndarray):
        raise ValueError('Input should be numpy arrays')
    if x.shape[0]!=w.shape[0]:
        raise ValueError('Input should have same numbers of rows')
    
    #----- handle nan 
    if handle_nan:
        idx_ok=np.nonzero(map(lambda x,y : np.isfinite(x) and np.isfinite(y),x,w))[0]
    else:
        idx_ok=np.array(range(0,x.shape[0]))
        
    #----- computation
    if idx_ok.shape[0]==0:
        return np.nan
        
    if mode.lower()=='mean':
        sum_w = np.sum(w[idx_ok])
        out=np.sum(x[idx_ok]*w[idx_ok],axis=0)/sum_w
        
    elif mode.lower()=='std':
        sum_w = np.sum(w[idx_ok])
        wmean = np.sum(x[idx_ok]*w[idx_ok],axis=0)/sum_w
        out = np.sqrt((np.sum(w[idx_ok] * np.power(x[idx_ok] - wmean,2),axis=0))/sum_w)  
        
    else:
        raise ValueError('Unknown mode <+'+mode+'>')
        
    return out

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
    import pytz 
    data=read_dataset.ftickdb(security_id=110,date='03/05/2013')
    vwasbp(data['bid'].values,data['ask'].values,data['price'].values,data['volume'].values,data['auction'].values) 
    first_finite(data[(data['bid']>8.14)]['bid'].values)


