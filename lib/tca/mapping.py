# -*- coding: utf-8 -*-
"""
Created on Thu Jun 13 10:49:01 2013

@author: njoseph
"""
import pandas as pd
import numpy as np
#from datetime import *
#import pytz

#------------------------------------------------------------------------------
# ExcludeAuction
#------------------------------------------------------------------------------
def ExcludeAuction(x):
    if x==0:
        out=[0,0,0,1]
    elif x==1:
        out=[1,1,1,1]
    elif x==2:
        out=[1,0,0,1]    
    elif x==3:
        out=[0,0,1,1]   
    else:
        raise NameError('mapping:ExcludeAuction - Bad inputs')
    return out
    