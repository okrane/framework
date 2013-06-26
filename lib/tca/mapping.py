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
    
#------------------------------------------------------------------------------
# StrategyName
#------------------------------------------------------------------------------
def StrategyName(x,y):
    if x==1:
        out="VWAP"
    elif x==2:
        out="TWAP"
    elif x==3:
        out="VOL"    
    elif x==4:
        out="ICEBERG"   
    elif x==5:
        out="DYNVOL"   
    elif x==6:
        out="IS"   
    elif x==7:
        out="CROSSFIRE"   
    elif x==8:
        out="CLOSE"   
    elif x==10:
        out="BLINK"    
    elif x==9:
        if y=="CF":
            out="CROSSFIRE" 
        elif y=="BF":
            out="BLINK" 
        elif y=="yes":
            out="HUNT" 
        else:
            raise NameError('mapping:StrategyName - Bad inputs')
    else:
        raise NameError('mapping:ExcludeAuction - Bad inputs')
    return out
