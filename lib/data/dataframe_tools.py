# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def rolling_aggregate_vwap(data, sampling, delta_t):
    data_sampling = data.groupby(pd.TimeGrouper(freq = '%dS' % sampling)).agg(lambda x:np.nan if len(x) == 0 else x[-1])    
    data_sampling = data_sampling[~np.isnan(data_sampling['volume'])]       
    datevals=np.array([x.to_datetime() for x in data.index])    
    value = []
    for i in range(len(data_sampling)):        
        d = data_sampling.index[i].to_datetime()
        idx = (datevals>d) & (datevals<=(d + timedelta(seconds=delta_t)))
        #if i%100 == 0: print sum(idx == True)
        data_temp = data[idx]        
        value.append(np.sum(data_temp['volume'] * data_temp['price']) / (np.sum(data_temp['volume'])) if np.sum(data_temp['volume']) > 0 else np.nan )
        
    return pd.Series(value, index = data_sampling.index), data_sampling


###############################################################################
# Aggregate dataframe
###############################################################################

def agg(data,
        group_vars=None,
        step_sec=None,
        stats=None,
        index=None):
        
    #---------------------------------------------------
    # TEST INPUTS + RENORMALIZE
    #---------------------------------------------------
    if not isinstance(data,pd.DataFrame) or stats is None or (group_vars is None and step_sec is None):
        raise ValueError('bad inputs')
        
    if data.shape[0]==0:
        return pd.DataFrame() 
        
    # renormalize group_vars
    if isinstance(group_vars,basestring):
        group_vars=[group_vars]
    group_var_names=group_vars
    
    # special case whane
    if step_sec is not None:
        _tmp=gridTime(date=data.index,step_sec=step_sec,out_mode='grid')
        group_vars=[_tmp[:,0],_tmp[:,1]]+group_vars
        group_var_names=['begin_slice','end_slice']+group_var_names
        index='end_slice'
        
    #---------------------------------------------------
    # compute
    #---------------------------------------------------        
    grouped=data.groupby(group_vars)
    list_dict_vals=[]
    # base dict for 
    for k,v in grouped:
        # --- group_vars info
        _tmp={}
        if len(group_vars)==1:
            _tmp.update({group_var_names[0]:k})
        else:
            for i in range(0,len(group_vars)):
                _tmp.update({group_var_names[i]:k[i]})
        # --- stats info    
        for x in stats.keys():
            _tmp.update({x : stats[x](v)})
        # ---add
        list_dict_vals.append(_tmp)
        
    out=pd.DataFrame(list_dict_vals)
    
    if index is not None:
        out=out.set_index(index)
        
    return out


def convertTime(dt,step_sec=60,out_mode='ceil'):
    refdt=datetime(dt.year,dt.month,dt.day,tzinfo=dt.tzinfo)
    microseconds = 1000000*(dt - refdt).seconds+(dt - refdt).microseconds
    floorTo=1000000*step_sec
    # // is a floor division, not a comment on following line:
    rounding = (microseconds) // floorTo * floorTo
    if (out_mode=='floor'): 
        return refdt + timedelta(microseconds=rounding)
    elif (out_mode=='ceil'): 
        return refdt + timedelta(microseconds=rounding+floorTo)
    elif (out_mode=='grid'):
        return [refdt + timedelta(microseconds=rounding),refdt + timedelta(microseconds=rounding+floorTo)]
    else:
        raise NameError('convertTime:out_mode - Unknown out_mode')


def gridTime(date=None,start_time=None,end_time=None,tz=None,step_sec=60,out_mode='ceil'):
    #-- 1/ convert dates
    if (date is not None):
        return np.array([convertTime(x,step_sec=step_sec,out_mode=out_mode) for x in date])
    #-- 2/ create a date range
    elif ((start_time is not None) and (end_time is not None)):
        if (out_mode=='floor') or (out_mode=='ceil'):
            startend=np.array([convertTime(x,step_sec=step_sec,out_mode=out_mode) for x in [start_time,end_time]])
            timevals=pd.date_range(startend[0],startend[1], freq=str(step_sec)+'s',tz=tz)
            return np.array([x.to_datetime() for x in timevals])
        elif (out_mode=='grid'):
            startend=np.array([convertTime(x,step_sec=step_sec,out_mode='ceil') for x in [start_time,end_time]])
            timevals=pd.date_range(startend[0],startend[1], freq=str(step_sec)+'s',tz=tz)
            timevals=[x.to_datetime() for x in timevals]
            return np.array([[x,x-timedelta(seconds=step_sec)] for x in timevals])       
        else:
            raise NameError('gridTime:out_mode - Unknown out_mode')            
    else:
        raise NameError('gridTime:input - Bad input')   
    



if __name__ == "__main__":
    
    import lib.dbtools.read_dataset as read_dataset
    from lib.data.ui.Explorer import Explorer
    # 
    data=read_dataset.ftickdb(security_id=110,date='03/09/2013')
    agg_exch=agg(data,group_vars=['exchange_id'],
        stats={'nb_deal': lambda df : np.size(df.volume),
               'volume': lambda df : np.sum(df.volume)})
    agg_slicer=agg(data,step_sec=300,group_vars=['exchange_id'],
        stats={'nb_deal': lambda df : np.size(df.volume),
               'volume': lambda df : np.sum(df.volume)})
    





