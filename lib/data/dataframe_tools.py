# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import math
import logging

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
# select_indexdatetime
###############################################################################

def select_sorted_index_datetime(data , start_date = None, end_date = None , start_strict = False , end_strict = True , verbose_time = False):
    ##############
    # CONVENTION BY DEFAULT: >= start_date AND < end_date
    
    ##############
    # TEST
    if not isinstance(data,pd.DataFrame):
        raise ValueError('data has to be a DataFrame')   
    
    if data.shape[0] == 0:
        return None   
    
    ##############
    # DO   
    idx_start = 0
    idx_end = data.shape[0] - 1
    
    index = data.index 
    if start_date is not None:
        
        first_ = index[idx_start].to_datetime()
        
        if start_strict and start_date < first_:
            idx_start = 0
        
        elif not start_strict and start_date <= first_:
            idx_start = 0
        
        else:
            idx_start_sup = idx_end
            idx_start_inf = idx_start
            
            nb_ = 0
            while (idx_start_sup - idx_start_inf) > 1:
                nb_ +=1
                idx_check = idx_start_inf + math.floor((idx_start_sup - idx_start_inf)/2)
                last_convert = index[idx_check].to_datetime()
                if (start_strict and last_convert <= start_date) or ( not start_strict and last_convert < start_date):
                    idx_start_inf = idx_check
                else:
                    idx_start_sup = idx_check
                    if end_date is not None and last_convert > end_date:
                        idx_end = idx_start_sup
            
            idx_start = idx_start_sup
    
    if end_date is not None:
        
        last_ = first_ = index[idx_end].to_datetime()
        
        if not (end_strict and end_date >= last_) and not (not end_strict and end_date > last_):
            idx_end_sup = idx_end
            idx_end_inf = idx_start
            nb_ = 0
            
            while (idx_end_sup - idx_end_inf) > 1:
                nb_ +=1
                idx_check = idx_end_inf + math.floor((idx_end_sup - idx_end_inf)/2)
                last_convert = index[idx_check].to_datetime()
                if (end_strict and last_convert < end_date) or (not end_strict and last_convert <= end_date):
                    idx_end_inf = idx_check
                else:
                    idx_end_sup = idx_check
                
            idx_end = idx_end_inf       
    
    if verbose_time:
        print '-----'
        print start_date,end_date
        print index[idx_start].to_datetime(),index[idx_end].to_datetime(),
        print '-----'
    
    #-- check conditon
    if ((start_strict and start_date is not None and not index[idx_start].to_datetime() > start_date) or (not start_strict and start_date is not None and not index[idx_start].to_datetime() >= start_date) or
        (end_strict and end_date is not None and not index[idx_end].to_datetime() < end_date) or (not end_strict and end_date is not None and not index[idx_end].to_datetime() <= end_date)):
        logging.info('select_sorted_index_datetime: no data matching')
        return [False] * data.shape[0]
    else:
        return map(lambda x: x >= idx_start and x <= idx_end, range(data.shape[0]))
    
                
#         # start superieur ou egal
#         idx_start = idx_start_inf
#         if start_date > index[idx_start_inf].to_datetime():
#             idx_start = idx_start_sup
            
        
    
    
    
    
    
###############################################################################
# add_grouppbyvar
###############################################################################

def add_grouppbyvar(data,add , inplace = False , out_numpy = False):
    # exemple de add : add = {'v1_gby' : {'v1' : [0,0.5}}
    if not isinstance(data,pd.DataFrame):
        raise ValueError('data has to be a DataFrame')    
        
    if not isinstance(add,dict):
        raise ValueError('add has to be a dict')
    
    if data.shape[0] == 0:
        return
    
    for out_col in add.keys():
        data[out_col] = 'Other'
        in_col = add[out_col].keys()[0]
        bounds = np.sort(add[out_col][in_col])
        
        for i in range(0,len(bounds)):
            
            if i == 0 and len(bounds) == 1:
                data[out_col][data[in_col] <= bounds[i]] = '(' + str(i) + ') :  <= ' + str(bounds[i])
                data[out_col][data[in_col] <= bounds[i]] = '(' + str(i+1) + ') :  > ' + str(bounds[i])
            elif i == 0:
                data[out_col][data[in_col] <= bounds[i]] = '(' + str(i) + ') : <= ' + str(bounds[i])
            elif i == len(bounds) - 1:
                data[out_col][(data[in_col] > bounds[i-1]) & (data[in_col] <= bounds[i])] = '(' + str(i) + ') : ] ' + str(bounds[i-1]) + ' , ' + str(bounds[i]) + ' ]'
                data[out_col][data[in_col] > bounds[i]] = '(' + str(i+1) + ') : > ' + str(bounds[i]) 
            else:
                data[out_col][(data[in_col] > bounds[i-1]) & (data[in_col] <= bounds[i])] = '(' + str(i) + ') : ] ' + str(bounds[i-1]) + ' , ' + str(bounds[i]) + ' ]'
                
    if not inplace:
        out = data[add.keys()[0]]
        data = data.drop(add.keys() , axis = 1)
        
        if out_numpy:
            return out.values
        else:
            return out
            
###############################################################################
# to_listdict
###############################################################################

def to_listdict(data,
        columns = None,
        type2native = False):
    
    if not isinstance(data,pd.DataFrame):
        raise ValueError('Input has to be a DataFrame')
    
    out = []
    
    if data.shape[0] == 0:
        return out
    
    for idx in range(0,data.shape[0]):
        #-- get dict
        if columns is not None:
            dict_add = data.ix[idx][columns].to_dict()
        else:
            dict_add = data.ix[idx].to_dict()
            
        #-- renormalize type
        if type2native:
            for x in dict_add.keys():
                if isinstance(dict_add[x], np.generic):
                    dict_add[x] = np.asscalar(dict_add[x])
        
        out += [dict_add]
        
    return out

###############################################################################
# Aggregate dataframe
###############################################################################

def agg(data,
        group_vars = None,
        step_sec = None,
        stats = None,
        index = None,
        index_drop = False):
        
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
        
    elif group_vars is None:
        group_vars = []
        
    group_var_names=group_vars
    
    # special case whane
    if step_sec is not None:
        _tmp=gridTime(date=data.index,step_sec=step_sec,out_mode='grid')
        if group_vars is None:
            group_vars=[_tmp[:,0],_tmp[:,1]]
            group_var_names=['begin_slice','end_slice']
        else:
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
        out.set_index(index , inplace = True , drop = index_drop)
          
    return out

###############################################################################
# Aggregate dataframe
###############################################################################

def resynchronyse(df , group_vars = None , sync_var = None):
    
    if not isinstance(df,pd.DataFrame) or not isinstance(sync_var,basestring):
        raise ValueError('bad inputs')
    
    # renormalize group_vars
    if isinstance(group_vars,basestring):
        group_vars=[group_vars]
        
    elif group_vars is None:
        group_vars = []   
    
    indexn =  df.index.name
    if indexn is None:
        indexn = 'index'
    
    cols = list(set(df.columns.tolist()) - set(group_vars + [sync_var]))
    
    #-- all time slice + group vars possibilities
    out = df[group_vars].reset_index().drop_duplicates(cols = group_vars + [indexn]).set_index(indexn)
    
    #-- all sync_var possibiloyies
    sync_var_values = np.unique(df[sync_var].values)
    for v in sync_var_values:
        tmp = df[df[sync_var] == v][group_vars + cols]
        tmp.rename(columns = dict((c,c + '_' + str(v)) for c in cols),inplace = True)
        out = out.merge(tmp,how = 'left' ,  on = group_vars , left_index = True)
           
    return out
        
        
###############################################################################
# Handle Time functions
###############################################################################

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
    
    print agg_slicer
    





