# -*- coding: utf-8 -*-
"""
Created on Fri Jun 07 09:18:30 2013

@author: njoseph
"""

import pandas as pd
import numpy as np
from datetime import *
import pytz
import lib.stats.slicer as slicer
import lib.stats.formula as formula
import lib.data.st_data as st_data

###------------------<------------------------------------------------------------
### TO ADD
###-----------------------------------------------------------------------------
# add le tick_size



###------------------<------------------------------------------------------------
### agg exec
###-----------------------------------------------------------------------------
## the call should be made after aggmarket !
def occ_aggexec(# order information
        data_order=pd.DataFrame(),data_deal=pd.DataFrame()
        ):
    ##############################################################
    # check input 
    ##############################################################
    if not isinstance(data_order,pd.DataFrame):
        raise NameError('occ_aggexec:Input - Bad input for data_order')            
        
    if data_order.shape[0]==0:
        return pd.DataFrame()
        
    ##############################################################
    # check input 
    ##############################################################            
    grouped=data_order.groupby(['p_occ_id'])
    out=pd.DataFrame([{'p_occ_id':k,
                       'occ_nb_strategy':len(np.unique(v.strategy_name_mapped)),
                       'occ_strategy_name_mapped': v.strategy_name_mapped[-1] if len(np.unique(v.strategy_name_mapped))==1 else 'MULTIPLE',
                       'occ_exec_qty':np.sum(v.exec_qty),
                       'occ_turnover':np.sum(v.turnover),
                       'occ_nb_exec':np.sum(v.nb_exec)} for k,v in grouped])
    return out

###----------------------------------------------------------------------------
### agg exec
###----------------------------------------------------------------------------
## the call should be made after aggmarket !
def aggexec(# order information
        data_order=pd.DataFrame(),data_deal=pd.DataFrame()
        ):
    
    ##############################################################
    # check input 
    ##############################################################
    if not ( (isinstance(data_order,pd.Series)) or (isinstance(data_order,pd.DataFrame) and data_order.shape[0]==1)):
        raise NameError('aggexec:Input - Bad input for data_order')            
         
    ##############################################################
    # default
    ############################################################## 
    indicators={'exec_quantity':0,
                'exec_nb_trades':0,
                'exec_turnover':0}
    # 
    if isinstance(data_order,pd.Series):               
        out=pd.DataFrame([data_order[['p_cl_ord_id','p_occ_id']].to_dict()])
    else:              
        out=pd.DataFrame([data_order.ix[0][['p_cl_ord_id','p_occ_id']].to_dict()])
                  
    ##############################################################
    # compute stats ON PERIOD
    ##############################################################
    if data_deal.shape[0]>0:
        indicators.update({'exec_quantity':np.sum(map(lambda x : float(x),data_deal['volume'])),
                    'exec_nb_trades': np.size(data_deal['volume']),
                    'exec_turnover': np.sum(map(lambda x,y : float(x)*float(y),data_deal['volume'],data_deal['price']))})
    
    out=out.join(pd.DataFrame([indicators])) 
    
    return out

#------------------------------------------------------------------------------
# agg market
#------------------------------------------------------------------------------
# renorm_datetime : is for handling times for occurence and algo sequence
def aggmarket(# market information
        data=pd.DataFrame(),exchange_id_main=None,
        # period information
        start_datetime=None,end_datetime=None,exclude_auction=[0,0,0,0],exclude_dark=False,
        # order information
        limit_price=0,side=1,
        # params
        out_datetime=True,renorm_datetime=False):
    
    ##############################################################
    # check input + default
    ##############################################################
    if exchange_id_main is None:
        raise NameError('aggmarket:Input - Bad input for exchange_id_main')
        
    if (side!=1) and (side!=-1):
        raise NameError('aggmarket:Input - Bad input for side')
        
    indicators_before={'arrival_price' : np.nan}
    
    indicators={'data' : 0,
                'volume_lit' : 0,
                'volume_lit_main' : 0, # include in volume_lit
                'volume_opening' : 0, # include in volume_lit
                'volume_closing' : 0, # include in volume_lit
                'volume_intraday' : 0, # include in volume_lit
                'volume_other_auctions' : 0, # include in volume_lit
                'volume_dark' : 0,
                'volume_lit_constr' : 0,
                'volume_lit_main_constr' : 0, # include in volume_lit_constr
                'volume_dark_constr' : 0,
                'turnover_lit' : 0,
                'turnover_lit_main' : 0, # include in turnover_lit
                'turnover_dark' : 0,
                'turnover_lit_constr' : 0,
                'turnover_lit_main_constr' : 0, # include in turnover_lit_constr
                'turnover_dark_constr' : 0,
                'nb_trades_lit_cont' : 0,
                'nb_trades_lit_cont_main' : 0,
                'open' : np.nan,
                'high' : np.nan,
                'low' : np.nan,
                'close' : np.nan,
                'vwas' : np.nan,
                'vwas_main' : np.nan,
                'vol_GK':np.nan}
                

    ##############################################################
    # compute stats ON PERIOD
    ##############################################################
    if data.shape[0]>0:
        
        indicators.update({'data' : 1})
        
        #--------------------------
        # -- find needed index to compute stats
        #--------------------------
        # base
        if renorm_datetime:
            start_datetime=start_datetime+timedelta(seconds=0.5)
            end_datetime=end_datetime+timedelta(seconds=0.5)
        
        idx_period=np.nonzero(map(lambda x : x>=start_datetime and x<end_datetime,[x.to_datetime() for x in data.index]))[0]
        if any(np.array(exclude_auction)==1):
            if exclude_auction[0]==1:
                idx_period=np.array(list(set(idx_period).difference(set(np.nonzero(data['opening_auction']==1)[0]))))
            if exclude_auction[1]==1:
                idx_period=np.array(list(set(idx_period).difference(set(np.nonzero(data['intraday_auction']==1)[0]))))
            if exclude_auction[2]==1:
                idx_period=np.array(list(set(idx_period).difference(set(np.nonzero(data['closing_auction']==1)[0]))))
            if exclude_auction[3]==1:
                idx_period=np.array(list(set(idx_period).difference(set(np.nonzero((data['auction']==1) & (data['opening_auction']==0) & (data['intraday_auction']==0) & (data['closing_auction']==0))[0]))))
        
        if exclude_dark:
            idx_period=np.array(list(set(idx_period).difference(set(np.nonzero(data['dark']==1)[0]))))
        
        # subidx
        idx_period_lit=np.array(list(set(idx_period) & set(np.nonzero(data['dark']==0)[0])))
        idx_period_lit_main=np.array(list(set(idx_period) & set(np.nonzero((data['dark']==0) & (data['exchange_id']==exchange_id_main))[0])))
        idx_period_lit_cont=np.array(list(set(idx_period) & set(np.nonzero((data['dark']==0) & (data['auction']==0))[0])))
        idx_period_lit_cont_main=np.array(list(set(idx_period) & set(np.nonzero((data['dark']==0) & (data['auction']==0) & (data['exchange_id']==exchange_id_main))[0])))
        idx_period_dark=np.array(list(set(idx_period) & set(np.nonzero(data['dark']==1)[0])))
        idx_period_opening=np.array(list(set(idx_period) & set(np.nonzero(data['opening_auction']==1)[0])))
        idx_period_closing=np.array(list(set(idx_period) & set(np.nonzero(data['closing_auction']==1)[0])))
        idx_period_intraday=np.array(list(set(idx_period) & set(np.nonzero(data['intraday_auction']==1)[0])))
        idx_period_other=np.array(list(set(idx_period) & set(np.nonzero((data['auction']==1) & (data['opening_auction']==0) & (data['intraday_auction']==0) & (data['closing_auction']==0))[0])))
        # contrs index
        idx_period_lit_constr=idx_period_lit
        idx_period_lit_main_constr=idx_period_lit_main
        idx_period_dark_constr=idx_period_dark
        if limit_price>0:
            idx_period_lit_constr=np.array(list(set(idx_period_lit_constr) & set(np.nonzero(data['price']*side<=limit_price*side)[0])))
            idx_period_lit_main_constr=np.array(list(set(idx_period_lit_main_constr) & set(np.nonzero(data['price']*side<=limit_price*side)[0])))
            idx_period_dark_constr=np.array(list(set(idx_period_dark_constr) & set(np.nonzero(data['price']*side<=limit_price*side)[0])))
        
        #--------------------------
        # -- compute
        #--------------------------
        if idx_period.shape[0]>0:
            indicators.update({'volume_lit' : np.sum(data.ix[idx_period_lit]['volume']),
                        'volume_lit_main' : np.sum(data.ix[idx_period_lit_main]['volume']),
                        'volume_opening' : np.sum(data.ix[idx_period_opening]['volume']), 
                        'volume_closing' : np.sum(data.ix[idx_period_closing]['volume']),
                        'volume_intraday' : np.sum(data.ix[idx_period_intraday]['volume']),
                        'volume_other_auctions' : np.sum(data.ix[idx_period_other]['volume']),
                        'volume_dark' : np.sum(data.ix[idx_period_dark]['volume']),
                        'volume_lit_constr' : np.sum(data.ix[idx_period_lit_constr]['volume']),
                        'volume_lit_main_constr' : np.sum(data.ix[idx_period_lit_main_constr]['volume']),
                        'volume_dark_constr' : np.sum(data.ix[idx_period_dark_constr]['volume']),
                        'turnover_lit' : np.sum(data.ix[idx_period_lit]['volume']*data.ix[idx_period_lit]['price']),
                        'turnover_lit_main' : np.sum(data.ix[idx_period_lit_main]['volume']*data.ix[idx_period_lit_main]['price']),
                        'turnover_dark' : np.sum(data.ix[idx_period_dark]['volume']*data.ix[idx_period_dark]['price']),
                        'turnover_lit_constr' : np.sum(data.ix[idx_period_lit_constr]['volume']*data.ix[idx_period_lit_constr]['price']),
                        'turnover_lit_main_constr' : np.sum(data.ix[idx_period_lit_main_constr]['volume']*data.ix[idx_period_lit_main_constr]['price']),
                        'turnover_dark_constr' : np.sum(data.ix[idx_period_dark_constr]['volume']*data.ix[idx_period_dark_constr]['price']),
                        'nb_trades_lit_cont' : np.size(data.ix[idx_period_lit_cont]['auction']),
                        'nb_trades_lit_cont_main' : np.size(data.ix[idx_period_lit_cont_main]['auction']),
                        'open' : data.ix[idx_period_lit[0]]['price'],
                        'high' : np.max(data.ix[idx_period_lit]['price']),
                        'low' : np.min(data.ix[idx_period_lit]['price']),
                        'close' : data.ix[idx_period_lit[-1]]['price'],
                        'vwas': slicer.vwas(data.ix[idx_period_lit]['bid'],data.ix[idx_period_lit]['ask'],data.ix[idx_period_lit]['price'],data.ix[idx_period_lit]['volume'],data.ix[idx_period_lit]['auction']),
                        'vwas_main': slicer.vwas(data.ix[idx_period_lit_main]['bid'],data.ix[idx_period_lit_main]['ask'],data.ix[idx_period_lit_main]['price'],data.ix[idx_period_lit_main]['volume'],data.ix[idx_period_lit_main]['auction']),
                        'vol_GK': formula.vol_gk(np.array([data.ix[idx_period_lit[0]]['price']]), np.array([np.max(data.ix[idx_period_lit]['price'].values)]), np.array([np.min(data.ix[idx_period_lit]['price'].values)]), 
                                                 np.array([data.ix[idx_period_lit[-1]]['price']]), np.array([np.size(data.ix[idx_period_lit_cont]['price'].values)]), np.array([end_datetime-start_datetime]))[0]}
                              )
    if out_datetime:
        indicators.update({'start_datetime':start_datetime,'end_datetime':end_datetime})
        
    out=pd.DataFrame([indicators])
    
    ##############################################################
    # compute stats BEFORE PERIOD
    ##############################################################
    if data.shape[0]>0:
        idx_before_lit=np.nonzero(map(lambda x : x<start_datetime,[x.to_datetime() for x in data.index]))[0]
        idx_before_lit=np.array(list(set(idx_before_lit).difference(set(np.nonzero(data['dark']==1)[0]))))
        if (idx_before_lit.shape[0]==0 and idx_period.shape[0]>0):
            indicators_before.update({'arrival_price' : out['open'].values[0]})
        elif idx_before_lit.shape[0]>0:
            indicators_before.update({'arrival_price' : data.ix[idx_before_lit[-1]]['price']})
            
    out=out.join(pd.DataFrame([indicators_before]))
    
    ##############################################################
    # output
    ##############################################################    
    return out


    