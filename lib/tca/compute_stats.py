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
#import os as os
#from lib.dbtools.connections import Connections
#import lib.dbtools.get_repository as get_repository
#from lib.data.matlabutils import *
#import lib.data.st_data as st_data




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
        
    default_indicators_before={'arrival_price' : np.nan}
    
    default_indicators={'volume_lit' : 0,
                        'volume_lit_main' : 0,
                        'volume_dark' : 0,
                        'volume_lit_constr' : 0,
                        'volume_lit_main_constr' : 0,
                        'volume_dark_constr' : 0,
                        'volume_opening' : 0,
                        'volume_closing' : 0,
                        'volume_intraday' : 0,
                        'volume_other_auctions' : 0,
                        'turnover_lit' : 0,
                        'turnover_lit_main' : 0,
                        'turnover_dark' : 0,
                        'turnover_lit_constr' : 0,
                        'turnover_lit_main_constr' : 0,
                        'turnover_dark_constr' : 0,
                        'nb_trades_cont' : 0,
                        'nb_trades_cont_main' : 0,
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
            idx_period_lit_constr=np.array(list(set(idx_period_lit_constr) & set(np.nonzero(data['price']*side>limit_price*side)[0])))
            idx_period_lit_main_constr=np.array(list(set(idx_period_lit_main_constr) & set(np.nonzero(data['price']*side>limit_price*side)[0])))
            idx_period_dark_constr=np.array(list(set(idx_period_dark_constr) & set(np.nonzero(data['price']*side>limit_price*side)[0])))
        
        #--------------------------
        # -- compute
        #--------------------------
        if idx_period.shape[0]>0:
            indicators={'volume_lit' : np.sum(data.ix[idx_period_lit]['volume']),
                        'volume_lit_main' : np.sum(data.ix[idx_period_lit_main]['volume']),
                        'volume_dark' : np.sum(data.ix[idx_period_dark]['volume']),
                        'volume_lit_constr' : np.sum(data.ix[idx_period_lit_constr]['volume']),
                        'volume_lit_main_constr' : np.sum(data.ix[idx_period_lit_main_constr]['volume']),
                        'volume_dark_constr' : np.sum(data.ix[idx_period_dark_constr]['volume']),
                        'volume_opening' : np.sum(data.ix[idx_period_opening]['volume']), 
                        'volume_closing' : np.sum(data.ix[idx_period_closing]['volume']),
                        'volume_intraday' : np.sum(data.ix[idx_period_intraday]['volume']),
                        'volume_other_auctions' : np.sum(data.ix[idx_period_other]['volume']),
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
                        'vol_gk': formula.vol_gk(np.array([data.ix[idx_period_lit[0]]['price']]), np.array([np.max(data.ix[idx_period_lit]['price'].values)]), np.array([np.min(data.ix[idx_period_lit]['price'].values)]), 
                                                 np.array([data.ix[idx_period_lit[-1]]['price']]), np.array([np.size(data.ix[idx_period_lit_cont]['auction'].values)]), np.array([end_datetime-start_datetime]))[0]}
        else:
            indicators=default_indicators
        indicators.update({'data' : 1})
        if out_datetime:
            indicators.update({'start_datetime':start_datetime,'end_datetime':end_datetime})
        out=pd.DataFrame([indicators])
    else:
        default_indicators=default_indicators.update({'data' : 0})
        if out_datetime:
            indicators.update({'start_datetime':start_datetime,'end_datetime':end_datetime})
        out=pd.DataFrame([default_indicators])        
    
    ##############################################################
    # compute stats BEFORE PERIOD
    ##############################################################
    idx_before_lit=np.nonzero(map(lambda x : x<start_datetime,[x.to_datetime() for x in data.index]))[0]
    idx_before_lit=np.array(list(set(idx_before_lit).difference(set(np.nonzero(data['dark']==1)[0]))))
    
    if ((data.shape[0]>0) and
        ((idx_before_lit.shape[0]==0 and idx_period.shape[0]>0))):
        out_tmp=pd.DataFrame([{'arrival_price' : out['open'].values[0]}])
    elif ((data.shape[0]>0) and idx_before_lit.shape[0]>0):
        out_tmp=pd.DataFrame([{'arrival_price' : data.ix[idx_before_lit[-1]]['price']}])
    else:
        out_tmp=pd.DataFrame([default_indicators_before])
    
    out=out.join(out_tmp)
    
    ##############################################################
    # output
    ##############################################################    
    return out






if __name__ == "__main__":
    import lib.dbtools.read_dataset as read_dataset
    from lib.data.ui.Explorer import Explorer
    import lib.dbtools.get_algodata as get_algodata
    # test on market data
    data=read_dataset.ftickdb(security_id=110,date='17/05/2013')
    stats=aggmarket(data=data,exchange_id_main=np.min(data['exchange_id']),start_datetime=data.index[1].to_datetime(),end_datetime=data.index[5000].to_datetime(),limit_price=8.23,side=-1)
    Explorer(stats)
    #------------ occurence ID
    #-- algodata
    occ_id='FY2000007382301'
    data_occ=get_algodata.sequence_info(occurence_id=occ_id)
    # data_occ=get_algodata.occurence_info(occurence_id=occ_id)
    #-- market data
    sec_id=data_occ.ix[0]['cheuvreux_secid']
    datestr=datetime.strftime(data_occ.index[0].to_datetime(), '%d/%m/%Y')
    datatick=pd.DataFrame()
    if sec_id>0:
        datatick=read_dataset.ftickdb(security_id=sec_id,date=datestr)
    #-- agg market data on sequence
    dataggseq=pd.DataFrame()
    for i in range(0,data_occ.shape[0]-1):
        #-- needed
        starttime=data_occ.index[i].to_datetime()
        endtime=data_occ.ix[i]['eff_endtime']
        if isinstance(endtime,datetime):
            endtime=endtime.replace(tzinfo=pytz.UTC)
        exchange_id_main=np.min(datatick['exchange_id'])
        #-- compute
        if isinstance(endtime,datetime):
            tmp=aggmarket(data=datatick,exchange_id_main=exchange_id_main,
                          start_datetime=starttime,end_datetime=endtime,
                          limit_price=data_occ.ix[i]['Price'],side=data_occ.ix[i]['Side'])
            tmp=tmp.join(pd.DataFrame([data_occ.ix[i][['ClOrdID','occ_ID']].to_dict()]))                 
            dataggseq=dataggseq.append(tmp)           
                          
    Explorer(data_occ)
    
    