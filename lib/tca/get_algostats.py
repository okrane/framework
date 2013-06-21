# -*- coding: utf-8 -*-
"""
Created on Tue Jun 11 11:27:13 2013

@author: njoseph
"""
import pandas as pd
import numpy as np
from datetime import *
import pytz
import lib.data.matlabutils as matlabutils
import lib.dbtools.get_algodata as get_algodata
import lib.dbtools.read_dataset as read_dataset
import lib.dbtools.get_repository as get_repository
import lib.tca.compute_stats as compute_stats
import lib.tca.mapping as mapping

    
#--------------------------------------------------------------------------
# GLOBAL needed
#--------------------------------------------------------------------------    
utc=pytz.UTC

#--------------------------------------------------------------------------
# sequence_info
#--------------------------------------------------------------------------
def sequence_info(**kwargs):    
    ###########################################################################
    #### extract algo DATA
    ###########################################################################
    # get all the sequences from sequence ids
    if "sequence_id" in kwargs.keys():
        data=get_algodata.sequence_info(sequence_id=kwargs["sequence_id"])
    # get all the sequences from occurence ids
    elif "occurence_id" in kwargs.keys():  
        data=get_algodata.sequence_info(occurence_id=kwargs["occurence_id"])
    else:
        raise NameError('get_algostats:sequence - Bad inputs')
    
    if data.shape[0]<=0:
        return data
    
    ###########################################################################
    #### ADD AGG MARKET STATS
    ###########################################################################
    #-- agg market data on sequence
    dataggseq=pd.DataFrame()
    
    #-----------------
    # default + NEEDED SECURITY DAY  
    #-----------------
    
    idx_valid=np.nonzero(map(lambda x : (not isinstance(x,float)) or (not np.isnan(x)),data['cheuvreux_secid'].values))[0]
    uni_vals,idx_in_uni_vals=matlabutils.uniqueext(np.dstack((data.ix[idx_valid]['cheuvreux_secid'].values,
                                     np.array([datetime.strftime(x.to_datetime(), '%d/%m/%Y') for x in data.index[idx_valid]])))[0],rows=True,return_inverse=True)
    
    for i in range(0,len(uni_vals)):  
        
        #-----------------
        # extract security info 
        #-----------------    
        sec_id=int(uni_vals[i][0])
        datestr=uni_vals[i][1]
        exchange_id_main=get_repository.exchangeidmain(security_id=sec_id)[0]
        # thours=get_repository.tradingtime(security_id=sec_id,date=datestr)
        
        #-----------------
        # extract TICK MARKET DATA 
        #-----------------            
        datatick=pd.DataFrame()
        if sec_id>0:
            datatick=read_dataset.ftickdb(security_id=sec_id,date=datestr)        
        
        #-----------------
        # Compute stats for each sequence
        #-----------------  
        idx_2compute=idx_valid[np.nonzero(map(lambda x : x==i,idx_in_uni_vals))[0]]
        
        for idx in idx_2compute:
            tmp_add=pd.DataFrame()
            
            ##########
            # needed
            ##########
            excl_auction=mapping.ExcludeAuction(data.ix[idx]['ExcludeAuction'])
            strategy_name=mapping.StrategyName(data.ix[idx]['StrategyName'],data.ix[idx]['SweepLit'])
            # bench start 
            bench_starttime=data.index[idx].to_datetime()
#            if isinstance(data.ix[idx]['StartTime'],datetime):
#                bench_starttime=max(bench_starttime,utc.localize(data.ix[idx]['StartTime']))
            # bench end time   
            bench_endtime=data.ix[idx]['eff_endtime']
#            if isinstance(bench_endtime,datetime):
#                bench_endtime=utc.localize(bench_endtime)
            if ((strategy_name=="VWAP") and 
            (data.ix[idx]['exec_qty']==(data.ix[idx]['OrderQty']-data.ix[idx]['occ_prev_exec_qty']))):
                # fullfiled vwap end time is the one set by the client if it is set or the end of trading
                if isinstance(data.ix[idx]['EndTime'],datetime):
                    bench_endtime=max(bench_endtime,utc.localize(data.ix[idx]['EndTime']))
                elif datatick.shape[0]>0:
                    bench_endtime=max(bench_endtime,datatick.index[-1].to_datetime())
                    
            ##########
            # extract sequence deals
            ##########
            # TODO: extract deals data
            
            ##########
            # compute AGG execution stats
            ##########
            # TODO: exec stats
                    
            ##########
            # compute AGG market stats
            ##########
            tmp_add=pd.DataFrame()
            if (not isinstance(bench_endtime,float)) and (not isinstance(bench_starttime,float)):
                tmp_add=compute_stats.aggmarket(data=datatick,exchange_id_main=exchange_id_main,
                              start_datetime=bench_starttime,end_datetime=bench_endtime,
                              exclude_auction=excl_auction,exclude_dark=True,
                              limit_price=data.ix[idx]['Price'],side=data.ix[idx]['Side'],
                              out_datetime=False,renorm_datetime=True)
                tmp_add=tmp_add.join(pd.DataFrame([data.ix[idx][['ClOrdID','occ_ID']].to_dict()]))  
            
            ##########
            # joint
            ##########
            tmp_add['bench_starttime']=bench_starttime
            tmp_add['bench_endtime']=bench_endtime
            dataggseq=dataggseq.append(tmp_add)  
            
    
    data=data.reset_index().merge(dataggseq, how="left",on=['ClOrdID','occ_ID']).set_index('SendingTime')
    
    return data

if __name__=='__main__':
    # ft london stock
    data=sequence(occurence_id='FY2000007382301')    