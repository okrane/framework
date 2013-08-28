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
import lib.tca.tools as tools
import lib.io.toolkit as toolkit
import lib.data.st_data as st_data
from lib.io.toolkit import get_traceback


#--------------------------------------------------------------------------
# GLOBAL needed
#--------------------------------------------------------------------------    
utc=pytz.UTC

#--------------------------------------------------------------------------
# sequence_info
#--------------------------------------------------------------------------
def occurrence_info_fe(sequence_id=None,occurence_id=None,start_date=None,end_date=None):  
    ###########################################################################
    #### extract algo DATA
    ###########################################################################
    # get all the sequences from sequence ids
    if sequence_id is not None:
        data_seq=get_algodata.sequence_info(sequence_id=sequence_id)
        data_occ=get_algodata.occurrence_info(sequence_id=sequence_id)
    # get all the sequences from occurence ids
    elif occurence_id is not None:  
        data_seq=get_algodata.sequence_info(occurence_id=occurence_id)
        data_occ=get_algodata.occurrence_info(occurence_id=occurence_id)
    elif (start_date is not None)  and (end_date is not None): 
        data_seq=get_algodata.sequence_info(start_date=start_date,end_date=end_date)
        data_occ=get_algodata.occurrence_info(start_date=start_date,end_date=end_date)
    else:
        raise NameError('get_algostats:sequence - Bad inputs')
        
    if data_occ.shape[0]<=0:
        return pd.DataFrame()
        
    ###########################################################################
    #### aggregate execution infos 
    ###########################################################################    
    data_occ_exec=compute_stats.occ_aggexec(data_order=data_seq)
    if data_occ_exec.shape[0]>0:
        data_occ=data_occ.reset_index().merge(data_occ_exec, how="left",on=['p_occ_id']).set_index('SendingTime')
        
    ###########################################################################
    #### compute slippage
    ###########################################################################
    data_occ['occ_fe_vwap']=((data_occ['occ_fe_inmkt_turnover']+data_occ['occ_fe_prv_turnover'])/
    (data_occ['occ_fe_inmkt_volume']+data_occ['occ_fe_prv_volume']))
    
    data_occ['slippage_vwap_bp']=(10000*data_occ['Side']*
    (data_occ['occ_fe_vwap']-data_occ['occ_turnover']/data_occ['occ_exec_qty'])/data_occ['occ_fe_vwap'])
    data_occ['slippage_vwap_bp'][(data_occ['occ_exec_qty']==0)  | (data_occ['occ_fe_vwap']==0)]=np.nan
    
    data_occ['slippage_is_bp']=(10000*data_occ['Side']*
    (data_occ['occ_fe_arrival_price']-data_occ['occ_turnover']/data_occ['occ_exec_qty'])/data_occ['occ_fe_arrival_price'])
    data_occ['slippage_vwap_bp'][(data_occ['occ_exec_qty']==0)  | (data_occ['occ_fe_arrival_price']==0)]=np.nan
    
    return data_occ

 
#--------------------------------------------------------------------------
# sequence_info
#--------------------------------------------------------------------------
def sequence_info(sequence_id=None,occurence_id=None,start_date=None,end_date=None):    
    ###########################################################################
    #### extract algo DATA
    ###########################################################################
    # get all the sequences from sequence ids
    if sequence_id is not None:
        data=get_algodata.sequence_info(sequence_id=sequence_id)
    # get all the sequences from occurence ids
    elif occurence_id is not None:  
        data=get_algodata.sequence_info(occurence_id=occurence_id)
    elif (start_date is not None)  and (end_date is not None): 
        data=get_algodata.sequence_info(start_date=start_date,end_date=end_date)
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
        
        print "get_algostats:sequence_info - advancement "+str(i)+" on "+str(len(uni_vals))
        
        #-----------------
        # extract security info 
        #-----------------    
        sec_id=int(float(uni_vals[i][0]))
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
            ##########
            # default
            ##########            
            tmp_add=pd.DataFrame([data.ix[idx][['p_cl_ord_id','p_occ_id']].to_dict()])
            
            ##########
            # needed information
            ##########
            excl_auction=mapping.ExcludeAuction(data.ix[idx]['ExcludeAuction'])
            # strategy_name=mapping.StrategyName(data.ix[idx]['StrategyName'],data.ix[idx]['SweepLit'])
            lastick_datetime=None
            if datatick.shape[0]>0:
                lastick_datetime=datatick.index[-1].to_datetime()
            
            # bench start 
            bench_starttime,bench_endtime=tools.extract_benchtime(data=data.ix[idx],lasttick_datetime=lastick_datetime)
            
            ##########
            # extract sequence deals
            ##########
            data_deal=get_algodata.deal(sequence_id=data.ix[idx]['p_cl_ord_id'])
            # TODO: renormalize auction deals/referential information
            
            ##########
            # compute AGG market stats
            ##########
            if isinstance(bench_endtime,datetime) and isinstance(bench_starttime,datetime):
                tmp_=compute_stats.aggmarket(data=datatick,exchange_id_main=exchange_id_main,
                              start_datetime=bench_starttime,end_datetime=bench_endtime,
                              exclude_auction=excl_auction,exclude_dark=True,
                              limit_price=data.ix[idx]['Price'],side=data.ix[idx]['Side'],
                              out_datetime=False,renorm_datetime=True)
                # tmp_add=tmp_add.join(pd.DataFrame([data.ix[idx][['ClOrdID','occ_ID']].to_dict()]))  
                tmp_add=tmp_add.join(tmp_)
            
            ##########
            # compute AGG execution stats
            ##########
            # TODO: exec stats
            tmp_=compute_stats.aggexec(data_order=tmp_add,data_deal=data_deal)
            tmp_add=tmp_add.merge(tmp_, how="left",on=['p_cl_ord_id','p_occ_id'])
            
            ##########
            # joint
            ##########
            tmp_add['bench_starttime']=bench_starttime
            tmp_add['bench_endtime']=bench_endtime
            dataggseq=dataggseq.append(tmp_add)  
            
    
    data=data.reset_index().merge(dataggseq, how="left",on=['p_cl_ord_id','p_occ_id']).set_index('SendingTime')
    
    return data




#--------------------------------------------------------------------------
# sequence_info
#--------------------------------------------------------------------------
def agg_daily_deal(start_date=None,end_date=None,step_sec=60*30,filter=None,group_var='strategy_name_mapped'):    
    ###########################################################################
    #### INPUT
    ###########################################################################
    if (start_date is not None)  and (end_date is not None): 
        sday=datetime.strptime(start_date, '%d/%m/%Y')
        eday=datetime.strptime(end_date, '%d/%m/%Y')
    else:
        raise NameError('get_algostats:sequence - Bad inputs')
    ###########################################################################
    ####  GET DAILY DATA INFO
    ###########################################################################
    group_var='strategy_name_mapped'
    out=pd.DataFrame()
    this_day=eday
    while this_day>=sday:
        # - get deals
        _date_str=datetime.strftime(this_day, '%d/%m/%Y')
        _tmp=get_algodata.deal(start_date=_date_str,end_date=_date_str,filter=filter)
        try :
        # - aggregate + add
            if _tmp.shape[0]>0:
                _tmp_grouped=_tmp.groupby( [st_data.gridTime(date=_tmp.index, step_sec=step_sec, out_mode='ceil'), group_var] )
                _tmp_grouped=pd.DataFrame([{'date':k[0],group_var:k[1],
                                      'mturnover_euro': np.sum(v.rate_to_euro*v.price*v.volume)*1e-6} for k,v in _tmp_grouped])
                _tmp_grouped=_tmp_grouped.set_index('date')
                # on passe en string parce que ca ne sorte pas sinon !!   
                _tmp_grouped['tmpindex']=[datetime.strftime(x.to_datetime(),'%Y%m%d-%H:%M:%S.%f') for x in _tmp_grouped.index]
                _tmp_grouped=_tmp_grouped.sort_index(by=['tmpindex',group_var]).drop(['tmpindex'],axis=1)
                out=out.append(_tmp_grouped)
        except:
            import logging
            logging.error(get_traceback())
            print this_day
            print _tmp
            print _tmp_grouped
        # - previous business day  
        this_day=toolkit.last_business_day(date=this_day)
        
    ###########################################################################
    ####  AGGREGATE
    ###########################################################################
    if out.shape[0]>0:
        out['date']=[datetime.combine(eday.date(),x.to_datetime().time()) for x in out.index]
        # - aggregate
        _out=out.groupby( ['date', group_var] )
        _out=pd.DataFrame([{'date':k[0],group_var:k[1],
                            'nb_day':len(v.mturnover_euro),
                            'mturnover_euro': np.sum(v.mturnover_euro)} for k,v in _out])
        _out=_out.set_index('date')
        # on passe en string parce que ca ne sorte pas sinon !!   
        _out['tmpindex']=[datetime.strftime(x.to_datetime(),'%Y%m%d-%H:%M:%S.%f') for x in _out.index]
        _out=_out.sort_index(by=['tmpindex',group_var]).drop(['tmpindex'],axis=1)
        
        out=_out
        
    return out


if __name__=='__main__':
    # ft london stock
    data=sequence_info(occurence_id='FY2000007382301')    