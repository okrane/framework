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
        # extract TICK MARKET DATA  + security info 
        #-----------------    
        sec_id=int(uni_vals[i][0])
        datestr=uni_vals[i][1]
        datatick=pd.DataFrame()
        if sec_id>0:
            datatick=read_dataset.ftickdb(security_id=sec_id,date=datestr)
        exchange_id_main=get_repository.exchangeidmain(security_id=sec_id)[0]
          
        #-----------------
        # Compute stats for each sequence
        #-----------------  
        idx_2compute=idx_valid[np.nonzero(map(lambda x : x==i,idx_in_uni_vals))[0]]
        
        for idx in idx_2compute:
            #-- needed
            starttime=data.index[idx].to_datetime()
            endtime=data.ix[idx]['eff_endtime']
            if isinstance(endtime,datetime):
                endtime=endtime.replace(tzinfo=pytz.UTC)
            excl_auction=mapping.ExcludeAuction(data.ix[idx]['ExcludeAuction'])
            
            #-- compute
            if (not isinstance(endtime,float)) and (not isinstance(starttime,float)):
                tmp=compute_stats.aggmarket(data=datatick,exchange_id_main=exchange_id_main,
                              start_datetime=starttime,end_datetime=endtime,
                              exclude_auction=excl_auction,exclude_dark=True,
                              limit_price=data.ix[idx]['Price'],side=data.ix[idx]['Side'],
                              out_datetime=False)
                tmp=tmp.join(pd.DataFrame([data.ix[idx][['ClOrdID','occ_ID']].to_dict()]))                 
                dataggseq=dataggseq.append(tmp)           
    
    data=data.reset_index().merge(dataggseq, how="left",on=['ClOrdID','occ_ID']).set_index('SendingTime')
    
    return data

if __name__=='__main__':
    # ft london stock
    data=sequence(occurence_id='FY2000007382301')    