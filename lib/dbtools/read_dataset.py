# -*- coding: utf-8 -*-
"""
Created on Fri May 24 16:04:51 2013

@author: njoseph
"""

import pandas as pd
import numpy as np
import scipy.io
from datetime import *
import os as os
from lib.dbtools.connections import Connections
# import lib.dbtools.get_repository as get_repository
from lib.data.matlabutils import *
import lib.data.st_data as st_data
import lib.stats.slicer as slicer
import lib.stats.formula as formula
from lib.logger import *
from lib.io.toolkit import get_traceback

if os.name != 'nt':
    import socket
    import paramiko

#--------------------------------------------------------------------------
#  GET_LOCAL : TOOL FUNCTION TO DOWNLOAD FILE IN LOCAL TEMP FOLDER
#--------------------------------------------------------------------------
def get_local(date, sec_id, srv_addr, local_temp = ''):
    
    day=datetime.strftime(datetime.strptime(date, '%d/%m/%Y'),'%Y_%m_%d')
    
    full_path = os.path.realpath(__file__)
    path, f = os.path.split(full_path)
    
    if local_temp == '':
        full_path = os.path.realpath(__file__)
        local_temp, f = os.path.split(full_path)
        

    remote_data_path = '/quant/kc_repository_/get_tick/ft/%s/%s.mat' %(sec_id, day)
    
# =======
#     if datetime.strptime(date, '%d/%m/%Y')<=datetime.strptime('11/07/2013', '%d/%m/%Y'):
#         remote_data_path = '/quant/kc_repository/get_tick/ft/%s/%s.mat' %(sec_id, day)
#     else:
#         remote_data_path = '/quant/test_kc_repository/get_tick/ft/%s/%s.mat' %(sec_id, day)
#         
# >>>>>>> a2b650edf088e4926707c59b77efebdeb2d0973a
    local_addr = socket.gethostbyname(socket.gethostname())
    local_data_path = '%s/temp_buffer/%s.mat' %(path, day) 
    
#     print "Importing file from distant repository :"
#     print "Source : %s @ %s ==>" %(remote_data_path, srv_addr)
#     print "Target : %s @ %s <==" %(local_data_path, local_addr)
    
    transport = paramiko.Transport((srv_addr, 22))
    transport.connect(username = 'flexsys', password = 'flexsys1')
    
    sftp = paramiko.SFTPClient.from_transport(transport)
    
    sftp.get(remote_data_path, '%s/%s.mat' %(local_temp,day))
    
    sftp.close()
    transport.close()
    return 0

#--------------------------------------------------------------------------
#  FT : LOAD MATFILES OF STOCK TBT DATA
#--------------------------------------------------------------------------
def ft(**kwargs):
    
    ##############################################################
    # input handling
    ##############################################################
    #---- security_info
    if "security_id" in kwargs.keys():  
        ids=kwargs["security_id"]
    else:
        raise NameError('read_dataset:ft - Bad input')
    #---- date info
    if "date" in kwargs.keys():
        date=kwargs["date"]
        date_newf=datetime.strftime(datetime.strptime(date, '%d/%m/%Y'),'%Y_%m_%d') 
    else:
        raise NameError('read_dataset:ft - Bad input') 
        
    #### CONFIG and CONNECT
    # TODO: in xml file
    if os.name=='nt':
        if datetime.strptime(date, '%d/%m/%Y')<=datetime.strptime('11/07/2013', '%d/%m/%Y'):
            ft_root_path="Q:\\kc_repository"
        else:
            ft_root_path="Q:\\test_kc_repository"
    else:
        if datetime.strptime(date, '%d/%m/%Y')<=datetime.strptime('11/07/2013', '%d/%m/%Y'):
            ft_root_path="/quant/kc_repository"
        else:
            ft_root_path="/quant/test_kc_repository"
    
    ##############################################################
    # load and format
    ##############################################################
    remote = False
    if 'remote' in kwargs.keys():
        remote = kwargs['remote']
        
    if remote == True and os.name != 'nt':
        
        get_local(date,kwargs['security_id'],'172.29.0.32')
        full_path = os.path.realpath(__file__)
        path, f = os.path.split(full_path)
        
        filename = '%s/%s.mat'%(path, date_newf)
    else:
        filename=os.path.join(ft_root_path,'get_tick','ft','%d'%(ids),'%s.mat'%(date_newf))

    try:
        mat = scipy.io.loadmat(filename, struct_as_record  = False)
        if remote == True and os.name != 'nt':
            os.remove(filename)
        print 'read_dataset:ft - File LOAD <'+filename+'>'
        
        return st_data.to_dataframe(mat['data'],timezone=True)
    except IOError:
        get_traceback()
#         raise NameError('read_dataset:ft - This file does not exist <'+filename+'>')
        logging.warning('read_dataset:ft - This file does not exist <'+filename+'>')
        
        return pd.DataFrame()
        
    


#--------------------------------------------------------------------------
#  ftickdb : filter and LOAD MATFILES OF STOCK TBT DATA
#--------------------------------------------------------------------------
def ftickdb(**kwargs):
    data=ft(**kwargs)
    if data.shape[0]>0:
        data=data[(data['trading_after_hours']==0) & (data['trading_at_last']==0) & (data['cross']==0)]
    return data


#--------------------------------------------------------------------------
# histocurrency 
#--------------------------------------------------------------------------
def histocurrencypair(date = None, last_date_from = None, start_date = None, end_date = None, currency = None, currency_ref = None):
    
    out = pd.DataFrame()
    
    ##############################################################
    # input handling
    ##############################################################
    #---- date info
    if date is not None:
        start_date = end_date = date
        req_n = 1
    elif last_date_from is not None:
        req_n = 2
    elif start_date is not None or end_date is not None:
        if start_date is None or end_date is None:
            raise NameError('read_dataset:histocurrencypair - Bad input for dates')        
        req_n = 1
    #---- currencyref info
    if currency_ref is not None: 
        raise NameError('read_dataset:histocurrencypair - currency_ref not available NOW') 
    else:
        curr_ref=['EUR']
        
    str_curr_ref="("+"".join(["'"+x+"'," for x in curr_ref])
    str_curr_ref=str_curr_ref[:-1]+")"
    
    #---- currency info
    if currency is not None:
        curr = currency 
        if isinstance(currency, basestring):
            curr=[currency]
        elif (not isinstance(currency, list)) and (not isinstance(curr, np.ndarray)):
            raise NameError('read_dataset:histocurrencypair - Bad input for currency ref')
        str_curr = "("+"".join(["'"+x+"'," for x in curr])
        str_curr=str_curr[:-1]+")" 
    else:
        curr=[]
        str_curr=[]
    
    ##############################################################
    # request and format
    ##############################################################

        
    ####  Build request
    if req_n == 1:
        req=("""SELECT 
                DATE,CCY,CCYREF,VALUE
                FROM
                    KGR..HISTOCURRENCYTIMESERIES
                WHERE
                    DATE>= '%s'
                    and DATE<= '%s'
                    and SOURCEID=1
                    and ATTRIBUTEID=43
                    and CCYREF in %s""" ) % (start_date, end_date, str_curr_ref)
    
    elif req_n == 2:
        req=( """SELECT TOP(%d)
              DATE,CCY,CCYREF,VALUE
              FROM
              KGR..HISTOCURRENCYTIMESERIES
              WHERE
                DATE<= '%s'
                and SOURCEID = 1
                and ATTRIBUTEID = 43
                and CCYREF in %s """) % (len(curr), last_date_from, str_curr_ref)    
    
    if not curr == []:
        req=req+((" and CCY in %s ") % (str_curr))
    req += 'ORDER BY DATE DESC  '
          
    #### EXECUTE REQUEST 
    vals=Connections.exec_sql('KGR',req)
    
    ####  OUTPUT 
    if not vals:
        return out  
     
    
    
    out=pd.DataFrame.from_records(vals,columns=['date','ccy','ccyref','rate'],index=['date'])
    out=out.sort_index()
    return out


#--------------------------------------------------------------------------
# lastrate2ref 
#--------------------------------------------------------------------------
def lastrate2ref(currency=None,date=None,nb_days=10):
    # TODO : only ref is euro rigt now
    out=np.nan
    data=histocurrencypair(currency=currency,
                           start_date=datetime.strftime(datetime.strptime(date, '%d/%m/%Y')-timedelta(days=nb_days),'%d/%m/%Y'),
                            end_date=date)
    if data.shape[0]>0:
        out=data.ix[data.shape[0]-1]['rate']
    return out
    

#--------------------------------------------------------------------------
# bic : basic indicatos compted
#--------------------------------------------------------------------------    
def bic(step_sec=300,exchange=False,**kwargs):
    
    ##############################################################
    # input handling
    ##############################################################
    #---- security_info
    if "security_id" in kwargs.keys():  
        ids=kwargs["security_id"]
    else:
        raise NameError('read_dataset:bic - Bad input')
    #---- date info
    if "date" in kwargs.keys():
        from_newf=datetime.strptime(kwargs["date"], '%d/%m/%Y')
        # from_newf=datetime.strftime(datetime.strptime(kwargs["date"], '%d/%m/%Y'),'%Y%m%d')
        to_newf=from_newf
    elif all([x in kwargs.keys() for x in ["start_date","end_date"]]):
        from_newf=datetime.strptime(kwargs["start_date"], '%d/%m/%Y')
        to_newf=datetime.strptime(kwargs["end_date"], '%d/%m/%Y')          
    else:
        raise NameError('read_dataset:bic - Bad input for dates') 
    
    ##############################################################
    # computation
    ##############################################################
    out=pd.DataFrame()
    curr_newf=from_newf
    while curr_newf<=to_newf:
        try:
            # --- get tick data
            datatick=ftickdb(security_id=ids,date=datetime.strftime(curr_newf, '%d/%m/%Y'))

            if datatick.shape[0]>0:
                # --- aggregate
                if not exchange:
                    #---- aggregation
                    grouped=datatick.groupby([st_data.gridTime(date=datatick.index,step_sec=step_sec,out_mode='ceil'),
                                          'opening_auction','intraday_auction','closing_auction'])
                    grouped_data=pd.DataFrame([{'date':k[0],
                                                'opening_auction':k[1],'intraday_auction':k[2],'closing_auction':k[3],
                    'time_open': v.index.max(),
                    'time_close': v.index.min(),
                    'nb_trades': np.size(v.volume),
                    'volume': np.sum(v.volume),
                    'vwap': slicer.vwap(v.price,v.volume),
                    'vwasbp': slicer.vwasbp(v.bid,v.ask,v.price,v.volume,v.auction),
                    'open': v.price[0],
                    'high': np.max(v.price),
                    'low': np.min(v.price),
                    'close': v.price[-1]} for k,v in grouped])
                    
                    grouped_data=grouped_data.set_index('date')
                    #---- after aggregation
                    grouped_data['vol_GK']=formula.vol_gk(grouped_data['open'].values,grouped_data['high'].values,grouped_data['low'].values,grouped_data['close'].values,grouped_data['nb_trades'].values,
                           np.tile(timedelta(seconds=step_sec),grouped_data.shape[0]))
                    #----- add
                    out=out.append(grouped_data)
                
        except Exception,e:
            print "%s" % e
        curr_newf=curr_newf+timedelta(days=1)
    
    return out       


#--------------------------------------------------------------------------
# lastrate2ref 
#--------------------------------------------------------------------------
def trading_daily(start_date=None,end_date=None,security_id=[],include_agg=False,exchange_id=None,out_colnames=None):
    """
    trading_daily
    """
    # current list of columns
    available_colnames=['security_id','trading_destination_id','volume','turnover','open_prc','high_prc','low_prc','close_prc','nb_deal','open_volume','open_turnover',
     'close_volume','close_turnover','average_spread_numer','average_spread_denom','intraday_volume','intraday_turnover','cross_volume','cross_turnover',
     'open_nb_deal','close_nb_deal','intraday_nb_deal','auction_volume','auction_turnover','auction_nb_deal','deal_amount_var_log','deal_amount_avg_log',
     'dark_volume','dark_turnover','dark_nb_deal','midclose_volume','midclose_turnover','midclose_nb_deal','end_volume','end_turnover',
     'off_volume','off_turnover','overbid_volume','overask_volume','last_before_close_prc']
    
    ### ATTENTION : ne fonctionne que pour l'europe
    ##############################################################
    # input handling
    ##############################################################
    #--- dates
    if (start_date is None) or (end_date is None):
        raise NameError('read_dataset:trading_daily - Dates is mandatory')
        
    start_date=datetime.strftime(datetime.strptime(start_date,'%d/%m/%Y'),'%Y%m%d')
    end_date=datetime.strftime(datetime.strptime(end_date,'%d/%m/%Y'),'%Y%m%d')
    
    #--- security_id
    if isinstance(security_id,list):
        security_id=np.array(security_id)
    elif not isinstance(security_id,np.ndarray):
        security_id=np.array([security_id])
        
    ##############################################################
    # construct the request
    ##############################################################
    #---- select
    if out_colnames is None:
        select_req=" SELECT tddaily.*,exchref.EXCHANGETYPE "
    else:
        if np.any([x not in available_colnames for x in out_colnames]):
            raise valueError('bad out_colnames')
        out_colnames=np.unique(out_colnames+['date','security_id','trading_destination_id'])
        select_req='SELECT '+''.join(['tddaily.'+x+',' for x in out_colnames])+'exchref.EXCHANGETYPE '
    #---- from
    from_req=(" FROM Market_data..trading_daily tddaily "
    " LEFT JOIN KGR..EXCHANGEREFCOMPL exchref ON ( "
    "    exchref.EXCHANGE=tddaily.trading_destination_id "
    " ) ")
    #---- order
    order_by_req=" ORDER BY tddaily.date,tddaily.security_id,tddaily.trading_destination_id "
    #---- where
    where_req=" WHERE tddaily.date>='"+start_date+"' AND tddaily.date<='"+end_date+"'"
    if security_id.shape[0]>0:
        str_lids="("+"".join([str(x)+',' for x in uniqueext(security_id)])
        str_lids=str_lids[:-1]+")"
        where_req=where_req+" AND tddaily.security_id in %s" % (str_lids)
                
    check_include_agg=True
    if exchange_id is not None:
        if isinstance(exchange_id,basestring) and exchange_id=="main":
            where_req=where_req+" AND exchref.EXCHANGETYPE='M' "
        elif isinstance(exchange_id,basestring) and exchange_id=="agg":
            where_req=where_req+" AND tddaily.trading_destination_id is NULL "
            check_include_agg=False
        else:
            if isinstance(exchange_id,list):
                exchange_id=np.array(exchange_id)
            elif not isinstance(exchange_id,np.ndarray):
                exchange_id=np.array([exchange_id])
            else:
                raise NameError('read_dataset:trading_daily - exchange_id')
            
            str_lids="("+"".join([str(x)+',' for x in uniqueext(exchange_id)])
            str_lids=str_lids[:-1]+")"
            where_req=where_req+" AND tddaily.trading_destination_id in %s" % (str_lids)
            
    if check_include_agg and (not include_agg):
        where_req=where_req+" AND tddaily.trading_destination_id is not NULL "
        
    req=select_req+from_req+where_req+order_by_req
    
    vals=Connections.exec_sql("MARKET_DATA",req,schema = True)
    
    ##############################################################
    # output
    ##############################################################    
    out=pd.DataFrame()
    if not (not vals):
        out=pd.DataFrame.from_records(vals[0],columns=vals[1])
        out['date']=[datetime.strptime(x,'%Y-%m-%d') for x in out['date']]
        out=out.set_index('date')
        
    return out
    
    
    
    
    

if __name__=='__main__':
    # ft london stock
    data=ft(security_id=10735,date='11/03/2013')
    # ft french stock
    data=ft(security_id=110,date='11/03/2013')
    # ft french stock (local)
    data=ft(security_id=110,date='11/03/2013', remote=True)
    # currency rate
    print histocurrencypair(start_date='20130501',end_date='20130502',currency=['GBX','SEK'])
    # currency rate
    print histocurrencypair(last_date_from='20130803', currency=['GBX','SEK'])
