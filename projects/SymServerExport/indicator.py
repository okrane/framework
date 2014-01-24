# -*- coding: utf-8 -*-
"""
Created on Tue Sep 24 11:12:35 2013

@author: njoseph
"""

import os
import pandas as pd
import datetime as dt
import numpy as np
import pytz
import numpy as np
import lib.dbtools.statistical_engine as se
from lib.dbtools.connections import Connections
from lib.logger import *
from lib.io.toolkit import get_traceback
from lib.data.ui.Explorer import Explorer
import lib.data.matlabutils as mutils


def mappingflid():
    out=[
        [ 1, 305 ],  # usual_daily_volume
        [ 2, 306 ],  # usual_opening_volume
        [ 3, 307 ],  # usual_closing_volume
        [ 21, 308 ],   # usual_midopening_volume
        [ 22, 309 ],  # usual_continuous_nb_deals
        [ 24, 310 ],  # usual_continuous_daily_volume
        [ 25, 311 ],   # average_trade_size
        [ 27, 312 ],  # beta_place
        [ 29, 313 ],   # msci_beta_sector
        [ 31, 314 ],   # msci_beta_zone
        [ 33, 315 ],   # beta_world
        [ 35, 316 ],  # usual_daily_amount
        [ 37, 317 ],  # volume_q1        
        [ 38, 318 ],   # volume_q2
        [ 39, 319 ],   # volume_q3
        [ 40, 320 ],   # volume_q4
        [ 73, 321 ],  # usual_daily_spread
        [ 75, 322 ],   # usual_daily_volatility
        ]
    
    return pd.DataFrame(out,columns=['indicator_id','flid'])


def export_symdata(data_security_referential = None,
                   path_export = None, filename_export = None,
                   indicators2export = [1, 2, 3, 21, 22, 24, 25, 27, 29, 31, 33, 35, 37, 38, 39, 40 , 73, 75]):

    #--------------------
    #- TEST INPUT
    #--------------------
    if data_security_referential is None or path_export is None or filename_export is None or indicators2export is None:
        raise ValueError('bad inputs')
        
    if not os.path.exists(path_export):
        raise ValueError('path not defined')
        
    if data_security_referential.shape[0]==0:
        raise ValueError('no security ref')
        
    logging.info('START export_symdata')
    
    
    #--------------------
    #- NEEDED DATA
    #--------------------   
    all_sec_ids = np.unique(data_security_referential['cheuvreux_secid'].values)
    str_indicators2export = "".join([str(x)+',' for x in indicators2export])[:-1]
    map_flid_indid = mappingflid()
    if np.any([x not in map_flid_indid['indicator_id'].values for x in indicators2export]):
        raise ValueError('one of the asked indictor do not have yet a flid')
       
    #--------------------
    #- ADD DATA (by chunks of securities in order to avoid memory leaks)
    #--------------------
    with_data_symbol = []
    out = open( os.path.join(path_export, filename_export), 'w' )
    
    NB_MIN_SYMBOL = 5000
    NB_MAX_SEC = 2000
    last_idx = -1
    NONE_ = -999
    
    while last_idx < len(all_sec_ids)-1:
        
        # logging.info('pct adv'+str(last_idx/(len(all_sec_ids))))
        #--------------------
        #- get indicators from database
        #--------------------
        s_idx = np.min([ last_idx+1, len(all_sec_ids)-1 ])
        last_idx = np.min([ last_idx+NB_MAX_SEC, len(all_sec_ids)-1 ])
        str_secids = "".join([str(x)+',' for x in all_sec_ids[range( s_idx, last_idx+1 )]])[:-1]
        
        query_indicatops=" SELECT si.security_id,isnull(si.trading_destination_id, %d) as trading_destination_id,si.indicator_id,si.indicator_value " \
            " FROM MARKET_DATA..ci_security_indicator si " \
            " LEFT JOIN KGR..EXCHANGEREFCOMPL erefcompl on ( " \
            " si.trading_destination_id=erefcompl.EXCHANGE " \
            " ) " \
            " WHERE si.security_id in (%s)  " \
            " AND si.indicator_id in (%s) " \
            " AND ( erefcompl.EXCHANGETYPE is NULL or erefcompl.EXCHANGETYPE = 'M' ) " \
            " ORDER BY si.security_id, trading_destination_id, si.indicator_id " % (NONE_,str_secids,str_indicators2export)
        
        vals=Connections.exec_sql('MARKET_DATA',query_indicatops,schema = True)
        if not vals[0]:
            continue
        
        vals = pd.DataFrame.from_records(vals[0],columns=vals[1])
        
        uni_ = mutils.uniqueext(vals[['security_id','trading_destination_id']].values, rows = True)
        
        #--------------------
        #- add to symdata
        #--------------------                
        for i in range(0, len(uni_)):
            #--- for each couple (sec_id, td_id)
            tmp_data = vals[(vals['security_id'] == uni_[i][0]) & (vals['trading_destination_id'] == uni_[i][1])]
            
            #--- for each flex symbol
            this_sec_info = data_security_referential[ data_security_referential['cheuvreux_secid'] ==  uni_[i][0]]
            
            for isec in range(0, this_sec_info.shape[0]):
                #-- get flex symbol
                symbol = this_sec_info['ticker'].values[isec]
                
                if uni_[i][1] == NONE_:
                    symbol = this_sec_info['tickerAG'].values[isec]
                    
                if symbol is None or not isinstance(symbol,basestring):
                    continue
                
                #-- write str
                add_str = ''
                for idata in range(0,tmp_data.shape[0]):
                    #--- for each indicators 
                    flidid = map_flid_indid[map_flid_indid['indicator_id'] == int(tmp_data.iloc[idata]['indicator_id'])]['flid'].values[0]
                    add_str += str(symbol) + ':' + str(flidid) + '=' + str(tmp_data.iloc[idata]['indicator_value']) + '\n'
                    
                add_str += '\n'
                out.write(add_str)
                with_data_symbol.append(symbol)
                
    out.close()   
    logging.info('END export_symdata: successfully create indicator export')
    
    #--------------------
    #- RETURN DATA
    #--------------------
    all_symbol = np.concatenate([np.unique(data_security_referential['ticker'].values),np.unique(data_security_referential['tickerAG'].values)])
    all_symbol = [all_symbol[x] for x in np.where([isinstance(x,basestring) for x in all_symbol])[0]]
    without_data_symbol = np.setdiff1d(all_symbol, with_data_symbol).tolist()
    
    #-------------------------------------------------------------------------
    # CHECK ADDED CURVES
    #-------------------------------------------------------------------------
    if len(with_data_symbol) < NB_MIN_SYMBOL:
        raise ValueError('generated file contain less than' + str(NB_MIN_SYMBOL) + ' symbol with indicators')
    
    return with_data_symbol , without_data_symbol
            
##------------------------------------------------------------------------------
# check database update
#------------------------------------------------------------------------------       
def check_db_update(date):
    
    out = False
    
    try:
        
        date_s = dt.datetime.strftime(date.date(),'%Y%m%d')
        date_e = dt.datetime.strftime((date + dt.timedelta(days=1)).date(),'%Y%m%d')
        date_f = dt.datetime.strftime((date - dt.timedelta(days=1)).date(),'%Y%m%d')
        
        query=""" SELECT date , jobname ,status
                  FROM Market_data..ciupdate_report
                  WHERE date >= '%s' and date < '%s' """ % (date_f,date_e)
                  
        vals = Connections.exec_sql('MARKET_DATA',query,schema = True)
        if not vals[0]:
            logging.warning('No info of indicator update for date : ' + date_s)
        else:
            data = pd.DataFrame.from_records(vals[0],columns=vals[1])
            #-- ciupdatesecurityindicator_all : day before
            if any((data['jobname'] == 'ciupdatesecurityindicator_all') & (data['date'] >= dt.datetime.strptime(date_f,'%Y%m%d'))):
                out = any(data[(data['jobname'] == 'ciupdatesecurityindicator_all') & (data['date'] >= dt.datetime.strptime(date_f,'%Y%m%d'))]['status'].values == 'O')
                
            #-- ciupdatesecurityindicator_all : day before
            if out and any((data['jobname'] == 'ciupdatesecurityindicator_ost') & (data['date'] >= dt.datetime.strptime(date_s,'%Y%m%d'))):
                out = any(data[(data['jobname'] == 'ciupdatesecurityindicator_ost') & (data['date'] >= dt.datetime.strptime(date_s,'%Y%m%d'))]['status'].values == 'O')                
                
            if not out:
                logging.warning('Indicator database has not been update properly for date : ' + date_s)
               
    except:
        logging.error('error in check_db_update func')
    
    return out


if __name__ == "__main__":
    
    Connections.change_connections('production_copy')
    
    GPATH = 'W:\\Global_Research\\Quant_research\\projets\\export_sym'
    
    security_ref = pd.read_csv(os.path.join(GPATH,'test','TRANSCOSYMBOLCHEUVREUX.csv'),sep = ';')
    security_ref = security_ref[['cheuvreux_secid', 'ticker', 'tickerAG']]
    
    export_symdata(data_security_referential = security_ref,
                       path_export = os.path.join(GPATH, 'test'), 
                       filename_export = 'symdata',
                       indicators2export = [25])

    