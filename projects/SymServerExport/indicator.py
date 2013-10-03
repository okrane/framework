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
    
    NB_MAX_SEC = 2000
    last_idx = -1
    
    while last_idx < len(all_sec_ids)-1:
        
        # logging.info('pct adv'+str(last_idx/(len(all_sec_ids))))
        
        #--------------------
        #- get indicators from database
        #--------------------
        s_idx = np.min([ last_idx+1, len(all_sec_ids)-1 ])
        last_idx = np.min([ last_idx+NB_MAX_SEC, len(all_sec_ids)-1 ])
        str_secids = "".join([str(x)+',' for x in all_sec_ids[range( s_idx, last_idx+1 )]])[:-1]
        
        query_indicatops=" SELECT si.security_id,si.trading_destination_id,si.indicator_id,si.indicator_value " \
            " FROM MARKET_DATA..ci_security_indicator si " \
            " LEFT JOIN KGR..EXCHANGEREFCOMPL erefcompl on ( " \
            " si.trading_destination_id=erefcompl.EXCHANGE " \
            " ) " \
            " WHERE si.security_id in (%s)  " \
            " AND si.indicator_id in (%s) " \
            " AND ( erefcompl.EXCHANGETYPE is NULL or erefcompl.EXCHANGETYPE = 'M' ) " \
            " ORDER BY si.security_id, si.trading_destination_id, si.indicator_id " % (str_secids,str_indicators2export)
        
        vals=Connections.exec_sql('MARKET_DATA',query_indicatops,schema = True)
        if not vals[0]:
            continue
        
        #--------------------
        #- add to symdata
        #--------------------
        last_symbol = ''
        
        for i in range(0, len(vals[0])):
            # l is one indicator line
            
            #-- flid id
            flidid = map_flid_indid[map_flid_indid['indicator_id'] == int(vals[0][i][2])]['flid'].values[0]
            
            #-- symbol
            sec_id = int(vals[0][i][0])
            this_sec_info = data_security_referential[ data_security_referential['cheuvreux_secid'] ==  sec_id]
            symbol = this_sec_info['ticker'].values[0]
            
            if vals[0][i][1] is None:
                symbol = this_sec_info['tickerAG'].values[0]
            
            if symbol is None or not isinstance(symbol,basestring):
                continue
            
            #-- write str
            # there is a double '\n' at each symbol change
            add_str = ''
            if i > 0 and last_symbol != symbol:
                add_str += '\n'
                
            add_str += str(symbol) + ':' + str(flidid) + '=' + str(vals[0][i][3]) + '\n'
            
            if i==len(vals[0])-1:
                add_str += '\n'
                
            out.write(add_str)
            
            #-- add to data
            if last_symbol != symbol:
                with_data_symbol.append(symbol)
            
            #-- for next loop
            last_symbol = symbol
            
            
    out.close()   
    logging.info('END export_symdata: successfully create indicator export')
    
    #--------------------
    #- RETURN DATA
    #--------------------
    all_symbol = np.concatenate([np.unique(data_security_referential['ticker'].values),np.unique(data_security_referential['tickerAG'].values)])
    all_symbol = [all_symbol[x] for x in np.where([isinstance(x,basestring) for x in all_symbol])[0]]
    without_data_symbol = np.setdiff1d(all_symbol, with_data_symbol).tolist()
    
    return with_data_symbol , without_data_symbol
            
            
    