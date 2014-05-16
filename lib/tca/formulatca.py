# -*- coding: utf-8 -*-
"""
Created on Thu Jan 16 17:25:10 2014

@author: njoseph
"""

import pandas as pd
import numpy as np
import datetime as dt
import pytz
import lib.stats.formula as formula
import re as re
import copy

class FormulaTCA:
    #-------------------------------
    # - INFOS
    #------------------------------- 
    @staticmethod
    def service(df = None):
        ##-------------------------    
        ##--- INPUTS + TESTS
        ##-------------------------
        #-- df
        if not isinstance(df,pd.DataFrame):
            raise ValueError('error inputs: df')        
        
        if df.shape[0] == 0:
            return None
        
        if 'TargetSubID' not in df.columns.tolist():
            raise ValueError('TargetSubID not in the input')
        
        return df['TargetSubID' ].apply(lambda x: 'Low Touch' if x in  ['ON1','ON2','ON3'] else 'High Touch')
    
    @staticmethod
    def bench_type(df = None):
        ##-------------------------    
        ##--- INPUTS + TESTS
        ##-------------------------    
        #-- df
        if not isinstance(df,pd.DataFrame):
            raise ValueError('error inputs: df')        
        
        if df.shape[0] == 0:
            return None
        
        if 'Symbol' not in df.columns.tolist():
            raise ValueError('Symbol not in the input')
        
        return df['Symbol' ].apply(lambda x: 'A' if isinstance(x,basestring) and len(x) > 2 and x[-2:] == 'AG' else 'M')
    
    #-------------------------------
    # - SLIPPAGE
    #-------------------------------  
    @staticmethod
    def slippage_tca(df = None, bench = None, units = 'bp', exclude_dark = True, constr = True, bench_type = False , agg=True, data_level = None ):    
        ##-------------------------    
        ##--- INPUTS + TESTS
        ##-------------------------
        #-- df
        if not isinstance(df,pd.DataFrame):
            raise ValueError('error inputs: df')
            
        #-- level - prefix
        if data_level == 'sequence':
            prefixlevel = ''
        elif data_level == 'occurrence':
            prefixlevel = 'occ_'
        else:
            raise ValueError('error inputs: data_level')
        
        if bench_type:
            ##--- test
            if prefixlevel + 'bench_type' not in df.columns.tolist():
                raise ValueError('columns ' + prefixlevel + 'bench_type has to be in dataset')
            
            ##--- create suffix
            suffix = ''
            if constr:
                suffix+= '_constr'
                
            #-- level
            if bench.lower()=='vwap':
                ##--- value bench
                if exclude_dark:
                    value_bench = df[prefixlevel+'m_p_turnover_lit'+suffix]/df[prefixlevel+'m_p_volume_lit'+suffix]
                    value_bench_valid = (df[prefixlevel+'m_p_turnover_lit'+suffix]>0) & (df[prefixlevel+'m_p_volume_lit'+suffix]>0)
                    
                    value_bench_main = df[prefixlevel+'m_p_turnover_lit_main'+suffix]/df[prefixlevel+'m_p_volume_lit_main'+suffix]
                    value_bench_valid_main  = (df[prefixlevel+'m_p_turnover_lit_main'+suffix]>0) & (df[prefixlevel+'m_p_volume_lit_main'+suffix]>0)
                    
                else:
                    value_bench = (df[prefixlevel+'m_p_turnover_lit'+suffix]+df[prefixlevel+'m_p_turnover_dark'+suffix])/(df[prefixlevel+'m_p_volume_lit'+suffix]+df[prefixlevel+'m_p_volume_dark'+suffix])
                    value_bench_valid = (df[prefixlevel+'m_p_turnover_lit'+suffix]>0) & (df[prefixlevel+'m_p_turnover_dark'+suffix]>0) & (value_bench>0)
                    
                    value_bench_main = (df[prefixlevel+'m_p_turnover_lit_main'+suffix]+df[prefixlevel+'m_p_turnover_dark'+suffix])/(df[prefixlevel+'m_p_volume_lit_main'+suffix]+df[prefixlevel+'m_p_volume_dark'+suffix])
                    value_bench_valid_main = (df[prefixlevel+'m_p_turnover_lit_main'+suffix]>0) & (df[prefixlevel+'m_p_turnover_dark'+suffix]>0) & (value_bench_main>0)
                                 
            elif bench.lower()=='is':
                value_bench = df[prefixlevel+'m_b_arrival_price_lit']
                value_bench_valid = (value_bench>0)
                # TODO
                value_bench_main = df[prefixlevel+'m_b_arrival_price_lit_main']
                value_bench_valid_main = value_bench_valid
            else:
                raise ValueError('formula:slippage_bp, bad input bench')
            
            # normalize
            value_bench[df[prefixlevel + 'bench_type'] =='M'] = value_bench_main[df[prefixlevel + 'bench_type'] =='M']
            value_bench_valid[df[prefixlevel + 'bench_type'] =='M'] = value_bench_valid_main[df[prefixlevel + 'bench_type'] =='M']
                         
        else:
            ##--- create suffix
            suffix_market = ''
            suffix_constr = ''
            if not agg:
                suffix_market = '_main'
            if constr:
                suffix_constr = '_constr'
                
            #-- level
            if bench.lower()=='vwap':
                ##--- value bench
                if exclude_dark:
                    value_bench = df[prefixlevel+'m_p_turnover_lit'+suffix_market + suffix_constr]/df[prefixlevel+'m_p_volume_lit'+suffix_market + suffix_constr]
                    value_bench_valid = (df[prefixlevel+'m_p_turnover_lit'+suffix_market + suffix_constr]>0) & (df[prefixlevel+'m_p_volume_lit'+suffix_market + suffix_constr]>0)
                    
                else:
                    value_bench = (df[prefixlevel+'m_p_turnover_lit'+suffix_market + suffix_constr]+df[prefixlevel+'m_p_turnover_dark'+suffix_constr])/(df[prefixlevel+'m_p_volume_lit'+suffix_market + suffix_constr]+df[prefixlevel+'m_p_volume_dark'+suffix_constr])
                    value_bench_valid = (df[prefixlevel+'m_p_turnover_lit'+suffix_market + suffix_constr]>0) & (df[prefixlevel+'m_p_turnover_dark'+suffix_constr]>0) & (value_bench>0)
                
            elif bench.lower()=='is':
                value_bench = df[prefixlevel + 'm_b_arrival_price_lit' + suffix_market]
                value_bench_valid = (value_bench>0)
                
            else:
                raise ValueError('formula:slippage_bp, bad input bench')
            
        ##-------------------------    
        ##--- output
        ##-------------------------
        exec_price = df[prefixlevel+'exec_turnover']/df[prefixlevel+'exec_qty']
        
        if units=='bp':
            out=10000*df['Side']*(value_bench - exec_price)/value_bench
            value_bench_valid = value_bench_valid & (df[prefixlevel+'exec_turnover'] > 0) & (df[prefixlevel+'exec_qty'] > 0)
            
        elif units=='spread':
            vwas = map(lambda x,y : min(x,y),df[prefixlevel+'m_p_vwas_lit_main'],df[prefixlevel+'m_p_vwas_lit'])
            out=df['Side']*(value_bench - exec_price)/vwas
            value_bench_valid = value_bench_valid & (df[prefixlevel+'exec_turnover'] > 0) & (df[prefixlevel+'exec_qty'] > 0) & (vwas>0)
            
        else:
            raise ValueError('formula:slippage_bp, bad input bench')
            
        out[~value_bench_valid] = np.nan
        
        return out
    
    #-------------------------------
    # - CONSTRAINED INFOS
    #-------------------------------
    @staticmethod 
    def sequence_constrained(df = None): 
        ##-------------------------    
        ##--- INPUTS + TESTS
        ##-------------------------
        #-- df
        if not isinstance(df,pd.DataFrame):
            raise ValueError('error inputs: df')
        
        if df.shape[0] == 0:
            return
           
        ##-------------------------    
        ##--- INPUTS + TESTS
        ##-------------------------                
        df['tmp_Constrained'] = 0
        
        if any(df['strategy_name_mapped'] == 'VWAP'):
            ids = (df['strategy_name_mapped'] == 'VWAP')
            df['tmp_Constrained'][ids] = map(lambda x1,x2,x3,x4,x5,x6 : x1 > 0.05 * x2 or x4 < 0.95 * x3 or x2 < 0.9 * (x5 - x6),
                                             df[ids].exec_qty_would.values,df[ids].exec_qty.values,df[ids].m_p_turnover_lit.values,df[ids].m_p_turnover_lit_constr.values,df[ids].OrderQty.values,df[ids]['occ_prev_exec_qty'].apply(lambda x : 0 if not x > 0 else x).values)
            
        if any(df['strategy_name_mapped'] != 'VWAP'):
            ids = (df['strategy_name_mapped'] != 'VWAP')
            df['tmp_Constrained'][ids] = map(lambda x1,x2,x3,x4: x1 > 0.05 * x2 or x4 < 0.95 * x3,
                                             df[ids].exec_qty_would.values,df[ids].exec_qty.values,df[ids].m_p_turnover_lit.values,df[ids].m_p_turnover_lit_constr.values)
            
        out = df['tmp_Constrained'].values
        df = df.drop('tmp_Constrained' , axis = 1)
        
        return out
        
        
        
        
               
    #-------------------------------
    # - LIQUIDITY STATS
    #-------------------------------
    @staticmethod 
    def liquidity_ratio(df = None, mode = 'lit' , period = 'period' , data_level = None):
        
        #-----------------
        #- TESTS AND INPUT
        #-----------------
        if df is None or mode is None or period is None or data_level is None:
            raise ValueError('bad inputs')
        
        if df.shape[0] == 0:
            return
        #--
        if mode == 'lit':
            mode_ = mode
        else:
            raise ValueError('Unknown mode + <' + mode +'>')
        #--
        if period == 'period':
            period_ = 'm_p_'
        elif period == 'daily':
            period_ ='m_d_'
        else:
            raise ValueError('Unknown period + <' + period +'>')       
        #--
        if data_level == 'sequence':
            level_ = ''
        elif data_level == 'occurrence': 
            level_ ='occ_'
        else:
            raise ValueError('Unknown data_level + <' + data_level +'>')        
        
        #-----------------
        #- COMPUTE
        #-----------------        
        out = [np.nan] * df.shape[0]
        
        for i in range(0,len(out)):
            if df[level_ + period_ + 'volume_' + mode_].values[i] > 0:
                out[i] = df[level_ + 'exec_qty'].values[i] / df[level_ + period_ + 'volume_' + mode_].values[i]
                
        return out
        
        
    #-------------------------------
    # - SPECIAL FE
    #-------------------------------
    @staticmethod 
    def slippage_tca_fe(df = None, bench = None):
        ##-------------------------    
        ##--- INPUTS + TESTS
        ##-------------------------
        #-- df
        if not isinstance(df,pd.DataFrame):
            raise ValueError('error inputs: df')
            
        ##-------------------------    
        ##--- COMPUTE
        ##-------------------------        
        out = df['Side'] * np.nan
        
        if bench == 'vwap':
            
            if not (('occ_fe_avg_price' not in df.columns.tolist()) or ('occ_fe_vwap' not in df.columns.tolist()) or ('occ_fe_exec_shares' not in df.columns.tolist())):
                out = 10000*df['Side']* (df['occ_fe_vwap'].apply(lambda x : float(x) if x > 0 else np.nan) - df['occ_fe_avg_price'].apply(lambda x : float(x) if x > 0 else np.nan)) / df['occ_fe_vwap'].apply(lambda x : float(x) if x > 0 else np.nan)
                
        elif bench == 'is':
            
            if not (('occ_fe_avg_price' not in df.columns.tolist()) or ('occ_fe_arrival_price' not in df.columns.tolist()) or ('occ_fe_exec_shares' not in df.columns.tolist())):
                out = 10000 * df['Side'] * (df['occ_fe_arrival_price'].apply(lambda x : float(x) if x > 0 else np.nan) - df['occ_fe_avg_price'].apply(lambda x : float(x) if x > 0 else np.nan)) / df['occ_fe_arrival_price'].apply(lambda x : float(x) if x > 0 else np.nan)
                
        else:
            raise ValueError('Unknown bench <' + bench + '>')
        
        return out
        
    @staticmethod 
    def fe_renormtime(df = None , mode = None):
        #--- TEST
        if not isinstance(df,pd.DataFrame):
            raise ValueError('df should be a DataFrame')
        
        if df.shape[0] == 0:
            return None
        
        if mode =='end':
            cols = 'occ_fe_endtime'
        elif mode =='start':
            cols = 'occ_fe_starttime'
        else:
            raise ValueError('Uknown mode <' + mode + '>')
        
        if cols not in df.columns.tolist():
            return None
            
        for i in range(0,df.shape[0]):
            if formula.isfinite([df[cols].values[i]])[0]:
                df[cols].values[i] = df[cols].values[i].zfill(6)
                
        return df[cols].values
               
    #-------------------------------
    # - STRATEGY INFOS
    #-------------------------------
    @staticmethod               
    def strategy_name_renorm(df = None , data_level = None):
        #--- TEST
        if not isinstance(df,pd.DataFrame):
            raise ValueError('bad inputs')
            
        if df.shape[0] == 0:
            return None   
          
        if data_level == 'occurrence':
            prefix = 'occ_'
        elif data_level == 'sequence':
            prefix = ''
        else:
            raise ValueError('Unknowm data_level')   
                 
        #--- 
        if not all([x in df.columns.tolist() for x in ['TargetSubID',prefix + 'strategy_name_mapped']]):
            raise ValueError('missing needed colnames')
        

            
        #-- replace Vwap by Vwap corpo
        id_vwap_corpo = map(lambda x,y : isinstance(x,basestring) and x.lower() == 'vwap' and isinstance(y,basestring) and y in ['NK1','NK2'],df[prefix + 'strategy_name_mapped'].values,df.TargetSubID.values)
        out = copy.deepcopy(df[prefix + 'strategy_name_mapped'])
        
        out[id_vwap_corpo] = 'VWAP CORPO'
        
        return out.values
        
              
        
        
            
        
        