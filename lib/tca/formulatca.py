# -*- coding: utf-8 -*-
"""
Created on Thu Jan 16 17:25:10 2014

@author: njoseph
"""

import pandas as pd
import numpy as np
import datetime as dt

class FormulaTCA:
    
    @staticmethod 
    def slippage_tca(df=None, bench=None, units='bp', exclude_dark=True, constr=True, agg=True, data_level=None ):    
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
            
        ##--- create suffix
        suffix=''
        if not agg:
            suffix+='_main'
        if constr:
            suffix+='_constr'
            
        #-- level
        if bench.lower()=='vwap':
            ##--- value bench
            if exclude_dark:
                value_bench=df[prefixlevel+'m_p_turnover_lit'+suffix]/df[prefixlevel+'m_p_volume_lit'+suffix]
                value_bench_valid = (df[prefixlevel+'m_p_turnover_lit'+suffix]>0) & (df[prefixlevel+'m_p_volume_lit'+suffix]>0)
                
            else:
                value_bench=(df[prefixlevel+'m_p_turnover_lit'+suffix]+df[prefixlevel+'m_p_turnover_dark'+suffix])/(df[prefixlevel+'m_p_volume_lit'+suffix]+df[prefixlevel+'m_p_volume_dark'+suffix])
                value_bench_valid = (df[prefixlevel+'m_p_turnover_lit'+suffix]>0) & (df[prefixlevel+'m_p_volume_lit'+suffix]>0) & (value_bench>0)
            
        elif bench.lower()=='is':
            value_bench = df[prefixlevel+'m_b_arrival_price_lit']
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
            out=df['Side']*(value_bench - exec_price)/df[prefixlevel+'vwas']
            value_bench_valid = value_bench_valid & (df[prefixlevel+'exec_turnover'] > 0) & (df[prefixlevel+'exec_qty'] > 0) & (df[prefixlevel+'vwas']>0)
            
        else:
            raise ValueError('formula:slippage_bp, bad input bench')
            
        out[~value_bench_valid] = np.nan
        
        return out