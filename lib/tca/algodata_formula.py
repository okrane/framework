# -*- coding: utf-8 -*-
"""
Created on Wed Oct 16 14:31:46 2013

@author: whuang
"""
import numpy as np

dict_occ_fe = {'mturnover_euro':      {'mturnover_euro': lambda df : np.sum(df.rate_to_euro*df.occ_fe_exec_shares*df.occ_fe_avg_price.apply(lambda x : float(x))*1e-6),
                                       'nb_occurrence': lambda df : len(df.p_occ_id)}, 
               'nb_occurrence':       {'mturnover_euro': lambda df : np.sum(df.rate_to_euro*df.occ_fe_exec_shares*df.occ_fe_avg_price.apply(lambda x : float(x))*1e-6),
                                       'nb_occurrence': lambda df : len(df.p_occ_id)},
               'avg_slippage_bp':     {'nb_occurrence': lambda df : len(df.p_occ_id),
                                       'avg_slippage_bp': lambda df: np.average(df.slippage_bp[np.isfinite(df.slippage_bp)])},
               'avg_slippage_spread': {'nb_occurrence': lambda df : len(df.p_occ_id),
                                       'avg_slippage_spread': lambda df: np.average(df.slippage_spread[np.isfinite(df.slippage_spread)])},
               'std_slippage_bp':     {'nb_occurrence': lambda df : len(df.p_occ_id),
                                       'std_slippage_bp': lambda df: np.std(df.slippage_bp[np.isfinite(df.slippage_bp)])},
               'std_slippage_spread': {'nb_occurrence': lambda df : len(df.p_occ_id),
                                       'std_slippage_spread': lambda df: np.std(df.slippage_spread[np.isfinite(df.slippage_spread)])}
              }
              
dict_seq = {'mturnover_euro':         {'mturnover_euro': lambda df : np.sum(df.rate_to_euro*df.turnover)*1e-6,
                                       'nb_occurrence':  lambda df : len(np.unique(df.p_occ_id))}
           }

def get_formula(level,key):
    if level == 'occ_fe':
        return dict_occ_fe[key]
    elif level == 'sequence':
        return dict_seq[key]
    