# -*- coding: utf-8 -*-
"""
Created on Fri Jan 17 12:52:36 2014

@author: njoseph
"""

import pandas as pd
import datetime as dt
import pytz
import time as time
import numpy as np
import lib.stats.slicer as slicer
import lib.stats.formula as formula
from lib.tca.formulatca import FormulaTCA
# structure
     

        
class AlgoComputeStatsConfig:

    @staticmethod 
    def db_one_occurrence():
        return {'get_data' : {'mode_colnames' : 'base' , 'level' : ['occurrence','sequence','deal']},
                'apply' : [{#-- EXEC INFO : AGG DEALS TO SEQUENCE
                            'mode' : 'slicer',
                            'info' : {'in_level' : 'deal',
                                      'out_level' : 'sequence',
                                      'group_vars' : ['p_cl_ord_id'],
                                      'replace_cols' : True,
                                      'slicer' : {'exec_qty':{'default': 0 ,'slicer' : lambda df : np.sum(df.volume.values)},
                                                      'exec_nb_trades': {'default': 0 ,'slicer' : lambda df : np.size(df.volume.values)},
                                                      'exec_turnover': {'default': 0.0 ,'slicer' : lambda df : np.sum(map(lambda x,y : x*y,df.volume.values,df.price.values))},
                                                      'exec_first_deal_time' : {'default': None ,'slicer' : lambda df : np.min([x.to_datetime() for x in df.index])},
                                                      'exec_last_deal_time' : {'default': None ,'slicer' : lambda df : np.max([x.to_datetime() for x in df.index])}
                                                      }
                                      }
                            },
                           {#-- EXEC INFO : cumulative stats at occurence level
                            'mode' : 'formula',
                            'info' : {'level' : 'sequence',
                                      'in_sort' : ['p_cl_ord_id','nb_replace'],
                                      'in_sort_ascending' : True,
                                      'formula' : {'occ_prev_exec_qty': lambda df : np.concatenate([[0.0],np.cumsum(df.exec_qty.values)[:-1]]),
                                                   'occ_prev_exec_turnover': lambda df : np.concatenate([[0.0],np.cumsum(df.exec_turnover.values)[:-1]])
                                                   }
                                      }
                            },
                           {#-- EXEC INFO : AGG SEQUENCE TO OCCURRENCE
                            'mode' : 'slicer',
                            'info' : {'in_level' : 'sequence',
                                      'out_level' : 'occurrence',
                                      'group_vars' : ['p_occ_id'],
                                      'replace_cols' : True,
                                      'slicer' : {'occ_last_strategy_name_mapped': {'default': None ,'slicer' : lambda df : slicer.last_finite(df.strategy_name_mapped.values)},
                                                  'occ_strategy_name_mapped': {'default': None ,'slicer' : lambda df : slicer.last_finite(df.strategy_name_mapped.values) if len(np.unique(df.strategy_name_mapped.values))==1 else 'MULTIPLE'},
                                                  'occ_order_qty': {'default': 0 ,'slicer' : lambda df : df.OrderQty.values[-1]},
                                                  'occ_nb_replace': {'default': 0 ,'slicer' : lambda df : np.size(df.p_occ_id.values)-1},
                                                  'occ_exec_qty': {'default': 0 ,'slicer' : lambda df : np.sum(df.exec_qty.values)},
                                                  'occ_exec_nb_trades': {'default': 0 ,'slicer' : lambda df : np.sum(df.exec_nb_trades.values)},
                                                  'occ_exec_turnover': {'default': 0.0 ,'slicer' : lambda df : np.sum(df.exec_turnover.values)}
                                                  }
                                      }
                            },
                            {#-- MARKET AGG STATS : ON DEALS
                            'mode' : 'aggregate_market_stats',
                            'info' : {'level' : 'sequence',
                                      'market_slicer' : {'before_lit' : {'order_constr' : False,
                                                             'period_time_constr' : True,
                                                             'period_phase_constr' : True,
                                                             'filter' : {'mode' : 'before' , 'market' : 'all' , 'market_visibility' : 'lit'},
                                                             'slicer' : {'m_b_arrival_price_lit' : {'default' : np.nan, 'slicer' : lambda df : slicer.last_finite(df.volume.values)}}
                                                             },
                                                         'period_lit' : {'order_constr' : False,
                                                                         'period_time_constr' : True,
                                                                         'period_phase_constr' : True,
                                                                         'filter' : {'mode' : 'during' , 'market' : 'all' , 'market_visibility' : 'lit'},
                                                                         'slicer' : {'m_p_volume_lit' : {'default' : 0 , 'slicer' : lambda df : np.sum(df.volume.values)},
                                                                                     'm_p_turnover_lit' : {'default' : 0.0 , 'slicer' : lambda df : np.sum(df.volume.values*df.price.values)},
                                                                                     'm_p_volume_opening' : {'default' : 0 , 'slicer' : lambda df : np.sum(df[df['opening_auction']==1].volume.values)},
                                                                                     'm_p_volume_closing' : {'default' : 0 , 'slicer' : lambda df : np.sum(df[df['closing_auction']==1].volume.values)},
                                                                                     'm_p_volume_intraday' : {'default' : 0 , 'slicer' : lambda df : np.sum(df[df['intraday_auction']==1].volume.values)},
                                                                                     'm_p_volume_other_auctions' : {'default' : 0 , 'slicer' : lambda df : np.sum(df[(df['auction']==1) & (df['opening_auction']==0) & (df['intraday_auction']==0) & (df['closing_auction']==0)].volume.values)},
                                                                                     'm_p_open_lit' : {'default' : np.nan , 'slicer' : lambda df : slicer.first_finite(df.price.values)},
                                                                                     'm_p_high_lit' : {'default' : np.nan , 'slicer' : lambda df : np.max(df.price.values)},
                                                                                     'm_p_low_lit' : {'default' : np.nan , 'slicer' : lambda df : np.min(df.price.values)},
                                                                                     'm_p_close_lit' : {'default' : np.nan , 'slicer' : lambda df : slicer.last_finite(df.price.values)},
                                                                                     'm_p_vwas_lit' : {'default' : np.nan , 'slicer' : lambda df : slicer.vwas(df.bid.values,df.ask.values,df.price.values,df.volume.values,df.auction.values)},
                                                                                     'm_p_vol_GK_lit' : {'default' : np.nan , 'slicer' : lambda df : float(formula.vol_gk(np.array([slicer.first_finite(df.price.values)]), np.array([np.max(df.price.values)]), np.array([np.min(df.price.values)]),
                                                                                                                np.array([slicer.last_finite(df.price.values)]),np.array([ np.size(df.price.values)]), 
                                                                                                                np.array([slicer.last_finite([x.to_datetime() for x in df.index])-slicer.first_finite([x.to_datetime() for x in df.index])])))},
                                                                                     'm_p_nb_trades_lit_cont' : {'default' : 0 , 'slicer' : lambda df : np.size(df[df['auction'] == 0].volume.values)}
                                                                                     }
                                                                         },
                                                         'period_lit_main' : {'order_constr' : False,
                                                                              'period_time_constr' : True,
                                                                              'period_phase_constr' : True,
                                                                              'filter' : {'mode' : 'during' , 'market' : 'main' , 'market_visibility' : 'lit'},
                                                                              'slicer' : {'m_p_volume_lit_main' : {'default' : 0 , 'slicer' : lambda df : np.sum(df.volume.values)},
                                                                                          'm_p_turnover_lit_main' : {'default' : 0.0 , 'slicer' : lambda df : np.sum(df.volume.values*df.price.values)},
                                                                                          'm_p_vwas_lit_main' : {'default' : np.nan , 'slicer' : lambda df : slicer.vwas(df.bid.values,df.ask.values,df.price.values,df.volume.values,df.auction.values)},
                                                                                          'm_p_nb_trades_lit_main_cont' : {'default' : 0 , 'slicer' : lambda df : np.size(df[df['auction'] == 0].volume.values)}
                                                                                          }
                                                                              },
                                                         'period_dark' : {'order_constr' : False,
                                                                          'period_time_constr' : True,
                                                                          'period_phase_constr' : True,
                                                                          'filter' : {'mode' : 'during' , 'market' : 'all' , 'market_visibility' : 'dark'},
                                                                          'slicer' : {'m_p_volume_dark' : {'default' : 0 , 'slicer' : lambda df : np.sum(df.volume.values)},
                                                                                      'm_p_turnover_dark' : {'default' : 0.0 , 'slicer' : lambda df : np.sum(df.volume.values*df.price.values)}
                                                                                      }
                                                                          },
                                                         'period_lit_constr' : {'order_constr' : True,
                                                                                'period_time_constr' : True,
                                                                                'period_phase_constr' : True,
                                                                                'filter' : {'mode' : 'during' , 'market' : 'all' , 'market_visibility' : 'lit'},
                                                                                'slicer' : {'m_p_volume_lit_constr' :  {'default' : 0 , 'slicer' : lambda df : np.sum(df.volume.values)},
                                                                                            'm_p_turnover_lit_constr' :  {'default' : 0.0 , 'slicer' : lambda df : np.sum(df.volume.values*df.price.values)}
                                                                                            }
                                                                                },
                                                         'period_lit_main_constr' : {'order_constr' : True,
                                                                                     'period_time_constr' : True,
                                                                                     'period_phase_constr' : True,
                                                                                     'filter' : {'mode' : 'during' , 'market' : 'main' , 'market_visibility' : 'lit'},
                                                                                     'slicer' : {'m_p_volume_lit_main_constr' :  {'default' : 0 , 'slicer' : lambda df : np.sum(df.volume.values)},
                                                                                                 'm_p_turnover_lit_main_constr' :  {'default' : 0.0 , 'slicer' : lambda df : np.sum(df.volume.values*df.price.values)}
                                                                                                 }
                                                                                     },
                                                         'period_dark_constr' : {'order_constr' : True,
                                                                                 'period_time_constr' : True,
                                                                                 'period_phase_constr' : True,
                                                                                 'filter' : {'mode' : 'during' , 'market' : 'all' , 'market_visibility' : 'dark'},
                                                                                 'slicer' : {'m_p_volume_dark_constr' :  {'default' : 0 , 'slicer' : lambda df : np.sum(df.volume.values)},
                                                                                             'm_p_turnover_dark_constr' :  {'default' : 0.0 , 'slicer' : lambda df : np.sum(df.volume.values*df.price.values)}
                                                                                             }
                                                                                 }
                                                         },
                                      'formula' : {'m_b_arrival_price_lit' : lambda df : map(lambda x,y : x if x > 0 else y, df.m_b_arrival_price_lit.values , df.m_p_open_lit.values)}
                                      }
                             },
                           {#-- MARKET AGG STATS : AGG SEQUENCE TO OCCURRENCE
                            'mode' : 'slicer',
                            'info' : {'in_level' : 'sequence',
                                      'out_level' : 'occurrence',
                                      'group_vars' : ['p_occ_id'],
                                      'replace_cols' : True,
                                      'slicer' : {'occ_exec_first_deal_time' : {'default': None ,'slicer' : lambda df : slicer.first_finite(df.exec_first_deal_time.values)},
                                                  'occ_exec_last_deal_time' : {'default': None ,'slicer' : lambda df : slicer.last_finite(df.exec_last_deal_time.values)},
                                                  'occ_bench_starttime' : {'default': None ,'slicer' : lambda df : slicer.first_finite(df.bench_starttime.values)},
                                                  'occ_bench_endtime' : {'default': None ,'slicer' : lambda df : slicer.last_finite(df.bench_endtime.values)},
                                                  #--- order market stats
                                                  'occ_m_p_vwap_lit' : {'default': np.nan ,'slicer' : lambda df : np.nan if np.sum(df.m_p_volume_lit.values) == 0 else np.sum(df.m_p_turnover_lit.values)/np.sum(df.m_p_volume_lit.values)},
                                                  'occ_m_p_volume_lit' : {'default': 0 ,'slicer' : lambda df : np.sum(df.m_p_volume_lit.values)},
                                                  'occ_m_p_turnover_lit' : {'default': 0.0 ,'slicer' : lambda df : np.sum(df.m_p_turnover_lit.values)},
                                                  'occ_m_p_vwap_lit_constr' : {'default': np.nan ,'slicer' : lambda df : np.nan if np.sum(df.m_p_volume_lit_constr.values) == 0 else np.sum(df.m_p_turnover_lit_constr.values)/np.sum(df.m_p_volume_lit_constr.values)},
                                                  'occ_m_p_volume_lit_constr' : {'default': np.nan ,'slicer' : lambda df : np.sum(df.m_p_volume_lit_constr.values)},
                                                  'occ_m_p_turnover_lit_constr' : {'default': 0.0 ,'slicer' : lambda df : np.sum(df.m_p_turnover_lit_constr.values)},
                                                  'occ_m_p_vwas_lit': {'default': np.nan ,'slicer' : lambda df : slicer.weighted_statistics(df.m_p_vwas_lit.values,df.m_p_volume_lit.values,mode='mean')},
                                                  'occ_m_p_vwas_lit_main': {'default': np.nan ,'slicer' : lambda df : slicer.weighted_statistics(df.m_p_vwas_lit_main.values,df.m_p_volume_lit_main.values,mode='mean')},
                                                  'occ_m_b_arrival_price_lit': {'default': np.nan ,'slicer' : lambda df : slicer.first_finite(df.m_b_arrival_price_lit.values)},
                                                  'occ_m_p_close_lit': {'default': np.nan ,'slicer' : lambda df : slicer.last_finite(df.m_p_close_lit.values)}
                                                  }
                                      }
                            },
                           {#-- EXEC INFO : cumulative stats at occurence level
                            'mode' : 'formula',
                            'info' : {'level' : 'occurrence',
                                      'formula' : {'occ_exec_vwap' : lambda df : df.occ_exec_turnover/df.occ_exec_qty,
                                                   #--- performance
                                                   'occ_slippage_vwap_bp' : lambda df : FormulaTCA.slippage_tca(df = df, bench='vwap', units='bp', exclude_dark=True, constr=False, agg=True, data_level='occurrence'),
                                                   'occ_slippage_vwap_constr_bp' :  lambda df : FormulaTCA.slippage_tca(df = df, bench='vwap', units='bp', exclude_dark=True, constr=True, agg=True, data_level='occurrence'),
                                                   'occ_slippage_is_bp' :  lambda df : FormulaTCA.slippage_tca(df = df, bench='is', units='bp', exclude_dark=True, constr=False, agg=True, data_level='occurrence'),
                                                   'occ_spread_bp' :  lambda df : map(lambda x,y : 10000 * min(x,y),df.occ_m_p_vwas_lit,df.occ_m_p_vwas_lit_main)/df.occ_m_p_vwap_lit,
                                                   #--- check against fe data
                                                   'occ_fe_turnover' :  lambda df : df.occ_fe_inmkt_turnover + df.occ_fe_prv_turnover,
                                                   'occ_fe_volume' : lambda df : df.occ_fe_inmkt_volume + df.occ_fe_prv_volume,
                                                   'occ_fe_vwap' : lambda df : ((df.occ_fe_inmkt_turnover + df.occ_fe_prv_turnover) / (df.occ_fe_inmkt_volume + df.occ_fe_prv_volume)),
                                                   'occ_fe_arrival_price' : lambda df : df.occ_fe_arrival_price.apply(lambda x : np.nan if x<=0.0 else x)
                                                   }
                                      }
                            }
                           ]
                }


if __name__=='__main__':
    print getattr(AlgoComputeStatsConfig,'db_one_occurrence', None)
    print getattr(AlgoComputeStatsConfig,'db_one_occurrence')()