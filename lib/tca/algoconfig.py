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
from lib.tca.slicertca import SlicerTCA
# structure



class AlgoComputeStatsConfig:
    
    @staticmethod 
    def db_one_occurrence():
        return {'get_data' : {'mode_colnames' : 'base_fe' , 'level' : ['sequence','occurrence','deal']},
                'apply' : [{#-- SECURITY INFO FROM REFERENTIAL to SEQUENCE
                            'mode' : 'merge_referential',
                            'info' : {'from_data' : 'security_info' , 'to_level' : 'sequence' , 'group_vars' : ['cheuvreux_secid'], 'cols' : ['code_bloomberg','ISIN']}
                            },  
                            {#-- SECURITY INFO FROM REFERENTIAL to OCCURRENCE
                            'mode' : 'merge_referential',
                            'info' : {'from_data' : 'security_info' , 'to_level' : 'occurrence' , 'group_vars' : ['cheuvreux_secid'], 'cols' : ['code_bloomberg','ISIN']}
                            },      
                            {#-- FLAG DEAL DATA
                            'mode' : 'flag_deals',
                            'info' : {}
                            },    
                           {#-- SOME MAPPING
                            'mode' : 'formula',
                            'info' : {'level' : 'sequence',
                                      'formula' : {'bench_type': lambda df : FormulaTCA.bench_type(df = df),
                                                   }
                                      }
                            },   
                            {#-- EXEC INFO : AGG DEALS TO SEQUENCE
                            'mode' : 'slicer',
                            'info' : {'in_level' : 'deal',
                                      'out_level' : 'sequence',
                                      'group_vars' : ['p_cl_ord_id'],
                                      'replace_cols' : True,
                                      'merge_out_cols' : {'from_level' : 'sequence' , 'to_level' : 'deal', 'group_vars' : ['p_cl_ord_id'], 'cols' : ['WouldLevel']},
                                      #'merge_referential_cols' : {'from_data' : 'exchange_info' , 'to_level' : 'deal', 'group_vars' : ['MIC'], 'cols' : ['EXCHANGETYPE']},
                                      'slicer_' : {'exec_qty':{'default': 0 ,'slicer' : lambda df : int(np.sum(df.volume.values))},
                                                  'exec_qty_dark':{'default': 0 ,'slicer' : lambda df : SlicerTCA.exec_qty_dark(df)},
                                                  'exec_qty_would':{'default': 0 ,'slicer' : lambda df : SlicerTCA.exec_qty_would(df)},
                                                  'exec_qty_main':{'default': 0 ,'slicer' : lambda df : SlicerTCA.exec_qty_main(df)},
                                                  'exec_qty_opening':{'default': 0 ,'slicer' : lambda df : SlicerTCA.exec_qty_tphase(df = df , mode ='opening')},
                                                  'exec_qty_closing':{'default': 0 ,'slicer' : lambda df : SlicerTCA.exec_qty_tphase(df , mode ='closing')},
                                                  'exec_qty_manual_fill':{'default': 0 ,'slicer' : lambda df : SlicerTCA.exec_qty_tphase(df , mode ='manual_fill')},
                                                  'exec_nb_trades': {'default': 0 ,'slicer' : lambda df : np.size(df.volume.values)},
                                                  'exec_turnover': {'default': 0.0 ,'slicer' : lambda df : np.sum(map(lambda x,y : x*y,df.volume.values,df.price.values))},
                                                  'exec_first_deal_price' : {'default': np.nan ,'slicer' : lambda df : slicer.first_finite(df.price.values , default = np.nan)},
                                                  'exec_last_deal_price' : {'default': np.nan ,'slicer' : lambda df : slicer.last_finite(df.price.values , default = np.nan)},
                                                  'exec_first_deal_time' : {'default': None ,'slicer' : lambda df : np.min([x.to_datetime() for x in df.index])},
                                                  'exec_last_deal_time' : {'default': None ,'slicer' : lambda df : np.max([x.to_datetime() for x in df.index])}
                                                  } # TODO : exec_passive/agressive/opening/closing/other with street orders data
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
                                      'slicer_' : {'occ_last_strategy_name_mapped': {'default': None ,'slicer' : lambda df : slicer.last_finite(df.strategy_name_mapped.values)},
                                                  'occ_strategy_name_mapped': {'default': None ,'slicer' : lambda df : slicer.last_finite(df.strategy_name_mapped.values) if len(np.unique(df.strategy_name_mapped.values))==1 else 'MULTIPLE'},
                                                  'occ_bench_type': {'default': None ,'slicer' : lambda df : slicer.last_finite(df.bench_type.values) if len(np.unique(df.bench_type.values))==1 else 'B'},
                                                  'occ_order_qty': {'default': 0 ,'slicer' : lambda df : slicer.last_finite(df.OrderQty.values)},
                                                  'occ_nb_replace': {'default': 0 ,'slicer' : lambda df : np.size(df.p_occ_id.values)-1},
                                                  'occ_exec_qty': {'default': 0 ,'slicer' : lambda df : np.sum(df.exec_qty.values)},
                                                  'occ_exec_qty_dark': {'default': 0 ,'slicer' : lambda df : np.sum(df.exec_qty_dark.values)},
                                                  'occ_exec_qty_would': {'default': 0 ,'slicer' : lambda df : np.sum(df.exec_qty_would.values)},
                                                  'occ_exec_qty_main': {'default': 0 ,'slicer' : lambda df : np.sum(df.exec_qty_main.values)},
                                                  'occ_exec_qty_opening': {'default': 0 ,'slicer' : lambda df : np.sum(df.exec_qty_opening.values)},
                                                  'occ_exec_qty_closing': {'default': 0 ,'slicer' : lambda df : np.sum(df.exec_qty_closing.values)},
                                                  'occ_exec_qty_manual_fill': {'default': 0 ,'slicer' : lambda df : np.sum(df.exec_qty_manual_fill.values)},
                                                  'occ_exec_nb_trades': {'default': 0 ,'slicer' : lambda df : np.sum(df.exec_nb_trades.values)},
                                                  'occ_exec_turnover': {'default': 0.0 ,'slicer' : lambda df : np.sum(df.exec_turnover.values)},
                                                  'occ_exec_first_deal_price': {'default': np.nan ,'slicer' : lambda df : slicer.first_finite(df.exec_first_deal_price.values , default = np.nan)},
                                                  'occ_exec_last_deal_price': {'default': np.nan ,'slicer' : lambda df : slicer.last_finite(df.exec_first_deal_price.values , default = np.nan)},
                                                  'occ_exec_first_deal_time': {'default': None ,'slicer' : lambda df : slicer.first_finite(df.exec_first_deal_time.values)},
                                                  'occ_exec_last_deal_time': {'default': None ,'slicer' : lambda df : slicer.last_finite(df.exec_last_deal_time.values)},
                                                  }
                                      }
                            },
                            {#-- MARKET AGG STATS : MARKET -> SEQUENCE
                            'mode' : 'aggregate_market_stats',
                            'info' : {'level' : 'sequence',
                                      'market_slicer' : {'before_lit' : {'order_constr' : False,
                                                             'period_time_constr' : True,
                                                             'period_phase_constr' : True,
                                                             'filter' : {'mode' : 'before' , 'market' : 'all' , 'market_visibility' : 'lit'},
                                                             'slicer' : {'m_b_arrival_price_lit' : {'default' : np.nan, 'slicer' : lambda df : slicer.last_finite(df.price.values)}}
                                                             },
                                                         'before_lit_main' : {'order_constr' : False,
                                                             'period_time_constr' : True,
                                                             'period_phase_constr' : True,
                                                             'filter' : {'mode' : 'before' , 'market' : 'main' , 'market_visibility' : 'lit'},
                                                             'slicer' : {'m_b_arrival_price_lit_main' : {'default' : np.nan, 'slicer' : lambda df : slicer.last_finite(df.price.values)}}
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
                                                                                     'm_p_open_lit' : {'default' : np.nan , 'slicer' : lambda df : slicer.first_finite(df.price.values , default = np.nan)},
                                                                                     'm_p_high_lit' : {'default' : np.nan , 'slicer' : lambda df : np.max(df.price.values)},
                                                                                     'm_p_low_lit' : {'default' : np.nan , 'slicer' : lambda df : np.min(df.price.values)},
                                                                                     'm_p_close_lit' : {'default' : np.nan , 'slicer' : lambda df : slicer.last_finite(df.price.values , default = np.nan)},
                                                                                     'm_p_close_lit_cont' : {'default' : np.nan , 'slicer' : lambda df : slicer.last_finite(df[df['auction'] == 0].price.values , default = np.nan)},
                                                                                     'm_p_vwas_lit' : {'default' : np.nan , 'slicer' : lambda df : slicer.vwas(df.bid.values,df.ask.values,df.price.values,df.volume.values,df.auction.values)},
                                                                                     'm_p_vol_gk_lit' : {'default' : np.nan , 'slicer' : lambda df : np.nan if df.shape[0] == 0 else float(formula.vol_gk(np.array([slicer.first_finite(df.price.values , default = np.nan)]), np.array([np.max(df.price.values)]), np.array([np.min(df.price.values)]),
                                                                                                                np.array([slicer.last_finite(df.price.values , default = np.nan)]),np.array([ np.size(df.price.values)]), 
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
                                      'formula' : {'m_b_arrival_price_lit' : lambda df : map(lambda x,y : x if x > 0 else y, df.m_b_arrival_price_lit.values , df.m_p_open_lit.values),
                                                   'm_b_arrival_price_lit_main' : lambda df : map(lambda x,y : x if x > 0 else y, df.m_b_arrival_price_lit_main.values , df.m_p_open_lit.values)}
                                      }
                             },
                            {#-- MARKET DAILY STATS : MARKET -> SEQUENCE
                            'mode' : 'daily_market_stats',
                            'info' : {'level' : 'sequence',
                                      'market_formula' : {'main' : {'filter' : {'market' : 'main'},
                                                                   'formula' : {'m_d_spread_bp_main' : lambda df : formula.none_modifier(df.average_spread_numer.values)/formula.none_modifier(df.average_spread_denom.values),
                                                                                'm_d_volume_lit_main' : lambda df : df.volume.values,
                                                                                'm_d_volume_auction_main' : lambda df : df.auction_volume.values}
                                                                   },
                                                          'all' : {'filter' : {'market' : 'all'},
                                                                   'formula' : {'m_d_spread_bp_all' : lambda df : formula.none_modifier(df.average_spread_numer.values)/formula.none_modifier(df.average_spread_denom.values),
                                                                                'm_d_vol_gk_bp' : lambda df : formula.vol_gk(df.open_prc.values , df.high_prc.values , df.low_prc.values , df.close_prc.values, df.nb_deal.values , [dt.timedelta(minutes = 8*60)]),
                                                                                'm_d_volume_lit' : lambda df : df.volume.values,
                                                                                'm_d_nb_trades_lit_cont' : lambda df : df.nb_deal.values - df.auction_nb_deal.values,
                                                                                'm_d_opening_price' : lambda df : df.open_prc.values,
                                                                                'm_d_closing_price' : lambda df : df.close_prc.values}
                                                                   }
                                                         }
                                      }
                             },
                           {#-- MARKET AGG STATS : MARKET -> OCCURRENCE
                            'mode' : 'aggregate_market_stats',
                            'info' : {'level' : 'occurrence',
                                      'market_slicer' : {'period_after_lit' : {'order_constr' : False,
                                                                               'period_time_constr' : True,
                                                                               'period_phase_constr' : False,
                                                                               'filter' : {'mode' : 'during_after' , 'market' : 'all' , 'market_visibility' : 'lit'},
                                                                               'slicer' : {'occ_m_pa_pvwap10_lit' :  {'default' : np.nan , 'dict_data' : ['occ_exec_qty'] , 'slicer' : lambda df,dictdata : SlicerTCA.pwp(df,pct = 10 , exec_qty = dictdata['occ_exec_qty'])},
                                                                                           'occ_m_pa_pvwap20_lit' :  {'default' : np.nan , 'dict_data' : ['occ_exec_qty'] , 'slicer' : lambda df,dictdata : SlicerTCA.pwp(df,pct = 20 , exec_qty = dictdata['occ_exec_qty'])},
                                                                                           'occ_m_pa_pvwap30_lit' :  {'default' : np.nan , 'dict_data' : ['occ_exec_qty'] , 'slicer' : lambda df,dictdata : SlicerTCA.pwp(df,pct = 30 , exec_qty = dictdata['occ_exec_qty'])}
                                                                                           },
                                                                               }
                                                         }
                                      }
                            },
                           {#-- MARKET AGG STATS : AGG SEQUENCE TO OCCURRENCE
                            'mode' : 'slicer',
                            'info' : {'in_level' : 'sequence',
                                      'out_level' : 'occurrence',
                                      'group_vars' : ['p_occ_id'],
                                      'replace_cols' : True,
                                      'slicer_' : {'occ_bench_starttime' : {'default': None ,'slicer' : lambda df : slicer.first_finite(df.bench_starttime.values)},
                                                  'occ_bench_endtime' : {'default': None ,'slicer' : lambda df : slicer.last_finite(df.bench_endtime.values)},
                                                  #--- order market stats (liquidity)
                                                  'occ_m_p_volume_lit' : {'default': 0 ,'slicer' : lambda df : np.sum(df.m_p_volume_lit.values)},
                                                  'occ_m_p_volume_lit_constr' : {'default': np.nan ,'slicer' : lambda df : np.sum(df.m_p_volume_lit_constr.values)},
                                                  'occ_m_p_volume_lit_main' : {'default': 0 ,'slicer' : lambda df : np.sum(df.m_p_volume_lit_main.values)},
                                                  'occ_m_p_volume_lit_main_constr' : {'default': 0 ,'slicer' : lambda df : np.sum(df.m_p_volume_lit_main_constr.values)},
                                                  'occ_m_p_volume_dark' : {'default': 0 ,'slicer' : lambda df : np.sum(df.m_p_volume_dark.values)},
                                                  'occ_m_p_volume_dark_constr' : {'default': 0 ,'slicer' : lambda df : np.sum(df.m_p_volume_dark_constr.values)},
                                                  'occ_m_p_volume_closing' : {'default': 0 ,'slicer' : lambda df : np.sum(df.m_p_volume_closing.values)},
                                                  'occ_m_p_volume_opening' : {'default': 0 ,'slicer' : lambda df : np.sum(df.m_p_volume_opening.values)},
                                                  'occ_m_p_volume_auction' : {'default': 0 ,'slicer' : lambda df : np.sum(df.m_p_volume_opening.values) + np.sum(df.m_p_volume_closing.values) + np.sum(df.m_p_volume_intraday.values) + np.sum(df.m_p_volume_other_auctions.values)},
                                                  'occ_m_p_turnover_lit' : {'default': 0.0 ,'slicer' : lambda df : np.sum(df.m_p_turnover_lit.values)},
                                                  'occ_m_p_turnover_lit_constr' : {'default': 0.0 ,'slicer' : lambda df : np.sum(df.m_p_turnover_lit_constr.values)},
                                                  'occ_m_p_turnover_lit_main' : {'default': 0.0 ,'slicer' : lambda df : np.sum(df.m_p_turnover_lit_main.values)},
                                                  'occ_m_p_turnover_lit_main_constr' : {'default': 0.0 ,'slicer' : lambda df : np.sum(df.m_p_turnover_lit_main_constr.values)},
                                                  'occ_m_p_turnover_dark' : {'default': 0.0 ,'slicer' : lambda df : np.sum(df.m_p_turnover_dark.values)},
                                                  'occ_m_p_turnover_dark_constr' : {'default': 0.0 ,'slicer' : lambda df : np.sum(df.m_p_turnover_dark_constr.values)},
                                                  'occ_m_p_nb_trades_lit_cont' : {'default': 0 ,'slicer' : lambda df : np.sum(df.m_p_nb_trades_lit_cont.values)},
                                                  #--- order market stats (price)
                                                  'occ_m_p_vwas_lit': {'default': np.nan ,'slicer' : lambda df : slicer.weighted_statistics(df.m_p_vwas_lit.values,df.m_p_volume_lit.values,mode='mean')},
                                                  'occ_m_p_vwas_lit_main': {'default': np.nan ,'slicer' : lambda df : slicer.weighted_statistics(df.m_p_vwas_lit_main.values,df.m_p_volume_lit_main.values,mode='mean')},
                                                  'occ_m_p_vwap_lit' : {'default': np.nan ,'slicer' : lambda df : np.nan if np.sum(df.m_p_volume_lit.values) == 0 else np.sum(df.m_p_turnover_lit.values)/np.sum(df.m_p_volume_lit.values)},
                                                  'occ_m_p_vwap_lit_constr' : {'default': np.nan ,'slicer' : lambda df : np.nan if np.sum(df.m_p_volume_lit_constr.values) == 0 else np.sum(df.m_p_turnover_lit_constr.values)/np.sum(df.m_p_volume_lit_constr.values)},
                                                  'occ_m_p_vol_gk_lit' : {'default' : np.nan , 'slicer' : lambda df : np.nan if df.shape[0] == 0 else float(formula.vol_gk(np.array([slicer.first_finite(df.m_p_open_lit.values , default = np.nan)]), np.array([np.max(df.m_p_high_lit.values)]), 
                                                                                                                                           np.array([np.min(df.m_p_low_lit.values)]),np.array([slicer.last_finite(df.m_p_close_lit.values , default = np.nan)]),
                                                                                                                                           np.array([0.5 * (np.max(df.m_p_high_lit.values)+np.min(df.m_p_low_lit.values))]),
                                                                                                                                           np.array([slicer.last_finite(df.bench_endtime.values)-slicer.first_finite(df.bench_starttime.values)])))},
                                                  'occ_m_b_arrival_price_lit': {'default': np.nan ,'slicer' : lambda df : slicer.first_finite(df.m_b_arrival_price_lit.values , default = np.nan)},
                                                  'occ_m_b_arrival_price_lit_main': {'default': np.nan ,'slicer' : lambda df : slicer.first_finite(df.m_b_arrival_price_lit_main.values , default = np.nan)},
                                                  'occ_m_p_open_lit': {'default': np.nan ,'slicer' : lambda df : slicer.first_finite(df.m_p_open_lit.values , default = np.nan)},
                                                  'occ_m_p_high_lit': {'default': np.nan ,'slicer' : lambda df : np.max(df.m_p_high_lit.values)},
                                                  'occ_m_p_low_lit': {'default': np.nan ,'slicer' : lambda df : np.min(df.m_p_low_lit.values)},
                                                  'occ_m_p_close_lit': {'default': np.nan ,'slicer' : lambda df : slicer.last_finite(df.m_p_close_lit.values , default = np.nan)},
                                                  'occ_m_p_close_lit_cont': {'default': np.nan ,'slicer' : lambda df : slicer.last_finite(df.m_p_close_lit_cont.values , default = np.nan)},
                                                  }
                                      }
                            },
                           {#-- TCA PURPOSE : daily market stats to occurrence
                            'mode' : 'slicer',
                            'info' : {'in_level' : 'sequence',
                                      'out_level' : 'occurrence',
                                      'group_vars' : ['p_occ_id'],
                                      'replace_cols' : True,
                                      'slicer_' : {'occ_m_d_volume_lit' : {'default': 0 ,'slicer' : lambda df : slicer.last_finite(df.m_d_volume_lit.values)},
                                                  'occ_m_d_volume_lit_main' : {'default': 0 ,'slicer' : lambda df : slicer.last_finite(df.m_d_volume_lit_main.values)},
                                                  'occ_m_d_volume_auction_main' : {'default': 0 ,'slicer' : lambda df : slicer.last_finite(df.m_d_volume_auction_main.values)},
                                                  'occ_m_d_nb_trades_lit_cont' : {'default': 0 ,'slicer' : lambda df : slicer.last_finite(df.m_d_nb_trades_lit_cont.values)},
                                                  'occ_m_d_vol_gk_bp' : {'default': np.nan ,'slicer' : lambda df : slicer.last_finite(df.m_d_vol_gk_bp.values)},
                                                  'occ_m_d_opening_price' : {'default': np.nan ,'slicer' : lambda df : slicer.last_finite(df.m_d_opening_price.values)},
                                                  'occ_m_d_closing_price' : {'default': np.nan ,'slicer' : lambda df : slicer.last_finite(df.m_d_closing_price.values)},
                                                  'occ_m_d_spread_bp' : {'default': np.nan ,'slicer' : lambda df : np.min([slicer.last_finite(df.m_d_spread_bp_main.values),slicer.last_finite(df.m_d_spread_bp_all.values)])}
                                                  }
                                      }
                            },
                           {#--TCA PURPOSE : FORMULA :  OTHER + PERFORMANCE INFO : will be pushed in algo TCA
                            'mode' : 'formula',
                            'info' : {'level' : 'occurrence',
                                      'formula' : {#-- exec info
                                                   'occ_exec_vwap' : lambda df : df.occ_exec_turnover/df.occ_exec_qty,
                                                   'occ_bench_duration_min' : lambda df : map(lambda x,y : ((y-x).total_seconds())/60 if (isinstance(x,dt.datetime) and isinstance(y,dt.datetime)) else np.nan,df.occ_bench_starttime,df.occ_bench_endtime),
                                                   #--- daily stats ( TO DO)
                                                   #'m_d_spread_bp' :  lambda df : map(lambda x,y : 10000 * min(x,y),df.occ_m_p_vwas_lit,df.occ_m_p_vwas_lit_main)/df.occ_m_p_vwap_lit,
                                                   #--- OCC (slippage occurrence)
                                                   'occ_slippage_vwap_bp' : lambda df : FormulaTCA.slippage_tca(df = df, bench='vwap', units='bp', exclude_dark=True, constr=False, bench_type = True, data_level='occurrence'),
                                                   'occ_slippage_vwap_constr_bp' :  lambda df : FormulaTCA.slippage_tca(df = df, bench='vwap', units='bp', exclude_dark=True, constr=True, bench_type = True, data_level='occurrence'),
                                                   'occ_slippage_is_bp' :  lambda df : FormulaTCA.slippage_tca(df = df, bench='is', units='bp', exclude_dark=True, constr=False, bench_type = True, data_level='occurrence'),
                                                   #--- OCC FE
                                                   'occ_fe_turnover' :  lambda df : df.occ_fe_inmkt_turnover + df.occ_fe_prv_turnover,
                                                   'occ_fe_volume' : lambda df : df.occ_fe_inmkt_volume + df.occ_fe_prv_volume,
                                                   'occ_fe_vwap' : lambda df : ((df.occ_fe_inmkt_turnover + df.occ_fe_prv_turnover) / (df.occ_fe_inmkt_volume + df.occ_fe_prv_volume)),
                                                   'occ_fe_arrival_price' : lambda df : df.occ_fe_arrival_price.apply(lambda x : np.nan if x<=0.0 else x),
                                                   'occ_fe_slippage_vwap_constr_bp' : lambda df : FormulaTCA.slippage_tca_fe(df = df, bench = 'vwap'),
                                                   'occ_fe_slippage_is_bp' : lambda df : FormulaTCA.slippage_tca_fe(df = df, bench = 'is'),
                                                   'occ_fe_starttime' : lambda df : FormulaTCA.fe_renormtime(df = df , mode = 'start'),
                                                   'occ_fe_endtime' : lambda df : FormulaTCA.fe_renormtime(df = df , mode = 'end'),
                                                   #--- OCC (stats)
                                                   'occ_spread_bp' :  lambda df : map(lambda x,y : 10000 * min(x,y),df.occ_m_p_vwas_lit,df.occ_m_p_vwas_lit_main)/df.occ_m_p_vwap_lit,
                                                   'occ_plr_lit' :  lambda df : FormulaTCA.liquidity_ratio(df = df, mode = 'lit' , period ='period', data_level = 'occurrence'),
                                                   'occ_dlr_lit' :  lambda df : FormulaTCA.liquidity_ratio(df = df, mode = 'lit' , period ='daily', data_level = 'occurrence'),
                                                   }
                                      }
                            },
                           {#-- AGGREGATED PERF : sequence to occurrence
                            'mode' : 'slicer',
                            'info' : {'in_level' : 'sequence',
                                      'out_level' : 'occurrence',
                                      'group_vars' : ['p_occ_id'],
                                      'replace_cols' : True,
                                      'slicer_' : {'occ_compperf_vwap_bp' : {'default': np.nan ,'slicer' : lambda df : SlicerTCA.slippage_agg(df = df, weight_euro = False , bench='vwap', units='bp', exclude_dark=True, constr=False, bench_type = True, data_level='sequence')},
                                                   'occ_compperf_vwap_constr_bp' : {'default': np.nan ,'slicer' : lambda df : SlicerTCA.slippage_agg(df = df, weight_euro = False , bench='vwap', units='bp', exclude_dark=True, constr=True, bench_type = True, data_level='sequence')},
                                                   'occ_compperf_is_bp' : {'default': np.nan ,'slicer' : lambda df : SlicerTCA.slippage_agg(df = df, weight_euro = False , bench='is', units='bp', exclude_dark=True, constr=False, bench_type = True, data_level='sequence')},
                                                  }
                                      }
                            },
                           {#-- JOBID for algo tca
                            'mode' : 'formula',
                            'info' : {'level' : 'occurrence',
                                      'formula' : {'job_id': lambda df : map(lambda x : 'ATCA' + dt.datetime.strftime(x,'%Y%m%d') if isinstance(x,dt.datetime) else '',df.occ_bench_starttime.values)
                                                   }
                                      }
                            },            
                           ]
                }


if __name__=='__main__':
    print getattr(AlgoComputeStatsConfig,'db_one_occurrence', None)
    print getattr(AlgoComputeStatsConfig,'db_one_occurrence')()