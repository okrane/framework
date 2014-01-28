# -*- coding: utf-8 -*-
"""
Created on Thu Sep 05 09:03:08 2013

@author: njoseph
"""
import pandas as pd
import datetime as dt
import time as time
import pytz
import numpy as np
# import lib.data.matlabutils as matlabutils
import logging
import lib.tca.mapping as mapping
from lib.tca.algodata import *
import lib.data.dataframe_tools as dftools
import lib.data.matlabutils as mutils
import lib.io.toolkit as toolkit
import re
import lib.dbtools.get_repository as get_repository
import lib.stats.slicer as slicer
import lib.stats.formula as formula
from lib.tca.formulatca import FormulaTCA
from lib.tca.algoconfig import AlgoComputeStatsConfig
from lib.io.toolkit import get_traceback

# m_ = 'market'
#_p_ : period / _b_ : before / _d_ : daily
CONFIG_SLICER_SEQUENCE_STATS = {'before_lit' :
                         {'order_constr' : False,
                          'period_time_constr' : True,
                          'period_phase_constr' : True,
                          'filter' : {'mode' : 'before' , 'market' : 'all' , 'market_visibility' : 'lit'},
                          'slicer' : {'m_b_arrival_price_lit' : lambda df : slicer.last_finite(df.volume.values)},
                         },
                         'period_lit' :
                         {'order_constr' : False,
                          'period_time_constr' : True,
                          'period_phase_constr' : True,
                          'filter' : {'mode' : 'during' , 'market' : 'all' , 'market_visibility' : 'lit'},
                          'slicer' : {'m_p_volume_lit' : lambda df : np.sum(df.volume.values),
                                      'm_p_turnover_lit' : lambda df : np.sum(df.volume.values*df.price.values),
                                      'm_p_volume_opening' : lambda df : np.sum(df[df['opening_auction']==1].volume.values),
                                      'm_p_volume_closing' : lambda df : np.sum(df[df['closing_auction']==1].volume.values),
                                      'm_p_volume_intraday' : lambda df : np.sum(df[df['intraday_auction']==1].volume.values),
                                      'm_p_volume_other_auctions' : lambda df : np.sum(df[(df['auction']==1) & (df['opening_auction']==0) & (df['intraday_auction']==0) & (df['closing_auction']==0)].volume.values),
                                      'm_p_open_lit' : lambda df : slicer.first_finite(df.price.values),
                                      'm_p_high_lit' : lambda df : np.max(df.price.values),
                                      'm_p_low_lit' : lambda df : np.min(df.price.values),
                                      'm_p_close_lit' : lambda df : slicer.last_finite(df.price.values),
                                      'm_p_vwas_lit' : lambda df : slicer.vwas(df.bid.values,df.ask.values,df.price.values,df.volume.values,df.auction.values),
                                      'm_p_vol_GK_lit' : lambda df : float(formula.vol_gk(np.array([slicer.first_finite(df.price.values)]), np.array([np.max(df.price.values)]), np.array([np.min(df.price.values)]),
                                                                np.array([slicer.last_finite(df.price.values)]),np.array([ np.size(df.price.values)]), 
                                                                np.array([slicer.last_finite([x.to_datetime() for x in df.index])-slicer.first_finite([x.to_datetime() for x in df.index])]))),
                                      'm_p_nb_trades_lit_cont' : lambda df : np.size(df[df['auction'] == 0].volume.values)},
                         },
                         'period_lit_main' :
                         {'order_constr' : False,
                          'period_time_constr' : True,
                          'period_phase_constr' : True,
                          'filter' : {'mode' : 'during' , 'market' : 'main' , 'market_visibility' : 'lit'},
                          'slicer' : {'m_p_volume_lit_main' : lambda df : np.sum(df.volume.values),
                                      'm_p_turnover_lit_main' : lambda df : np.sum(df.volume.values*df.price.values),
                                      'm_p_vwas_lit_main' : lambda df : slicer.vwas(df.bid.values,df.ask.values,df.price.values,df.volume.values,df.auction.values),
                                      'm_p_nb_trades_lit_main_cont' : lambda df : np.size(df[df['auction'] == 0].volume.values)},
                         },
                         'period_dark' :
                         {'order_constr' : False,
                          'period_time_constr' : True,
                          'period_phase_constr' : True,
                          'filter' : {'mode' : 'during' , 'market' : 'all' , 'market_visibility' : 'dark'},
                          'slicer' : {'m_p_volume_dark' : lambda df : np.sum(df.volume.values),
                                      'm_p_turnover_dark' : lambda df : np.sum(df.volume.values*df.price.values)},
                         },
                         'period_lit_constr' :
                         {'order_constr' : True,
                          'period_time_constr' : True,
                          'period_phase_constr' : True,
                          'filter' : {'mode' : 'during' , 'market' : 'all' , 'market_visibility' : 'lit'},
                          'slicer' : {'m_p_volume_lit_constr' : lambda df : np.sum(df.volume.values),
                                      'm_p_turnover_lit_constr' : lambda df : np.sum(df.volume.values*df.price.values)},
                         },
                         'period_lit_main_constr' :
                         {'order_constr' : True,
                          'period_time_constr' : True,
                          'period_phase_constr' : True,
                          'filter' : {'mode' : 'during' , 'market' : 'main' , 'market_visibility' : 'lit'},
                          'slicer' : {'m_p_volume_lit_main_constr' : lambda df : np.sum(df.volume.values),
                                      'm_p_turnover_lit_main_constr' : lambda df : np.sum(df.volume.values*df.price.values)},
                         },
                         'period_dark_constr' :
                         {'order_constr' : True,
                          'period_time_constr' : True,
                          'period_phase_constr' : True,
                          'filter' : {'mode' : 'during' , 'market' : 'all' , 'market_visibility' : 'dark'},
                          'slicer' : {'m_p_volume_dark_constr' : lambda df : np.sum(df.volume.values),
                                      'm_p_turnover_dark_constr' : lambda df : np.sum(df.volume.values*df.price.values)},
                         },
                        }

CONFIG_FORMULA_SEQUENCE_STATS = {'m_b_arrival_price_lit' : lambda df : map(lambda x,y : x if x > 0 else y, df.m_b_arrival_price_lit.values , df.m_p_open_lit.values)}


CONFIG_SLICER_TCA = {'occ_exec_first_deal_time' : {'default': None ,'slicer' : lambda df : df.exec_first_deal_time.values[0]},
                    'occ_exec_last_deal_time' : {'default': None ,'slicer' : lambda df : df.exec_last_deal_time.values[-1]},
                    'occ_bench_starttime' : {'default': None ,'slicer' : lambda df : df.bench_starttime.values[0]},
                    'occ_bench_endtime' : {'default': None ,'slicer' : lambda df : df.bench_endtime.values[-1]},
                    #--- order market stats
                    'occ_m_p_vwap_lit' : {'default': np.nan ,'slicer' : lambda df : np.nan if np.sum(df.m_p_volume_lit.values) == 0 else np.sum(df.m_p_turnover_lit.values)/np.sum(df.m_p_volume_lit.values)},
                    'occ_m_p_volume_lit' : {'default': np.nan ,'slicer' : lambda df : np.sum(df.m_p_volume_lit.values)},
                    'occ_m_p_turnover_lit' : {'default': np.nan ,'slicer' : lambda df : np.sum(df.m_p_turnover_lit.values)},
                    'occ_m_p_vwap_lit_constr' : {'default': np.nan ,'slicer' : lambda df : np.nan if np.sum(df.m_p_volume_lit_constr.values) == 0 else np.sum(df.m_p_turnover_lit_constr.values)/np.sum(df.m_p_volume_lit_constr.values)},
                    'occ_m_p_volume_lit_constr' : {'default': np.nan ,'slicer' : lambda df : np.sum(df.m_p_volume_lit_constr.values)},
                    'occ_m_p_turnover_lit_constr' : {'default': np.nan ,'slicer' : lambda df : np.sum(df.m_p_turnover_lit_constr.values)},
                    'occ_m_p_vwas_lit': {'default': np.nan ,'slicer' : lambda df : slicer.weighted_statistics(df.m_p_vwas_lit.values,df.m_p_volume_lit.values,mode='mean')},
                    'occ_m_p_vwas_lit_main': {'default': np.nan ,'slicer' : lambda df : slicer.weighted_statistics(df.m_p_vwas_lit_main.values,df.m_p_volume_lit_main.values,mode='mean')},
                    'occ_m_b_arrival_price_lit': {'default': np.nan ,'slicer' : lambda df : slicer.first_finite(df.m_b_arrival_price_lit.values)},
                    'occ_m_p_close_lit': {'default': np.nan ,'slicer' : lambda df : slicer.last_finite(df.m_p_close_lit.values)}}

CONFIG_FORMULA_TCA={'occ_exec_vwap' : lambda df : df.occ_exec_turnover/df.occ_exec_qty,
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
         
                
class AlgoStatsProcessor(AlgoDataProcessor):
    
    ###########################################################################
    # INIT
    ###########################################################################
    def __init__(self,*args,**kwargs):
        # Parents attr
        AlgoDataProcessor.__init__(self,*args,**kwargs)
        
        #-- info on update columns (by data type)
        self.db_stats_enrichment = {}
        
        #-- data
        self.data_intraday_agg_deals = None
        
        
    ###########################################################################
    # SELF GET DATA METHODS
    ###########################################################################
    def get_occ_fe_data(self):
        
        if self.data_occurrence is not None:
            raise ValueError('nothing should be loaded')
        
        #-----------------------------------
        # GET DATA (XLS drom from Flex Stats)
        #-----------------------------------
        # --
        self.get_xls_occ_fe_data()
        dates = [x.to_datetime() for x in self.data_xls_occ_fe.index]
        max_date_xls = pytz.UTC.localize(dt.datetime.combine(np.max(dates).date(),dt.time(23,59)))
        
        # -- filter start end
        if self.start_date is not None and self.end_date is not None:
            sdate = self.start_date
            edate = self.end_date
            if sdate.tzinfo is None:
                sdate = pytz.UTC.localize(sdate)
            if edate.tzinfo is None:
                edate = pytz.UTC.localize(edate)
                
            self.data_xls_occ_fe = self.data_xls_occ_fe[ map(lambda x : x >= sdate and x <= edate, dates) ]
            
        # -- filter start end
        if self.filter is not None:
            try:     
                reg_client = self.filter['Account']['$regex']
            except:
                raise ValueError('only accept "import lib.dbtools.get_repository as get_repositoryAccount" and "$regex" for now')
            if self.data_xls_occ_fe.shape[0] > 0:
                self.data_xls_occ_fe = self.data_xls_occ_fe[self.data_xls_occ_fe['Account'].apply(lambda x : True if isinstance(x,basestring) and re.match(reg_client, x, flags=0) is not None else False)]
           
        #-----------------------------------
        # Merge with occurrence
        #-----------------------------------
        add_occurrence = pd.DataFrame()
        
        if (self.end_date is None) or (self.start_date is not None and self.end_date is not None and edate > max_date_xls):
            self.get_db_data(level = 'occurrence')
            #-----------------------------------
            # APPLY EXEC FORMULA STATS
            #-----------------------------------
            if self.data_occurrence.shape[0]>0:
                if 'occ_fe_strategy_name_mapped' not in self.data_occurrence.columns.values:
                    data_seq2load=False
                    if self.data_sequence is None:
                        data_seq2load=True
                        # bidouille 1 on load les sequence pour calculer le occ_strategy_name_
                        self.get_db_data(level='sequence',force_colnames_only=['strategy_name_mapped','OrderQty'])
                    
                    config_stats={'occ_fe_strategy_name_mapped': {'default': 'Unknown' ,'slicer' : lambda df : df.strategy_name_mapped.values[-1]},
                        'occ_fe_order_qty': {'default': 'Unknown' ,'slicer' : lambda df : df.OrderQty.values[-1]}}
                        
                    _add_data=dftools.agg(self.data_sequence,group_vars='p_occ_id',
                                          stats=dict([(x,config_stats[x]['slicer']) for x in config_stats.keys()]))
                                          
                    if _add_data.shape[0]>0:
                        index_name=self.data_occurrence.index.name
                        self.data_occurrence=self.data_occurrence.reset_index().merge(_add_data,how='left',on=['p_occ_id']).set_index(index_name)
                        
                    # bidouille 2 : on efface...
                    if data_seq2load:
                        self.data_sequence = None
                        
            add_occurrence = self.data_occurrence[map(lambda x : x > max_date_xls, [x.to_datetime() for x in self.data_occurrence.index])]
            self.data_occurrence = None
            
            
        self.data_occurrence = self.data_xls_occ_fe.append(add_occurrence)
        self.data_occurrence = self.data_occurrence.sort_index()
        self.data_xls_occ_fe = None
        # ATTENTION : Sale, mais, c'est deja n'importe quoi :)
        self.entry_level = 'occ_fe'

            
        #-----------------------------------
        # APPLY MARKET FORMULA STATS
        #-----------------------------------
        if self.data_occurrence.shape[0]>0:
            
            self.data_occurrence['occ_fe_turnover']=self.data_occurrence['occ_fe_inmkt_turnover']+self.data_occurrence['occ_fe_prv_turnover']
                        
            self.data_occurrence['occ_fe_vwap']=(self.data_occurrence['occ_fe_turnover']/
            (self.data_occurrence['occ_fe_inmkt_volume']+self.data_occurrence['occ_fe_prv_volume']))
            
            self.data_occurrence['slippage_vwap_bp']=(10000*self.data_occurrence['Side']*
                (self.data_occurrence['occ_fe_vwap']-self.data_occurrence['occ_fe_avg_price'].apply(lambda x : float(x)))/self.data_occurrence['occ_fe_vwap'])
            self.data_occurrence['slippage_vwap_bp'][(self.data_occurrence['occ_fe_exec_shares']==0)  | (self.data_occurrence['occ_fe_vwap']==0)]=np.nan
            
            self.data_occurrence['slippage_is_bp']=(10000*self.data_occurrence['Side']*
                (self.data_occurrence['occ_fe_arrival_price']-self.data_occurrence['occ_fe_avg_price'].apply(lambda x : float(x)))/self.data_occurrence['occ_fe_arrival_price'])
            self.data_occurrence['slippage_is_bp'][(self.data_occurrence['occ_fe_exec_shares']==0)  | (self.data_occurrence['occ_fe_arrival_price']==0)]=np.nan
            
            # --------------add slippage_bp by the benchmark
            self.data_occurrence['slippage_bp'] = np.nan
            idx_vwap = (self.data_occurrence['occ_fe_strategy_name_mapped'] == 'VWAP') | (self.data_occurrence['occ_fe_strategy_name_mapped'] == 'VOL') | (self.data_occurrence['occ_fe_strategy_name_mapped'] == 'DYNVOL')
            idx_is = self.data_occurrence['occ_fe_strategy_name_mapped'] == 'IS'
            
            self.data_occurrence['slippage_bp'][idx_vwap] = self.data_occurrence['slippage_vwap_bp'][idx_vwap].tolist()
            self.data_occurrence['slippage_bp'][idx_is] = self.data_occurrence['slippage_is_bp'][idx_is].tolist()
            
            # -- add avg spread bp                    
            self.data_occurrence['avg_spread_bp'] = 10000*self.data_occurrence['occ_fe_avg_sprd']/self.data_occurrence['occ_fe_avg_price'].apply(lambda x : float(x))
            self.data_occurrence['avg_spread_bp'][(self.data_occurrence['avg_spread_bp']<=0) | (-np.isfinite(self.data_occurrence['avg_spread_bp']))] = np.nan
            
            # --------------add slippage spread
            self.data_occurrence['slippage_spread'] = np.nan
            self.data_occurrence['slippage_spread'][idx_vwap] = (self.data_occurrence['slippage_vwap_bp'][idx_vwap]/self.data_occurrence['avg_spread_bp'][idx_vwap]).tolist()
            self.data_occurrence['slippage_spread'][idx_is] = (self.data_occurrence['slippage_is_bp'][idx_is]/self.data_occurrence['avg_spread_bp'][idx_is]).tolist()
            
                        
    def get_intraday_agg_deals_data(self,group_var='strategy_name_mapped',step_sec=60*30): 
        #-----------------------------------
        # INPUT
        #-----------------------------------
        #--- test 
        if self.data_intraday_agg_deals is not None:
            logging.info('data_intraday_agg_deals already loaded')
            return
        
        # TODO : aggréger ces infos dans la base de données
        #-----------------------------------
        # COMPUTE AGG DATA
        #-----------------------------------
        self.__compute_data_intraday_agg_deals(group_var=group_var,step_sec=step_sec)
        
        

    ###########################################################################
    # SELF COMPUTE METHODS USING MARKET DATA (so for once occurrence at a time)
    ###########################################################################
    def __add_benchtime(self, level = None , market_data = None):
        #-----------------------------------
        # TESTS
        #-----------------------------------
        if level is None or getattr(self,'data_' + level) is None:
            raise ValueError('level should be indicated')
            
        if getattr(self,'data_' + level).shape[0] == 0:
            logging.info('no data on which to add benchtime')
            return
            
        #-----------------------------------
        # Add benchtime
        #-----------------------------------        
        if level == 'sequence':
            #--
            #- test if only on one occurrence and market data correspond to the right security
            if len(np.unique(getattr(self,'data_' + level)['p_occ_id'].values).tolist()) > 1:
                logging.error('__add_bentime only works for one occurrence')
                raise ValueError('__add_bentime only works for one occurrence')
            
            # TO DO : test date too !
            if market_data is not None and (getattr(self,'data_' + level)['cheuvreux_secid'].values[0] != market_data.security_id):
                logging.error('__add_benchtime only works with market data from same security and date')
                raise ValueError('__add_benchtime only works with market data from same security and date')
            
            #--
            # get market information
            lasttick_datetime = None
            if market_data is not None and market_data.data_tick.shape[0]>0:
                lasttick_datetime = market_data.data_tick.index[-1].to_datetime()

            
            #-- initialize
            self.data_sequence['bench_starttime'] = None
            self.data_sequence['bench_endtime'] = None
            
            #-- sort
            getattr(self,'data_' + level).sort_index( by = ['p_cl_ord_id','nb_replace'], ascending = True , inplace = True)
            
            #--- we try to keep in one occurrence separate period for each sequence: add 0.5 seconds to start
            diff_2apply = dt.timedelta(seconds=0.5)
            
            uni_nb_replace = np.unique(self.data_sequence['nb_replace'])
            
            for i in range(0,len(uni_nb_replace)):
                
                id_seq = (self.data_sequence['nb_replace']==uni_nb_replace[i])
                #---------
                #--- bench_starttime
                
                if i == 0:
                    self.data_sequence['bench_starttime'][id_seq]=self.data_sequence[id_seq].index[0].to_datetime()
                else:
                    self.data_sequence['bench_starttime'][id_seq]=self.data_sequence[id_seq].index[0].to_datetime() + diff_2apply
                    
                if (isinstance(self.data_sequence['StartTime'][id_seq],dt.datetime) and 
                    self.data_sequence['StartTime'][id_seq].astimezone(tz=pytz.utc)>self.data_sequence['bench_starttime'][id_seq]):
                    
                    self.data_sequence['bench_starttime'][id_seq]=self.data_sequence['StartTime'][id_seq].astimezone(tz=pytz.utc)
                    
                #---------
                # -- bench_endtime
                if i < (len(uni_nb_replace)-1):
                    # if not last sequence : endtime = start time next sequence
                    id_next_seq = (self.data_sequence['nb_replace']==uni_nb_replace[i+1])
                    self.data_sequence['bench_endtime'][id_seq] = self.data_sequence[id_next_seq].index[0].to_datetime() + diff_2apply
                    
                else:
                    # if last sequence : max(start time, eff_endtime,last_deal)
                    if isinstance(self.data_sequence['eff_endtime'][id_seq],dt.datetime):
                        self.data_sequence['bench_endtime'][id_seq] = max(self.data_sequence['bench_starttime'][id_seq],self.data_sequence['eff_endtime'][id_seq]) + dt.timedelta(seconds=1)
                    else:
                        self.data_sequence['bench_endtime'][id_seq] = self.data_sequence['bench_starttime'][id_seq] + dt.timedelta(seconds=1)
                    
                    if (self.data_sequence['strategy_name_mapped'][id_seq].values[0].lower()=="vwap" and 
                            self.data_sequence['reason'][id_seq].values[0].lower()=="filled"):
                                # fullfiled vwap end time is the one set by the client if it is set or the end of trading
                                if isinstance(self.data_sequence['EndTime'][id_seq],dt.datetime):
                                    self.data_sequence['bench_endtime'][id_seq]=max(self.data_sequence['bench_endtime'][id_seq],self.data_sequence['EndTime'][id_seq].astimezone(tz=pytz.utc))
                                    
                    self.data_sequence['bench_endtime'][id_seq] = max(self.data_sequence['bench_endtime'][id_seq],self.data_sequence['exec_last_deal_time'][id_seq])        
                    
                # on coupe 5 secondes après notre dernière données tick                    
                if isinstance(lasttick_datetime,dt.datetime):
                    self.data_sequence['bench_endtime'][id_seq]=min(self.data_sequence['bench_endtime'][id_seq].values[0],lasttick_datetime+dt.timedelta(seconds=5))
            
            #--- update enrichment colnames
            self.__update_stats_enrichment( mode = 'db' ,level = level , columns = ['bench_starttime','bench_endtime'])
            
        else:
            raise ValueError('Not defined for level <' + level + '>')   
        
    def compute_tca_stats(self, market_data = None , referential_data = None , mode_colnames_out = 'tca'):
        #-----------------------------------
        # GET DATA
        #-----------------------------------
        # TODO :
        # what should be done, is extracting the right columns from the database, here we have to compute it !
        self.compute_db_stats(market_data=market_data,referential_data=referential_data)
                
        #-----------------------------------
        # AGG STATS BY OCCURENCE
        #-----------------------------------        
        # TCA is on occurrence, we have to aggregate execution and market
        self.apply_slicer(in_level = 'sequence' , out_level = 'occurrence', 
                          group_vars = ['p_occ_id'] , slicer = CONFIG_SLICER_TCA) 
                               
        #-----------------------------------
        # FORMULA ON OCCURENCE
        #-----------------------------------      
        # A TESTER
        self.apply_formula(level = 'occurrence', formula = CONFIG_FORMULA_TCA)
          
        #-----------------------------------
        # CONSTRAINt ON COLNAMES
        #----------------------------------- 
        keep_colnames = self.get_colnames(level = 'occurrence', mode = mode_colnames_out)
        self.data_occurrence = self.data_occurrence[keep_colnames]
        
        
    def compute_db_stats(self,market_data=None,referential_data=None):
        #-----------------------------------
        # GET DATA
        #-----------------------------------
        self.get_db_data(level='occurrence')
        self.get_db_data(level='sequence')
        self.get_db_data(level='deal')
        
        #-----------------------------------
        # TESTS INPUTS
        #-----------------------------------
        if (self.data_occurrence is None or self.data_sequence is None or self.data_deal is None or 
            market_data is None or referential_data is None):
            logging.error('bad inputs')
            raise ValueError('bad inputs')
            
        if self.data_occurrence.shape[0] != 1:
            logging.error('only works for one occurrence')
            raise ValueError('only works for one occurrence')
            
        if (market_data.security_id!=referential_data.security_id or 
            self.data_occurrence['cheuvreux_secid'].values[0]!=market_data.security_id):
            logging.error('not the same security')
            raise ValueError('not the same security')           
            
        #-----------------------------------
        # UPDATE DEALS
        #-----------------------------------
        # TO DO
        
        #-----------------------------------
        # EXEC STATS
        #-----------------------------------        
        self.__compute_db_exec_stats()
        
        #-----------------------------------
        # MARKET STATS
        #-----------------------------------        
        self.__compute_db_market_stats(market_data=market_data,referential_data=referential_data)
        
        
    def __compute_db_market_stats(self, market_data=None, referential_data=None):
        
        self.apply_aggregate_market_stats(level = 'sequence', 
                                          market_slicer = CONFIG_SLICER_SEQUENCE_STATS, formula = CONFIG_FORMULA_SEQUENCE_STATS, 
                                          market_data = market_data, referential_data = referential_data)
        
#         #-----------------------------------
#         # add benchtime
#         #-----------------------------------
#         lasttick_datetime=None
#         if market_data.data_tick.shape[0]>0:
#             lasttick_datetime=market_data.data_tick.index[-1].to_datetime()
#             
#         self.__add_benchtime(lasttick_datetime=lasttick_datetime)
#         
#         #-----------------------------------
#         # ADD intraday market stats on SEQUENCE
#         #-----------------------------------  
#         new_data_sequence = pd.DataFrame()
#         new_added_columns = []
#         
#         for iseq in range(0,self.data_sequence.shape[0]):
#             
#             data_this_seq = self.data_sequence[self.data_sequence['p_cl_ord_id'] == self.data_sequence.ix[iseq]['p_cl_ord_id']]
#             
#             #---
#             #- compute stats
#             data_stats = {}
#             for periodstats in CONFIG_SLICER_SEQUENCE_STATS.keys():
#                 print periodstats
#                 #-- create filter
#                 filter_ = CONFIG_SLICER_SEQUENCE_STATS[periodstats]['filter']
#                 
#                 if CONFIG_SLICER_SEQUENCE_STATS[periodstats]['period_time_constr']:
#                     filter_.update({'start_date' : data_this_seq['bench_starttime'].values[0], 'end_date': data_this_seq['bench_endtime'].values[0]})
#                     
#                 if CONFIG_SLICER_SEQUENCE_STATS[periodstats]['period_phase_constr']:
#                     filter_.update({'exclude_auction': mapping.ExcludeAuction(data_this_seq['ExcludeAuction'].values[0])})
#                 
#                 if CONFIG_SLICER_SEQUENCE_STATS[periodstats]['order_constr']:
#                     filter_.update({'order_limit': data_this_seq['Price'].values[0], 'order_side': data_this_seq['Side'].values[0]})
#                     
#                 #-- compute           
#                 # To DO : gérer si il n y ' a pas de data ? valeur par défaut ?            
#                 data_stats.update(market_data.compute_agg_period_tick(filterd = filter_, 
#                                                     slicerd = CONFIG_SLICER_SEQUENCE_STATS[periodstats]['slicer']))
#                 
#             new_added_columns = list(set(new_added_columns+data_stats.keys()))
#             data_stats = pd.DataFrame([data_stats])
#             data_stats['p_cl_ord_id'] = data_this_seq['p_cl_ord_id'].values[0]
#             
#             #---
#             #- add
#             index_name = data_this_seq.index.name
#             data_this_seq = data_this_seq.reset_index().merge(data_stats,how='left',on=['p_cl_ord_id']).set_index(index_name)
#             new_data_sequence = new_data_sequence.append(data_this_seq)
#             
#         #--- update data + enrichment colnames
#         self.data_sequence = new_data_sequence
#         self.__update_stats_enrichment( mode = 'db' ,level = 'sequence' , columns = new_added_columns)
#         del new_data_sequence
#         del new_added_columns 
#         
#         #-----------------------------------
#         # Apply formula
#         #-----------------------------------
#         self.apply_formula(level = 'sequence' ,  formula = CONFIG_FORMULA_SEQUENCE_STATS)
            
            
        

        
        
            
    def __compute_db_exec_stats(self):
        #-----------------------------------
        # TESTS
        #-----------------------------------
        if self.data_deal is None:
            logging.error('data_deal should be loaded')
            raise ValueError('data_deal should be loaded')
        
        #-----------------------------------
        # UPDATE Sequence
        #-----------------------------------
        if self.data_sequence is not None and self.data_sequence.shape[0]>0:
            
            #----------------
            # STEP 1 : compute aggregate stats from deals
            #----------------
            
            #---  config des stats to compute
            config_stats={'exec_qty':{'default': 0.0 ,'slicer' : lambda df : np.sum(df.volume.values)},
                    'exec_nb_trades': {'default': 0.0 ,'slicer' : lambda df : np.size(df.volume.values)},
                    'exec_turnover': {'default': 0.0 ,'slicer' : lambda df : np.sum(map(lambda x,y : x*y,df.volume.values,df.price.values))},
                    'exec_first_deal_time' : {'default': None ,'slicer' : lambda df : np.min([x.to_datetime() for x in df.index])},
                    'exec_last_deal_time' : {'default': None ,'slicer' : lambda df : np.max([x.to_datetime() for x in df.index])}}
                            
            #--- compute
            _add_data=pd.DataFrame()
            if self.data_deal.shape[0]>0:
                _add_data=dftools.agg(self.data_deal,
                                      group_vars='p_cl_ord_id',
                                      stats=dict([(x,config_stats[x]['slicer']) for x in config_stats.keys()]))
            #---  initialize columns
            for x in config_stats.keys():
                self.data_sequence[x]=config_stats[x]['default']
                    
            #---  update data
            if _add_data.shape[0]>0:
                idx_col_in=mutils.ismember(np.array(_add_data.columns),np.array(self.data_sequence.columns))[1].tolist()
                for idx in range(0,_add_data.shape[0]):
                    idx_in=np.nonzero(self.data_sequence['p_cl_ord_id']==_add_data.iloc[idx]['p_cl_ord_id'])[0]
                    self.data_sequence.iloc[idx_in,idx_col_in]=_add_data.iloc[idx].values.tolist()
                    
            #--- update enrichment colnames
            self.__update_stats_enrichment( mode = 'db' ,level = 'sequence' , columns = config_stats.keys())
            

            #----------------
            # STEP 2 : compute cumulative stats at occurence level : prefixed by occ_prev_
            #----------------
            
            self.data_sequence.sort_index( by = ['p_cl_ord_id','nb_replace'], ascending = True , inplace = True)
            
            #---  config des stats to compute
            config_stats={'occ_prev_exec_qty': {'default': 0.0 ,'formula' : lambda df : np.concatenate([[0.0],np.cumsum(df.exec_qty.values)[:-1]])},
                    'occ_prev_exec_turnover': {'default': 0.0 ,'formula' : lambda df : np.concatenate([[0.0],np.cumsum(df.exec_turnover.values)[:-1]])}}            
            
            #---  initialize columns 
            for x in config_stats.keys():
                self.data_sequence[x]=config_stats[x]['default']            
                
            #---  update data
            for p_occ_id in np.unique(self.data_sequence['p_occ_id']):
                for x in config_stats.keys():
                    self.data_sequence[x][self.data_sequence['p_occ_id']==p_occ_id]=config_stats[x]['formula'](self.data_sequence[self.data_sequence['p_occ_id']==p_occ_id])
                    
            #--- update enrichment colnames
            self.__update_stats_enrichment( mode = 'db' ,level = 'sequence' , columns = config_stats.keys())
                          
            # print self.data_sequence[['exec_turnover','turnover','exec_qty','occ_prev_exec_turnover','occ_prev_exec_qty']]
            
        #-----------------------------------
        # UPDATE occurrence
        #-----------------------------------
        if self.data_occurrence is not None and self.data_occurrence.shape[0]>0:
            
            if self.data_sequence is None or self.data_sequence.shape[0]==0:
                logging.error('data_sequence should be loaded')
                raise ValueError('data_sequence should be loaded')
                
            #----------------
            # STEP 1 : compute aggregate stats from sequence
            
            #---  config des stats to compute
            config_stats={'occ_strategy_name_mapped':
                            {'default': np.nan ,'slicer' : lambda df : df.strategy_name_mapped.values[-1] if len(np.unique(df.strategy_name_mapped.values))==1 else 'MULTIPLE'},
                    'occ_order_qty': {'default': 0.0 ,'slicer' : lambda df : df.OrderQty.values[-1]},
                    'occ_nb_replace':
                            {'default': 0.0 ,'slicer' : lambda df : np.size(df.p_occ_id.values)-1},
                    'occ_exec_qty':
                            {'default': 0.0 ,'slicer' : lambda df : np.sum(df.exec_qty.values)},
                    'occ_exec_nb_trades': 
                            {'default': 0.0 ,'slicer' : lambda df : np.sum(df.exec_nb_trades.values)},
                    'occ_exec_turnover':
                            {'default': 0.0 ,'slicer' : lambda df : np.sum(df.exec_turnover.values)}}
               
            #--- compute
            _add_data=dftools.agg(self.data_sequence,
                                  group_vars='p_occ_id',
                                  stats=dict([(x,config_stats[x]['slicer']) for x in config_stats.keys()]))
                                    
            #---  initialize columns
            for x in config_stats.keys():
                self.data_occurrence[x]=config_stats[x]['default']
                
            #---  update data
            if _add_data.shape[0]>0:
                idx_col_in=mutils.ismember(np.array(_add_data.columns),np.array(self.data_occurrence.columns))[1].tolist()
                for idx in range(0,_add_data.shape[0]):
                    idx_in=np.nonzero(self.data_occurrence['p_occ_id']==_add_data.iloc[idx]['p_occ_id'])[0]
                    self.data_occurrence.iloc[idx_in,idx_col_in]=_add_data.iloc[idx].values.tolist()                
            
            #--- update enrichment colnames
            self.__update_stats_enrichment( mode = 'db' ,level = 'occurrence' , columns = config_stats.keys())
            
            
            
    def __update_stats_enrichment(self, mode = None , level = None, columns = None):
        #-----------------------------------
        # TESTS
        #-----------------------------------
        if mode is None or level is None or columns is None or not isinstance(columns,list):
            raise ValueError('bad inputs')
        
        if mode =='db':
            x = self.db_stats_enrichment
            
        else:
            raise ValueError('Unknown mode <' + mode + '>')
        #-----------------------------------
        # DO
        #-----------------------------------        
        if level not in x.keys():
            x.update({level : columns})
        else:
            x.update({level : list(set(x[level]+columns))})
            
            
    ###########################################################################
    # SELF COMPUTE METHODS ON DEALS
    ###########################################################################
    def __compute_data_intraday_agg_deals(self,group_var='strategy_name_mapped',step_sec=60*30):
        #-----------------------------------
        # INPUT
        #-----------------------------------
        if self.data_deal is None:
            raise ValueError('data_deal should be loaded')
            
        #--- initialize 
        self.data_intraday_agg_deals=pd.DataFrame()
        
        if self.data_deal.shape[0]==0:
            return
        
        #-----------------------------------
        # CONFIG
        #-----------------------------------         
        stats_config={'mturnover_euro': lambda df : np.sum(df.rate_to_euro*df.price*df.volume)*1e-6}
        
        #-----------------------------------
        # COMPUTE BY DAY
        #-----------------------------------            
        #--- add temporary
        self.data_deal['tmp_date']=[x.to_datetime().date() for x in self.data_deal.index]
        
        for day in np.unique(self.data_deal['tmp_date']):
            # - get deals
            _tmp=self.data_deal[self.data_deal['tmp_date']==day]
            
            # - aggregate by day + add
            if (_tmp.shape[0]>0) and (group_var in _tmp.columns.values):
                
                _tmp_grouped = dftools.agg(_tmp,group_vars=group_var,step_sec=step_sec,stats=stats_config)
                
                if _tmp_grouped.shape[0] == 0:
                    logging.warning('variable <'+group_var+'> has no finite values')
                else:
                    self.data_intraday_agg_deals=self.data_intraday_agg_deals.append(_tmp_grouped)
                    
            elif (_tmp.shape[0]>0) and (group_var not in _tmp.columns.values):
                logging.error('variable <'+group_var+'> is not in the database')
                raise ValueError('')
                
        #--- del temporary
        self.data_deal=self.data_deal.drop(['tmp_date'],axis=1)
        
        #--- sort
        if self.data_intraday_agg_deals.shape[0]>0:
            self.data_intraday_agg_deals['tmpindex']=[x.to_datetime() for x in self.data_intraday_agg_deals.index]
            self.data_intraday_agg_deals=self.data_intraday_agg_deals.sort_index(by=['tmpindex',group_var]).drop(['tmpindex'],axis=1)
            
            
    def __flag_deals(self,mktdataprocessor=None,refdataprocessor=None):
        # check que dans deal il n'y qu'un security_id et qu'une date !
        a=1
        
    ###########################################################################
    # METHOD GET COLNAMES
    ###########################################################################
    def get_colnames(self, level=None, mode='tca', only=None, at_least=None):             
        #-----------------------------------
        # TEST input
        #-----------------------------------
        if level is None or mode is None:
            logging.error('bad inputs')
            raise ValueError('bad inputs')
            
        #-----------------------------------
        # mandatory cols
        #-----------------------------------
        if level=='sequence':
            mandatory_cols=['_id','p_cl_ord_id']
            all_cols = self.data_sequence.columns.values.tolist()
            
        elif level=='occurrence':
            mandatory_cols=['_id','p_occ_id']
            all_cols = self.data_occurrence.columns.values.tolist()
            
        elif  level=='deal':
            mandatory_cols=['_id','p_exec_id']
            all_cols = self.data_deal.columns.values.tolist()
            
        else:
            logging.error('unknown level <'+level+'>')
            raise ValueError('unknown level <'+level+'>')
        
        #-----------------------------------
        # mode cols
        #-----------------------------------
        # CASE 1 :
        if only is not None:
            out=list(set(mandatory_cols+only))  
        # CASE 2 :
        else:
            # get all and add colnames
            add_cols=mandatory_cols
            if at_least is not None:
                add_cols=list(set(add_cols+at_least))
                
            # by mode
            if mode == 'tca':
                
                if level == 'occurrence':
                    out = [#-- infos on order
                         'ClOrdID','Account','Side','Symbol','occ_order_qty','occ_strategy_name_mapped','ExDestination','Currency','rate_to_euro',
                         #-- exec infos
                         'occ_bench_starttime','occ_bench_endtime','occ_exec_qty','occ_exec_vwap','occ_exec_turnover','occ_nb_replace',
                         'occ_exec_first_deal_time','occ_exec_last_deal_time',
                         #-- market price infos
                         'occ_m_b_arrival_price_lit','occ_m_p_vwap_lit','occ_m_p_vwap_lit_constr','occ_m_p_close_lit','occ_spread_bp',
                         #-- market volume infos         
                         'occ_m_p_volume_lit','occ_m_p_volume_lit_constr',    
                         #--- performance
                         'occ_slippage_vwap_bp','occ_slippage_vwap_constr_bp','occ_slippage_is_bp',
                         #--- compare to front end export
                         'occ_fe_turnover','occ_fe_volume','occ_fe_vwap','occ_fe_arrival_price']
                    
                else:
                    logging.error('not  level <'+level+'>')
                    raise ValueError('unknown level <'+level+'>')  
                                
            elif mode == 'all':
                out = getattr(self,'data_' + level).columns.tolist()
                
            else:
                raise ValueError('Unknown mode :<' + mode + '>')        
                    
            out=list(set(out+add_cols))
        #-----------------------------------
        # out + check
        #-----------------------------------
        if not all([x in all_cols for x in out]):
            out_missing = ''
            for x in out:
                if x not in all_cols:
                    out_missing+=x
            logging.error('asked colnames (' + out_missing + ')should be available in database')
            raise ValueError('asked colnames (' + out_missing + ') should be available in database')
        
        return out
        
               
    ###########################################################################
    # OTHERS
    ###########################################################################
    def agg_stats(self,level='sequence',stats_config=None,gvar=None,gvar_vals=None,agg_step=None):
        # -- input check
        #if gvar is None:
        #    raise ValueError('gvar should be present')        
        #-----------------------------------
        # GET DATA
        #-----------------------------------
        if level == 'sequence':
            if self.data_sequence is None:
                raise ValueError('data_sequence should be present')
            else:
                data=self.data_sequence.copy()                   
        elif level == 'occ_fe':
            if self.data_occurrence is None:
                raise ValueError('data_occurrence should be present')
            else:
                data=self.data_occurrence.copy()
        else:
            raise ValueError('unknown error')
        #-----------------------------------
        # AGG DATA
        #-----------------------------------  
        # -- config var
        if stats_config is None:
            raise ValueError('unknown var stats')
        # -- handle gvar
        if gvar in data.columns.values:
            logging.debug('gvar in data')
        elif gvar == 'place':
            places = get_repository.tag100_to_place_name()
            data[gvar]=[exdest2place(x,places) for x in data['ExDestination']]
        elif gvar == 'occ_fe_strategy_name_mapped':
            idx_nan = [type(x) is float for x in data['occ_fe_strategy_name_mapped']]
            data[gvar][idx_nan]='Not Available'
        elif gvar == 'is_dma': 
            data[gvar]=data['TargetSubID' ].apply(lambda x: 'Algo DMA' if x in  ['ON1','ON2','ON3'] else 'Other')
        elif gvar is not None:
            raise ValueError('gvar not in data')  
            
        # -- handle gvar_vals
        if gvar_vals == 'is_dma':
            data[gvar_vals]=data['TargetSubID' ].apply(lambda x: 'Algo DMA' if x in  ['ON1','ON2','ON3'] else 'Other')
            gvar=[gvar]+[gvar_vals]
        elif gvar_vals is not None:
            if gvar_vals not in data.columns.values:
                raise ValueError('gvar_vals not in data')
            gvar=[gvar]+[gvar_vals]
        # -- handle agg_step
        if agg_step == 'day':
            data['tmp_date_end']=[dt.datetime.combine(x.to_datetime().date(),dt.time(0,0,0)) for x in data.index]
            data['tmp_date_start']=[dt.datetime.combine((x.to_datetime()-dt.timedelta(days=1)).date(),dt.time(0,0,0)) for x in data.index]
        elif agg_step == 'week':
            data['tmp_date_end'] = [dt.datetime.combine(x.to_datetime().date()-dt.timedelta(days=x.to_datetime().date().weekday()-4),dt.time(0,0,0)) for x in data.index]
            data['tmp_date_start'] = [dt.datetime.combine(x.to_datetime().date()-dt.timedelta(days=x.to_datetime().date().weekday()),dt.time(0,0,0)) for x in data.index]
        elif agg_step == 'month':
            data['tmp_date_end'] = [dt.datetime.combine(x.to_datetime().date().replace(month=x.to_datetime().date().month % 12 + 1, day = 1) - dt.timedelta(days=1),dt.time(0,0,0)) for x in data.index]
            data['tmp_date_start'] = [dt.datetime.combine(x.to_datetime().date().replace(day=1),dt.time(0,0,0)) for x in data.index]
        elif agg_step == 'year':
            data['tmp_date_start'] = [dt.datetime.combine(x.to_datetime().date().replace(month=1,day=1),dt.time(0,0,0)) for x in data.index]
            data['tmp_date_end'] = [dt.datetime.combine(x.to_datetime().date().replace(month=12,day=31),dt.time(0,0,0)) for x in data.index]
        elif agg_step is None:
            if gvar is not None:
                data = dftools.agg(data,group_vars=gvar,stats=stats_config)
            else:
                raise ValueError('gvar and agg_step can not equal to None at the same time')
            return data
        else:
            raise ValueError('agg_step unknown %s' % (agg_step))
        # -- value aggregation
        #data = dftools.agg(data,group_vars=['tmp_date_start','tmp_date_end',gvar],stats=stats_config)
        if gvar is None:
            data = dftools.agg(data,group_vars=['tmp_date_start','tmp_date_end'],stats=stats_config)
        else:
            data = dftools.agg(data,group_vars=['tmp_date_start','tmp_date_end',gvar],stats=stats_config)
        return data
        
               
    ###########################################################################
    # GENERIC FUNCTIONS OF APPLYING formula / slicer to attribut of the class
    ###########################################################################
    def compute_stats(self, config_mode = None , market_data = None, referential_data = None):
        #--------------------
        #-- TEST
        #--------------------
        if getattr(AlgoComputeStatsConfig,config_mode, None) is None:
            raise ValueError('Unknwon config_mode <' + config_mode +'>')
        
        config = getattr(AlgoComputeStatsConfig,config_mode)()
        
        #--------------------
        #-- GET_DATA
        #--------------------
        if 'get_data' in config.keys():
            #-- change mode_colnames ?
            if 'mode_colnames' in config['get_data'].keys():
                self.set_mode_colnames(config['get_data']['mode_colnames'])
                
            #-- load levels
            if 'level' in config['get_data'].keys():
                
                if not isinstance(config['get_data']['level'],list):
                    raise ValueError('level has to be a list')
                    
                for level in  config['get_data']['level']:
                    self.get_db_data(level = level)
                    
        #--------------------
        #-- APPLY
        #--------------------            
        if 'apply' in config.keys():
            if not isinstance(config['apply'],list):
                raise ValueError('apply has to be a list')
            
            for i in range(0,len(config['apply'])):
                #--- test
                if not isinstance(config['apply'][i],dict):
                    raise ValueError('apply has to be a list of dict')     
                
                if 'mode' not in config['apply'][i].keys() or 'info' not in config['apply'][i].keys():
                    raise ValueError('apply member has to be a dict with mode and info')
                    
                #-- apply
                if config['apply'][i]['mode'] == 'formula':
                    self.apply_formula(**config['apply'][i]['info'])
                
                elif config['apply'][i]['mode'] == 'slicer':
                    self.apply_slicer(**config['apply'][i]['info'])
                
                elif config['apply'][i]['mode'] == 'aggregate_market_stats':
                    config['apply'][i]['info'].update({'market_data' : market_data ,'referential_data' : referential_data})
                    self.apply_aggregate_market_stats(**config['apply'][i]['info'])
                
                else:
                    raise ValueError('Unknwon apply_mode <' + config['apply'][i]['mode'] +'>')
                
                    
    def apply_formula(self, level = None , formula = None , in_sort = None , in_sort_ascending = True):
        #--------------------
        #-- TEST
        #--------------------
        #-
        if level is None:
            raise ValueError('level should be indicated')
            
        elif getattr(self,'data_' + level) is None:
            raise ValueError('level data should be loaded')
        
        if not isinstance(formula,dict):
            raise ValueError('formula should be a dict')
        #-
        if (in_sort is not None) and (not isinstance(in_sort,list)):
            raise ValueError('in_sort has to be a list')
        
        elif (in_sort is not None) and (getattr(self,'data_' + level).shape[0] > 0) and any([not x in getattr(self,'data_' + level).columns.tolist() for x in in_sort]):
            raise ValueError('all in_sort should be into in_level')
        
        #--------------------
        #-- Apply
        #--------------------
        if getattr(self,'data_' + level).shape[0] == 0:
            logging.info('no data on which to apply formula')
            return
        
        #-- sort input if needed
        if in_sort is not None and len(in_sort) > 0:
            getattr(self,'data_' + level).sort_index( by = in_sort, ascending = in_sort_ascending , inplace = True)
            
        if getattr(self,'data_' + level).shape[0] > 0 and len(formula.keys()) > 0:
            #-- apply all formulas
            for name in formula.keys():
                try:
                    getattr(self,'data_' + level)[name] = formula[name](getattr(self,'data_' + level))
                except:
                    raise ValueError('Error on formula named :<' + name +'>')
            
            #-- update the enrichment !!
            self.__update_stats_enrichment(mode = 'db' ,level = level , columns = formula.keys())
            
            
    def apply_slicer(self, in_level = None , out_level = None, group_vars = None , slicer = {} , in_sort = None , in_sort_ascending = True , replace_cols = False):
        #--------------------
        #-- TEST
        #--------------------
        # - in data
        if in_level is None or getattr(self,'data_' + in_level) is None:
            raise ValueError('in_level data should be extracted')
            
        # - out data
        if out_level is not None and getattr(self,'data_' + out_level) is None:
            raise ValueError('out_level data should be extracted')   
            
        # - group_vars
        if group_vars is None:
            raise ValueError('group_vars should be indicated')
        
        elif isinstance(group_vars,basestring):
            group_vars = [group_vars]
        
        if (getattr(self,'data_' + in_level).shape[0] > 0) and any([not x in getattr(self,'data_' + in_level).columns.tolist() for x in group_vars]):
            raise ValueError('all groyp_vars should be into in_level')
        
        if (out_level is not None) and (getattr(self,'data_' + out_level).shape[0] > 0) and any([not x in getattr(self,'data_' + out_level).columns.tolist() for x in group_vars]):
            raise ValueError('all group_vars should be into in_level')
        
        # - others
        if not isinstance(slicer,dict):
            raise ValueError('slicer has to be a dict')
            
        if (in_sort is not None) and (not isinstance(in_sort,list)):
            raise ValueError('in_sort has to be a list')
        elif (in_sort is not None) and (getattr(self,'data_' + in_level).shape[0] > 0) and any([not x in getattr(self,'data_' + in_level).columns.tolist() for x in in_sort]):
            raise ValueError('all in_sort should be into in_level')
        
        #--------------------
        #-- SLICER
        #--------------------
        _add_agg_data = pd.DataFrame()
        
        if len(slicer) == 0:
            logging.info('no slicer to add')
            return
        
        if getattr(self,'data_' + in_level).shape[0] > 0:
            #-- sort input if needed
            if in_sort is not None and len(in_sort) > 0:
                getattr(self,'data_' + in_level).sort_index( by = in_sort, ascending = in_sort_ascending , inplace = True)
                
            #-- compute
            try:
                _add_agg_data = dftools.agg(getattr(self,'data_' + in_level),
                                                group_vars = group_vars,
                                                stats = dict([(x, slicer[x]['slicer']) for x in slicer.keys()]))
            except:
                raise ValueError('Error on applying slicers>')
            
        #--------------------
        #-- OUTPUT
        #--------------------        
        if out_level is not None:
            #--
            if getattr(self,'data_' + out_level).shape[0] == 0:
                logging.info('no out_level data')
                return
            
            #--
            if getattr(self,'data_' + in_level).shape[0] == 0:
                #- no in data, default value is used
                for x in slicer.keys():
                    getattr(self,'data_' + out_level)[x] = slicer[x]['default']
            else:
                #-- if slicer compute values that already exist : parameter replace
                #common_cols = list(set(_add_agg_data.columns.tolist()) - set(getattr(self,'data_' + out_level).columns.tolist()))
                # TO DO !
                
                common_cols = list((set(_add_agg_data.columns.tolist()) - set(group_vars)).intersection(list(set(getattr(self,'data_' + out_level).columns.tolist()) - set(group_vars))))
                
                if len(common_cols) > 0:
                    if replace_cols:
                        # erase cols in the self.data_ ...
                        setattr(self,'data_' + out_level, getattr(self,'data_' + out_level)[list(set(getattr(self,'data_' + out_level).columns.tolist()) - set(common_cols)) ])
                    else:
                        raise ValueError('common columns ')
                    
                index_name = getattr(self,'data_' + out_level).index.name
                setattr(self,'data_' + out_level,
                            getattr(self,'data_' + out_level).reset_index().merge(_add_agg_data, how = 'left' , on = group_vars).set_index(index_name))
            
            #-- update the enrichment !!
            self.__update_stats_enrichment(mode = 'db' ,level = out_level , columns = slicer.keys())
            
        else:
            return _add_agg_data
        
        
    def apply_aggregate_market_stats(self, level = None , market_slicer = {} , formula = {} , market_data = None, referential_data = None):
        #-----------------------------------
        # TESTS INPUT
        #-----------------------------------
        # -
        if level is None or getattr(self,'data_' + level) is None:
            raise ValueError('level data should be extracted')
        # -
        if (not isinstance(market_slicer,dict)) or (not isinstance(formula,dict)):
            raise ValueError('market_slicer and formula has to be dict')
        # -
        if market_data is None or referential_data is None:
            raise ValueError('market data and referential data has to be in input')
        
        #-----------------------------------
        # TEST LEVEL DATA
        #-----------------------------------
        if level == 'sequence':
            primary_key_cols = 'p_cl_ord_id'
        else:
            raise ValueError('This level is not yet handled <' + level + '>')
        
        # check inputs security matches
        if (market_data.security_id != referential_data.security_id or 
            len(np.unique(getattr(self,'data_' + level)['cheuvreux_secid']).tolist()) != 1 or
            getattr(self,'data_' + level)['cheuvreux_secid'].values[0] != market_data.security_id):
            logging.error('not the same security')
            raise ValueError('not the same security')  
        
        # TO DO : check also the date
        
        #-----------------------------------
        # add benchtime
        #-----------------------------------
        self.__add_benchtime(level = level, market_data = market_data)
        
        #-----------------------------------
        # COMPUTE AGGREGATE MARKET STATS
        #-----------------------------------  
        if getattr(self,'data_' + level).shape[0] == 0:
            logging.info('no data on which to add market stats')
            return
        
        if len(market_slicer) == 0:
            logging.info('no market_slicer to compute')
            return            
        
        new_data = pd.DataFrame()
        new_added_columns = []
        
        for iseq in range(0,getattr(self,'data_' + level).shape[0]):
            
            data_this_seq = getattr(self,'data_' + level)[getattr(self,'data_' + level)[primary_key_cols] == getattr(self,'data_' + level).ix[iseq][primary_key_cols]]
            
            #---
            #- compute stats
            data_stats = {}
            for periodstats in market_slicer.keys():
                
                #-- create filter
                filter_ = market_slicer[periodstats]['filter']
                
                if market_slicer[periodstats]['period_time_constr']:
                    filter_.update({'start_date' : data_this_seq['bench_starttime'].values[0], 'end_date': data_this_seq['bench_endtime'].values[0]})
                    
                if market_slicer[periodstats]['period_phase_constr']:
                    filter_.update({'exclude_auction': mapping.ExcludeAuction(data_this_seq['ExcludeAuction'].values[0])})
                
                if market_slicer[periodstats]['order_constr']:
                    filter_.update({'order_limit': data_this_seq['Price'].values[0], 'order_side': data_this_seq['Side'].values[0]})
                    
                #-- compute           
                # To DO : gérer si il n y ' a pas de data ? valeur par défaut ?            
                data_stats.update(market_data.compute_agg_period_tick(filterd = filter_, 
                                                    slicerd = market_slicer[periodstats]['slicer']))
                
            new_added_columns = list(set(new_added_columns+data_stats.keys()))
            data_stats = pd.DataFrame([data_stats])
            data_stats[primary_key_cols] = data_this_seq[primary_key_cols].values[0]
            
            #---
            #- add
            index_name = data_this_seq.index.name
            data_this_seq = data_this_seq.reset_index().merge(data_stats,how='left',on=[primary_key_cols]).set_index(index_name)
            new_data = new_data.append(data_this_seq)
            
        #--- update data + enrichment colnames
        setattr(self,'data_' + level,new_data) 
        self.__update_stats_enrichment( mode = 'db' ,level = level , columns = new_added_columns)
        del new_data
        del new_added_columns 
        
        #-----------------------------------
        # Apply formula
        #-----------------------------------
        if len(formula) > 0:
            self.apply_formula(level = level ,  formula = formula)        
            
            
def exdest2place(x,allx):
    ids=[tmp==x for tmp in allx['suffix']]
    if any(ids):
        out=allx['name'][ids].values[0]
    elif type(x) is unicode:
        out='Unknown (%s)'%(x)
    else:
        out='Value Not Available'
    return out
    
def checkspread(x):
    if x > 0 and np.isfinite(x):
        return x
    else:
        return np.nan
    
if __name__=='__main__':
    
    from lib.data.ui.Explorer import Explorer
    from lib.tca.referentialdata import ReferentialDataProcessor
    from lib.tca.marketdata import MarketDataProcessor
    from lib.dbtools.connections import Connections
    
    #--- occurence sending before market open
    # '20140109FY2DG14375c6673b-000WATFLT01'
    #-- occurrence with multiple sequence
    # '20130521FY000008193901LUIFLT01'
    
#     #-----  TEST AGG DEAL
#     test = AlgoStatsProcessor(start_date = dt.datetime(2013,8,12), end_date = dt.datetime(2013,8,14))
#     test.get_db_data(level='deal')
#     test.get_intraday_agg_deals_data()
#     print test.data_intraday_agg_deals

#     #-----  TEST APPLY STATS
#     occ_id = '20130521FY000008193901LUIFLT01'
#     env = 'production_copy'
#     
#     Connections.change_connections(env)
#     
#     occ_data = AlgoStatsProcessor(filter = {"p_occ_id": {"$in" : [occ_id]}})
#     occ_data.get_db_data(level = 'sequence')    
#     occ_data.apply_formula(level = 'sequence' , formula = {'test1' : lambda df : df.exec_qty.values*df.turnover.values})
    
    
#     #-----  COMPUTE TCA STATS 
#     occ_id = '20130521FY000008193901LUIFLT01'
#     env = 'production_copy'
#     
#     Connections.change_connections(env)
#     
#     occ_data = AlgoStatsProcessor(filter = {"p_occ_id": {"$in" : [occ_id]}})
#     occ_data.get_db_data(level = 'occurrence')
#     sec_id = occ_data.data_occurrence['cheuvreux_secid'].values[0]
#     date = occ_data.data_occurrence.index[0].to_datetime()
#     
#     mkt_data = MarketDataProcessor(security_id = sec_id , date = date)
#     mkt_data.get_data_tick()
#     mkt_data.get_data_daily()
#     
#     ref_data = ReferentialDataProcessor(security_id = sec_id , date = date)
#     ref_data.get_data_exchange_info()
#     
#     occ_data.compute_tca_stats(market_data=mkt_data,referential_data=ref_data)
#     print 'a'
    
    #-----  COMPUTE TCA STATS   (base)
    occ_id = '20130521FY000008193901LUIFLT01'
    env = 'production_copy'
    
    Connections.change_connections(env)
    
    occ_data = AlgoStatsProcessor(filter = {"p_occ_id": {"$in" : [occ_id]}})
    occ_data.get_db_data(level = 'occurrence')
    sec_id = occ_data.data_occurrence['cheuvreux_secid'].values[0]
    date = occ_data.data_occurrence.index[0].to_datetime()
    
    mkt_data = MarketDataProcessor(security_id = sec_id , date = date)
    mkt_data.get_data_tick()
    mkt_data.get_data_daily()
    
    ref_data = ReferentialDataProcessor(security_id = sec_id , date = date)
    ref_data.get_data_exchange_info()
    
    occ_data = AlgoStatsProcessor(filter = {"p_occ_id": {"$in" : [occ_id]}})
    occ_data.compute_stats(config_mode = 'db_one_occurrence' , market_data = mkt_data , referential_data = ref_data)
    
    print occ_data.data_sequence 
    print occ_data.data_occurrence 
    print occ_data.db_stats_enrichment
    
#     #-----  COMPUTE STATS (base)
#     occ_id = '20130521FY000008193901LUIFLT01'
#     env = 'production_copy'
#     
#     Connections.change_connections(env)
#     
#     occ_data = AlgoStatsProcessor(filter = {"p_occ_id": {"$in" : [occ_id]}})
#     occ_data.compute_stats(config_mode = 'db_one_occurrence')
#     
#     print occ_data.data_sequence 
#     print occ_data.data_occurrence 
    
    # Explorer(test.data_intraday_agg_deals)
# 
#     #-----  ENTRY OCCURENCE 
#     test = AlgoStatsProcessor(filter = {"p_occ_id": {"$in" : ['20130603FY000009436101LUIFLT01','20130603FY71306030000015RLUIFLT01','20130904FY2000019979301WATFLT01']}})    
#     
#     #-----  ENTRY OCCURENCE 
#     test = AlgoStatsProcessor(filter = {"p_occ_id": {"$in" : ['20130603FY000009436101LUIFLT01','20130603FY71306030000015RLUIFLT01','20130904FY2000019979301WATFLT01']}})
#     test.get_db_data(level='occurrence')
#     test.get_db_data(level='sequence')
#     test.get_db_data(level='deal')
#     
#     
#     print test.data_sequence.shape
#     print test.data_occurrence.shape
#     print test.data_deal.shape
#     print test.data_sequence[['bench_starttime','bench_endtime']]
    
    