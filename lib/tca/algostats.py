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
import copy

        
# derived from the class algodataprocessor, for computing statistics

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
        max_date_xls = pytz.UTC.localize(dt.datetime.combine(max(dates).date(),dt.time(23,59)))
        
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
        
        # TODO : aggreger ces infos dans la base de donnees
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
                    
                if (isinstance(self.data_sequence['StartTime'][id_seq].values[0],dt.datetime) and 
                    self.data_sequence['StartTime'][id_seq].values[0].astimezone(tz=pytz.utc) > self.data_sequence['bench_starttime'][id_seq].values[0]):
                    
                    self.data_sequence['bench_starttime'][id_seq]=self.data_sequence['StartTime'][id_seq].values[0].astimezone(tz=pytz.utc)
                    
                #---------
                # -- bench_endtime
                if i < (len(uni_nb_replace)-1):
                    # if not last sequence : endtime = start time next sequence
                    id_next_seq = (self.data_sequence['nb_replace'] == uni_nb_replace[i+1])
                    self.data_sequence['bench_endtime'][id_seq] = self.data_sequence[id_next_seq].index[0].to_datetime() + diff_2apply
                    
                else:
                    # if last sequence : max(start time, eff_endtime,last_deal)
                    if isinstance(self.data_sequence['eff_endtime'][id_seq].values[0],dt.datetime):
                        self.data_sequence['bench_endtime'][id_seq] = max(self.data_sequence['bench_starttime'][id_seq].values[0],self.data_sequence['eff_endtime'][id_seq].values[0]) + dt.timedelta(seconds=1)
                    else:
                        self.data_sequence['bench_endtime'][id_seq] = self.data_sequence['bench_starttime'][id_seq] + dt.timedelta(seconds=1)
                        
                    if isinstance(self.data_sequence['exec_last_deal_time'][id_seq].values[0],dt.datetime):
                        # bidouille si proche du time de closing de notre base, on prend bien une marge
                        if isinstance(lasttick_datetime,dt.datetime) and (abs((self.data_sequence['exec_last_deal_time'][id_seq].values[0] - lasttick_datetime).total_seconds()) < 5):
                            self.data_sequence['bench_endtime'][id_seq] = lasttick_datetime + dt.timedelta(seconds=5)
                            
                        self.data_sequence['bench_endtime'][id_seq] = max(self.data_sequence['bench_endtime'][id_seq].values[0],self.data_sequence['exec_last_deal_time'][id_seq].values[0] + dt.timedelta(seconds=1)) 
                        
                    if (isinstance(self.data_sequence['strategy_name_mapped'][id_seq].values[0],basestring) and self.data_sequence['strategy_name_mapped'][id_seq].values[0].lower()=="vwap" and 
                            self.data_sequence['reason'][id_seq].values[0].lower()=="filled"):
                                # fullfiled vwap end time is the one set by the client if it is set or the end of trading
                                if isinstance(self.data_sequence['EndTime'][id_seq].values[0],dt.datetime):
                                    self.data_sequence['bench_endtime'][id_seq]=max(self.data_sequence['bench_endtime'][id_seq].values[0],self.data_sequence['EndTime'][id_seq].values[0].astimezone(tz=pytz.utc))
                                     
                # on coupe 5 secondes apres notre derniere donnees tick                    
                if isinstance(lasttick_datetime,dt.datetime):
                    self.data_sequence['bench_endtime'][id_seq]=min(self.data_sequence['bench_endtime'][id_seq].values[0],lasttick_datetime+dt.timedelta(seconds=5))
            
            #--- update enrichment colnames
            self.__update_stats_enrichment( mode = 'db' ,level = level , columns = ['bench_starttime','bench_endtime'])
            
        elif level == 'occurrence':
            #--
            #- test if only on one occurrence and market data correspond to the right security
            if len(np.unique(getattr(self,'data_' + level)['p_occ_id'].values).tolist()) > 1:
                logging.error('__add_bentime only works for one occurrence')
                raise ValueError('__add_bentime only works for one occurrence')
            
            #--
            #- only works if it has been computed at the sequence level                      
            if (getattr(self,'data_sequence') is None) or (getattr(self,'data_sequence').shape[0] == 0) or ('bench_starttime' not in getattr(self,'data_sequence').columns.tolist()):
                raise ValueError('__add_bench_time should be launch before at sequence level')
            
            self.data_occurrence['occ_bench_starttime'] = [slicer.first_finite(self.data_sequence['bench_starttime'].values)]
            self.data_occurrence['occ_bench_endtime'] = [slicer.last_finite(self.data_sequence['bench_endtime'].values)]
            
        else:
            raise ValueError('Not defined for level <' + level + '>')   
        
        
        
    def __flag_deals(self, market_data = None, referential_data = None):
        # create columns columns 'phase'
        # value taken : 'MF': manual fill / 'AO' : opening / 'AC' : closing
        
        #-----------------
        #-- TEST
        #-----------------
        if self.data_deal is None:
            raise ValueError('data_deal has to be loaded')
            
        if market_data is None or market_data.data_tick is None:
            raise ValueError('market_data tick has to be loaded')
            
        if referential_data is None or referential_data.data_exchange_info is None:
            raise ValueError('market_data tick has to be loaded')
            
        if self.data_deal.shape[0] == 0:
            return
            
        #-----------------
        #-- PREPROCESS
        #-----------------
        #-- in case, already the variable
        add_columns = ['tphase','exchange_id','EXCHANGETYPE','dark']
        for cols_ in add_columns:
            if cols_ in self.data_deal.columns.tolist():
                self.data_deal.drop(cols_ ,axis = 1)  
            
        self.data_deal['tphase'] = None
        self.data_deal['dark'] = None
        self.data_deal['EXCHANGETYPE'] = None
        #--
        if market_data.data_tick.shape[0] == 0:
            self.data_deal['exchange_id'] = None
            return
        
        #-- dark
        self.data_deal['dark'][map(lambda x : x in ['BATD','CHID','BLNK','SGMX','TRQM','XUBS','XPOS'],self.data_deal['MIC'])] = 1
        
        #--
        #-- bidouille vu que les bases sont merdiques !
        #-- create a fake MIC_ , and then merge with the referential
        if referential_data.data_exchange_info.shape[0] == 0:
            self.data_deal['exchange_id'] = None
            return
        
        #-- fake MIC to handle dark
        self.data_deal['MIC_'] = self.data_deal['MIC']
        self.data_deal['MIC_'][map(lambda x : isinstance(x,basestring) and x == 'CHID',self.data_deal['MIC_'])] = 'CHIX'
        self.data_deal['MIC_'][map(lambda x : isinstance(x,basestring) and x == 'TRQM',self.data_deal['MIC_'])] = 'TRQX'
        self.data_deal['MIC_'][map(lambda x : isinstance(x,basestring) and x == 'BATD',self.data_deal['MIC_'])] = 'BATE'
        referential_data.data_exchange_info.rename(columns = {'MIC' : 'MIC_'} , inplace = True)
        
        #-- add exchange_id
        index_name = self.data_deal.index.name
        self.data_deal = self.data_deal.reset_index().merge(referential_data.data_exchange_info[['MIC_','exchange_id']], how = 'left' , on = ['MIC_']).set_index(index_name)
        
        #-- fake MIC switch back
        referential_data.data_exchange_info.rename(columns = {'MIC_' : 'MIC'} , inplace = True)
        referential_data.data_exchange_info.sort_index() # to keep the ranking fine
        self.data_deal.drop('MIC_' ,axis = 1)
        
        #-- bidouille with exchange main..only the first
        main_exchange_id = None
        if any(referential_data.data_exchange_info['EXCHANGETYPE'] == 'M'):
            main_exchange_id = referential_data.data_exchange_info[referential_data.data_exchange_info['EXCHANGETYPE'] == 'M']['exchange_id'].values[0]
            self.data_deal['EXCHANGETYPE'][self.data_deal['exchange_id'] == main_exchange_id] = 'M'
            
        #-----------------
        #-- COMPUTE
        #-----------------
        self.data_deal.sort_index(inplace = True)
        #- check for the opening
        if any(market_data.data_tick['opening_auction'] == 1):
            
            price_ = market_data.data_tick[market_data.data_tick['opening_auction'] == 1]['price'].values[0]
            volume_ = sum(market_data.data_tick[market_data.data_tick['opening_auction'] == 1]['volume'])
            
            #- start
            times_ = market_data.data_tick[market_data.data_tick['opening_auction'] == 1].index[0].to_datetime()
            times_ = times_ - dt.timedelta(microseconds = times_.timetz().microsecond + 1) 
            #- end
            timee_ = market_data.data_tick[market_data.data_tick['opening_auction'] == 1].index[-1].to_datetime()
            timee_ = timee_ - dt.timedelta(microseconds = timee_.timetz().microsecond) + dt.timedelta(seconds = 2) # idee on garde les deals a la même seconde +1...vu que nos deals sont flagge a la seconde
            
            ids_open = ((self.data_deal['exchange_id'] == main_exchange_id) & 
                        (self.data_deal['price'] == price_) &
                        (self.data_deal['volume'] < volume_) &
                        (dftools.select_sorted_index_datetime(self.data_deal , start_date = times_, end_date = timee_ , start_strict = False , end_strict = True))
                        )
            
#             if 'liquidity_ind' in self.data_deal.columns.tolist():
#                 ids_open = ids_open & (formula.isfinite(self.data_deal.liquidity_ind.values , notfinite = True))
                
            idx_open = np.nonzero(ids_open)[0]
            if len(idx_open) > 0:
                for idx in idx_open:
                    self.data_deal['tphase'].values[idx] = 'AO'
                
        #- check for the closing + manual fill
        # TODO : manual fill should be donne with 'MAA...'
        if any(market_data.data_tick['closing_auction'] == 1):
            
            price_ = market_data.data_tick[market_data.data_tick['closing_auction'] == 1]['price'].values[0]
            volume_ = sum(market_data.data_tick[market_data.data_tick['closing_auction'] == 1]['volume'])
            #- start
            times_ = market_data.data_tick[market_data.data_tick['closing_auction'] == 1].index[0].to_datetime()
            times_ = times_ - dt.timedelta(microseconds = times_.timetz().microsecond + 1) - dt.timedelta(seconds = 5)
            #- end
            timee_ = market_data.data_tick[market_data.data_tick['closing_auction'] == 1].index[-1].to_datetime()
            timee_ = timee_ - dt.timedelta(microseconds = timee_.timetz().microsecond) + dt.timedelta(seconds = 5) # idee on garde les deals a la même seconde +4...vu que nos deals sont flagge a la seconde
            
            ids_close = ((self.data_deal['exchange_id'] == main_exchange_id) & 
                         (self.data_deal['price'] == price_) &
                         (self.data_deal['volume'] <= volume_) &
                         (dftools.select_sorted_index_datetime(self.data_deal , start_date = times_, end_date = timee_ , start_strict = False , end_strict = True))
                         )
            
#             if 'liquidity_ind' in self.data_deal.columns.tolist():
#                 ids_close = ids_close & (formula.isfinite(self.data_deal.liquidity_ind.values , notfinite = True))
                
            idx_close = np.nonzero(ids_close)[0]
            
            idx_manual = np.nonzero(map(lambda x,y : x is None and y > timee_,self.data_deal['MIC'],[x.to_datetime() for x in self.data_deal.index]))[0]
            
            if len(idx_close) > 0:
                for idx in idx_close:
                    self.data_deal['tphase'].values[idx] = 'AC'
                
            if len(idx_manual) > 0:
                for idx in idx_manual:
                    self.data_deal['tphase'].values[idx] = 'MF'  
                    
                         
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
            
            t0 = time.clock()
            
            #-- change mode_colnames ?
            if 'mode_colnames' in config['get_data'].keys():
                self.set_mode_colnames(config['get_data']['mode_colnames'])
                
            #-- load levels
            if 'level' in config['get_data'].keys():
                
                if not isinstance(config['get_data']['level'],list):
                    raise ValueError('level has to be a list')
                    
                for level in  config['get_data']['level']:
                    self.get_db_data(level = level)
            
            logging.info('get_data took <%3.2f> secs ' %(time.clock()-t0))
            
        #--------------------
        #-- APPLY
        #--------------------            
        if 'apply' in config.keys():
            
            t0 = time.clock()
            
            if not isinstance(config['apply'],list):
                raise ValueError('apply has to be a list')
            
            for i in range(0,len(config['apply'])):

                config_tmp = copy.deepcopy(config['apply'][i])
                t_1 = time.clock()
                
                #--- test
                if not isinstance(config_tmp,dict):
                    raise ValueError('apply has to be a list of dict')     
                
                if 'mode' not in config_tmp.keys() or 'info' not in config_tmp.keys():
                    raise ValueError('apply member has to be a dict with mode and info')
                    
                #-- apply
                if config_tmp['mode'] == 'formula':
                    self.apply_formula(**config_tmp['info'])
                
                elif config_tmp['mode'] == 'slicer':
                    config_tmp['info'].update({'market_data' : market_data ,'referential_data' : referential_data})
                    self.apply_slicer(**config_tmp['info'])
                
                elif config_tmp['mode'] == 'aggregate_market_stats':
                    config_tmp['info'].update({'market_data' : market_data ,'referential_data' : referential_data})
                    self.apply_aggregate_market_stats(**config_tmp['info'])
                
                elif config_tmp['mode'] == 'daily_market_stats':
                    config_tmp['info'].update({'market_data' : market_data ,'referential_data' : referential_data})
                    self.apply_daily_market_stats(**config_tmp['info'])
                
                elif config_tmp['mode'] == 'merge_level':
                    self.__merge_level(**config_tmp['info'])
                    
                elif config_tmp['mode'] == 'merge_referential':
                    config_tmp['info'].update({'referential_data' : referential_data})
                    self.__merge_referential(**config_tmp['info'])
                    
                elif config_tmp['mode'] == 'flag_deals':
                    config_tmp['info'].update({'market_data' : market_data ,'referential_data' : referential_data})
                    self.__flag_deals(**config_tmp['info'])
                                                           
                else:
                    raise ValueError('Unknwon apply_mode <' + config_tmp['mode'] +'>')
                
                logging.debug('apply mode : ' + config_tmp['mode']  + ' <%3.2f> secs ' % (time.clock()-t_1))
                
            logging.info('apply took <%3.2f> secs ' %(time.clock()-t0))    
                    
    def apply_formula(self, level = None , formula = None , in_sort = None , in_sort_ascending = True):
        #--------------------
        #-- compute statistics according to formula, which is a dict containing the name of the statistics and its formula
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
            
            
    def apply_slicer(self, in_level = None , out_level = None, group_vars = None , group_vars_step = None , slicer_ = {} , 
                     in_sort = None , in_sort_ascending = True , 
                     replace_cols = False , merge_out_cols = None , merge_referential_cols = None ,
                     market_data = None, referential_data = None):
        
        #--------------------
        #-- compute aggregated statistics, return either a dataframe of the statistics (out_level = None) or add the statistics to the self.dataxxx
        #--------------------
        # - in data
        if in_level is None or getattr(self,'data_' + in_level) is None:
            raise ValueError('in_level data should be extracted')
            
        # - out data
        if out_level is not None and getattr(self,'data_' + out_level) is None:
            raise ValueError('out_level data should be extracted')   
            
        # - group_vars
        if group_vars is None and group_vars_step is None:
            raise ValueError('at least group_vars or group_vars_step has to be indicated')
        
        elif isinstance(group_vars,basestring):
            group_vars = [group_vars]
        
        if (getattr(self,'data_' + in_level).shape[0] > 0) and any([not x in getattr(self,'data_' + in_level).columns.tolist() for x in group_vars]):
            raise ValueError('all groyp_vars should be into in_level')
        
        if (out_level is not None) and (getattr(self,'data_' + out_level).shape[0] > 0) and any([not x in getattr(self,'data_' + out_level).columns.tolist() for x in group_vars]):
            raise ValueError('all group_vars should be into in_level')
        
        # - others
        if not isinstance(slicer_,dict):
            raise ValueError('slicer_ has to be a dict')
            
        if (in_sort is not None) and (not isinstance(in_sort,list)):
            raise ValueError('in_sort has to be a list')
        elif (in_sort is not None) and (getattr(self,'data_' + in_level).shape[0] > 0) and any([not x in getattr(self,'data_' + in_level).columns.tolist() for x in in_sort]):
            raise ValueError('all in_sort should be into in_level')
            
        #--------------------
        #-- slicer_
        #--------------------
        _add_agg_data = pd.DataFrame()
        
        if len(slicer_) == 0:
            logging.info('no slicer_ to add')
            return
        
        if getattr(self,'data_' + in_level).shape[0] > 0:
                        
            #-- merge info from out to in
            if (merge_out_cols is not None):
                tmp_dict = copy.deepcopy(merge_out_cols)
                self.__merge_level(**copy.deepcopy(merge_out_cols))
                
            #-- merge info from out to in
            if (merge_referential_cols is not None):
                tmp_dict = copy.deepcopy(merge_referential_cols)
                tmp_dict.update({'referential_data' : referential_data})
                self.__merge_referential(**tmp_dict)                
                
            #-- sort input if needed
            if in_sort is not None and len(in_sort) > 0:
                getattr(self,'data_' + in_level).sort_index( by = in_sort, ascending = in_sort_ascending , inplace = True)
            
            
            #-- if there is a group_vars_step
            agg_step_sec = None
            
            if group_vars_step is not None:
                
                #--- add temporary info on the in_level
                if isinstance(group_vars_step,basestring):
                    if group_vars_step == 'day':
                        getattr(self,'data_' + in_level)['end_slice']=[dt.datetime.combine(x.to_datetime().date(),dt.time(23,59,59)) for x in getattr(self,'data_' + in_level).index]
                        getattr(self,'data_' + in_level)['begin_slice']=[dt.datetime.combine(x.to_datetime().date(),dt.time(0,0,0)) for x in getattr(self,'data_' + in_level).index]
                    elif group_vars_step == 'week':
                        getattr(self,'data_' + in_level)['end_slice'] = [dt.datetime.combine(x.to_datetime().date()-dt.timedelta(days=x.to_datetime().date().weekday()-4),dt.time(0,0,0)) for x in getattr(self,'data_' + in_level).index]
                        getattr(self,'data_' + in_level)['begin_slice'] = [dt.datetime.combine(x.to_datetime().date()-dt.timedelta(days=x.to_datetime().date().weekday()),dt.time(0,0,0)) for x in getattr(self,'data_' + in_level).index]
                    elif group_vars_step == 'month':
                        getattr(self,'data_' + in_level)['end_slice'] = [dt.datetime.combine(x.to_datetime().date().replace(month=x.to_datetime().date().month % 12 + 1, day = 1) - dt.timedelta(days=1),dt.time(0,0,0)) for x in getattr(self,'data_' + in_level).index]
                        getattr(self,'data_' + in_level)['begin_slice'] = [dt.datetime.combine(x.to_datetime().date().replace(day=1),dt.time(0,0,0)) for x in getattr(self,'data_' + in_level).index]
                    elif group_vars_step == 'year':
                        getattr(self,'data_' + in_level)['end_slice'] = [dt.datetime.combine(x.to_datetime().date().replace(month=1,day=1),dt.time(0,0,0)) for x in getattr(self,'data_' + in_level).index]
                        getattr(self,'data_' + in_level)['begin_slice'] = [dt.datetime.combine(x.to_datetime().date().replace(month=12,day=31),dt.time(0,0,0)) for x in getattr(self,'data_' + in_level).index]
                    else:
                        raise ValueError('Unknown group_vars_step <'+ group_vars_step + '>')
                    
                    #--- add to group_vars
                    if group_vars is not None:
                        group_vars = ['begin_slice','end_slice'] + group_vars
                    else:
                        group_vars = ['begin_slice','end_slice']
                    
                elif group_vars_step > 0:
                    agg_step_sec = group_vars_step
                    
                else:
                    raise ValueError('Bad inputs group_vars_step')
                

            
            #-- compute
            try:
                _add_agg_data = dftools.agg(getattr(self,'data_' + in_level),
                                                group_vars = group_vars,
                                                step_sec = agg_step_sec,
                                                stats = dict([(x, slicer_[x]['slicer']) for x in slicer_.keys()]))
            except:
                get_traceback()
                raise ValueError('Error on applying slicers>')
            
            #-- merge info from out to in, erase columns added
            merge_cols_remove = []
            
            if (merge_out_cols is not None):
                merge_cols_remove += merge_out_cols['cols']
            
            if (merge_referential_cols is not None):
                merge_cols_remove += merge_referential_cols['cols']
            
            if len(merge_cols_remove) > 0:
                setattr(self, 'data_' + in_level , getattr(self,'data_' + in_level).drop(merge_cols_remove, axis = 1))
            
            #-- erase temporary group_vars_step
            if group_vars_step is not None and isinstance(group_vars_step,basestring):
                setattr(self, 'data_' + in_level , getattr(self,'data_' + in_level).drop(['begin_slice','end_slice'], axis = 1))
            
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
                for x in slicer_.keys():
                    getattr(self,'data_' + out_level)[x] = slicer_[x]['default']
            else:
                #-- if slicer compute values that already exist : parameter replace
                common_cols = list((set(_add_agg_data.columns.tolist()) - set(group_vars)).intersection(list(set(getattr(self,'data_' + out_level).columns.tolist()) - set(group_vars))))
                
                if len(common_cols) > 0:
                    if replace_cols:
                        # erase cols in the self.data_ ...
                        setattr(self,'data_' + out_level, getattr(self,'data_' + out_level)[list(set(getattr(self,'data_' + out_level).columns.tolist()) - set(common_cols)) ])
                    else:
                        raise ValueError('common columns ')
                    
                #-- case if one of the group vars is not in the in_level, you have to put the default values in _add_agg_data
                for i_ in range(0,getattr(self,'data_' + out_level).shape[0]):
                    #-- find the group var in the slicer values _add_agg_data
                    ids = (_add_agg_data[group_vars[0]] == getattr(self,'data_' + out_level)[group_vars[0]].values[i_])
                    if len(group_vars) > 1:
                        for icol in range(1,len(group_vars)):
                            ids = ids & (_add_agg_data[group_vars[icol]] == getattr(self,'data_' + out_level)[group_vars[icol]].values[i_])
                        
                    if not any(ids):
                        #-- a default line has to be added to _add_agg_data
                        tmp_ = dict([(x,slicer_[x]['default']) for x in slicer_.keys()])
                        tmp_.update(dict([( x , getattr(self,'data_' + out_level)[x].values[i_]) for x in group_vars]))
                        tmp_ = pd.DataFrame([tmp_])
                        _add_agg_data = _add_agg_data.append(tmp_)
                            
                #-- if slicer compute values that already exist : parameter replace
                
                index_name = getattr(self,'data_' + out_level).index.name
                setattr(self,'data_' + out_level,
                            getattr(self,'data_' + out_level).reset_index().merge(_add_agg_data, how = 'left' , on = group_vars).set_index(index_name))
                
                       
                        
            #-- update the enrichment !!
            self.__update_stats_enrichment(mode = 'db' ,level = out_level , columns = slicer_.keys())
            
        else:
            return _add_agg_data
        
        
    def apply_daily_market_stats(self, level = None , market_formula = {} , market_data = None, referential_data = None):
        #-----------------------------------
        # TESTS INPUT
        #-----------------------------------
        # -
        if level is None or getattr(self,'data_' + level) is None:
            raise ValueError('level data should be extracted')
        
        # -
        if (not isinstance(market_formula,dict)):
            raise ValueError('market_formula has to be dict')
        
        # check inputs security matches
        if (market_data.security_id != referential_data.security_id or 
            len(np.unique(getattr(self,'data_' + level)['cheuvreux_secid']).tolist()) != 1 or
            getattr(self,'data_' + level)['cheuvreux_secid'].values[0] != market_data.security_id):
            logging.error('not the same security')
            raise ValueError('not the same security')        
        
        #-----------------------------------
        # ADD DAILY MARKET STATS
        #-----------------------------------  
        if getattr(self,'data_' + level).shape[0] == 0:
            logging.info('no data on which to add market daily stats')
            return      
        
        #--- compute
        data_stats = {}
        for periodstats in market_formula.keys():
            #------
            #-- create entry
            formula_ = None
            cols_ = None
            filter_ = copy.deepcopy(market_formula[periodstats]['filter'])
            
            if 'formula' in market_formula[periodstats].keys():
                formula_ = copy.deepcopy(market_formula[periodstats]['formula'])
                
            if 'cols' in market_formula[periodstats].keys():
                cols_ = copy.deepcopy(market_formula[periodstats]['cols'])   
                
            #------
            #-- compute
            data_stats.update(market_data.compute_formula_daily(filterd = filter_, cols = cols_ , formulad = formula_ , output_mode = 'dict'))
        
        #--- add to level
        for name in data_stats:
            getattr(self,'data_' + level)[name] = data_stats[name]
            
        self.__update_stats_enrichment( mode = 'db' ,level = level , columns = data_stats.keys())
        
        
    def apply_aggregate_market_stats(self, level = None , market_slicer = {} , formula = {} , market_data = None, referential_data = None):
        #-----------------------------------
        # compute aggregate market data statistics, and add them to self.dataxxx, one 
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
            dict_cols = {'primary_key' : 'p_cl_ord_id',
                         'bench_start' : 'bench_starttime',
                         'bench_end' : 'bench_endtime',
                         'side' : 'Side',
                         'limit_price' : 'Price',
                         'exec_opening' : 'exec_qty_opening',
                         'exec_closing' : 'exec_qty_closing'}
            
        elif level == 'occurrence':
            dict_cols = {'primary_key' : 'p_occ_id',
                         'bench_start' : 'occ_bench_starttime',
                         'bench_end' : 'occ_bench_endtime',
                         'side' : 'Side',
                         'exec_opening' : 'occ_exec_qty_opening',
                         'exec_closing' : 'occ_exec_qty_closing'}         
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
            
            data_this_seq = getattr(self,'data_' + level)[getattr(self,'data_' + level)[dict_cols['primary_key']] == getattr(self,'data_' + level).ix[iseq][dict_cols['primary_key']]]
            
            #---
            #- compute stats
            data_stats = {}
            for periodstats in market_slicer.keys():
                
                tp_0 = time.clock()
                
                # TODO 
                #market_slicer[periodstats]['slicer'].
                #------
                #-- create filter and slicer
                slicer_ = copy.deepcopy(market_slicer[periodstats]['slicer'])
                filter_ = copy.deepcopy(market_slicer[periodstats]['filter'])
                
                if market_slicer[periodstats]['period_time_constr']:
                    filter_.update({'start_date' : data_this_seq[dict_cols['bench_start']].values[0], 'end_date': data_this_seq[dict_cols['bench_end']].values[0]})
                    
                if market_slicer[periodstats]['period_phase_constr']:
                    filter_.update({'exclude_auction': mapping.ExcludeAuction(data_this_seq['ExcludeAuction'].values[0] , 
                                                                              exec_opening = data_this_seq[dict_cols['exec_opening']].values[0] ,
                                                                              exec_closing = data_this_seq[dict_cols['exec_closing']].values[0] ,)})
                
                if market_slicer[periodstats]['order_constr']:
                    filter_.update({'order_limit': data_this_seq[dict_cols['limit_price']].values[0], 'order_side': data_this_seq[dict_cols['side']].values[0]})
                
                #------
                #-- transform slicer dict_data !
                for tmpkeys in slicer_.keys():
                    if 'dict_data' in slicer_[tmpkeys]: 
                        #-- check if it is a list
                        if not isinstance(slicer_[tmpkeys]['dict_data'],list):
                            raise ValueError('dict_data has to be a list in the config, slicer name : <'+ tmpkeys +'>')
                            
                        #-- transform list to dict
                        if not all([x in data_this_seq.columns.tolist() for x in slicer_[tmpkeys]['dict_data']]):
                            raise ValueError('all dict_data columns has to be in the dataset !!')
                            
                        tmp_ditc = {}
                        for col in slicer_[tmpkeys]['dict_data']:
                            tmp_ditc.update({col : data_this_seq[col].values[0]})
                            
                        slicer_[tmpkeys]['dict_data'] = tmp_ditc
                        
                #------
                #-- compute
                data_stats.update(market_data.compute_agg_period_tick(filterd = filter_, slicerd = slicer_))
                
                logging.debug('market slicer period ' + periodstats + '  took <%3.2f> secs ' %(time.clock()-tp_0))
                
            new_added_columns = list(set(new_added_columns+data_stats.keys()))
            data_stats = pd.DataFrame([data_stats])
            data_stats[dict_cols['primary_key']] = data_this_seq[dict_cols['primary_key']].values[0]
            
            #---
            #- add
            index_name = data_this_seq.index.name
            data_this_seq = data_this_seq.reset_index().merge(data_stats,how='left',on=[dict_cols['primary_key']]).set_index(index_name)
            new_data = new_data.append(data_this_seq)
            new_data.index.name = index_name
            
            
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
            
            
    def __merge_level(self, from_level = None , to_level = None, group_vars = None, cols = None):
        # add to to_level dataframe, the data "cols" from from_level dataframe, that we merged by "group_vars"
        #-----------------------------------
        # TESTS INPUT
        #-----------------------------------
        if from_level is None or to_level is None or group_vars is None or cols is None:
            raise ValueError('bad inputs')
        
        if getattr(self,'data_' + from_level) is None or getattr(self,'data_' + to_level) is None:
            raise ValueError('from_level and to_level has to be loaded')
        
        if not isinstance(cols,list) or not isinstance(group_vars,list):
            raise ValueError('cols and group_vars has be list')
        
        #idea : add out colnames to the to_level
        if any([not x in getattr(self,'data_' + from_level).columns.tolist() for x in group_vars]) or any([not x in getattr(self,'data_' + to_level).columns.tolist() for x in group_vars]):
            raise ValueError('bad inputs group_vars, should be present in from_level and to_level')
        
        if any([not x in getattr(self,'data_' + from_level).columns.tolist() for x in cols]) or any([x in getattr(self,'data_' + to_level).columns.tolist() for x in cols]):
            raise ValueError('bad inputs cols, should be present in from_level and not in to_level')
        
        #-----------------------------------
        # COMPUTE
        #-----------------------------------              
        if (getattr(self,'data_' + from_level).shape[0] == 0) or (getattr(self,'data_' + to_level).shape[0] == 0):
            logging.info('__merge_level nothing to add')
            return
        
        index_name = getattr(self,'data_' + to_level).index.name
        setattr(self,'data_' + to_level,
                getattr(self,'data_' + to_level).reset_index().merge(getattr(self,'data_' + from_level)[group_vars + cols], how = 'left' , on = group_vars).set_index(index_name))
            
            
    def __merge_referential(self, from_data = None , to_level = None, group_vars = None, cols = None ,  referential_data = None):
        # add to to_level dataframe, the data "cols" from from_level dataframe, that we merged by "group_vars"
        #-----------------------------------
        # TESTS INPUT
        #-----------------------------------
        if from_data is None or to_level is None or group_vars is None or cols is None or referential_data is None:
            raise ValueError('bad inputs')
        
        if getattr(self,'data_' + to_level) is None or getattr(referential_data,'data_' + from_data) is None:
            raise ValueError('from_data and to_level has to be loaded')
        
        if not isinstance(cols,list) or not isinstance(group_vars,list):
            raise ValueError('cols and group_vars has be list')
        
        #idea : add out colnames to the to_level
        if any([not x in getattr(referential_data,'data_' + from_data).columns.tolist() for x in group_vars]) or any([not x in getattr(self,'data_' + to_level).columns.tolist() for x in group_vars]):
            raise ValueError('bad inputs group_vars, should be present in from_level and to_level')
        
        if any([not x in getattr(referential_data,'data_' + from_data).columns.tolist() for x in cols]) or any([x in getattr(self,'data_' + to_level).columns.tolist() for x in cols]):
            raise ValueError('bad inputs cols, should be present in from_level and not in to_level')
        
        #-----------------------------------
        # COMPUTE
        #-----------------------------------              
        if (getattr(referential_data,'data_' + from_data).shape[0] == 0) or (getattr(self,'data_' + to_level).shape[0] == 0):
            logging.info('__merge_referential nothing to add')
            return
        
        index_name = getattr(self,'data_' + to_level).index.name
        setattr(self,'data_' + to_level,
                getattr(self,'data_' + to_level).reset_index().merge(getattr(referential_data,'data_' + from_data)[group_vars + cols], how = 'left' , on = group_vars).set_index(index_name))                


        
        
                    
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
    
#     Connections.change_connections('dev')    
#     test = AlgoStatsProcessor(filter = {"p_occ_id": {"$in" : ['20140306FY200017893259ESLO0WATFLT01']}} , mode_colnames = 'all')
#     test.get_db_data(level = 'sequence')
#     test.data_sequence['exec_first_deal_time']

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
    
#     #-----  COMPUTE TCA STATS   (base)
#     ## occ with ùutliple sequence filled
#     occ_id = '20130521FY000008193901LUIFLT01'
#     ## occ with would level
#     #occ_id = '20140127FY2000054382501WATFLT01'    
#     ###-- occ with dark
#     #occ_id = '20140130FY000055104901LUIFLT01'
#     ###-- close
#     #occ_id ='20130521FY000008263201LUIFLT01'
#     ##-- south africa order
#     #occ_id = '20140103FY2DG14356f0d5a3-000WATFLT01'
#     
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
#     ref_data.get_data_security_info(ch_security_id = 'cheuvreux_secid')
#     
#     occ_data = AlgoStatsProcessor(filter = {"p_occ_id": {"$in" : [occ_id]}})
#     occ_data.compute_stats(config_mode = 'db_one_occurrence' , market_data = mkt_data , referential_data = ref_data)
#     
#     print occ_data.data_sequence 
#     print occ_data.data_occurrence 
#     print occ_data.db_stats_enrichment
#     print occ_data.data_occurrence.columns.tolist()
    
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
    
    