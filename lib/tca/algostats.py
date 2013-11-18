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

class AlgoStatsProcessor(AlgoDataProcessor):
    
    ###########################################################################
    # INIT
    ###########################################################################
    def __init__(self,*args,**kwargs):
        # Parents attr
        AlgoDataProcessor.__init__(self,*args,**kwargs)
        
        # data
        self.data_intraday_agg_deals=None
        
        
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
                        self.get_db_data(level='sequence',force_colnames_only=['strategy_name_mapped'])
                    
                    config_stats={'occ_fe_strategy_name_mapped':
                        {'default': 'Unknown' ,'slicer' : lambda df : df.strategy_name_mapped.values[-1]}}
                        
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
    # SELF COMPUTE METHODS FOR ONE OCCURRENCE
    ###########################################################################
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
            
        if self.data_occurrence.shape[0]!=1:
            logging.error('only works for one occurrence')
            raise ValueError('only works for one occurrence')
            
        if (market_data.security_id!=referential_data.security_id or 
            self.data_occurrence['cheuvreux_secid'].values[0]!=market_data.security_id):
            logging.error('')
            raise ValueError('not the same security')           
        
        #-----------------------------------
        # UPDATE DEALS
        #-----------------------------------
        # TO OD
        
        #-----------------------------------
        # EXEC STATS
        #-----------------------------------        
        self.__compute_db_exec_stats()
        
        #-----------------------------------
        # MARKET STATS
        #-----------------------------------        
        self.__compute_db_market_stats(market_data=market_data,referential_data=referential_data)
        
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
        
    def __compute_db_market_stats(self,market_data=None,referential_data=None):
        #-----------------------------------
        # add benchtime
        #-----------------------------------
        lasttick_datetime=None
        if market_data.data_tick.shape[0]>0:
            lasttick_datetime=market_data.data_tick.index[-1].to_datetime()
            
        self.__add_benchtime(lasttick_datetime=lasttick_datetime)
        
        #-----------------------------------
        # market stats on period
        #-----------------------------------        
        # TO DO
        
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
                    'exec_turnover': {'default': 0.0 ,'slicer' : lambda df : np.sum(map(lambda x,y : x*y,df.volume.values,df.price.values))}}
                            
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
                    
            #----------------
            # STEP 2 : compute cumulative stats at occurence level : prefixed by occ_prev_
            #----------------
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
    # SELF OTHER STATS METHODS
    ###########################################################################
    def __add_benchtime(self,lasttick_datetime=None):
        #-----------------------------------
        # TESTS
        #-----------------------------------
        if self.data_sequence is None:
            logging.error('data_sequence should be loaded')
            raise ValueError('data_sequence should be loaded')
        
        if self.data_sequence.shape[0]==0:
            return
        
        #-----------------------------------
        # UPDATE SEQUENCE
        #-----------------------------------
        # initialize
        self.data_sequence['bench_starttime']=None
        self.data_sequence['bench_endtime']=None
        
        for p_cl_ord_id in np.unique(self.data_sequence['p_cl_ord_id']):
            id=self.data_sequence['p_cl_ord_id']==p_cl_ord_id
            #-- bench_starttime
            self.data_sequence['bench_starttime'][id]=self.data_sequence[id].index[0].to_datetime()
            if (isinstance(self.data_sequence['StartTime'][id],dt.datetime) and 
                self.data_sequence['StartTime'][id].astimezone(tz=pytz.utc)>self.data_sequence['bench_starttime'][id]):
                
                self.data_sequence['bench_starttime'][id]=self.data_sequence['StartTime'][id].astimezone(tz=pytz.utc)
            
            # -- bench_endtime
            self.data_sequence['bench_endtime'][id]=self.data_sequence['eff_endtime'][id]
            
            if (self.data_sequence['strategy_name_mapped'][id].values[0].lower()=="vwap" and 
                    self.data_sequence['reason'][id].values[0].lower()=="filled"):
                        # fullfiled vwap end time is the one set by the client if it is set or the end of trading
                        if isinstance(self.data_sequence['EndTime'][id],dt.datetime):
                            self.data_sequence['bench_endtime'][id]=max(self.data_sequence['bench_endtime'][id],self.data_sequence['EndTime'][id].astimezone(tz=pytz.utc))
                        elif isinstance(lasttick_datetime,dt.datetime):
                            self.data_sequence['bench_endtime'][id]=max(self.data_sequence['bench_endtime'][id],lasttick_datetime+dt.timedelta(seconds=5))

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
    
    #-----  TEST AGG DEAL
    test = AlgoStatsProcessor(start_date = dt.datetime(2013,8,12), end_date = dt.datetime(2013,8,14))
    test.get_db_data(level='deal')
    test.get_intraday_agg_deals_data()
    print test.data_intraday_agg_deals
    
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
    
    