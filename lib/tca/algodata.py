# -*- coding: utf-8 -*-
"""
Created on Tue Sep 03 10:25:23 2013

@author: njoseph
"""
from pymongo import *
import pandas as pd
import datetime as dt
import time as time
import numpy as np
# import lib.data.matlabutils as matlabutils
import logging
from lib.logger.custom_logger import *
import lib.tca.mapping as mapping
from lib.dbtools.connections import Connections
import lib.data.matlabutils as matlabutils
import lib.dbtools.read_dataset as read_dataset
import lib.dbtools.get_repository as get_repository
import os

#-----------------------------------
# EXPLANATION
#-----------------------------------
# AlgoDataProcessor object is coherent to the class_mode 'sequence', 'occurrence', 'deal'
# this is like the entry point of the object, meaning filters etc will be related to this object
# 1/ we want to analyse starting from deals, then we use the self. parameters
#  -> deal_mode='self'
# 2/  we want to analyse starting from orders, and then getting deals related to this order, then we use the cl_ord_id_list
#  -> deal_mode='sequence' or 'occurrence'


class AlgoDataProcessor(object):
    
    ###########################################################################
    # INIT
    ###########################################################################
    def __init__(self,date = None, start_date = None, end_date = None, filter = None,
                 mode_colnames = 'base'):
        
        #---- INPUT
        """start_date and end_date are datetime"""
        self.entry_level=None
        if date is not None:
            self.start_date = dt.datetime.combine(date.date(),dt.datetime.strptime('00:00:00','%H:%M:%S').time())
            self.end_date = dt.datetime.combine(date.date(),dt.datetime.strptime('23:59:59','%H:%M:%S').time())
        else:
            self.start_date = start_date
            self.end_date = end_date
        self.filter = filter
        self.mode_colnames=mode_colnames
        self.merge_colnames_sequence2deal=None # Now only for deals when we merge data
        
        #---- DATABASE ALGO
        self.data_sequence=None
        self.data_occurrence=None
        self.data_deal=None
        
        #---- CONNECTION INFO
        self.db_name = 'Mars'
        self.algo_collection_name = 'AlgoOrders'
        self.deal_collection_name = 'OrderDeals'
        
        #---- excel INFO
        self.xls_occ_fe_pathfilename=os.path.join('H:\\Data','Export_EOD_Flex_2013_extract.csv')
        self.data_xls_occ_fe=None
        
            
    ###########################################################################
    # METHOD GET DATA
    ###########################################################################
    def get_db_data(self, level=None,
                    force_colnames_only=None):
        
        t0=time.clock()
        
        #-----------------------------------
        # DETERMINE ENTRY LEVEL
        #-----------------------------------
        if self.data_deal is None and self.data_occurrence is None and self.data_sequence is None:
            self.entry_level=level
            
        #-----------------------------------
        # TEST INPUT + DEFAULT OUTPUT  + PARAMS
        #-----------------------------------
        if level == 'sequence':
            if self.data_sequence is not None:
                logging.info('get_db_data is already loaded')
                return
                
            self.data_sequence=pd.DataFrame()
            
            cname=self.algo_collection_name
        elif level == 'occurrence':
            if self.data_occurrence is not None:
                logging.info('get_db_data is already loaded')
                return
                
            self.data_occurrence=pd.DataFrame()
            
            cname=self.algo_collection_name
        elif level == 'deal':
            if self.data_deal is not None:
                logging.info('get_db_data is already loaded')
                return
                
            self.data_deal=pd.DataFrame() 
            
            cname=self.deal_collection_name       
        else:
            logging.error('level should be sequence or occurrence')
            raise ValueError('Bad inputs')
        
        NEEDED_COLNAMES=self.get_db_colnames(level=level,mode=self.mode_colnames,only=force_colnames_only)
          
        #-----------------------------------
        # CONNECTION DB  
        #-----------------------------------        
        client = Connections.getClient(self.db_name.upper())
        occ_db = client[self.db_name][cname]
        
        #-----------------------------------
        # CONSTRUCT REQUEST
        #----------------------------------- 
        # --- TEST INPUTS
        if self.start_date is None and self.end_date is None and self.filter is None:
            logging.error('Bad inputs')
            raise ValueError('Bad inputs')
            
        # --- CRITERION 
        criterion = []
        index_colname='SendingTime'
        if level == 'deal':
            index_colname='TransactTime'
            
        #--- specific occurrence
        if level == 'occurrence':
            criterion.append({'MsgType':'D'})
            
        if self.entry_level == level:
            # CASE 1 : entry level, then we use the inputs information !   
            #--- Date Filters
            if self.start_date is not None and self.end_date is not None:
                #TO DO : check timezone
                criterion.append({index_colname: {"$gte":self.start_date , "$lte":self.end_date }})
                
            #--- Filters   
            if self.filter is not None:
                
                if np.any([x not in NEEDED_COLNAMES for x in self.filter.keys()]):
                    raise ValueError('one filter keys is not in the needed colnames')
                    
                criterion.append(self.filter)
                
        else:
            # CASE 2 : all merge is done by sequence information
            if self.entry_level == 'sequence':
                
                if self.data_sequence.shape[0]==0:
                    return
                criterion.append({'p_cl_ord_id': {"$in": np.unique(self.data_sequence['p_cl_ord_id']).tolist()}})
                
            elif self.entry_level == 'deal':
                
                if self.data_deal.shape[0]==0:
                    return
                criterion.append({'p_cl_ord_id': {"$in": np.unique(self.data_deal['p_cl_ord_id']).tolist()}})
                
            elif self.entry_level == 'occurrence':
                
                if self.data_occurrence.shape[0]==0:
                    return
                    
                if level == 'sequence':
                    criterion.append({'p_occ_id': {"$in": np.unique(self.data_occurrence['p_occ_id']).tolist()}})
                    
                elif level == 'deal':
                    if self.data_sequence is None:
                        # TO DO when p_occ_id in deal
                        raise ValueError('for now : data_sequence should be get before !')
                        
                    if self.data_sequence.shape[0]==0:
                        return
                        
                    criterion.append({'p_cl_ord_id': {"$in": np.unique(self.data_sequence['p_cl_ord_id']).tolist()}})
                    
        # --- Request        
        req = criterion[0] if (len(criterion) == 1) else {'$and' : criterion}  
        res = occ_db.find(req,NEEDED_COLNAMES)
        client.close()
        
        #-----------------------------------
        # CONSTRUCT DATAFRAME
        #-----------------------------------  
        # Create needed
        documents=[]
        columns=[]
        for v in res:
            documents.append(v)
            columns.extend(v.keys())
            columns=list(set(columns)) 
            
        if not documents:
            return
              
        # Dataframe
        data=pd.DataFrame.from_records(documents, columns=columns,index=index_colname)
        data=data.sort_index()  
        
        NEEDED_COLNAMES=np.setdiff1d(NEEDED_COLNAMES,[index_colname])
        
        # HANDLING COLNAMES : drop
        for x in data.columns.tolist():
            if x not in NEEDED_COLNAMES:
                data=data.drop([x],axis=1)
                
        # HANDLING COLNAMES : add
        for x in NEEDED_COLNAMES:
            if x not in data.columns.tolist():
                data[x]=np.NaN 
                
        #-----------------------------------
        # RENORMALIZATION
        #-----------------------------------
        #--- ALL 
        # Side
        if ('Side' in data.columns.tolist()):
            data['Side']=mapping.Side(data['Side'])
            if np.any(np.isnan(data['Side'])):
                logging.error('Side : strange values')
                raise ValueError('Side : strange values')
                
        #--- DEAL 
        if level == 'deal':
            # rename
            dict_rename={'LastPx': 'price','LastShares': 'volume', 'LastMkt' :'MIC'}
            for x in dict_rename.keys():
                if x in data.columns.tolist():
                    data = data.rename(columns={x:dict_rename[x]})
                    
        #--- ORDERS : 
        if level in ['sequence','occurrence']:
            # nb_replace
            if (('nb_replace' in data.columns.tolist()) and (any(np.isnan(data['nb_replace'].values)))):
                tmp=data['nb_replace']
                tmp[np.nonzero(np.isnan(data['nb_replace'].values))[0]]=0
                data['nb_replace']=tmp
                
        #-----------------------------------
        # MERGE DATA
        #-----------------------------------
        if level == 'deal' and data.shape[0]>0:
            if (self.merge_colnames_sequence2deal is not None):
                # data_sequence has to be present for now !
                if self.data_sequence is None or any([x not in self.data_sequence.columns.tolist() for x in self.merge_colnames_sequence2deal]):
                    logging.error('data_sequence should contains the colnames')
                    raise ValueError('data_sequence should contains the colnames')
                    
                # initialize columns
                for x in self.merge_colnames_sequence2deal:
                    data[x]=None
                    
                # add
                if self.data_sequence.shape[0]>0:
                    for idx in range(0,self.data_sequence.shape[0]):
                        idx_in=np.nonzero(data['p_cl_ord_id'].values==self.data_sequence.ix[idx]['p_cl_ord_id'])[0]        
                        for x in self.merge_algo_colnames:
                            data[x][idx_in]=self.data_sequence.ix[idx][x]
                            
        #-----------------------------------
        # OUT
        #-----------------------------------        
        if level == 'sequence':
            self.data_sequence = data
        elif level == 'occurrence':
            self.data_occurrence = data
        elif level == 'deal':
            self.data_deal = data
                        
        logging.debug('get_db_data  took <%3.2f> secs ' %(time.clock()-t0))
        
        
    def get_xls_occ_fe_data(self):    
        
        if self.data_xls_occ_fe is not None:
            logging.info('get_xls_occ_fe_data is already loaded')
            
        #------ LOAD HISTO DATA OCC FE DATA
        if not os.path.isfile(self.xls_occ_fe_pathfilename):
            raise ValueError('the occ_fe_file is missing')
            
        self.data_xls_occ_fe=pd.read_csv(self.xls_occ_fe_pathfilename,sep=';')
        
        # NORMALIZE like the db              
        # rate_to_euro, cheuvreux_secid, Side, occ_fe_strategy_name_mapped
        dict_rename={
            'STARTTIME':'SendingTime',
            'ENDTIME':'eff_endtime', 
            'STRATEGY':'occ_fe_strategy_name_mapped',
            'CURRENCY':'Currency',
            'TICKERFLEX':'Symbol',
            'USRID':'TargetSubID',
            'OMSREF':'Account',
            'EXECQTY':'occ_fe_exec_shares',
            'EXECPRICE' : 'occ_fe_avg_price',
            'MARKETTURNOVER':'occ_fe_inmkt_turnover',
            'MARKETVOLUME':'occ_fe_inmkt_volume',
            'ARRIVALPRICE':'occ_fe_arrival_price',
            'HIGH':'occ_fe_period_high',
            'LOW':'occ_fe_period_low',
            'FINALPRICE':'occ_fe_final_price',
            'TICKSIZE':'occ_fe_ticksize',
            'AVGSPREAD':'occ_fe_avg_sprd',
            'SIDE':'Side',
            'PORTFOLIO':'p_occ_id',
            'PARTVOL':'occ_fe_order_perc'}
         
        #---- drop non needed colnames
        self.data_xls_occ_fe=self.data_xls_occ_fe.drop(np.setdiff1d(self.data_xls_occ_fe.columns.values.tolist(),dict_rename.keys()).tolist(),axis=1)
         
        #---- rename colnames
        self.data_xls_occ_fe=self.data_xls_occ_fe.rename(columns=dict_rename)
         
        #--- add needed basic colnames
        self.data_xls_occ_fe['occ_fe_prv_turnover']=0
        self.data_xls_occ_fe['occ_fe_prv_volume']=0
        self.data_xls_occ_fe['occ_fe_prv_wexec']=0
         
        #---- basic formula
        self.data_xls_occ_fe['SendingTime']=map(lambda x : dt.datetime.strptime(x,'%d.%m.%Y %H:%M'),self.data_xls_occ_fe['SendingTime'].values)
        self.data_xls_occ_fe['eff_endtime']=map(lambda x : dt.datetime.strptime(x,'%d.%m.%Y %H:%M'),self.data_xls_occ_fe['eff_endtime'].values)
        self.data_xls_occ_fe['Side']=self.data_xls_occ_fe['Side'].apply(lambda x : 1 if x=='B' else -1)
         
        #----- cheuvreux_secid
        uni_,idx_in_uni_=matlabutils.uniqueext(self.data_xls_occ_fe['Symbol'].values,return_inverse=True)
        uni_vals=map(lambda x : get_repository.get_symbol6_from_ticker(x),uni_)
        vals=[np.nan]*len(idx_in_uni_)
        for i in range(0,len(vals)):
            vals[i]=uni_vals[idx_in_uni_[i]]
        self.data_xls_occ_fe['cheuvreux_secid']=vals
         
        #----- strategy name
        uni_,idx_in_uni_=matlabutils.uniqueext(self.data_xls_occ_fe['occ_fe_strategy_name_mapped'].values,return_inverse=True)
        uni_vals=map(lambda x : str(mapping.StrategyName(x)),uni_)
        vals=['']*len(idx_in_uni_)
        for i in range(0,len(vals)):
            vals[i]=uni_vals[idx_in_uni_[i]]
        self.data_xls_occ_fe['occ_fe_strategy_name_mapped']=vals
         
        #----- rate to euro 
        uni_curr=np.unique(self.data_xls_occ_fe['Currency'].values).tolist()
        data_rte=read_dataset.histocurrencypair(start_date = dt.datetime.strftime(np.min(self.data_xls_occ_fe['SendingTime']),'%Y%m%d'), 
                                                end_date = dt.datetime.strftime(np.max(self.data_xls_occ_fe['eff_endtime']),'%Y%m%d'),currency = uni_curr)
        data_rte['tmpmergetime']=map(lambda x : dt.datetime.strftime(x.to_datetime(),'%Y-%m-%d'),data_rte.index)
        self.data_xls_occ_fe['tmpmergetime']=map(lambda x : dt.datetime.strftime(x,'%Y-%m-%d'),self.data_xls_occ_fe['SendingTime'])
        data_rte=data_rte.rename(columns={'ccy':'Currency'}).drop('ccyref',axis=1)
        self.data_xls_occ_fe=pd.merge(left=self.data_xls_occ_fe,right=data_rte,how='left',on=['tmpmergetime','Currency'])
        self.data_xls_occ_fe=self.data_xls_occ_fe.rename(columns={'rate':'rate_to_euro'}).drop('tmpmergetime',axis=1)
        
        #----- index
        self.data_xls_occ_fe=self.data_xls_occ_fe.set_index('SendingTime')


    ###########################################################################
    # METHOD GET COLNAMES
    ###########################################################################
    def get_db_colnames(self,level=None,mode='base',only=None,at_least=None):
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
            mandatory_cols=['SendingTime','p_cl_ord_id','p_occ_id']
        elif level=='occurrence':
            mandatory_cols=['SendingTime','MsgType','p_occ_id']
        elif  level=='deal':
            mandatory_cols=['TransactTime','p_exec_id','p_cl_ord_id']
        else:
            logging.error('unknown level <'+level+'>')
            raise ValueError('unknown level <'+level+'>')
        
        #-----------------------------------
        # mode cols
        #-----------------------------------
        all_cols=None
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
            if mode=='all':
                
                if all_cols is None:
                    if level=='deal':
                        out=get_field_list(cname=self.deal_collection_name, db_name=self.db_name)
                    else:
                        out=get_field_list(cname=self.algo_collection_name, db_name=self.db_name)
                else:
                    out=all_cols
                    
            elif mode=='base':
                
                if level=='sequence':
                    out = [ # - id/order infos
                        u'_id',u'p_cl_ord_id',u'p_occ_id',
                        # - user/client infos
                        u'ClientID',u'TargetSubID',u'Account', u'MsgType',u'server',
                        #- security symbol
                        u'Symbol',u'cheuvreux_secid',u'ExDestination',u'Currency',u'rate_to_euro',
                        #- info at occurence level
                        u'occ_prev_exec_qty',u'occ_prev_turnover',   
                        #- exec info
                        u'exec_qty',u'turnover',u'volume_at_would',u'nb_replace',u'nb_exec',
                        #- time infos
                        u'eff_endtime',u'duration',u'first_deal_time',
                        #- parameter info
                        u'Side',u'OrderQty',u'StrategyName',u'strategy_name_mapped',
                        u'StartTime',u'EndTime',u'ExcludeAuction',
                        u'Price',u'WouldLevel',
                        u'MinPctVolume',u'MaxPctVolume',u'AuctionPct',
                        u'AggreggatedStyle',u'WouldDark', u'MinSize',u'MaxFloor',u'BenchPrice',u'ExecutionStyle',u'OBType', u'SweepLit', u'MinQty',
                        # - others
                        u'reason']
                        
                elif level=='deal':
                    out = [ # - id/order infos
                        "_id","p_exec_id","p_cl_ord_id",
                         # - deal infos
                        "Side","Symbol","LastPx","LastShares","LastMkt","ExecType","Currency",
                        "rate_to_euro","cheuvreux_secid","strategy_name_mapped"]
                        
                elif level=='occurrence':
                    out = [ # - id/order infos
                        u'_id',u'p_occ_id',
                        # - user/client infos
                        u'ClientID',u'TargetSubID',u'Account',
                        #- security symbol
                        u'Symbol',u'cheuvreux_secid',u'ExDestination',u'Currency',u'rate_to_euro',
                        #- info at occurence level
                        u'Side']
                        
                    if all_cols is None:
                        all_cols=get_field_list(cname=self.algo_collection_name, db_name=self.db_name)           
                    # add occ_  
                    out=out+all_cols[np.nonzero([x[:min(4,len(x))]=='occ_' for x in all_cols])[0]].tolist() 
            else:
                logging.error('unknown mode <'+mode+'>')
                raise ValueError('unknown mode <'+mode+'>')                    
            
            out=list(set(out+add_cols))
            
        #-----------------------------------
        # out + check
        #-----------------------------------
        
        # check if in database
        if level=='deal':
            all_cols=get_field_list(cname=self.deal_collection_name, db_name=self.db_name)
        else:
            all_cols=get_field_list(cname=self.algo_collection_name, db_name=self.db_name)
            
        if not all([x in all_cols for x in out]):
            logging.error('asked colnames should be available in database')
            raise ValueError('asked colnames hould be available in database')
            
        return out


###########################################################################
# FUNCTIONS
########################################################################### 
def get_sequence_data_from_cl_ord_id(cl_ord_id=None,colnames=None,db_name=None,algo_collection_name=None):
    #-----------------------------------
    # CONNECTION DB  
    #----------------------------------- 
    client = Connections.getClient(db_name.upper())
     
    #-----------------------------------
    # CONSTRUCT REQUEST
    #----------------------------------- 
    # --- TEST INPUTS
    if cl_ord_id is None or colnames is None or db_name is None or algo_collection_name is None:
        logging.error('Bad inputs')
        raise ValueError('Bad inputs')
        
    # --- CONSTRUCT 
    colnames=list(set(colnames+['SendingTime','p_cl_ord_id']))
    res = client[db_name][algo_collection_name].find({"p_cl_ord_id": {"$in": cl_ord_id}},dict((k,1) for k in colnames))
    client.close()
    
    #-----------------------------------
    # CONSTRUCT DATAFRAME
    #-----------------------------------  
    # Create needed
    documents=[]
    columns=[]
    for v in res:
        documents.append(v)
        columns.extend(v.keys())
        columns=list(set(columns)) 
        
    if not documents:
        return pd.DataFrame()
    
    # Dataframe
    out=pd.DataFrame.from_records(documents, columns=columns,index='SendingTime')
    out=out.sort_index() 
    
    return out
    

def get_field_list(cname=None, db_name="Mars"):
    #-----------------------------------
    # CONNECTION DB  
    #----------------------------------- 
    map_name="field_map"
    client = Connections.getClient(db_name.upper())
    
    #-----------------------------------
    # CONSTRUCT REQUEST
    #----------------------------------- 
    req_=client[db_name][map_name].find({"collection_name":cname},{"list_columns":1,"_id":0}) 
    client.close()
    
    #-----------------------------------
    # CONSTRUCT DATAFRAME
    #-----------------------------------      
    out=[]
    for v in req_:
        out.append(v)
        
    return np.array(out[0]['list_columns']+['_id'])







if __name__=='__main__':
    a=1
#     from lib.dbtools.connections import Connections
#   
#     #-----  EXCEL VALS
#     test = AlgoDataProcessor(filter = {"p_occ_id": {"$in" : ['20130603FY71306030000015RLUIFLT01']}})
#     test.get_xls_occ_fe_data()
#     print test.data_xls_occ_fe
#
#     #-----  ENTRY OCCURENCE 
#     test = AlgoDataProcessor(filter = {"p_occ_id": {"$in" : ['20130603FY71306030000015RLUIFLT01']}})
#     test.get_db_data(level='occurrence')
#     test.get_db_data(level='sequence')
#     test.get_db_data(level='deal')
#     
#     print test.data_sequence.shape
#     print test.data_occurrence.shape
#     print test.data_deal.shape
    
#     #-----  ENTRY OCCURENCE 
#     test = AlgoDataProcessor(date=dt.datetime(2013,8,30,0,0,0))
#     test.get_db_data(level='occurrence')
#     test.get_db_data(level='sequence')
#     test.get_db_data(level='deal')
#      
#     print test.data_sequence.shape
#     print test.data_occurrence.shape
#     print test.data_deal.shape
#     
#     #-----  ENTRY SEQUENCE 
#     test = AlgoDataProcessor(start_date=dt.datetime(2013,8,30,0,0,0),end_date=dt.datetime(2013,8,31,0,0,0))
#     test.get_db_data(level='sequence')
#     test.get_db_data(level='occurrence')
#     test.get_db_data(level='deal')
#     
#     print test.data_sequence.shape
#     print test.data_occurrence.shape
#     print test.data_deal.shape
#     
#     #-----  ENTRY SEQUENCE 
#     test = AlgoDataProcessor(start_date=dt.datetime(2013,8,30,0,0,0),end_date=dt.datetime(2013,8,31,0,0,0))
#     test.get_db_data(level='deal')
#     test.get_db_data(level='sequence')
#     test.get_db_data(level='occurrence')
#     
#     print test.data_sequence.shape
#     print test.data_occurrence.shape
#     print test.data_deal.shape    
    
    #-----  ENTRY DEAL 
#     test = AlgoDataProcessor(start_date=dt.datetime(2013,8,30,12,0,0),end_date=dt.datetime(2013,8,30,16,0,0))
#     test.get_order_data(level='deal')
#     
#     print test.data_deal.shape    
    
    
#     #-----  SEQUENCE : get sequence/ occurence
#     test = AlgoDataProcessor(start_date=dt.datetime(2013,8,30,12,0,0),end_date=dt.datetime(2013,8,30,14,0,0))
#     test.get_order_data(level='sequence')
#     
#     print test.data_sequence.shape[0]
#     
#     #-----  SEQUENCE : get_deal
#     test = AlgoDataProcessor(start_date=dt.datetime(2013,8,30,12,0,0),end_date=dt.datetime(2013,8,30,14,0,0))
#     test.get_order_data(level='sequence')
#     test.get_deal_data(merge_algo_colnames=['OrderQty','Price'])
#     
#     print test.data_deal.shape  
