# -*- coding: utf-8 -*-
"""
Created on Tue Sep 03 10:25:23 2013

@author: njoseph
"""
from pymongo import *
import pandas as pd
import datetime as dt
import pytz
import time as time
import numpy as np
# import lib.data.matlabutils as matlabutils
from lib.logger.custom_logger import *
import lib.tca.mapping as mapping
from lib.dbtools.connections import Connections
import lib.data.matlabutils as matlabutils
import lib.dbtools.read_dataset as read_dataset
import lib.dbtools.get_repository as get_repository
import lib.stats.formula as formula
import os

#-----------------------------------
# NOMENCKATURE
#-----------------------------------
# prefix 1 : 'occ_' if info is at order level / '' if it is at sequence level
# prefix 2 : 'exec_' if it is related stats of execution 
#            else 'm_' if market stats calculated with tickdata
#            prefix 2b /


#-----------------------------------
# EXPLANATION of entry level
#-----------------------------------
# AlgoDataProcessor object is coherent to the class_mode 'sequence', 'occurrence', 'deal'
# this is like the entry point of the object, meaning filters etc will be related to this object
# 1/ we want to analyse starting from deals, then we use the self. parameters
#  -> deal_mode='self'
# 2/  we want to analyse starting from orders, and then getting deals related to this order, then we use the cl_ord_id_list
#  -> deal_mode='sequence' or 'occurrence'

# purpose of this class is to read data from the base, with different modes (different columes and different filters)
# Data is stocked in class.data_xxx
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
        self.mode_colnames = mode_colnames
        self.merge_colnames_sequence2deal = None # Now only for deals when we merge data
        
        #---- CONNECTION INFO
        self.database_name = 'Mars'
        self.database_server = 'MARS'
        # this dict links the type of data with the data base name
        self.level_collection_dict = {'sequence' : 'AlgoOrders',
                                      'occurrence' : 'AlgoOrders',
                                      'deal' : 'OrderDeals',
                                      'tca' : 'AlgoTCA'}
        
        #---- DATABASE ALGO
        for level in self.level_collection_dict.keys():
            setattr(self, 'data_' + level, None)   
            
        #---- excel INFO
        if os.name == 'nt':
            self.xls_occ_fe_path = 'W:\\Global_Research\\Quant_research\\Data\\tca'
        else:
            self.xls_occ_fe_path = '/home/quant/prod/tca'
        
        self.xls_occ_fe_filename = 'Export_EOD_Flex_2013_comma.csv'
        self.xls_occ_fe_filenameg = 'export_renormalized.csv'
        self.data_xls_occ_fe=None
        
    ###########################################################################
    # METHOD GET DATA
    ###########################################################################
    def set_mode_colnames(self, mode ):
        #-----
        #- TEST
        #- can only be changed if the no data has been extracted !
        if any([getattr(self,'data_' + level) is not None for level in self.level_collection_dict.keys()]):
            raise ValueError('set_mode_colnames can only be used if no data has already been extracted')
        
        self.mode_colnames = mode
        
    ###########################################################################
    # METHOD GET DATA
    ###########################################################################
    def get_db_data(self, level=None,
                    force_colnames_only=None):
        
        t0=time.clock()
        
        #-----------------------------------
        # DETERMINE ENTRY LEVEL
        #-----------------------------------
        if self.entry_level is None:
            self.entry_level = level
            
        #-----------------------------------
        # TEST INPUT + DEFAULT OUTPUT  + PARAMS
        #-----------------------------------
        if level in self.level_collection_dict.keys():
            if getattr(self, 'data_' + level) is not None:
                logging.info('get_db_data is already loaded')
                return        
                
            setattr(self, 'data_' + level, pd.DataFrame())
            
            cname=self.level_collection_dict[level]
            
        else:
            logging.error('Undefined level <' + level +'>')
            raise ValueError('Bad inputs')            
            
        
        # extract only the colnames that are in the interception of mode_colnames and force_colnames_only
        NEEDED_COLNAMES=self.get_db_colnames(level=level,mode=self.mode_colnames,only=force_colnames_only)
          
        #-----------------------------------
        # CONNECTION DB  
        #-----------------------------------        
        client = Connections.getClient(self.database_server)
        occ_db = client[self.database_name][cname]
        
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
            
        elif level == 'occurrence':
            criterion.append({'MsgType':'D'})
            
        elif level == 'tca':
            index_colname='SendingTime_'
            
        if self.entry_level == level:
            # CASE 1 : entry level, then we use the inputs information !   
            #--- Date Filters
            if self.start_date is not None and self.end_date is not None:
                #TO DO : check timezone
                criterion.append({index_colname: {"$gte":self.start_date , "$lte":self.end_date }})
                
            #--- Filters   
            if self.filter is not None:
                
                if np.any([x not in NEEDED_COLNAMES for x in self.filter.keys()]):
                    cols = self.filter.keys()
                    raise ValueError('at least one of the filter keys <' + cols[np.nonzero([x not in NEEDED_COLNAMES for x in self.filter.keys()])[0][0]] + '> is not in the needed colnames')
                    
                criterion.append(self.filter)
                
        else:
            #--- 
            # CASE 2 : all merge is done by sequence information
            if self.entry_level == 'sequence':
                
                if self.data_sequence.shape[0]==0:
                    return
                criterion.append({'p_cl_ord_id': {"$in": np.unique(self.data_sequence['p_cl_ord_id']).tolist()}})
                
            elif self.entry_level == 'deal':
                
                if self.data_deal.shape[0]==0:
                    return
                criterion.append({'p_cl_ord_id': {"$in": np.unique(self.data_deal['p_cl_ord_id']).tolist()}})
                
            elif self.entry_level == 'occurrence' or self.entry_level == 'tca':
                
                if getattr(self,'data_' + self.entry_level).shape[0]==0:
                    return
                    
                if level == 'sequence':
                    criterion.append({'p_occ_id': {"$in": np.unique(getattr(self,'data_' + self.entry_level)['p_occ_id']).tolist()}})
                    
                elif level == 'deal':
                    if self.data_sequence is None:
                        # TO DO when p_occ_id in deal
                        raise ValueError('for now : data_sequence should be get before !')
                        
                    if self.data_sequence.shape[0]==0:
                        return
                        
                    criterion.append({'p_cl_ord_id': {"$in": np.unique(self.data_sequence['p_cl_ord_id']).tolist()}})
                    
        # --- Request        
        req = criterion[0] if (len(criterion) == 1) else {'$and' : criterion} 
        res = occ_db.find(req, NEEDED_COLNAMES)
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
        #data=pd.DataFrame.from_records(documents, columns=columns , index=index_colname)
        # bidouille pour conserver l'index sous un autre nom (au cas ou)
        data=pd.DataFrame.from_records(documents, columns=columns)
        data.set_index(index_colname , drop = False , inplace = True)
        data.rename(columns = {index_colname : index_colname +'_'} , inplace = True)
        data=data.sort_index()  
        
        NEEDED_COLNAMES=np.setdiff1d(NEEDED_COLNAMES + [index_colname + '_'],[index_colname]).tolist()
        
        # HANDLING COLNAMES : drop
        for x in data.columns.tolist():
            if x not in NEEDED_COLNAMES:
                data=data.drop([x],axis=1)
                
        # HANDLING COLNAMES : add
        for x in NEEDED_COLNAMES:
            if x not in data.columns.tolist():
                data[x] = None 
                
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
            dict_rename={'LastPx': 'price','LastShares': 'volume', 'LastMkt' :'MIC' , 'LastLiquidityInd' : 'liquidity_ind'}
            for x in dict_rename.keys():
                if x in data.columns.tolist():
                    data = data.rename(columns={x:dict_rename[x]})
                    
        #--- ORDERS : 
        if level in ['sequence','occurrence']:
            # nb_replace
            if (('nb_replace' in data.columns.tolist()) and (any(formula.isfinite(data['nb_replace'].values , notfinite = True)))):
                tmp = data['nb_replace']
                tmp[np.nonzero(formula.isfinite(data['nb_replace'].values , notfinite = True))[0]]=0
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
        setattr(self , 'data_' + level, data)
                        
        logging.debug('get_db_data  took <%3.2f> secs ' %(time.clock()-t0))
        
        
    def get_xls_occ_fe_data(self):    
        
        tzparis=pytz.timezone('Europe/Paris')
        #------ LOAD HISTO DATA ALREADY FORMATED
        if os.path.exists(os.path.join(self.xls_occ_fe_path,self.xls_occ_fe_filenameg)):
            self.data_xls_occ_fe = pd.read_csv(os.path.join(self.xls_occ_fe_path,self.xls_occ_fe_filenameg))
            self.data_xls_occ_fe['SendingTime']=map(lambda x : tzparis.localize(dt.datetime.strptime(x,'%Y-%m-%d %H:%M:%S')),self.data_xls_occ_fe['SendingTime'].values)
            self.data_xls_occ_fe['eff_endtime']=map(lambda x : tzparis.localize(dt.datetime.strptime(x,'%Y-%m-%d %H:%M:%S')),self.data_xls_occ_fe['eff_endtime'].values)
            self.data_xls_occ_fe = self.data_xls_occ_fe.set_index('SendingTime')
            return
            
        #------ LOAD HISTO DATA OCC FE DATA + save the formatted version
        if self.data_xls_occ_fe is not None:
            logging.info('get_xls_occ_fe_data is already loaded')
            
        #------ LOAD HISTO DATA OCC FE DATA
        if not os.path.isfile(os.path.join(self.xls_occ_fe_path,self.xls_occ_fe_filename)):
            raise ValueError('the occ_fe_file is missing')
            
        self.data_xls_occ_fe=pd.read_csv(os.path.join(self.xls_occ_fe_path,self.xls_occ_fe_filename),sep=',')
        
        # NORMALIZE like the db              
        # rate_to_euro, cheuvreux_secid, Side, occ_fe_strategy_name_mapped
        dict_rename={
            'EXECDATE':'EXECDATE',
            'STARTTIME':'STARTTIME',
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
        self.data_xls_occ_fe['SendingTime']=map(lambda x,y : dt.datetime.combine(dt.datetime.strptime(x,'%d.%m.%Y %H:%M').date(),dt.datetime.strptime(y,'%d.%m.%Y %H:%M').time()),
                                                self.data_xls_occ_fe['EXECDATE'].values,self.data_xls_occ_fe['STARTTIME'].values)
        self.data_xls_occ_fe['eff_endtime']=map(lambda x,y : dt.datetime.combine(dt.datetime.strptime(x,'%d.%m.%Y %H:%M').date(),dt.datetime.strptime(y,'%d.%m.%Y %H:%M').time()),
                                                self.data_xls_occ_fe['EXECDATE'].values,self.data_xls_occ_fe['eff_endtime'].values)
        self.data_xls_occ_fe = self.data_xls_occ_fe.drop(['EXECDATE','STARTTIME'],axis=1)
        self.data_xls_occ_fe['Side']=self.data_xls_occ_fe['Side'].apply(lambda x : 1 if x=='B' else -1)
        # np.unique(self.data_xls_occ_fe.ix[np.where(self.data_xls_occ_fe['Currency'] == 'EUR')[0]]['SendingTime'].values)
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
        uni_curr = np.unique(self.data_xls_occ_fe['Currency'].values).tolist()
        uni_curr = [uni_curr[x] for x in np.where([isinstance(x,basestring) for x in uni_curr])[0]]
        data_rte=read_dataset.histocurrencypair(start_date = dt.datetime.strftime(np.min(self.data_xls_occ_fe['SendingTime']),'%Y%m%d'), 
                                                end_date = dt.datetime.strftime(np.max(self.data_xls_occ_fe['eff_endtime']),'%Y%m%d'),
                                                currency = uni_curr,
                                                all_business_day = True)
        data_rte['tmpmergetime']=map(lambda x : dt.datetime.strftime(x.to_datetime(),'%Y-%m-%d'),data_rte.index)
        self.data_xls_occ_fe['tmpmergetime']=map(lambda x : dt.datetime.strftime(x,'%Y-%m-%d'),self.data_xls_occ_fe['SendingTime'])
        data_rte=data_rte.rename(columns={'ccy':'Currency'}).drop('ccyref',axis=1)
        self.data_xls_occ_fe=pd.merge(left=self.data_xls_occ_fe,right=data_rte,how='left',on=['tmpmergetime','Currency'])
        self.data_xls_occ_fe=self.data_xls_occ_fe.rename(columns={'rate':'rate_to_euro'}).drop('tmpmergetime',axis=1)
        
        #----- index
        self.data_xls_occ_fe=self.data_xls_occ_fe.set_index('SendingTime')
        self.data_xls_occ_fe.to_csv(os.path.join(self.xls_occ_fe_path,self.xls_occ_fe_filenameg))


    ###########################################################################
    # METHOD GET COLNAMES
    ###########################################################################
    def get_db_colnames(self,level = None , mode = 'base' , only = None , at_least = None , check = False):
        # get the colnames for different modes, only: return only the columns in the list + the mandatory ones; atleast: union
        # TODO : create a classe !!!
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
            mandatory_cols=['_id','SendingTime','p_cl_ord_id','p_occ_id']
        elif level=='occurrence':
            mandatory_cols=['_id','SendingTime','MsgType','p_occ_id']
        elif level=='tca':
            mandatory_cols=['_id','SendingTime_','p_occ_id']
        elif  level=='deal':
            mandatory_cols=['_id','TransactTime','p_exec_id','p_cl_ord_id']
        else:
            logging.error('unknown level <'+level+'>')
            raise ValueError('unknown level <'+level+'>')
        
        #-----------------------------------
        # mode cols
        #-----------------------------------
        all_cols = get_field_list(cname = self.level_collection_dict[level], db_server = self.database_server , db_name=self.database_name)
        
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
                    out = get_field_list(cname = self.level_collection_dict[level] , db_server = self.database_server , db_name=self.database_name)
                    
                else:
                    out=all_cols
                    
            elif mode == 'base' or  mode == 'base_fe':
                
                if level=='sequence':
                    
                    #-- comes from db
                    out = [ # - id/order infos
                        u'p_cl_ord_id',u'p_occ_id',u'ClOrdID',
                        # - user/client infos
                        u'ClientID',u'TargetSubID',u'Account', u'MsgType',u'server',u'ProgramName',u'OnBehalfOfCompID',
                        #- security symbol
                        u'Symbol',u'cheuvreux_secid',u'ExDestination',u'Currency',u'rate_to_euro',   
                        #- exec info
                        u'exec_qty',u'turnover',u'nb_replace',u'nb_exec',
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
                        # u'volume_at_would'
                    #-- TODO : add needed stats for base_stats
                    
                elif level=='deal':
                    out = [ # - id/order infos
                        "p_exec_id","p_cl_ord_id",
                         # - deal infos
                        "Side","Symbol","LastPx","LastShares","LastMkt","LastLiquidityInd","ExecType","Currency",
                        "rate_to_euro","cheuvreux_secid","strategy_name_mapped"]
                        
                elif level=='occurrence':
                    out = [ # - id/order infos
                        u'p_occ_id',u'ClOrdID',
                        # - user/client infos
                        u'ClientID',u'TargetSubID',u'Account',u'ProgramName',u'OnBehalfOfCompID',u'server',
                        #- security symbol
                        u'Symbol',u'cheuvreux_secid',u'ExDestination',u'Currency',u'rate_to_euro',
                        #- database infos on occurrence
                        #u'occ_duration',u'occ_nb_replace',
                        #- info at occurrence level
                        u'Side']
                        
                    # add occ_fe_  
                    if mode == 'base_fe':
                        all_cols = np.array(all_cols)
                        out=out + all_cols[np.nonzero([x[:min(7,len(x))]=='occ_fe_' for x in all_cols])[0]].tolist()
                        all_cols = all_cols.tolist()
                        
                    # add stats
                    # Attention, bien ajouter le 'm_d_'...  
                    
                elif level == 'tca':
                    out = []
                    # bidouille for now
                            
            else:
                logging.error('unknown mode <'+mode+'>')
                raise ValueError('unknown mode <'+mode+'>')                    
            
            out=list(set(out+add_cols))
            
        #-----------------------------------
        # out + check
        #-----------------------------------
        if check:
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
    

def get_field_list(cname=None, db_server = 'MARS' , db_name="Mars"):
    #-----------------------------------
    # CONNECTION DB  
    #----------------------------------- 
    map_name="field_map"
    client = Connections.getClient(db_server)
    
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
        
    return np.array(out[0]['list_columns']+['_id']).tolist()
    
    
    
if __name__=='__main__':
    from lib.dbtools.connections import Connections
    
#     #-----  EXCEL VALS
#     test = AlgoDataProcessor()
#     test.get_xls_occ_fe_data()
#     print test.data_xls_occ_fe
#     a=1

    #-----  ENTRY OCCURENCE 
    test = AlgoDataProcessor(filter = {"p_occ_id": {"$in" : ['20130603FY71306030000015RLUIFLT01']}})
    test.get_db_data(level='occurrence')
    test.get_db_data(level='sequence')
    test.get_db_data(level='deal')
    print test.data_occurrence
    
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
#     test.get_db_data(level='sequence',force_colnames_only=['turnover'])
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
