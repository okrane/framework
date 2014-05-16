# -*- coding: utf-8 -*-
"""
Created on Fri Dec 13 10:45:02 2013

@author: njoseph
"""

import pandas as pd
import datetime as dt
import time as time
import pytz
import numpy as np
# import lib.data.matlabutils as matlabutils
import lib.tca.mapping as mapping
from lib.tca.algodata import *
import lib.data.dataframe_tools as dftools
import lib.data.matlabutils as mutils
import lib.io.toolkit as toolkit
import re
import lib.dbtools.get_repository as get_repository
import lib.stats.slicer as slicer
import lib.stats.formula as formula
from lib.io.toolkit import get_traceback
from lib.dbtools.connections import Connections
from lib.tca.referentialdata import ReferentialDataProcessor
from lib.tca.marketdata import MarketDataProcessor
from lib.tca.algostats import AlgoStatsProcessor
from lib.tca.algodata import AlgoDataProcessor
from lib.tca.algoconfig import AlgoComputeStatsConfig
import copy
from lib.logger import *
from lib.io.toolkit import send_email
# Logger(level = 5, capture_stderr  = True)


class AlgoDatabasePlug(object):
    
    ###########################################################################
    # INIT
    ###########################################################################
    def __init__(self,date = None, start_date = None, end_date = None, filter = None, env = 'dev',
                 push_compute_stats_mode ='db_one_occurrence' ,
                 push_params = {'sequence' : {'from_level' : 'sequence','to_level' : 'sequence','merge_columns' : ['_id','p_cl_ord_id']}},
                 send_mail_config = {'_to' : ['njoseph@keplercheuvreux.com'],'_from' : ['njoseph@keplercheuvreux.com']}):
        
        #---- INPUT
        """start_date and end_date are datetime"""
        # date can be None when we remove data from a database !
        if date is not None:
            self.start_date = dt.datetime.combine(date.date(),dt.time(0,0,0))
            self.end_date = dt.datetime.combine(date.date(),dt.time(0,0,0))
        elif start_date is not None and end_date is not None:
            self.start_date = dt.datetime.combine(start_date.date(),dt.time(0,0,0))
            self.end_date = dt.datetime.combine(end_date.date(),dt.time(0,0,0))
            
        self.filter = filter # filter is here only for occurrence level !
        
        #---- CONNECTION INFO
        self.database_name = 'Mars'
        self.database_server = 'MARS'
        self.level_collection_dict = {'sequence' : 'AlgoOrders',
                                      'occurrence' : 'AlgoOrders',
                                      'deal' : 'OrderDeals',
                                      'tca' : 'AlgoTCA'}
        
        self.field_collection_name = 'field_map'
        self.client = None
        self.env = env
        self.is_connected = False
        
        #---- Missing infos when pushing data
        self.push_compute_stats_mode = push_compute_stats_mode
        self.push_params = push_params
        self.push_enrichment = {}
        self.push_missing_sec_id = [] # list of dictionary
        self.push_missing_tickdata = [] # list of dictionary
        self.push_missing_refdata = [] # list of dictionary
        self.push_missing_errorstats = [] # list of dictionary
        self.push_missing_errorpush = [] # list of dictionary
        self.push_missing_nopush = [] # list of dictionary
        self.push_nb_occurrence = 0
        
        #---- Missing infos when pushing data
        self.send_mail_config = send_mail_config
        
                
    ###########################################################################
    # METHODS NEEDED
    ###########################################################################
    def __update_enrichment(self, level = None, columns = None):
        #-----------------------------------
        # TESTS
        #-----------------------------------
        if level is None or columns is None or not isinstance(columns,list):
            raise ValueError('bad inputs')
            
        #-----------------------------------
        # DO
        #-----------------------------------        
        if level not in self.push_enrichment.keys():
            self.push_enrichment.update({level : columns})
        else:
            self.push_enrichment.update({level : list(set(self.push_enrichment[level]+columns))})
            
    def __connect(self):
        if not self.is_connected:
            Connections.change_connections(self.env)
            self.client = Connections.getClient(self.database_server)
            self.is_connected = True
            
    def __disconnect(self):
        if self.is_connected:
            self.client.close()
            self.is_connected = False
            
    ###########################################################################
    # METHODS UPDATE DATABASE
    ###########################################################################
    
    def update_fieldmap(self,collection_name = None, columns = None, remove_columns = None , drop_collection = False):
        # update the field map collection that keeps the list of columns active in the database
        # corresponded table in mongodb:  self.field_collection_name
        
        self.__connect()
        
        if collection_name is None or (columns is None and remove_columns is None):
            raise ValueError('bad inputs')
            
        # -- do you have to create the fieldmap collection ?
        if self.field_collection_name not in self.client[self.database_name].collection_names():
            self.client[self.database_name][self.field_collection_name].insert({'collection_name' : collection_name, 'list_columns' : columns})
            logging.info(self.field_collection_name +'  collection created')
            return
        
        # -- do you have to create the collection_name ?
        l = list(self.client[self.database_name][self.field_collection_name].find({'collection_name':collection_name}))
        if len(l) == 0:
            self.client[self.database_name][self.field_collection_name].insert({'collection_name' : collection_name, 'list_columns' : columns}) 
            logging.info(collection_name + ' collection name added')
            return
        
        # -- get actual colnames columns list
        fields = []
        for el in l:
            if el['collection_name'] == collection_name:
                fields = el['list_columns']
                    
        # -- add new colnames to 
        to_add = False
        if columns is not None:
            for col in columns:
                if col not in fields:
                    fields.append(col)
                    to_add = True
                
        # -- add new colnames to 
        to_remove = False
        if remove_columns is not None:
            for col in remove_columns:
                if col in fields:
                    fields.remove(col)
                    to_remove = True
                    
        # -- update database
        if to_add or to_remove:
            self.client[self.database_name][self.field_collection_name].update({'collection_name' : collection_name},{'$set': {'list_columns' : []}})
            self.client[self.database_name][self.field_collection_name].update({'collection_name' : collection_name},{'$set': {'list_columns' : fields}})      
            logging.info('update collection ' + self.field_collection_name)
            
            if len(fields) == 0 and drop_collection:
                self.client[self.database_name].drop_collection(collection_name)
                logging.warning('collection <' + collection_name + '> has been dropped as it does not contain any columns')
                
        else:
            logging.debug('no field database update')


        
    def push_algostats(self):
        
        NONE_SECID = -999.0
        
        self.__connect()
        #---
        # stats are computed by day / by security_id / by occurrence
        # stats are added by sequence
        
        date = self.start_date
        
        # ---
        # LOOP ON DATES
        #----
        while date <= self.end_date:
            
            logging.info('DATE: '+dt.datetime.strftime(date,'%Y%m%d'))
            
            # --- 
            # check number of data and security ids
            tmp = AlgoDataProcessor(date = date,filter = self.filter)
            tmp.get_db_data(level = 'occurrence')
            if tmp.data_occurrence.shape[0]==0:
                date+= dt.timedelta(days=1)
                logging.info('No data to push')
                del tmp
                continue
            
            # ---
            # sec_ids infos
            
            # STEP 1 : check if we have the sec_id
            id_with_sec_ids = tmp.data_occurrence['cheuvreux_secid'].apply(lambda x : x is not None and x == x)
            occ_id_whithout_sec_id = []
            
            # STEP 2 : check if we can find the secid and then push it into the database
            if not np.all(id_with_sec_ids):
                
                occ_id_whithout_sec_id = tmp.data_occurrence[~id_with_sec_ids]['p_occ_id'].values.tolist()
                _occ_id_whithout_sec_id = copy.deepcopy(occ_id_whithout_sec_id)
                
                for occ_id in _occ_id_whithout_sec_id:
                    symbol = tmp.data_occurrence[tmp.data_occurrence['p_occ_id'] == occ_id]['Symbol'].values[0]
                    #  test if get cheuvreux
                    try:
                        ch_sec_id = get_repository.get_symbol6_from_ticker(symbol)
                        if ch_sec_id > 0:
                            ch_sec_id = float(ch_sec_id)
                            tmp.data_occurrence.cheuvreux_secid[tmp.data_occurrence['p_occ_id'] == occ_id] = ch_sec_id
                            
                    except:
                        ch_sec_id = -1
                    
                    # push into db
                    if ch_sec_id > 0:
                        occ_id_whithout_sec_id.remove(occ_id)
                        info_push = dbmodif_fromdf(mode = 'update',
                                       data = tmp.data_occurrence[tmp.data_occurrence['p_occ_id'] == occ_id], 
                                       collection = self.client[self.database_name][self.level_collection_dict['occurrence']],
                                       add_columns = ['cheuvreux_secid'],
                                       merge_columns = ['p_occ_id'],
                                       multi_update = True)
                        
                        if len(info_push[0])>0:
                            logging.info('this occ_id/symbol/cheuvreux_secid has been changed in the database: '+ occ_id + '/' + symbol + '/' + str(ch_sec_id))
                
                #--- reload
                tmp = AlgoDataProcessor(date = date,filter = self.filter)
                tmp.get_db_data(level = 'occurrence')
                
                id_with_sec_ids = tmp.data_occurrence['cheuvreux_secid'].apply(lambda x : x is not None and x == x)
                occ_id_whithout_sec_id = tmp.data_occurrence[~id_with_sec_ids]['p_occ_id'].values.tolist()
                
                if not np.all(id_with_sec_ids):
                    self.push_missing_sec_id += dftools.to_listdict(tmp.data_occurrence[~id_with_sec_ids], columns = ['p_occ_id','Symbol'])
                    logging.info('p_occ_id with missing sec ids: '+','.join(np.unique(tmp.data_occurrence['p_occ_id'][~id_with_sec_ids].values).tolist()))
                
                  
            sec_ids_list = np.unique(tmp.data_occurrence['cheuvreux_secid'][id_with_sec_ids].apply(lambda x : float(x)).values).tolist()
            if len(occ_id_whithout_sec_id) > 0:
                sec_ids_list = [NONE_SECID] + sec_ids_list
            del tmp
            
            # ---
            # LOOP ON SEC IDS
            #----
            for sec_id in sec_ids_list:
                
                logging.info('SECURITY_ID : ' + str(sec_id))
                
                if sec_id == NONE_SECID:
                    has_tickmktdata = False
                    has_refdata = False
                    uni_occ_id = occ_id_whithout_sec_id
                    
                else:
                    #----------------
                    # GET MARKET and REFERENTIAL data
                    #----------------
                    mkt_data = MarketDataProcessor(security_id=sec_id,date=date)
                    mkt_data.get_data_tick()
                    mkt_data.get_data_daily()
                    
                    has_tickmktdata = (mkt_data.data_tick.shape[0]>0)
                    
                    ref_data = ReferentialDataProcessor(security_id=sec_id,date=date)
                    ref_data.get_data_exchange_info()
                    ref_data.get_data_security_info(ch_security_id = 'cheuvreux_secid')
                    
                    has_refdata = (ref_data.data_exchange_info.shape[0] > 0 and ref_data.data_security_info.shape[0] > 0)
                    
                    #----------------
                    # GET occurrence list for this security_id
                    #----------------
                    if self.filter is not None:
                        filter_ = dict(self.filter.items() + {"cheuvreux_secid": {"$in" : [sec_id]}}.items())
                    else:
                        filter_ = {"cheuvreux_secid": {"$in" : [sec_id]}}
                        
                    tmp = AlgoDataProcessor(date = date,filter = filter_)
                    tmp.get_db_data(level = 'occurrence')
                    uni_occ_id = np.unique(tmp.data_occurrence['p_occ_id'].values).tolist()
                    del tmp
                
                # ---
                # LOOP ON ORDERS
                #----
                for occ_id in uni_occ_id:
                    logging.info('compute stats on occurrence :' + occ_id)
                    t0 = time.clock()
                    
                    occ_data = AlgoStatsProcessor(filter = {"p_occ_id": {"$in" : [occ_id]}} , mode_colnames = 'base_fe')
                    
                    #----------------
                    # COMPUTE STATS
                    #----------------
                    has_compute = True
                    #-- if no market data : we do not compute anything
                    if sec_id == NONE_SECID:
                        #-- only load occurrence info
                        occ_data.get_db_data(level = 'occurrence')
                        occ_data.get_db_data(level = 'sequence')
                        occ_data.get_db_data(level = 'deal')                        
                        
                    elif not has_tickmktdata:
                        #-- only load occurrence info
                        occ_data.get_db_data(level = 'occurrence')
                        occ_data.get_db_data(level = 'sequence')
                        occ_data.get_db_data(level = 'deal')
                        self.push_missing_tickdata += dftools.to_listdict(occ_data.data_occurrence, columns = ['p_occ_id','Symbol','cheuvreux_secid'])
                        logging.info('p_occ_id with no tick data : ' + str(occ_id))
                        
                    elif not has_refdata:
                        #-- only load occurrence info
                        occ_data.get_db_data(level = 'occurrence')
                        occ_data.get_db_data(level = 'sequence')
                        occ_data.get_db_data(level = 'deal')
                        self.push_missing_refdata += dftools.to_listdict(occ_data.data_occurrence, columns = ['p_occ_id','Symbol','cheuvreux_secid'])
                        logging.info('p_occ_id with no ref data : ' + str(occ_id))
                        
                    else:
                        try: 
                            occ_data.compute_stats(config_mode = self.push_compute_stats_mode , market_data = mkt_data , referential_data = ref_data)
                        except:
                            #TODO : handle properly the error
                            has_compute = False
                            get_traceback()
                            logging.error('Error in compute stats for occ_id :'+ occ_id)
                            self.push_missing_errorstats += [{'p_occ_id' : occ_id}]
                            
                    logging.info('load and compute  took <%3.2f> secs ' %(time.clock()-t0))
                    t0 = time.clock()
                    
                    #----------------
                    # PUSH data
                    #----------------
                    if has_compute and len(self.push_params) == 0:
                        logging.info('stats computed but not pushed')
                        
                    elif has_compute and len(self.push_params) > 0:
                        for push_ in self.push_params.keys():
                            
                            add_columns = []
                            #-- add columns
                            if 'all_columns' in self.push_params[push_].keys():
                                # merge add_columns with the one actually in the dataset
                                add_columns = np.intersect1d(self.push_params[push_]['all_columns'] , getattr(occ_data,'data_' + self.push_params[push_]['from_level']).columns.tolist()).tolist()           
                                                 
                            elif 'add_columns' in self.push_params[push_].keys():
                                # merge add_columns with the computed cols
                                if has_tickmktdata and has_refdata:
                                    add_columns = np.intersect1d(self.push_params[push_]['add_columns'] , occ_data.db_stats_enrichment[self.push_params[push_]['from_level']]).tolist()
                                
                            else:
                                if has_tickmktdata and has_refdata:
                                    add_columns = occ_data.db_stats_enrichment[self.push_params[push_]['from_level']]
                            
                            #-- push
                            if len(add_columns) > 0:
                                self.push_nb_occurrence += 1
                                # push the python dataframe to the mongodb
                                info_push = dbmodif_fromdf(mode = self.push_params[push_]['update_mode'], 
                                                           data = getattr(occ_data,'data_' + self.push_params[push_]['from_level']), 
                                                           collection = self.client[self.database_name][self.level_collection_dict[self.push_params[push_]['to_level']]],
                                                           add_columns = add_columns,
                                                           merge_columns = self.push_params[push_]['merge_columns'])
                                
                                #-- add info : enrichment and collection names
                                if len(info_push[0])>0:
                                    self.__update_enrichment(level = self.push_params[push_]['to_level'], columns = info_push[0])
                                    self.update_fieldmap(collection_name = self.level_collection_dict[self.push_params[push_]['to_level']], columns = info_push[0])
                                    
                                if len(info_push[1])>0:
                                    self.push_missing_errorpush += info_push[1]
                                    
                                if len(info_push[2])>0:
                                    self.push_missing_nopush += info_push[2]
                                    
                    logging.info('push took <%3.2f> secs ' %(time.clock()-t0))
                    
            #-- next date                                
            date += dt.timedelta(days=1)
                                
        self.__disconnect()
        
    ###########################################################################
    # METHODS UPDATE DATABASE
    ###########################################################################  
    def remove_algostats(self , mode = None , level = None , columns = None , drop_collection = False):
        
        self.__connect()
        
        if mode is None or level is None or columns is None:
            raise ValueError('bad inputs')
            
        if mode == 'all':
            #--- removed columns from collection
            remove_columns(collection = self.client[self.database_name][self.level_collection_dict[level]], 
                               columns = columns)
            
            #--- removed columns from list of colnames
            self.update_fieldmap(collection_name = self.level_collection_dict[level], remove_columns = columns , drop_collection = drop_collection)
            
        else:
            raise ValueError('Unknown mode <' + mode + '>')
            
        self.__disconnect()

        
    def send_email(self):
        #--------------
        # --- SUMMARY (message)
        
        html_message = ''
        html_message += '<h1> Add databse Stats FROM : ' + dt.datetime.strftime(self.start_date,'%Y-%m-%d') + ' TO : '+ dt.datetime.strftime(self.end_date,'%Y-%m-%d') +  '</h1>'
        html_message += 'Number of pushed occurrence : ' + str(self.push_nb_occurrence)
        
        html_message += '<h1> Missing SYMBOL6 </h1>'
        #
        if len(self.push_missing_sec_id) == 0:
            html_message += '<br/> NONE <br/>'
        else:
            html_message += pd.DataFrame(self.push_missing_sec_id).to_html()
            html_message += '<br/>'
        
        html_message += '<h1> Missing TICK DATA </h1>' 
        #
        if len(self.push_missing_tickdata) == 0:
            html_message += '<br/> NONE <br/>'
        else:
            html_message += pd.DataFrame(self.push_missing_tickdata).to_html()
            html_message += '<br/>'
            
        html_message += '<h1> Missing REFENTIAL DATA </h1>' 
        #
        if len(self.push_missing_refdata) == 0:
            html_message += '<br/> NONE <br/>'
        else:
            html_message += pd.DataFrame(self.push_missing_refdata).to_html()
            html_message += '<br/>'  
                 
        html_message += '<h1> ERROR IN STATS COMPUTING </h1>' 
        #
        if len(self.push_missing_errorstats) == 0:
            html_message += '<br/> NONE <br/>'
        else:
            html_message += pd.DataFrame(self.push_missing_errorstats).to_html()
            html_message += '<br/>'           
            
        html_message += '<h1> ERROR IN DATABSE PUSH </h1>'          
        #
        if len(self.push_missing_errorpush) == 0:
            html_message += '<br/> NONE <br/>'
        else:
            html_message += pd.DataFrame(self.push_missing_errorpush).to_html()
            html_message += '<br/>'   
                    
        html_message += '<h1> NO INFO PUSHED </h1>'          
        #
        if len(self.push_missing_nopush) == 0:
            html_message += '<br/> NONE <br/>'
        else:
            html_message += pd.DataFrame(self.push_missing_nopush).to_html()
            html_message += '<br/>'
            
        #--------------
        # --- SEND EMAIL
        send_email(_to = self.send_mail_config['_to'], 
               _from = self.send_mail_config['_from'],
               _subject = "[AlgoTCA] Add Database Stats",
               _message = html_message)
        
        
###########################################################################
# STATIC FUNCTIONS On MONGODB and dataframe or else
###########################################################################   
def remove_columns(collection = None, columns = None):
    if collection is None or columns is None:
        logging.error('bad inputs')
        raise ValueError('bad inputs')
        
    for col in columns:
        collection.update({col: { '$exists': 1 } },{'$unset': { col : 1  } }, multi = True)
        logging.info('columns ' + col + ' has been removed')

def typeconverter(dict_):
    for x in dict_.keys():
        if isinstance(dict_[x], np.generic):
            dict_[x] = np.asscalar(dict_[x])

# this function push a python dataframe to the mongo db                                              
def dbmodif_fromdf(mode = None,
           collection = None,
           data = None,
           add_columns = None,
           merge_columns = ['_id'],
           multi_update = False):
    # multi_update: in case several rows are found the the dict key, if multi_update = 1, the new columns will be added to all these rows 
    # elsewise, the new columns will be only added into the first row.
    #---------
    #- default
    #---------               
    out_error = []
    out_no_update = []           
    
    #---------
    #- Input TEST
    #---------
    if not isinstance(data,pd.DataFrame) or collection is None:
        logging.error('bad inputs')
        raise ValueError('bad inputs')
    
    nb_data = data.shape[0]
    
    if nb_data == 0:
        logging.info('No data')
        return add_columns,out_error,out_no_update
    
    if len(add_columns) == 0:
        logging.info('No columns to add')
        return add_columns,out_error,out_no_update    
    
    #---------
    #- PUSH or ERASE
    #---------
    if mode == 'update' or mode == 'erase' or mode == 'update_or_add' :
        
        #-- input test
        if any([x not in data.columns.tolist() for x in merge_columns]):
            logging.error('all merge_columns has to be ok')
            raise ValueError('all merge_columns has to be ok')           
            
        if add_columns is not None and any([x not in data.columns.tolist() for x in add_columns]):
            logging.error('all add_columns has to be in the data')
            raise ValueError('ll add_columns has to be in the data')
            
        #-- renorm
        if add_columns is None:
            add_columns = list(set(data.columns.tolist()) - set(merge_columns))
            
        #-- push by rows by rows
        for idx in range(0,nb_data):
            
            #-- get data
            # dict_to_merge = data.ix[idx][merge_columns].to_dict()
            dict_to_merge = data.ix[idx][merge_columns].to_dict()
            typeconverter(dict_to_merge)
            
            if mode == 'erase':
                do_update = { '$unset': dict((k,1) for k in add_columns)}
                
            elif mode == 'update' or mode == 'update_or_add':
                dict_to_add = data.ix[idx][add_columns].to_dict()
                typeconverter(dict_to_add)
                do_update = {'$set' : dict_to_add }
                
            #-- update collection
            try:
                
                info_update = collection.update(dict_to_merge,
                    do_update, multi = multi_update)
                
                # special case for update_or_add
                # info_update['updatedExisting'] tells if the id to update exists already in the database or not. 1: exist, 0: non-exist.
                if mode == 'update_or_add' and not info_update['updatedExisting']:
                    tmp_dict = copy.deepcopy(dict_to_merge)
                    tmp_dict.update(dict_to_add)
                    collection.insert(tmp_dict)
                    
                if mode != 'update_or_add' and not info_update['updatedExisting']:
                    # bug added to the output.
                    out_no_update.append(dict_to_merge)
                elif mode == 'update_or_add' and not info_update['updatedExisting']:
                    logging.debug('collection has been inserted with ' + ' / '.join([x + ' : ' + str(dict_to_merge[x]) for x in dict_to_merge]))
                else:
                    logging.debug('collection has been updated with' + ' / '.join([x + ' : ' + str(dict_to_merge[x]) for x in dict_to_merge]))
                    
            except:
                logging.error('pushing ' + ' / '.join([x + ' : ' + str(dict_to_merge[x]) for x in dict_to_merge]))
                get_traceback()
                out_error.append(dict_to_merge)
                # TODO : traceback
                
    else:
        raise ValueError('Unknown mode <'+mode+'>')
    # add_columns: the list of columns pushed
    # out_error: list of dicts which are not successfully pushed into the db
    # out_no_update:  list of dicts which are not successfully updated
    return add_columns,out_error,out_no_update
    
    
    
if __name__=='__main__':
    #-----------------------
    # - TEST COMPUTE STATS WHITOUT PUSH
    #----------------------       
    # exemple where Front End is wrong on volume !
    # 20140116FY2000052309901WATFLT01
#     #-----------------------
#     # - TEST COMPUTE STATS WHITOUT PUSH
#     #----------------------    
#     #------ config
#     date_ = dt.datetime(2013,5,21)
#     filter_ = {'p_occ_id' : {'$in' : ['20130521FYIT1861-1640-20130521LUIFLT01']}}
#     #filter_ = {'p_occ_id' : {'$in' : ['20130521FY000008283001WATFLT01']}}
#     #filter_ = {'p_occ_id' : {'$in' : ['20130521FY000008259201WATFLT01']}}
#     #filter_ = None
#      
#     t1 = AlgoDatabasePlug(date = date_, 
#                           filter = filter_,
#                           push_compute_stats_mode = 'db_one_occurrence',
#                           env = 'dev',
#                           push_params = {})
#       
#     #-----  test push_algostats
#     t1.push_algostats()
#     print t1.push_enrichment
#     print t1.push_missing_errorpush    
    
    
    #-----------------------
    # - PLUG TEST IN DEV for one occurrence
    #----------------------
    #------ config
    # 
    #FILTER = None
    ENV = 'dev'
    COMPUTE_STATS_MODE = 'db_one_occurrence'
    
    SDATE = dt.datetime(2013,5,21)
    EDATE = dt.datetime(2013,5,21)
    FILTER = {'p_occ_id' : {'$in' : ['20130521FYBKyAA0737-20130521LUIFLT01']}}
    #FILTER = {'cheuvreux_secid' : {'$in' : [16832]}}
    #FILTER = None
    
    PUSH_PARAMS = {'sequence' : {'update_mode' : 'update',
                                 'from_level' : 'sequence',
                                 'to_level' : 'sequence',
                                 'merge_columns' : ['_id','p_cl_ord_id']},
                   'deal' :{'update_mode' : 'update_or_add',
                            'from_level' : 'deal',
                            'to_level' : 'deal',
                            'merge_columns' : ['_id','p_exec_id'],
                            'all_columns' : ['tphase' , 'exchange_id' , 'dark']},                            
                   'tca' : {'update_mode' : 'update_or_add',
                            'from_level' : 'occurrence',
                            'to_level' : 'tca',
                            'merge_columns' : ['p_occ_id'],
                            'all_columns' : [#-- DB : order info
                                             'job_id','p_occ_id','ClOrdID','server',
                                             'Account','ProgramName','OnBehalfOfCompID','TargetSubID',
                                             'ExDestination','Symbol','rate_to_euro', 'Currency','cheuvreux_secid', 'Side', 'SendingTime_',
                                             #-- DB : occ_fe
                                             'occ_fe_assign', 'occ_fe_average_trade_size', 'occ_fe_avg_price', 'occ_fe_avg_sprd', 'occ_fe_buy_exec', 'occ_fe_curncy', 'occ_fe_endtime', 
                                             'occ_fe_exec_shares', 'occ_fe_final_price', 'occ_fe_inmkt_turnover', 'occ_fe_inmkt_volume', 'occ_fe_limit_price', 'occ_fe_manual_input', 'occ_fe_oid', 'occ_fe_oms_ref', 
                                             'occ_fe_order_perc', 'occ_fe_period_high', 'occ_fe_period_low', 'occ_fe_port', 'occ_fe_prv_b_exec', 'occ_fe_prv_s_exec', 'occ_fe_prv_turnover', 'occ_fe_prv_volume', 'occ_fe_prv_wexec',
                                             'occ_fe_replace_mode', 'occ_fe_sell_exec', 'occ_fe_side', 'occ_fe_starttime', 'occ_fe_strat', 'occ_fe_sym', 'occ_fe_tag_115', 'occ_fe_tick_size', 'occ_fe_timing_opt', 'occ_fe_usual_closing_volume',
                                             'occ_fe_usual_continuous_daily_volume', 'occ_fe_usual_continuous_nb_deals', 'occ_fe_usual_daily_amount', 'occ_fe_usual_daily_spread', 'occ_fe_usual_daily_volatility', 'occ_fe_usual_daily_volume',
                                             'occ_fe_usual_opening_volume', 'occ_fe_volume_q1', 'occ_fe_volume_q2', 'occ_fe_volume_q3', 'occ_fe_volume_q4', 'occ_fe_wod', 'occ_fe_would_exec', 'occ_fe_would_level',
                                             #-- Referantial_info 
                                             'code_bloomberg','ISIN',
                                             #-- EXEC computed  (1)
                                             'occ_last_strategy_name_mapped','occ_strategy_name_mapped','occ_bench_type','occ_order_qty','occ_nb_replace','occ_exec_qty','occ_exec_qty_dark','occ_exec_qty_would','occ_exec_qty_main',
                                             'occ_exec_qty_opening','occ_exec_qty_closing','occ_exec_qty_manual_fill','occ_exec_nb_trades',
                                             'occ_exec_turnover','occ_exec_first_deal_price','occ_exec_last_deal_price','occ_exec_first_deal_time','occ_exec_last_deal_time',
                                             #-- MARKET :  stats on tick data
                                             'occ_m_pa_pvwap10_lit','occ_m_pa_pvwap20_lit','occ_m_pa_pvwap30_lit',
                                             #-- MARKET : stats from sequence
                                             'occ_bench_starttime','occ_bench_endtime',
                                             'occ_m_p_volume_lit','occ_m_p_volume_lit_constr','occ_m_p_volume_lit_main','occ_m_p_volume_lit_main_constr',
                                             'occ_m_p_volume_dark','occ_m_p_volume_dark_constr','occ_m_p_volume_closing','occ_m_p_volume_opening',
                                             'occ_m_p_volume_auction','occ_m_p_turnover_lit','occ_m_p_turnover_lit_constr','occ_m_p_turnover_lit_main',
                                             'occ_m_p_turnover_lit_main_constr','occ_m_p_turnover_dark','occ_m_p_turnover_dark_constr','occ_m_p_nb_trades_lit_cont',
                                             'occ_m_p_vwas_lit','occ_m_p_vwas_lit_main','occ_m_p_vwap_lit','occ_m_p_vwap_lit_constr',
                                             'occ_m_p_vol_gk_lit','occ_m_b_arrival_price_lit','occ_m_b_arrival_price_lit_main','occ_m_p_open_lit','occ_m_p_high_lit','occ_m_p_low_lit','occ_m_p_close_lit','occ_m_p_close_lit_cont',
                                             #-- MARKET (daily) 
                                             'occ_m_d_volume_lit','occ_m_d_volume_lit_main','occ_m_d_volume_auction_main','occ_m_d_vol_gk_bp','occ_m_d_nb_trades_lit_cont',
                                             'occ_m_d_open_price','occ_m_d_close_price','occ_m_d_spread_bp',
                                             #-- EXEC computed  (2)
                                             'occ_exec_vwap','occ_bench_duration_min',
                                             #-- OCC FE : COMPUTED
                                             'occ_fe_turnover','occ_fe_volume','occ_fe_vwap','occ_fe_arrival_price',
                                             'occ_fe_slippage_vwap_constr_bp','occ_fe_slippage_is_bp',
                                             #-- PERF : COMPUTED
                                             'occ_slippage_vwap_bp','occ_slippage_vwap_constr_bp','occ_slippage_is_bp',
                                             #-- EXEC computed (3)
                                             'occ_spread_bp' , 'occ_plr_lit','occ_dlr_lit',
                                             #-- PERF COMPOUNDED: COMPUTED
                                             'occ_compperf_vwap_bp','occ_compperf_vwap_constr_bp','occ_compperf_is_bp',
                                             ]}}
    Connections.change_connections(ENV)
    
    t1 = AlgoDatabasePlug(start_date = SDATE, 
                          end_date = EDATE , 
                          filter = FILTER,
                          push_compute_stats_mode = COMPUTE_STATS_MODE,
                          env = ENV , 
                          push_params = PUSH_PARAMS)
    
    #-----  test push_algostats
    t1.push_algostats()
    #t1.send_email()
    print t1.push_enrichment
    print t1.push_missing_errorpush
    
#     #-----  erase what has been pushed (all database !)
#     t_push = AlgoDatabasePlug()
#     t_data = AlgoDataProcessor()
#     for level in t1.push_enrichment.keys():
#         if level == 'tca':
#             list_add = t1.push_enrichment[level]
#         else:
#             list_in_db = t_data.get_db_colnames(level = level, mode = 'base_fe')
#             list_add = list(set(t1.push_enrichment[level]) - set(list_in_db))
#              
#         t_push.remove_algostats(mode = 'all' , level = level , columns = list_add , drop_collection = True)

#     #-----  test push_algostats
#     testtca = AlgoStatsProcessor(date = DATE , mode_colnames = 'all') 
#     testtca.get_db_data(level = 'tca')
#     print testtca.data_tca
    
    
    
    