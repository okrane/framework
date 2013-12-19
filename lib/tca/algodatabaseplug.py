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
from lib.logger import *
Logger(level = 5, capture_stderr  = True)

class AlgoDatabasePlug(object):
    
    ###########################################################################
    # INIT
    ###########################################################################
    def __init__(self,date = None, start_date = None, end_date = None, filter = None, env = 'dev'):
        
        #---- INPUT
        """start_date and end_date are datetime"""
        # date can be None when we remove data from a database !
        if date is not None:
            self.start_date = dt.datetime.combine(date.date(),dt.time(0,0,0))
            self.end_date = dt.datetime.combine(date.date(),dt.time(0,0,0))
        elif start_date is not None and end_date is not None:
            self.start_date = dt.datetime.combine(start_date.date(),dt.time(0,0,0))
            self.end_date = dt.datetime.combine(end_date.date(),dt.time(0,0,0))
            
        self.filter = filter
        
        #---- CONNECTION INFO
        self.database_name = 'Mars'
        self.database_server = 'MARS'
        self.sequence_collection_name = 'AlgoOrders'
        self.occurrence_collection_name = 'AlgoOrders'
        self.deal_collection_name = 'OrderDeals'
        self.field_collection_name = 'field_map'
        self.client = None
        self.env = env
        self.is_connected = False
        
        #---- Missing infos when pushing data
        self.push_params = {'sequence' : {'merge_columns' : ['_id','p_cl_ord_id']} ,
                            'occurrence' : {'merge_columns' : ['_id','p_occ_id']}}
        self.push_enrichment = {}
        self.push_missing_sec_id = [] # list of dictionary
        self.push_missing_tickdata = [] # list of dictionary
        self.push_missing_errorstats = [] # list of dictionary
        self.push_missing_errorpush = [] # list of dictionary
        self.push_missing_nopush = [] # list of dictionary
        
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
    
    def update_fieldmap(self,collection_name = None, columns = None, remove_columns = None):
        # update the field map collection that keeps the list of columns active in the database
        
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
            self.client[self.database_name][self.field_collection_name].remove({'collection_name' : collection_name})
            self.client[self.database_name][self.field_collection_name].insert({'collection_name' : collection_name, 'list_columns' : fields})         
            logging.info('update collection ' + self.field_collection_name)
            
        else:
            logging.info('no field database update')


        
    def push_algostats(self):
        
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
            id_with_sec_ids = tmp.data_occurrence['cheuvreux_secid'].apply(lambda x : x is not None and x == x)
            
            if not np.all(id_with_sec_ids):
                self.push_missing_sec_id += dftools.to_listdict(tmp.data_occurrence[~id_with_sec_ids], columns = ['p_occ_id','Symbol'])
                logging.info('p_occ_id with missing sec ids: '+','.join(np.unique(tmp.data_occurrence['p_occ_id'][~id_with_sec_ids].values).tolist()))
                
            if not np.any(id_with_sec_ids):
                date += dt.timedelta(days=1)
                del tmp
                continue
                
            sec_ids_list = np.unique(tmp.data_occurrence['cheuvreux_secid'][id_with_sec_ids].apply(lambda x : float(x)).values)
            del tmp
            
            # ---
            # LOOP ON SEC IDS
            #----
            for sec_id in sec_ids_list:
                
                logging.info('SECURITY_ID : ' + str(sec_id))
                
                #----------------
                # GET MARKET and REFERENTIAL data
                #----------------
                mkt_data = MarketDataProcessor(security_id=sec_id,date=date)
                mkt_data.get_data_tick()
                mkt_data.get_data_daily()
                
                has_tickmktdata = (mkt_data.data_tick.shape[0]>0)
                
                ref_data = ReferentialDataProcessor(security_id=sec_id,date=date)
                ref_data.get_data_exchange_info()
                
                #----------------
                # GET occurrence list for this security_id
                #----------------
                tmp = AlgoDataProcessor(date = date,filter = dict(self.filter.items() + {"cheuvreux_secid": {"$in" : [sec_id]}}.items()))
                tmp.get_db_data(level = 'occurrence')
                uni_occ_id = np.unique(tmp.data_occurrence['p_occ_id'].values).tolist()
                
                #----------------
                # ADD STATS
                #----------------
                
                if not has_tickmktdata:
                    self.push_missing_tickdata += dftools.to_listdict(tmp.data_occurrence, columns = ['p_occ_id','Symbol','cheuvreux_secid'])
                    logging.info('p_occ_id with no tick data: '+','.join(uni_occ_id))
                    del tmp
                    continue
                
                del tmp
                
                # ---
                # LOOP ON ORDERS
                #----
                for occ_id in uni_occ_id:
                    logging.info('compute stats on occurrence :' + occ_id)
                    
                    #----------------
                    # COMPUTE STATS
                    #----------------
                    occ_data = AlgoStatsProcessor(filter = {"p_occ_id": {"$in" : [occ_id]}})
                    has_compute = True
                    try: 
                        occ_data.compute_tca_stats(market_data = mkt_data , referential_data = ref_data , mode_colnames_out = 'all')
                    except:
                        #TODO : handle properly the error
                        has_compute = False
                        get_traceback()
                        logging.error('Error in compute stats for occ_id :'+ occ_id)
                        self.push_missing_errorstats += [{'p_occ_id' : occ_id}]
                        
                    #----------------
                    # PUSH data
                    #----------------
                    if has_compute:
                        for level in self.push_params.keys():
                            #-- push
                            info_push = dbmodif_fromdf(mode = 'update', 
                                                       data = getattr(occ_data,'data_' + level), 
                                                       collection = self.client[self.database_name][getattr(self,level + '_collection_name')],
                                                       add_columns = occ_data.db_stats_enrichment[level],
                                                       merge_columns = self.push_params[level]['merge_columns'])
                            
                            #-- add info : enrichment and collection names
                            if len(info_push[0])>0:
                                self.__update_enrichment(level = level, columns = info_push[0])
                                self.update_fieldmap(collection_name = getattr(self,level + '_collection_name'), columns = info_push[0])
                                
                            if len(info_push[1])>0:
                                self.push_missing_errorpush += info_push[1]
                                
                            if len(info_push[2])>0:
                                self.push_missing_nopush += info_push[2]
            
            #-- next date                                
            date+= dt.timedelta(days=1)
                                
        self.__disconnect()
        
    ###########################################################################
    # METHODS UPDATE DATABASE
    ###########################################################################  
    def remove_algostats(self , mode = None , level = None , columns = None):
        
        self.__connect()
        
        if mode is None or level is None or columns is None:
            raise ValueError('bad inputs')
            
        if mode == 'all':
            #--- removed columns from collection
            remove_columns(collection = self.client[self.database_name][getattr(self,level + '_collection_name')], 
                               columns = columns)
            
            #--- removed columns from list of colnames
            self.update_fieldmap(collection_name = getattr(self,level + '_collection_name'), remove_columns = columns)
            
        else:
            raise ValueError('Unknown mode <' + mode + '>')
            
        self.__disconnect()
            
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
                                              
def dbmodif_fromdf(mode = None,
           collection = None,
           data = None,
           add_columns = None,
           merge_columns = ['_id']):
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
        logging.info('Nop data')
        return add_columns,out_error,out_no_update
    
    #---------
    #- PUSH or ERASE
    #---------
    if mode == 'update' or mode == 'erase' :
        
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
                
            elif mode == 'update':
                dict_to_add = data.ix[idx][add_columns].to_dict()
                typeconverter(dict_to_add)
                do_update = {'$set' : dict_to_add }
                
            #-- update collection
            try:
                info_update = collection.update(dict_to_merge,
                    do_update, multi = False)
                
                if not info_update['updatedExisting']:
                    out_no_update.append(dict_to_merge)
                else:
                    logging.debug('collection has been updated ' + ' / '.join([x + ' : ' + str(dict_to_merge[x]) for x in dict_to_merge]))
                    
            except:
                logging.error('pushing ' + ' / '.join([x + ' : ' + str(dict_to_merge[x]) for x in dict_to_merge]))
                get_traceback()
                out_error.append(dict_to_merge)
                # TODO : traceback
                
    else:
        raise ValueError('Unknown mode <'+mode+'>')
    
    return add_columns,out_error,out_no_update
    
    
    
if __name__=='__main__':
    
    #-----------------------
    # - PLUG TEST IN DEV
    #----------------------
    #------ config
    t1 = AlgoDatabasePlug(date = dt.datetime(2013,5,21), 
                          filter = {'p_occ_id' : {'$in' : ['20130521FY000008193901LUIFLT01']}}
                          ,env = 'dev')  
    
    #-----  test fieldmap
    t1.update_fieldmap(collection_name = t1.sequence_collection_name, columns = [])
    
    #-----  test push_algostats
    t1.push_algostats()
    print t1.push_enrichment
    print t1.push_missing_errorpush
    
    #-----  erase what has been pushed (all database !)
    t_push = AlgoDatabasePlug()
    t_data = AlgoDataProcessor()
    for level in t1.push_enrichment.keys():
        list_in_db = t_data.get_db_colnames(level = level , mode = 'base')
        list_add = list(set(t1.push_enrichment[level]) - set(list_in_db))
        t_push.remove_algostats(mode = 'all' , level = level , columns = list_add)
    
    
    
    