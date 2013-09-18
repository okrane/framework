# -*- coding: utf-8 -*-
"""
Created on Tue Sep 10 10:57:11 2013

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
from lib.dbtools.connections import Connections
from lib.tca.referentialdata import ReferentialDataProcessor
from lib.tca.marketdata import MarketDataProcessor
from lib.tca.algostats import AlgoStatsProcessor
from lib.tca.algodata import AlgoDataProcessor


class PostDbAdd(object):
    
    ###########################################################################
    # INIT
    ###########################################################################
    def __init__(self,date=None,sec_ids_list=None):
        #----------------
        # TEST
        #----------------
        if date is None or sec_ids_list is None or len(sec_ids_list)==0:
            raise ValueError('bad inputs')
        self.date=date
        self.sec_ids_list = sec_ids_list
        
        # DATA
        self.data_sequence=pd.DataFrame()
        self.data_occurrence=pd.DataFrame()
        self.data_deal=pd.DataFrame()
        
    ###########################################################################
    # METHOD GET DATA
    ###########################################################################
    def get_add_stats(self):
        # LOPP ON SEC IDS
        for sec_id in self.sec_ids_list:
            #----------------
            # GET MARKET and REFERENTIAL data
            #----------------
            mkt_data = MarketDataProcessor(security_id=sec_id,date=self.date)
            mkt_data.get_data_tick()
            mkt_data.get_data_daily()
            
            ref_data = ReferentialDataProcessor(security_id=sec_id,date=self.date)
            ref_data.get_data_exchange_info()
            
            #----------------
            # GET occurrence list for this security_id
            #----------------
            tmp = AlgoDataProcessor(date = self.date,filter = {"cheuvreux_secid": {"$in" : [sec_id]}})
            tmp.get_db_data(level = 'occurrence')
            uni_occ_id = np.unique(tmp.data_occurrence['p_occ_id'].values)
            del tmp
            
            for occ_id in uni_occ_id:
                #----------------
                # COMPUTE STATS
                #----------------
                occ_data = AlgoStatsProcessor(filter = {"p_occ_id": {"$in" : [occ_id]}})
                occ_data.compute_db_stats(market_data=mkt_data,referential_data=ref_data)
                #----------------
                # ADD to 
                #----------------
                self.data_sequence=self.data_sequence.append(occ_data.data_sequence)
                self.data_occurrence=self.data_occurrence.append(occ_data.data_occurrence)
                #self.data_deal=self.data_deal.append(occ_data.data_deal)
                
                
                
                

if __name__=='__main__':
    
    from lib.data.ui.Explorer import Explorer
    #-----  ENTRY OCCURENCE 
    test = PostDbAdd(date = dt.datetime(2013,8,30,0,0,0), sec_ids_list = [110,2,26])
    test.get_add_stats()
    print test.data_occurrence               
    print test.data_sequence  
            
            
            
            
            
            
            
        
            
    
    
    
    