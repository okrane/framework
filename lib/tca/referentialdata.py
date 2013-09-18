# -*- coding: utf-8 -*-
"""
Created on Mon Sep 09 11:17:50 2013

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
import lib.dbtools.get_repository as get_repository


class ReferentialDataProcessor(object):
    
    ###########################################################################
    # INIT
    ###########################################################################
    def __init__(self , date = None, security_id = None):
        
        # MANDATORY INPUT
        if date is None or security_id is None:
            logging.error('bad mandatory colnames')
            raise ValueError('bad mandatory colnames')
            
        # INPUT
        """date"""
        self.date=date
        self.date_str=dt.datetime.strftime(date,'%d/%m/%Y')
        self.security_id=security_id
        
        # DATA
        self.data_exchange_info=None
        
    ###########################################################################
    # METHOD GET DATA
    ###########################################################################
    def get_data_exchange_info(self):
        
        if self.data_exchange_info is not None:
            logging.info('get_data_exchange_info is already loaded')
            return
            
        #----
        self.data_exchange_info=get_repository.exchangeinfo(security_id=self.security_id,date=self.date_str)
        
        #----add trading hours
        td_add=get_repository.tradingtime(date=self.date_str,data_exchange_info=self.data_exchange_info)
        
        if td_add.shape[0]>0:
            self.data_exchange_info=pd.merge(right=td_add,left=self.data_exchange_info,how='left',on=['exchange_id','quotation_group'])

if __name__=='__main__':
    
    from lib.data.ui.Explorer import Explorer
    #-----  ENTRY OCCURENCE 
    test = ReferentialDataProcessor(date = dt.datetime(2013,8,30,0,0,0), security_id = 110)
    test.get_data_exchange_info()
    print test.data_exchange_info

    