'''
Created on 1 juin 2010

@author: benca
'''

import simep
from simep.funcs.dbtools.securities_by_sqlite import SecuritiesBySqlite
if not simep.__standalone__:
    from simep.funcs.dbtools.securities_by_sybase import SecuritiesBySybase
else:
    from simep.funcs.dbtools.securities_by_sybase_op import SecuritiesBySybase


class SecuritiesTools:
    
    UNKNOWN_DATABASE_ERROR = "FATAL ERROR : unknown database [usual databases : sqlite, sybase]"
    
    @staticmethod
    def get_ric(database, security_id, trading_venue_id=None):
        if   database == "sybase":
            return SecuritiesBySybase.get_ric(security_id, trading_venue_id)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_ric(security_id, trading_venue_id)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR
            
    @staticmethod
    def get_ric_from_execution_venue_id(database, security_id, trading_venue_id=None):
        if   database == "sybase":
            return SecuritiesBySybase.get_ric_from_execution_venue_id(security_id, trading_venue_id)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_ric_from_execution_venue_id(security_id, trading_venue_id)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR
    
    @staticmethod
    def get_security_id(database, ric):
        if   database == "sybase":
            return SecuritiesBySybase.get_security_id(ric)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_security_id(ric)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR
    
    @staticmethod
    def get_market_code(database, security_id, trading_destination_id):
        if   database == "sybase":
            return SecuritiesBySybase.get_market_code(security_id, trading_destination_id)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_market_code(security_id, trading_destination_id)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR
    
    @staticmethod
    def get_trading_venue_id(database, trading_venue_name):
        if   database == "sybase":
            return SecuritiesBySybase.get_trading_venue_id(trading_venue_name)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_trading_venue_id(trading_venue_name)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR
    
    @staticmethod
    def get_trading_venue_name(database, trading_venue_id):
        if   database == "sybase":
            return SecuritiesBySybase.get_trading_venue_name(trading_venue_id)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_trading_venue_name(trading_venue_id)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR
    
    @staticmethod
    def get_trading_destination_id(database, trading_venue, security_id):
        if   database == "sybase":
            return SecuritiesBySybase.get_trading_destination_id(trading_venue, security_id)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_trading_destination_id(trading_venue)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR
    


    
    
    @staticmethod
    def get_main_trading_venue_id(database, security):
        if   database == "sybase":
            return SecuritiesBySybase.get_main_trading_venue_id(security)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_main_trading_venue_id(security)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR
            
    @staticmethod
    def get_trading_destination_id_from_venue_id(database,venue_id, security_id):
        if   database == "sybase":
            return SecuritiesBySybase.get_trading_destination_id_from_venue_id(venue_id, security_id)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_trading_destination_id_from_venue_id(venue_id, security_id)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR
    @staticmethod
    def get_trading_venue_names(database, security):
        if   database == "sybase":
            return SecuritiesBySybase.get_trading_venue_names(security)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_trading_venue_names(security)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR
        
    @staticmethod
    def get_trading_hours(database, security_id, trading_destination_id, date):
        if   database == "sybase":
            return SecuritiesBySybase.get_trading_hours(security_id, trading_destination_id, date)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_trading_hours(security_id, trading_destination_id, date)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR
    
    @staticmethod
    def get_trading_venue_type(database, trading_venue):
        if   database == "sybase":
            return SecuritiesBySybase.get_trading_venue_type(trading_venue)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_trading_venue_type(trading_venue)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR
    
    @staticmethod
    def get_trading_venue_reference(database, trading_venue):
        if   database == "sybase":
            return SecuritiesBySybase.get_trading_venue_reference(trading_venue)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_trading_venue_reference(trading_venue)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR
    
    @staticmethod
    def get_is_primary(database, trading_venue, security_id):
        if   database == "sybase":
            return SecuritiesBySybase.get_is_primary(trading_venue, security_id)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_is_primary(trading_venue, security_id)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR
    
    @staticmethod
    def get_reference_and_source_id_and_source_name(database, security_id, trading_venue_id):
        if   database == "sybase":
            return SecuritiesBySybase.get_reference_and_source_id_and_source_name(security_id, trading_venue_id)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_reference_and_source_id_and_source_name(security_id, trading_venue_id)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR
    
    @staticmethod
    def get_tick_sizes(database, security_id, trading_destination_id):
        if   database == "sybase":
            return SecuritiesBySybase.get_tick_sizes(security_id, trading_destination_id)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_tick_sizes(security_id, trading_destination_id)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR
    
    @staticmethod
    def get_indexes_list(database):
        if   database == "sybase":
            return SecuritiesBySybase.get_indexes_list()
        elif database == "sqlite":
            return SecuritiesBySqlite.get_indexes_list()
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR
            
    @staticmethod
    def get_index_comp(database, index_name):
        if   database == "sybase":
            return SecuritiesBySybase.get_index_comp(index_name)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_index_comp(index_name)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR
            
    @staticmethod
    def search_ric(database, security):
        if   database == "sybase":
            return SecuritiesBySybase.search_ric(security)
        elif database == "sqlite": 
            return SecuritiesBySqlite.search_ric(security)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR
            
    @staticmethod
    def get_daily_stock_info(database, sec_id, trading_destination, date_start, date_end):
        if   database == "sybase":
            return SecuritiesBySybase.get_daily_stock_info(sec_id, trading_destination, date_start, date_end)
        elif database == "sqlite": 
            return SecuritiesBySqlite.get_daily_stock_info(sec_id, trading_destination, date_start, date_end)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR
            
    @staticmethod
    def get_main_trading_destination_id(database, security):
        if   database == "sybase":
            return SecuritiesBySybase.get_main_trading_destination_id(security)
        elif database == "sqlite": 
            return SecuritiesBySqlite.get_main_trading_destination_id(security)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR

    @staticmethod
    def set_scenario(database, configuration, scenario_file, slave_file):
        if database == "sybase":
            return SecuritiesBySybase.set_scenario(configuration, scenario_file, slave_file)
        elif database == "sqlite":
            return SecuritiesBySqlite.set_scenario(configuration, scenario_file, slave_file)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR
            
    @staticmethod
    def get_scenario_id(database, configuration):
        if database == "sybase":
            return SecuritiesBySybase.get_scenario_id(configuration)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_scenario_id(configuration)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR
            
    @staticmethod
    def get_scenario_config(database, ids):
        if database == "sybase":
            return SecuritiesBySybase.get_scenario_config(ids)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_scenario_config(ids)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR

    @staticmethod
    def delete_scenario_config(database, scenario_ids):
        if database == "sybase":
            return SecuritiesBySybase.delete_scenario_config(scenario_ids)
        elif database == "sqlite":
            return SecuritiesBySqlite.delete_scenario_config(scenario_ids)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR
        
    @staticmethod
    def get_auction_info(database, security_id, trading_destination_id, date):
        if database == "sybase":
            return SecuritiesBySybase.get_auction_info(security_id, trading_destination_id, date)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_auction_info(security_id, trading_destination_id, date)
        else:
            print SecuritiesTools.UNKNOWN_DATABASE_ERROR 

