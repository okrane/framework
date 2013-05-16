'''
Created on 1 juin 2010

@author: benca
'''

from cheuvreux.dbtools.connections import Connections
import sqlite3



class SecuritiesTools:

    @staticmethod
    def get_ric(database, security_id, trading_destination_id):
        if   database == "sybase":
            return SecuritiesBySybase.get_ric(security_id, trading_destination_id)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_ric(security_id, trading_destination_id)
        else:
            print "FATAL ERROR : unknown database [usual databases : sqlite, sybase]"

    @staticmethod
    def get_security_id(database, ric):
        if   database == "sybase":
            return SecuritiesBySybase.get_security_id(ric)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_security_id(ric)
        else:
            print "FATAL ERROR : unknown database [usual databases : sqlite, sybase]"

    @staticmethod
    def get_trading_destination_id(database, trading_destination_name):
        if   database == "sybase":
            return SecuritiesBySybase.get_trading_destination_id(trading_destination_name)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_trading_destination_id(trading_destination_name)
        else:
            print "FATAL ERROR : unknown database [usual databases : sqlite, sybase]"

    @staticmethod
    def get_trading_destination_name(database, trading_destination_id):
        if   database == "sybase":
            return SecuritiesBySybase.get_trading_destination_name(trading_destination_id)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_trading_destination_name(trading_destination_id)
        else:
            print "FATAL ERROR : unknown database [usual databases : sqlite, sybase]"

    @staticmethod
    def get_main_trading_destination_id(database, security):
        if   database == "sybase":
            return SecuritiesBySybase.get_main_trading_destination_id(security)
        elif database == "sqlite":
            return SecuritiesBySqlite.get_main_trading_destination_id(security)
        else:
            print "FATAL ERROR : unknown database [usual databases : sqlite, sybase]"



class SecuritiesBySybase:

    @staticmethod
    def get_params():
        SourceID = 2
        ServerName = "BSIRIUS"
        return [SourceID, ServerName]

    @staticmethod
    def get_ric(security_id, trading_destination_id):
        params = SecuritiesBySybase.get_params()
        my_query  = "select reference from repository..security_source where security_id=%d and source_id=%d and trading_destination_id=%d" %(security_id, params[0], trading_destination_id)
        try:
            ric = Connections.exec_sql(params[1], my_query)[0][0]
        except:
            print "FATAL ERROR : problem with SQL request : couldn't get security key"
        return ric

    @staticmethod
    def get_trading_destination_name(trading_destination_id):
        params = SecuritiesBySybase.get_params()
        my_query  = "select short_name from repository..trading_destination where trading_destination_id=%d" %(trading_destination_id)
        try:
            trading_destination_name = Connections.exec_sql(params[1], my_query)[0][0]
        except:
            print "FATAL ERROR : problem with SQL request : couldn't get security key"
        return trading_destination_name

    @staticmethod
    def get_security_id(ric):
        params = SecuritiesBySybase.get_params()
        my_query  = "select security_id from repository..security_source where reference=\'%s\' order by security_id"
        my_query = my_query %(ric)
        try:
            security_id = Connections.exec_sql(params[1], my_query)[0][0]
        except:
            print "FATAL ERROR : problem with SQL request : couldn't get security id"
        return security_id

    @staticmethod
    def get_trading_destination_id(trading_destination_name):
        params = SecuritiesBySybase.get_params()
        my_query  = "select trading_destination_id from repository..trading_destination where short_name='%s' order by trading_destination_id" %(trading_destination_name)
        try:
            trading_destination_id = Connections.exec_sql(params[1], my_query)[0][0]
        except:
            print "FATAL ERROR : problem with SQL request : couldn't get security key"
        return trading_destination_id

    @staticmethod
    def get_main_trading_destination_id(security):
        if isinstance(security, str):
            params = SecuritiesBySybase.get_params()
            my_query  = "select trading_destination_id from repository..security_source where reference='%s'" %(security)
            try:
                trading_destination_ids = Connections.exec_sql(params[1], my_query)
                main_trading_destination_id = 1e20
                for td in trading_destination_ids:
                    if td[0] < main_trading_destination_id:
                        main_trading_destination_id = td[0]
            except:
                print "FATAL ERROR : problem with SQL request : couldn't get security key"
            return main_trading_destination_id
        elif isinstance(security, int):
            params = SecuritiesBySybase.get_params()
            my_query  = "select trading_destination_id from repository..security_source where security_id=%d" %(security)
            try:
                trading_destination_ids = Connections.exec_sql(params[1], my_query)
                main_trading_destination_id = 1e20
                for td in trading_destination_ids:
                    if td[0] < main_trading_destination_id:
                        main_trading_destination_id = td[0]
            except:
                print "FATAL ERROR : problem with SQL request : couldn't get security key"
            return main_trading_destination_id



class SecuritiesBySqlite:

    @staticmethod
    def get_params():
        database_path = 'C:/st_sim/simep/projects/databases/repository'
        source_name = "IDN_SELECTFEED"
        return [source_name, database_path]

    @staticmethod
    def get_ric(security_id, trading_destination_id):
        params = SecuritiesBySqlite.get_params()
        my_query  = "select ss.reference from security_source ss, source src where src.source_id = ss.source_id and src.name=\'%s\' and ss.security_id=%d and ss.trading_destination_id=%d" %(params[0], security_id, trading_destination_id)
        try:
            R = sqlite3.connect(params[1]).cursor()
            R.execute(my_query)
            ric = str(R.fetchone()[0])
            R.close()
        except:
            print "FATAL ERROR : problem with SQL request : couldn't get security key"
        return ric

    @staticmethod
    def get_trading_destination_name(trading_destination_id):
        params = SecuritiesBySqlite.get_params()
        my_query  = "select short_name from trading_destination where trading_destination_id=%d" %(trading_destination_id)
        try:
            R = sqlite3.connect(params[1]).cursor()
            R.execute(my_query)
            trading_destination_name = str(R.fetchone()[0])
            R.close()
        except:
            print "FATAL ERROR : problem with SQL request : couldn't get security key"
        return trading_destination_name

    @staticmethod
    def get_security_id(ric):
        params = SecuritiesBySqlite.get_params()
        my_query  = "select security_id from security_source where reference=\'%s\' order by security_id asc" %(ric)
        try:
            R = sqlite3.connect(params[1]).cursor()
            R.execute(my_query)
            security_id = str(R.fetchone()[0])
            R.close()
            return security_id
        except:
            print "FATAL ERROR : problem with SQL request : couldn't get security id"

    @staticmethod
    def get_trading_destination_id(trading_destination_name):
        params = SecuritiesBySqlite.get_params()
        my_query  = "select trading_destination_id from trading_destination where short_name='%s' order by trading_destination_id" %(trading_destination_name)
        try:
            R = sqlite3.connect(params[1]).cursor()
            R.execute(my_query)
            trading_destination_id = str(R.fetchone()[0])
            R.close()
        except:
            print "FATAL ERROR : problem with SQL request : couldn't get security key"
        return trading_destination_id

    @staticmethod
    def get_main_trading_destination_id(security):
        if isinstance(security, str):
            params = SecuritiesBySqlite.get_params()
            my_query  = "select ss.trading_destination_id from security_source ss, source src where src.source_id = ss.source_id and src.name=\'%s\' and ss.reference='%s'" %(params[0], security)
            try:
                R = sqlite3.connect(params[1]).cursor()
                R.execute(my_query)
                trading_destination_ids = str(R.fetchone()[0])
                R.close()
                main_trading_destination_id = min(trading_destination_ids)
            except:
                print "FATAL ERROR : problem with SQL request : couldn't get security key"
            return main_trading_destination_id
        elif isinstance(security, int):
            params = SecuritiesBySqlite.get_params()
            my_query  = "select ss.trading_destination_id from security_source ss, source src where src.source_id = ss.source_id and src.name=\'%s\' and ss.security_id=%d" %(params[0], security)
            try:
                R = sqlite3.connect(params[1]).cursor()
                R.execute(my_query)
                trading_destination_ids = str(R.fetchone()[0])
                R.close()
                main_trading_destination_id = min(trading_destination_ids)
            except:
                print "FATAL ERROR : problem with SQL request : couldn't get security key"
            return main_trading_destination_id
