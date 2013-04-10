'''
Created on 6 oct. 2010

@author: benca
'''

import os
import sqlite3
from simep import __simep_directory__, __config__
from simep.tools import ampm2date
from simep.funcs.utils.paramstools import ParamsTools 
from simep.funcs.dbtools.securities_by_sybase import SecuritiesBySybase 

 
class SecuritiesBySqlite:
    
    """
    In this class, all the following tables are used :
    
    -> execution_market
    -> historic_place_timezone
    -> indice
    -> indice_component
    -> price_scale_rule_threshold
    -> quotation_group_trading_hours
    -> security_market
    -> security_source
    -> source
    -> trading_destination
    -> trading_venue_info
    """
    
    SRC = "IDN_SELECTFEED"
    DB  = sqlite3.connect(__simep_directory__ + '/databases/repository')
    
    
    
    '''######################################################################################################
    ##########################################   UPDATE FUNCTION   ##########################################
    ######################################################################################################'''
    
    @staticmethod
    def timestamp_modifier(datestr, enable_hour):
        MM = datestr[:3]
        if MM == 'Jan':
            MM = 1
        elif MM == 'Feb':
            MM = 2
        elif MM == 'Mar':
            MM = 3
        elif MM == 'Apr':
            MM = 4
        elif MM == 'May':
            MM = 5
        elif MM == 'Jun':
            MM = 6
        elif MM == 'Jul':
            MM = 7
        elif MM == 'Aug':
            MM = 8
        elif MM == 'Sep':
            MM = 9
        elif MM == 'Oct':
            MM = 10
        elif MM == 'Nov':
            MM = 11
        elif MM == 'Dec':
            MM = 12
        DD = int(datestr[4:6])
        YY = int(datestr[7:11])
        if enable_hour:
            time = ampm2date(datestr[12:])
            return '%04d%02d%02d %s' %(YY, MM, DD, time)
        else:
            return '%04d%02d%02d' %(YY, MM, DD)
    
    
    @staticmethod
    def generate_sql_import_cmds():
        path = __simep_directory__ + '/databases/'
        f    = open(path + 'sql_lite_command_file.input', 'w')
        if os.name == 'nt':
            path = __simep_directory__.replace('/','\\\\') + '\\\\databases\\\\'
        w    = f.write
        w("delete from execution_market;\n")
        w("delete from historic_place_timezone;\n")
        w("delete from indice;\n")
        w("delete from indice_component;\n")
        w("delete from price_scale_rule_threshold;\n")
        w("delete from quotation_group_trading_hours;\n")
        w("delete from security_market;\n")
        w("delete from security_source;\n")
        w("delete from source;\n")
        w("delete from trading_destination;\n")
        w("delete from trading_venue_info;\n")
        w("delete from trading_daily;\n")
        w("delete from ExecutionVenue;\n")
        w("\n")
        w(".separator \"|#|\"  \n")
        w(".import " + path + "execution_market.txt execution_market\n")
        w(".import " + path + "historic_place_timezone.txt historic_place_timezone\n")
        w(".import " + path + "indice.txt indice\n")
        w(".import " + path + "indice_component.txt indice_component\n")
        w(".import " + path + "price_scale_rule_threshold.txt price_scale_rule_threshold\n")
        w(".import " + path + "quotation_group_trading_hours.txt quotation_group_trading_hours\n")
        w(".import " + path + "security_market.txt security_market\n")
        w(".import " + path + "security_source.txt security_source\n")
        w(".import " + path + "source.txt source\n")
        w(".import " + path + "trading_destination.txt trading_destination\n")
        w(".import " + path + "trading_venue_info.txt trading_venue_info\n")
        w(".import " + path + "trading_daily.txt trading_daily\n");
        w(".import " + path + "ExecutionVenue.txt ExecutionVenue\n");
        w("\n")
        f.close()
    
    
    @staticmethod
    def update():
        
        """
        CLOSE CONNECTION TO REPOSITORY AND CLEAR TXT FILES
        """
        path = __simep_directory__ + '/databases'
        SecuritiesBySqlite.DB.close()
        
        
        
        
        if os.name == 'nt':
            winpath = path.replace('/','\\') + '\\'
            if os.path.exists(winpath + '*.txt'):
                os.system('del ' + winpath + '*.txt /F')
            if os.path.exists(winpath + '*.log'):
                os.system('del ' + winpath + '*.log /F')
            if os.path.exists(winpath + 'sql_lite_command_file.input'):
                os.system('del ' + winpath + 'sql_lite_command_file.input /F')
        else:
            raise NotImplementedError('SecuritiesBySqlite.update() not implemented for linux')
        
        """
        GET ALL THE REQUIRED TABLES FROM THE DATABASE
        """
        
        print ("Dumping necessaries DBs ...")
        cmd = "bcp %s out " + path + "/%s.txt -t \"|#|\" -U batch -P delphi -S %s -c >>" + path + "/bcp_dump_log.txt"
#        os.system(cmd %('market..execution_market'                  , 'execution_market'                , 'VEGA'   ))
#        os.system(cmd %('repository..historic_place_timezone'       , 'historic_place_timezone'         , 'BSIRIUS'))
#        os.system(cmd %('market..indice'                            , 'indice'                          , 'VEGA'   ))
#        os.system(cmd %('market..indice_component'                  , 'indice_component'                , 'VEGA'   ))
#        os.system(cmd %('market..price_scale_rule_threshold'        , 'price_scale_rule_threshold'      , 'BILBO'  ))
#        os.system(cmd %('market..quotation_group_trading_hours'     , 'quotation_group_trading_hours'   , 'VEGA'   ))
#        os.system(cmd %('temp_works..recorded_security_market'      , 'security_market'                 , 'BSIRIUS'))
#        os.system(cmd %('temp_works..recorded_security_source'      , 'security_source'                 , 'BSIRIUS'))
#        os.system(cmd %('market..source'                            , 'source'                          , 'VEGA'   ))
#        os.system(cmd %('market..trading_destination'               , 'trading_destination'             , 'VEGA'   ))
#        os.system(cmd %('market..trading_venue_info'                , 'trading_venue_info'              , 'VEGA'   ))
#        os.system(cmd %('market..trading_destination_dictionary'    , 'trading_destination_dictionary'  , 'VEGA'   ))
#        os.system(cmd % ('temp_works..recorded_trading_daily'    , 'trading_daily'  , 'BSIRIUS'   ))
#        print ("Dumping necessaries DBs -> Done")
#        
        os.system(cmd %('repository..execution_market'                  , 'execution_market'                , 'BSIRIUS'   ))
        os.system(cmd %('repository..historic_place_timezone'       , 'historic_place_timezone'         , 'BSIRIUS'))
        os.system(cmd %('repository..indice'                            , 'indice'                          , 'BSIRIUS'   ))
        os.system(cmd %('repository..indice_component'                  , 'indice_component'                , 'BSIRIUS'   ))
        os.system(cmd %('repository..price_scale_rule_threshold'        , 'price_scale_rule_threshold'      , 'BSIRIUS'  ))
        os.system(cmd %('repository..quotation_group_trading_hours'     , 'quotation_group_trading_hours'   , 'BSIRIUS'   ))
        os.system(cmd %('temp_works..recorded_security_market'      , 'security_market'                 , 'BSIRIUS'))
        os.system(cmd %('temp_works..recorded_security_source'      , 'security_source'                 , 'BSIRIUS'))
        os.system(cmd %('repository..source'                            , 'source'                          , 'BSIRIUS'   ))
        os.system(cmd %('repository..trading_destination'               , 'trading_destination'             , 'BSIRIUS'   ))
        os.system(cmd %('repository..trading_venue_info'                , 'trading_venue_info'              , 'BSIRIUS'   ))
        os.system(cmd %('repository..trading_destination_dictionary'    , 'trading_destination_dictionary'  , 'BSIRIUS'   ))
        os.system(cmd %('repository..ExecutionVenue'    , 'ExecutionVenue'  , 'BSIRIUS'   ))
        os.system(cmd % ('temp_works..recorded_trading_daily'    , 'trading_daily'  , 'BSIRIUS'   ))
        
        """
        MODIFY TIMESTAMPS OF THE TABLES HISTORIC_PLACE_TIMEZONE AND QUOTATION_GROUP_TRADING_HOURS
        """
        
        f1 = open(path + '/historic_place_timezone.txt', 'r')
        lines = f1.readlines()
        f1.close()
        f2 = open(path + '/historic_place_timezone.txt', 'w')
        for line in lines:
            fields    = line.split('|#|')
            fields[4] = SecuritiesBySqlite.timestamp_modifier(fields[4], True)
            fields[5] = SecuritiesBySqlite.timestamp_modifier(fields[5][:26], True)+'\n' if len(fields[5]) >= 10 else fields[5]
            f2.write('|#|'.join(fields))
        f2.close()
        f1 = open(path + '/quotation_group_trading_hours.txt', 'r')
        lines = f1.readlines()
        f1.close()
        f2 = open(path + '/quotation_group_trading_hours.txt', 'w')
        for line in lines:
            fields    = line.split('|#|')
            fields[2] = SecuritiesBySqlite.timestamp_modifier(fields[2], False) if len(fields[2]) >= 8 else fields[2]
            f2.write('|#|'.join(fields))
        f2.close()
        
        """
        MODIFY TIMESTAMPS OF THE TABLES HISTORIC_PLACE_TIMEZONE AND QUOTATION_GROUP_TRADING_HOURS
        """
        if os.name == 'nt':
            os.system('del ' + winpath + 'repository /F')
            SecuritiesBySqlite.generate_sql_import_cmds()
            sqlite_path = __config__['sqlite_path']
            os.system(sqlite_path +' '+ winpath + 'repository <' + winpath + 'local_table_schema.schema >    ' + winpath + 'import_result.log')
            os.system(sqlite_path +' '+ winpath + 'repository <' + winpath + 'sql_lite_command_file.input >> ' + winpath + 'import_result.log')
            os.system('del ' + winpath + '*.txt /F')
            os.system('del ' + winpath + '*.log /F')
            os.system('del ' + winpath + 'sql_lite_command_file.input /F')
        else:
            raise NotImplementedError('SecuritiesBySqlite.update() not implemented for linux')
        
        
        """
        RECONNECT TO THE NEW REPOSITORY
        """
        SecuritiesBySqlite.DB  = sqlite3.connect(__simep_directory__ + '/databases/repository')
    
    
    
    '''######################################################################################################
    ######################################   SQL REQUESTS FUNCTIONS   #######################################
    ######################################################################################################'''
    
    @staticmethod
    def get_ric(security_id, trading_venue_id):
        if trading_venue_id == None:
            my_query  = "select ss.reference from security_source ss, trading_venue_info tvi where ss.source_id=2 and ss.trading_destination_id=tvi.trading_destination_id and tvi.is_primary=1 and ss.security_id=%d" %(security_id)
        else:
            my_query  = "select ss.reference from security_source ss, trading_venue_info tvi where ss.source_id=2 and ss.trading_destination_id=tvi.trading_destination_id and ss.security_id=%d and tvi.trading_venue_id=%d" %(security_id, trading_venue_id)
        R = SecuritiesBySqlite.DB.cursor()
        R.execute(my_query)
        ric = str(R.fetchone()[0])
        R.close()
        return ric
    @staticmethod
    def get_ric_from_execution_venue_id(security_id, trading_venue_id):
        #trading_destination_id = SecuritiesBySybase.get_trading_destination_id(trading_venue_id)
        #raise "SecuritiesBySybase : method not implemented"
        R = SecuritiesBySqlite.DB.cursor()
        if trading_venue_id == None:
            my_query  = "select ss.reference from repository..security_source ss inner join repository..security_market sm on ss.security_id=sm.security_id and ss.trading_destination_id=sm.trading_destination_id \
                        where ss.source_id=2 and sm.security_id=%d and sm.ranking=1" %(security_id)
            '''
            my_query  = "select distinct ss.reference from repository..security_source ss inner join repository..security_market sm on ss.security_id=sm.security_id and ss.trading_destination_id=sm.trading_destination_id \
                        where sm.security_id=%d and sm.ranking=1" %(security_id)
            '''
        else:
            #my_query  = "select reference from repository..security_source where source_id=%d and security_id=%d and trading_destination_id=%d" %(params[0], security_id,trading_destination_id)
            my_query = "select ss.reference from security_source ss inner join trading_destination td on ss.trading_destination_id=td.trading_destination_id inner join ExecutionVenue ev on td.execution_market_id=ev.ExecutionMarket where ss.source_id=2 and ss.security_id=%d and ev.ExecutionVenue=%d" % (security_id, trading_venue_id)
        R.execute(my_query)
        print "Running -> " + my_query
        result = R.fetchone()
        
        if result == None:
            ric = None 
            print "no results"
        else : 
            ric = result[0]
            print "RIC : " + ric
            
        R.close()
        return str(ric)
    
    
    
    @staticmethod
    def get_security_id(security_code):
        my_query  = "select security_id from security_source where reference=\'%s\'" %(security_code)
        R = SecuritiesBySqlite.DB.cursor()
        R.execute(my_query)
        security_id = str(R.fetchone()[0])
        R.close()
        return security_id
    
    @staticmethod
    def get_market_code(security_id, trading_destination_id):
        my_query  = "select market_code from security_market where security_id=%d and trading_destination_id=%d" %(security_id, trading_destination_id)
        R = SecuritiesBySqlite.DB.cursor()
        R.execute(my_query)
        market_code = str(R.fetchone()[0])
        R.close()
        return market_code
    
#    @staticmethod
#    def get_trading_venue_id(trading_venue_name):
#        my_query  = "select trading_venue_id from trading_venue_info where name='%s' order by trading_venue_id" %(trading_venue_name)
#        R = SecuritiesBySqlite.DB.cursor()
#        R.execute(my_query)
#        trading_venue_id = str(R.fetchone()[0])
#        R.close()
#        return trading_venue_id
    @staticmethod
    def get_trading_venue_id(trading_venue_name):
        my_query  = "select ExecutionVenue from ExecutionVenue where Name='%s'" %(trading_venue_name)
        try:
            R = SecuritiesBySqlite.DB.cursor()
            R.execute(my_query)
            trading_venue_id = str(R.fetchone()[0])
        except:
            print "FATAL ERROR : problem with SQL request : couldn't get trading venue ID"
        
        R.close()
        return trading_venue_id
 
    
#    @staticmethod
#    def get_trading_venue_name(trading_venue_id):
#        my_query  = "select name from trading_venue_info where trading_venue_id=%d" %(trading_venue_id)
#        R = SecuritiesBySqlite.DB.cursor()
#        R.execute(my_query)
#        trading_venue_name = str(R.fetchone()[0])
#        R.close()
#        return trading_venue_name
    
    @staticmethod
    def get_trading_venue_name(trading_venue_id):
        R = SecuritiesBySqlite.DB.cursor()
        my_query  = "select Name from ExecutionVenue where ExecutionVenue=%d" %(trading_venue_id)
        try:
            R.execute(my_query)
            trading_destination_name = R.fetchone()[0]
        except:
            print "FATAL ERROR : problem with SQL request : couldn't get venue name"
        R.close()
        return str(trading_destination_name)    
    
    @staticmethod
    def get_trading_destination_id(trading_venue):
        if isinstance(trading_venue, (str, unicode)):
            my_query  = "select trading_destination_id from trading_venue_info where name='%s' order by trading_destination_id" %(trading_venue)
        else:
            my_query  = "select trading_destination_id from trading_venue_info where trading_venue_id=%d order by trading_destination_id" %(trading_venue)
        R = SecuritiesBySqlite.DB.cursor()
        R.execute(my_query)
        trading_destination_id = R.fetchone()[0]
        R.close()
        return trading_destination_id
    
    @staticmethod   
    def get_trading_destination_id_from_venue_id(venue_id, security_id):
        R = SecuritiesBySqlite.DB.cursor()
        
        
        if isinstance(venue_id, (str, unicode)):
            my_query  = "select distinct sm.trading_destination_id from security_market sm inner join ExecutionVenue ev \
                        on sm.execution_market_id=ev.ExecutionMarket where ev.execution_market_id='%d' and sm.security_id=%d" % (venue_id, security_id)
        elif isinstance(venue_id, int):
#            my_query  = "select distinct sm.trading_destination_id from security_market sm inner join ExecutionVenue ev \
#                        on sm.execution_market_id=ev.ExecutionMarket where ev.ExecutionVenue=%d and sm.security_id=%d" %(venue_id, security_id)
            my_query  = "select distinct  sm.trading_destination_id from security_market sm inner join ExecutionVenue ev \
                        on sm.execution_market_id=ev.ExecutionMarket where ev.ExecutionVenue=%d and sm.security_id=%d"%(venue_id, security_id)
        print "Running -> " + my_query 
        R.execute(my_query)
        result = R.fetchone()
        if result == None:
            trading_destination_id = None
            print "No data found"
        else :
            trading_destination_id = result[0]
            print "data found : %d" %trading_destination_id 
        
        R.close()
        return trading_destination_id
        
        
    @staticmethod
    def get_main_trading_venue_id(security):
        #raise "SecuritiesBySybase : method not implemented"
        R = SecuritiesBySqlite.DB.cursor()
        if isinstance(security, str):
            #my_query  = "select distinct trading_destination_id from repository..security_source where reference='%s and source_id=%d'" %(security)
            my_query = "select ExecutionVenue from ExecutionVenue ev inner join security_market sm on sm.execution_market_id=ev.ExecutionMarket \
                        where sm.ranking=1 and sm.security_id=(select distinct security_id from security_source where reference='%s' and source_id=2)" % (security)
        elif isinstance(security, int):
            my_query  = "select ExecutionVenue from repository..ExecutionVenue ev inner join repository..security_market sm on sm.execution_market_id=ev.ExecutionMarket \
                        where sm.ranking=1 and sm.security_id=%d" % (security)
        try:
            R.execute(my_query)
            trading_destination_ids = R.fetchall()
            main_trading_destination_id = 1e20
            for td in trading_destination_ids:
                if td[0] < main_trading_destination_id:
                    main_trading_destination_id = td[0]
        except:
            print "FATAL ERROR : problem with SQL request : couldn't get main trading destination ID"
        R.close()
        return main_trading_destination_id
    
    
#    @staticmethod
#    def get_main_trading_venue_id(security):
#        R = SecuritiesBySqlite.DB.cursor()
#        if isinstance(security, (str,unicode)):
#            my_query  = "select tvi.trading_venue_id from security_source ss, trading_venue_info tvi where ss.trading_destination_id=tvi.trading_destination_id and tvi.is_primary=1 and ss.reference='%s'" %security
#        else:
#            my_query  = "select tvi.trading_venue_id from security_source ss, trading_venue_info tvi where ss.trading_destination_id=tvi.trading_destination_id and tvi.is_primary=1 and ss.security_id=%d" %security
#        R.execute(my_query)
#        trading_venue_id = R.fetchone()[0]
#        R.close()
#        return trading_venue_id
    
    @staticmethod
    def get_trading_venue_names(security):
        if isinstance(security, (str,unicode)):
            my_query  = "select distinct tvi.name from trading_venue_info tvi, security_source ss where ss.security_id=(select distinct security_id from security_source where reference='%s') and ss.trading_destination_id=tvi.trading_destination_id order by tvi.name asc" %(security)
        else:
            my_query  = "select distinct tvi.name from trading_venue_info tvi, security_market sm where sm.security_id=%d and sm.trading_destination_id=tvi.trading_destination_id order by tvi.name asc" %(security)
        R = SecuritiesBySqlite.DB.cursor()
        R.execute(my_query)
        trading_venue_names = [str(x[0]) for x in R.fetchall()]
        R.close()
        return trading_venue_names
    
    @staticmethod
    def get_trading_hours(security_id, trading_destination_id, date):
        R = SecuritiesBySqlite.DB.cursor()
        my_query = "select em.place_id, em.place_timezone from execution_market em, trading_destination td where em.execution_market_id=td.execution_market_id and td.trading_destination_id=%d" % (trading_destination_id)
        R.execute(my_query)
        tmp = R.fetchone()
        place_id = tmp[1] if tmp[1] != None else tmp[0]
        my_query = "select quotation_group from security_market where security_id=%d and trading_destination_id=%d" % (security_id, trading_destination_id)
        R.execute(my_query)
        quotation_group = str(R.fetchone()[0])
        my_query = "select opening, closing, opening_auction, closing_auction, intraday_resumption_auction, intraday_resumption, end_date from quotation_group_trading_hours where trading_destination_id=%d and quotation_group='%s' and (end_date>='%s' or end_date=null or end_date='')" % (trading_destination_id, quotation_group, date)
        R.execute(my_query)
        fields = map(str, R.fetchall()[0])
        opening = ampm2date(fields[0])
        closing = ampm2date(fields[3] if fields[3] else fields[1])
        opening_auction = ampm2date(fields[2]) if fields[2] else ''
        closing_auction = ampm2date(fields[1]) if fields[3] else ''
        intraday_resumption_auction = ampm2date(fields[4]) if fields[4] else ''
        intraday_resumption = ampm2date(fields[5]) if fields[5] else ''
        ext_date = '%s 12:00:00:000' % date
        my_query = "select gmt_offset_hours from historic_place_timezone where place_id=%d and gmt_begin_date<='%s' and (gmt_end_date>='%s' or gmt_end_date=null or gmt_end_date='')" % (place_id, ext_date, ext_date)
        R.execute(my_query)
        delta_t = int(R.fetchone()[0])
        opening = '%02d%s' % (int(opening[:2]) - delta_t, opening[2:])
        closing = '%02d%s' % (int(closing[:2]) - delta_t, closing[2:])
        opening_auction = '%02d%s' % (int(opening_auction[:2]) - delta_t, opening_auction[2:]) if len(opening_auction) > 0 else ''
        closing_auction = '%02d%s' % (int(closing_auction[:2]) - delta_t, closing_auction[2:]) if len(closing_auction) > 0 else ''
        intraday_resumption_auction = '%02d%s' %(int(intraday_resumption_auction[:2])-delta_t, intraday_resumption_auction[2:]) if intraday_resumption_auction else ''
        intraday_resumption = '%02d%s' % (int(intraday_resumption[:2])-delta_t, intraday_resumption[2:]) if intraday_resumption else ''
        R.close()
        return (opening, closing, opening_auction, closing_auction, intraday_resumption_auction, intraday_resumption)
    
    
    @staticmethod
    def get_trading_venue_type(trading_venue):
        R = SecuritiesBySqlite.DB.cursor()
        if isinstance(trading_venue, str):
            my_query = "select OrderBook from ExecutionVenue where name='%s'" %trading_venue
        elif isinstance(trading_venue, int):
            my_query = "select OrderBook from ExecutionVenue where ExecutionVenue=%d" % trading_venue
        try:
            print "Running -> " + my_query
            R.execute(my_query)
            result = R.fetchone()
            if result == None : 
                print "no result"
                venue_type = None
            else : 
                venue_type  = str(result[0])
                print "venue type : " + venue_type
        except:
            print "FATAL ERROR : problem with SQL request : couldn't get trading venue type"
        R.close()
        return venue_type
    
    @staticmethod
    def get_trading_venue_reference(trading_venue):
        if isinstance(trading_venue, (str, unicode)):
            my_query  = "select reference from trading_destination_dictionary where dictionary_id = %d and trading_destination_id in (select trading_destination_id from trading_venue_info where name = '%s')" %(140, trading_venue)
        else:
            my_query  = "select reference from trading_destination_dictionary where dictionary_id = %d and trading_destination_id in (select trading_destination_id from trading_venue_info where trading_venue_id = %d)" %(140, trading_venue)
        R = SecuritiesBySqlite.DB.cursor()
        R.execute(my_query)
        trading_venue_reference = str(R.fetchone()[0])
        R.close()
        return trading_venue_reference
    
#    @staticmethod
#    def get_is_primary(trading_venue):
#        if isinstance(trading_venue, (str, unicode)):
#            my_query  = "select is_primary from trading_venue_info where name='%s'" %trading_venue
#        else:
#            my_query  = "select is_primary from trading_venue_info where trading_venue_id=%d" %trading_venue
#        R = SecuritiesBySqlite.DB.cursor()
#        R.execute(my_query)
#        is_primary = bool(R.fetchone()[0])
#        R.close()
#        return is_primary
    
    
    @staticmethod
    def get_is_primary(trading_venue, security_id):
        R = SecuritiesBySqlite.DB.cursor()
        if isinstance(trading_venue, (str, unicode)):
            my_query  = "select distinct ranking from security_market sm inner join ExecutionVenue ev on sm.execution_market_id=ev.ExecutionMarket \
                        where ev.Name='%s' and sm.security_id=%d" % (trading_venue, security_id)
        else:
            my_query  = "select distinct ranking from security_market sm inner join  ExecutionVenue ev on sm.execution_market_id=ev.ExecutionMarket \
                        where ev.ExecutionVenue=%d and sm.security_id=%d" % (trading_venue, security_id)
        R.execute(my_query)
        is_primary = True if R.fetchone()[0] == 1 else False
        R.close()
        return is_primary
    
    @staticmethod
    def get_reference_and_source_id_and_source_name(security_id, trading_venue_id):
        my_query  = "select ss.reference, ss.source_id, s.name from security_source ss, source s, trading_venue_info tvi where ss.security_id=%d and ss.trading_destination_id=tvi.trading_destination_id and tvi.trading_venue_id=%d and ss.source_id=s.source_id and ss.defaut=1" %(security_id, trading_venue_id)
        R = SecuritiesBySqlite.DB.cursor()
        R.execute(my_query)
        results    = list(R.fetchall()[0])
        results[0] = str(results[0])
        results[2] = str(results[2]).lower()
        R.close()
        return results
    
    @staticmethod
    def get_tick_sizes(security_id, trading_destination_id):
        my_query  = "select price_scale_id from security_market where security_id=%s and trading_destination_id=%d" % (security_id, trading_destination_id)
        R = SecuritiesBySqlite.DB.cursor()
        R.execute(my_query)
        price_scale_id = int(str(R.fetchone()[0]))
        R.close()
        my_query  = "select threshold, tick_size from price_scale_rule_threshold where price_scale_id =%d" %price_scale_id
        R = SecuritiesBySqlite.DB.cursor()
        R.execute(my_query)
        r = [(float(e[0]), float(e[1])) for e in R.fetchall()]
        R.close()
        return r
    
    @staticmethod
    def get_indexes_list():
        my_query  = "select name from indice where indice_id in (select distinct indice_id from indice_component)"
        R = SecuritiesBySqlite.DB.cursor()
        R.execute(my_query)
        r = map(str, [s[0] for s in R.fetchall()])
        R.close()
        return r
    
    @staticmethod
    def get_index_comp(index_name):
        my_query  = "select distinct ss.reference from security_market sm, security_source ss, indice_component ind_c, indice ind where ind.name='%s' and ind.indice_id=ind_c.indice_id and sm.security_id=ind_c.security_id and sm.ranking=1 and ss.source_id=2 and sm.security_id=ss.security_id and ss.trading_destination_id=sm.trading_destination_id" %index_name
        R = SecuritiesBySqlite.DB.cursor()
        R.execute(my_query)
        r = map(str, [s[0] for s in R.fetchall()])
        R.close()
        return r
    
    @staticmethod
    def search_ric(database, security):
        '''not implemented'''
        pass
    
  
    @staticmethod
    def get_main_trading_destination_id(security):
        '''not implemented'''
        pass
    
        
    @staticmethod
    def set_scenario(configuration, scenario_file, slave_file):
        if os.name == "posix":
            username = os.environ['USER']
        else :
            username = os.environ['USERNAME']
            
        R = SecuritiesBySqlite.DB.cursor()
        """
        insert the new scenario into the scenario_result table
        """
        config = slave_file[:-10].split('__')
        engine = (configuration['engine'].keys())[0]
        dates = configuration['dates'].keys()
        dates.sort()
        my_query = "INSERT INTO scenario (user,engine,agents,stocks,dates,scenario_file,slave_file)\
                    VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (username, engine, config[1], config[2], '_'.join(dates), scenario_file, slave_file)
        R.execute(my_query)   
        my_query = "SELECT last_insert_rowid()"
        R.execute(my_query)
        scenario_id = int(R.fetchone()[0])             

        """
        insert the new scenario into the scenario_configuration table
        """
        for category, value1 in configuration.iteritems():   
            for sub_category, value2 in value1.iteritems():
                if type(value2).__name__ == 'dict':
                    if not value2:
                        my_query = "INSERT INTO scenario_configuration (scenario_id,category,sub_category)\
                                    VALUES (%d, '%s', '%s')" % (scenario_id, category, sub_category)   
                        R.execute(my_query)
                        continue
                    for parameter, config_value in value2.iteritems():
                        config_type = type(config_value).__name__
                        config_value = ParamsTools.format_to_str(config_value, config_type)
                        my_query = "INSERT INTO scenario_configuration (scenario_id,category,sub_category,parameter,config_type,config_value)\
                                    VALUES (%d, '%s', '%s', '%s', '%s', '%s')" % (scenario_id, category, sub_category, parameter, config_type, config_value)
                        R.execute(my_query) 
                else:
                    config_type = type(value2).__name__
                    config_value = ParamsTools.format_to_str(value2, config_type)
                    my_query = "INSERT INTO scenario_configuration (scenario_id,category,parameter,config_type,config_value)\
                                VALUES (%d, '%s', '%s', '%s', '%s')" % (scenario_id, category, sub_category, config_type, config_value)
                    R.execute(my_query)
        #  save the change
        SecuritiesBySqlite.DB.commit()
        R.close()
        
    @staticmethod
    def get_scenario_id(configuration):      
        """
        query the scenario_configuration table
        """
        R = SecuritiesBySqlite.DB.cursor()  
        count = 0
        my_query = "SELECT scenario_id FROM scenario_configuration WHERE "
        for category, value1 in configuration.iteritems():
            for sub_category, value2 in value1.iteritems():
                if type(value2).__name__ == 'dict':
                    if not value2:
                        my_query += "(category='%s' AND sub_category='%s') OR " % (category, sub_category)
                        count += 1
                        continue
                    for parameter, config_value in value2.iteritems():
                        config_type = type(config_value).__name__
                        config_value = ParamsTools.format_to_str(config_value, config_type)
                        my_query += "(category='%s' AND sub_category='%s' AND parameter='%s' AND config_value='%s') OR " % (category, sub_category, parameter, config_value)                                             
                        count += 1
                else:
                    config_type = type(value2).__name__
                    config_value = ParamsTools.format_to_str(value2, config_type)
                    my_query += "(category='%s' AND parameter='%s' AND config_value='%s') OR " % (category, sub_category, config_value)          
                    count += 1
        my_query = my_query[:-3]
        my_query += "GROUP BY scenario_id HAVING COUNT(*)=%d" % count
        R.execute(my_query)
        ids = ','. join([str(s[0]) for s in R.fetchall()])
        my_query = "SELECT * FROM scenario WHERE scenario_id in (%s)" % ids
        R.execute(my_query)
        results = R.fetchall()
        R.close()
        return results
    
    @staticmethod
    def get_scenario_config(ids):
        """
        get the scenario configuration against the scenario_id
        """
        configuration = {}
        R = SecuritiesBySqlite.DB.cursor()
        for scenario_id in ids:
            my_query = "SELECT category, sub_category, parameter, config_type, config_value FROM scenario_configuration WHERE scenario_id=%d" % scenario_id
            R.execute(my_query)
            results = R.fetchall()
            configuration[scenario_id] = results
        R.close()
        return configuration
    
    @staticmethod
    def delete_scenario_config(ids):
        """
        delete the metascenario in both scenario and scenario_configuration tables
        """
        conn = SecuritiesBySqlite.DB
        R = conn.cursor()
        selected_ids = ','.join(map(str, ids))
        my_query = "delete from scenario where scenario_id in(%s)" % selected_ids
        R.execute(my_query)
        conn.commit()
        my_query = "delete from scenario_configuration where scenario_id in (%s)" % selected_ids
        R.execute(my_query)
        conn.commit()
        R.close()
    
    @staticmethod
    def create_tables():
        print "create scenario table"
        R = SecuritiesBySqlite.DB.cursor()
        """
        create scenario table
        """   
        #my_query = "DROP TABLE scenario"   
        #R.execute(my_query)    
        my_query = "CREATE TABLE scenario (scenario_id integer PRIMARY KEY AUTOINCREMENT,\
                                           user varchar(16),\
                                           engine varchar(16),\
                                           agents varchar(128),\
                                           stocks varchar(128),\
                                           dates varchar(128),\
                                           scenario_file varchar(512),\
                                           slave_file varchar(512),\
                                           output_file varchar(512))"                                            
        R.execute(my_query)
        """
        create scenario_configuration table
        """
        #my_query = "DROP TABLE scenario_configuration\n"
        #R.execute(my_query)
        my_query = "CREATE TABLE scenario_configuration (scenario_id int,\
                                                         category varchar(16),\
                                                         sub_category varchar(128),\
                                                         parameter varchar(128),\
                                                         config_type varchar(8),\
                                                         config_value varchar(512))"

        R.execute(my_query)
        R.close()
        print "create scenario table---done"

    @staticmethod
    def get_daily_stock_info(sec_id, trading_destination_id, date_start, date_end):
        R = SecuritiesBySqlite.DB.cursor()
        my_query = "SELECT date, volume,turnover,open_prc,high_prc,low_prc,close_prc,nb_deal," + "sqrt(power(high_prc-low_prc,2)/2-(2*log(2)-1)*power(open_prc-close_prc,2)) "+"FROM trading_daily \
                    WHERE security_id = %d AND trading_destination_id = %d AND date >= '%s' AND date <= '%s' " % (sec_id, trading_destination_id, date_start, date_end)
        R.execute(my_query)
        results = R.fetchall()
        R.close()
        return results

    @staticmethod
    def get_auction_info(security_id, trading_destination_id, date):
        from datetime import datetime
        R = SecuritiesBySqlite.DB.cursor()
        my_query = "SELECT open_prc, open_volume, close_prc, close_volume, intraday_turnover, intraday_volume FROM trading_daily \
                    WHERE security_id=%d AND (trading_destination_id=%d or trading_destination_id='') AND date='%s'" % (security_id, trading_destination_id, datetime.strptime(date, '%Y%m%d').strftime('%b %d %Y'))
        R.execute(my_query)
        results = R.fetchone()
        R.close()
        return results
    
    
if __name__ == "__main__":
    print SecuritiesBySqlite.get_trading_venue_id(SecuritiesBySqlite.get_trading_venue_id)
    print SecuritiesBySqlite.get_trading_hours(26, 61, '20110119')
    print SecuritiesBySqlite.get_trading_hours(26, 81, '20110119')
    print SecuritiesBySqlite.get_trading_hours(26, 89, '20110119')

    