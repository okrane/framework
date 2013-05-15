#!/usr/bin/python

'''
    CACVersion BlueboxAlgoLog Version 1.4 20111017_0800

    Modification History

    Date    : January 07, 2011
    Author  : Sylvain Archenault
    Changes : Initial script

    Date    : April 13, 2011
    Author  : Sylvain Archenault
    Changes : Fix bug with automaton name for Custom and Beta algos

    Date    : April 29, 2011
    Author  : Sylvain Archenault
    Changes : Exclude GTC / GTD orders already processed

    Date    : June 20, 2011
    Author  : Sylvain Archenault
    Changes : Use only file per date

    Date    : October 17, 2011
    Author  : Sylvain Archenault
    Changes : Fix AutomatonStartTime, AutomatonEndTime and DealTime
                - Make sure DealTime for "new" order are equals to AutomatonStartTime
                - Make sure that AutomatonStartTime and AutomatonEndTime are equals in all lines
              Differentiate BUYBACK order

    Date    : February 10, 2012
    Author  : Sylvain Archenault
    Changes : Add mapping for new algo name

'''

from cheuvreux.dbtools.Sqlite import SQLiteBase
from cheuvreux.fidessa import orderdb
from cheuvreux.utils.date import previous_weekday, workday_range, parse_date
import datetime
import getopt
import os
import sys
import time
import traceback

db = SQLiteBase(':memory:')

AlgoNameMap = {'POV':'EVP', 'PCTVOL': 'EVP',
               'TARGETCLOSE': 'TargetClose', 'EXIT':'TargetClose',
               'IS': 'ImplementationShortFall', 'ARRIVAL':'ImplementationShortFall' }

tradingDestinationMap = {'ARCX' : 99,
                         'ARCA' : 99,
                         'BATS' : 100,
                         'FLOW' : 113,
                         'EDGX' : 111,
                         'EDGA' : 110,
                         'NSDQ' : 95,
                         'NYSE' : 25,
                         'NQBX' : 106,
                         'BYXX' : 138}

def mapAutomatonName(name):
    '''
        Map cust_xp_data_1 field to algorithm name
    '''

    if name in AlgoNameMap:
        return AlgoNameMap[name]
    else:
        return name

def getTradingDestinationId(execMktId):
    if execMktId in tradingDestinationMap:
        return tradingDestinationMap[execMktId]
    else:
        return 0

def format(row, start_time=None, end_time=None):
    '''
        Log row formatter
    '''

    if row['LiquitidyIndicator'] is None:
        row['LiquitidyIndicator'] = ''
    if row['AutomatonName'] is None or row['AutomatonName'] == '':
        AutomatonName = get_algo_name(row['IdOccurence'])
    else:
        AutomatonName = row['AutomatonName']

    if AutomatonName == 'CUSTOM':
        AutomatonName = handle_custom_algo(row['CUST_XP_DATA_4'], row['ClientOrderId'])

    index = AutomatonName.find('_')
    if index > 0:
        if AutomatonName[0:index] == 'BETA':
            index = AutomatonName.find('_', index+1)
            if index > 0:
                AutomatonName = AutomatonName[0:index]
        else:
            AutomatonName = AutomatonName[0:index]
    else:
        AutomatonName = AutomatonName

    if AutomatonName in ('VWAP', 'TWAP') and row['ExecutionStyle'] == 'BBACK':
        AutomatonName = 'BUYBACK'

    AutomatonName = mapAutomatonName(AutomatonName)

    if start_time:
        AutomatonStartTime = start_time
    else:
        AutomatonStartTime =  row['AutomatonStartTime']

    # For NEW order, make sure the DealTime == AutomatonStartTime
    if row['MsgType'] == 'N':
        dealTime = AutomatonStartTime
    else:
        dealTime = row['DealTime']

    if end_time:
        AutomatonEndTime = end_time
    else:
        AutomatonEndTime = row['AutomatonEndTime']

    if AutomatonEndTime == '00000000000000':
        # Sometimes, the end time set by Fidessa is invalid, default to 17:00:00
        AutomatonEndTime = AutomatonStartTime[0:8] + '170000'

    if len(AutomatonEndTime) == 8: # default end time
        AutomatonEndTime = AutomatonEndTime + '170000'

    mktId = getTradingDestinationId(row['MarketId'])

    return ('%-17s|%s|%-16s|%5s|%1s|%-16s|%-16s|%-16s|%1d|%8s|%-16s|%16d|%125s|%32s|'
            '%14s|%16.2f|%16d|%16d|%16d|%14s|%16.2f|%16s|%16s|%-16s|%1s|%1s|%-5s|'
            '%13s|%11s|%32s|%16s|%14s|%16d|%16.4f|%-2s|%1s|%1s|'
          % (row['MsgId'], row['MsgType'], row['IdOccurence'], row['TraderId'], row['ClientOrderSource'],
             row['ClientOrderId'], row['SecurityId'], row['RefCompl'], row['Side'], row['OrderDate'],
             AutomatonName, row['FavorId'], row['ClientData'], row['ClientRef'],
             AutomatonStartTime, row['Limit'], row['AskedQuantity'], row['MinPercentVolume'],
             row['MinPercentVolume'], AutomatonEndTime, row['WouldLevel'], row['ReferencePrice'],
             row['TraderPrice'], row['ExecutionStyle'], row['CrossFlag'], row['FlagSor'], mktId,
             row['OperatorId'], row['ServerId'], row['DealId'], row['ExecMarketId'], dealTime,
             row['Quantity'], row['Price'], row['LiquitidyIndicator'].strip(), row['Filler1'], row['FlagPrimaryOnly'])
            )

def get_algo_name(order_id):
    '''
        Returns the algo name from the bluebox table
    '''

    query = 'SELECT strategy FROM bluebox WHERE order_id = "%s"' % order_id

    rows = db.selectOne(query)
    if rows:
        return rows[0]
    else :
        return None

def handle_custom_algo(cust_xp_data_4, order_id):
    '''
        For CUSTOM order, this method tries, based on CUST_XP_DATA_4 field
        to detect the algo that has been used
    '''

    if cust_xp_data_4 == 'PASS':
        return 'SMARTX'
    elif cust_xp_data_4 in ('AGGR', 'STRIKE', 'MIDPOINT'):
        return 'CROSSFIRE'
    elif cust_xp_data_4 is not '':
        return cust_xp_data_4
    elif not cust_xp_data_4 and order_id:
        # First try to get the strategy from the Bluebox table
        name = get_algo_name(order_id)
        if name and name != '':
            return name
        else:
            # Try to look for version 1, sometimes cust xp data 4 might be dropped with new version
            name = db.selectOne('SELECT cust_xp_data_4 FROM "orders" WHERE order_id = ? and version = 1', order_id)
            return handle_custom_algo(name[0], None)

    return 'CUSTOM'


def get_start_time(order_id, version=None):
    '''
        Returns the algo real start time.

        This method used different "heuristic" to detect it
    '''
    if not version:
        version = 1

    #1. Try ALGO START TIME
    query = ''' SELECT SUBSTR(ALGO_START_DATETIME,1,8)  ||
                       SUBSTR(ALGO_START_DATETIME,10,2) ||
                       SUBSTR(ALGO_START_DATETIME,13,2) ||
                       SUBSTR(ALGO_START_DATETIME,16,2)
            FROM bluebox WHERE order_id = "%s"
        ''' % (order_id)

    row = db.selectOne(query)
    if row and row[0] != '00000000000000':
        return row[0]


    #2. Try cust_xp_data_2
    query = ''' SELECT SUBSTR(ENTERED_DATETIME,1,8)  ||
                       SUBSTR(CUST_XP_DATA_2,1,2) ||
                       SUBSTR(CUST_XP_DATA_2,4,2) ||
                       SUBSTR(CUST_XP_DATA_2,7,2)
                FROM orders WHERE order_id = "%s" and version = %d
                     and length(CUST_XP_DATA_2) = 15
            ''' % (order_id, version)
    row = db.selectOne(query)
    if row:
        return row[0]

    #2. Try cust_xp_data_2
    query = ''' SELECT SUBSTR(ENTERED_DATETIME,1,8)  ||
                       SUBSTR(ENTERED_DATETIME,10,2) ||
                       SUBSTR(ENTERED_DATETIME,13,2) ||
                       SUBSTR(ENTERED_DATETIME,16,2)
                FROM orders WHERE order_id = "%s" and version = 1
            ''' % (order_id)
    row = db.selectOne(query)
    if row:
        return row[0]

    # Cant find start time
    return None


def get_end_time(order_id, version=None):
    '''
        Returns the algo real end time.

        This method used different "heuristic" to detect it
    '''

    if not version:
        version = 1

    #1. Try ALGO START TIME
    query = ''' SELECT SUBSTR(ALGO_END_DATETIME,1,8)  ||
                       SUBSTR(ALGO_END_DATETIME,10,2) ||
                       SUBSTR(ALGO_END_DATETIME,13,2) ||
                       SUBSTR(ALGO_END_DATETIME,16,2)
            FROM bluebox WHERE order_id = "%s"
        ''' % (order_id)

    row = db.selectOne(query)
    if row and row[0] is not '00000000000000':
        return row[0]

    #2. Try cust_xp_data_3
    query = ''' SELECT SUBSTR(ENTERED_DATETIME,1,8)  ||
                       SUBSTR(CUST_XP_DATA_3,1,2) ||
                       SUBSTR(CUST_XP_DATA_3,4,2) ||
                       SUBSTR(CUST_XP_DATA_3,7,2)
                FROM orders WHERE order_id = "%s" and version = %d
                     and length(CUST_XP_DATA_2) = 15
            ''' % (order_id, version)
    row = db.selectOne(query)
    if row:
        return row[0]

    # Cant find start time
    return None

def log(parent_order_id, version, output):
    '''
        Write into "output" the log of the order "parent_order_id"

        @param parent_order_id Algo order id
        @param vesion Order last version
        @param output File in which log is written
    '''

    # 1/ Get first line (Basic information on the order
    query = ''' select TradeDate || ID MsgId,
                'N' MsgType,
                ORDER_ID IdOccurence,
                PRIMARY_TRADER TraderId,
                'O' ClientOrderSource,
                ORDER_ID ClientOrderId,
                INSTRUMENT_CODE SecurityId,
                CASE MARKET_ID WHEN 'TOR-TSX' THEN 'TO' WHEN 'TOR-TVX' THEN 'TO' ELSE 'N' END RefCompl,
                CASE BUY_SELL WHEN 'B' THEN 0 ELSE 1 END Side,
                TradeDate OrderDate,
                CUST_XP_DATA_1 AS AutomatonName,
                0 FavorId,
                '' ClientData,
                COUNTERPARTY_CODE ClientRef,
                SUBSTR(ENTERED_DATETIME,1,8) || SUBSTR(ENTERED_DATETIME,10,2) || SUBSTR(ENTERED_DATETIME,13,2) || SUBSTR(ENTERED_DATETIME,16,2) as AutomatonStartTime,
                LIMIT_PRICE "Limit",
                TRADING_QUANTITY AskedQuantity,
                0 MinPercentVolume,
                0 MaxPercentVolume,
                SUBSTR(EXPIRY_DATETIME,1,8) || SUBSTR(EXPIRY_DATETIME,10,2) || SUBSTR(EXPIRY_DATETIME,13,2) || SUBSTR(EXPIRY_DATETIME,16,2) as AutomatonEndTime,
                0.0 WouldLevel,
                '' ReferencePrice,
                '' TraderPrice,
                CUST_XP_DATA_6 as ExecutionStyle,
                'N' as CrossFlag,
                'N' as FlagSor,
                '' as MarketId,
                '<Operator_id>' as OperatorId,
                '<Server_id>' as ServerId,
                '' as DealId,
                '' as ExecMarketId,
                SUBSTR(ENTERED_DATETIME,1,8) || SUBSTR(ENTERED_DATETIME,10,2) || SUBSTR(ENTERED_DATETIME,13,2) || SUBSTR(ENTERED_DATETIME,16,2) as DealTime,
                0 as Quantity,
                0 as Price,
                '' as LiquitidyIndicator,
                '' as Filler1,
                'N' as FlagPrimaryOnly,
                CUST_XP_DATA_4
                from "orders"
                where ORDER_ID = ? and version = %d
            ''' % (version)
    row = db.selectOne(query, parent_order_id)

    start_time, end_time = get_start_time(parent_order_id, 1), get_end_time(parent_order_id, version)
    output.write(format(row, start_time, end_time) + '\n')

    # 2/ Get child order
    query = ''' select o.TradeDate || t.ID MsgId,
                'F' MsgType,
                root.ORDER_ID IdOccurence,
                o.PRIMARY_TRADER TraderId,
                'O' ClientOrderSource,
                root.ORDER_ID ClientOrderId,
                o.INSTRUMENT_CODE SecurityId,
                CASE o.MARKET_ID WHEN 'TOR-TSX' THEN 'TO' WHEN 'TOR-TVX' THEN 'TO' ELSE 'N' END RefCompl,
                CASE o.BUY_SELL WHEN 'B' THEN 0 ELSE 1 END Side,
                o.TRADEDATE OrderDate,
                root.CUST_XP_DATA_1 AS AutomatonName,
                0 FavorId,
                '' ClientData,
                o.COUNTERPARTY_CODE ClientRef,
                SUBSTR(o.ENTERED_DATETIME,1,8) || SUBSTR(o.ENTERED_DATETIME,10,2) || SUBSTR(o.ENTERED_DATETIME,13,2) || SUBSTR(o.ENTERED_DATETIME,16,2) as AutomatonStartTime,
                root.LIMIT_PRICE "Limit",
                root.TRADING_QUANTITY AskedQuantity,
                0 MinPercentVolume,
                0 MaxPercentVolume,
                SUBSTR(root.EXPIRY_DATETIME,1,8) || SUBSTR(root.EXPIRY_DATETIME,10,2) || SUBSTR(root.EXPIRY_DATETIME,13,2) || SUBSTR(root.EXPIRY_DATETIME,16,2) as AutomatonEndTime,
                0.0 WouldLevel,
                '' ReferencePrice,
                '' TraderPrice,
                o.CUST_XP_DATA_6 as ExecutionStyle,
                'N' as CrossFlag,
                'N' as FlagSor,
                t.EXECUTION_VENUE as MarketId,
                '<Operator_id>' as OperatorId,
                '<Server_id>' as ServerId,
                t.TRADE_ID as DealId,
                EXECUTION_ID as ExecMarketId,
                SUBSTR(t.ENTERED_DATETIME,1,8) || SUBSTR(t.ENTERED_DATETIME,10,2) || SUBSTR(t.ENTERED_DATETIME,13,2) || SUBSTR(t.ENTERED_DATETIME,16,2)  as DealTime,
                QUANTITY as Quantity,
                DEALT_PRICE as Price,
                LIQUIDITY_INDICATOR as LiquitidyIndicator,
                '' as Filler1,
                'N' as FlagPrimaryOnly,
                root.CUST_XP_DATA_4
                from orders root
                join orders o on o.root_order_id = root.root_order_id and o.version = 1
                join trades t on t.order_id = o.order_id and t.version = 1
                where root.version = %d
                and root.order_id = ? and o.order_id <> o.root_order_id
                order by t.ENTERED_DATETIME
            ''' % (version)

    for row in db.select(query, parent_order_id):
        output.write(format(row, start_time, end_time) + '\n')

def get_order_ids():
    '''
        Returns the list of "Bluebox" order id

        @return List of tuple (order_id, max_version)
    '''

    # we are making a join between bluebox orders in orders
    # and bluebox tables
    query = '''
        SELECT order_id, max(version) FROM (
                SELECT o.order_id as order_id, max(version) AS version FROM orders o
                  JOIN bluebox b ON o.order_id = b.order_id
                  WHERE parent_order_id NOT IN (select order_id from bluebox)
                  GROUP BY o.order_id
            UNION
                SELECT order_id, max(version) AS version FROM orders
                   WHERE cust_xp_data_1 != ''
                     AND cust_xp_data_1 NOT IN ('0','NONE','CS')
                     AND parent_order_id NOT IN (SELECT order_id FROM bluebox)
                   GROUP BY order_id
        )
        GROUP BY order_id
            '''
    tmp = set([(o[0], o[1]) for o in db.select(query)])
    tmp_order_ids = set([o[0] for o in tmp])
    order_ids = set()
    for order_id in tmp:

        # Make sure the version 1 is present, otherwise it means that
        # the order was already present yesterday
        row = db.selectOne(''' SELECT order_id FROM orders
                                WHERE order_id = ? and version = 1
                           ''', order_id[0])
        if not row:
            #print order_id[0]
            continue

        # Ma
        row = db.selectOne(''' SELECT root_order_id FROM orders
                                WHERE order_id = ? AND root_order_id <> order_id
                            ''', order_id[0])
        if not row or row[0] not in tmp_order_ids:
            if row:
                tmp = db.selectOne('SELECT order_id FROM orders WHERE order_id = ?', row[0])
                if tmp:
                    order_ids.add(order_id)
            else:
                order_ids.add(order_id)

    return order_ids

def clean_pid_file():
    '''
        Removes the PIDFILE if exists.
    '''
    pidfile = os.getenv("PIDFILE")

    try:
        if pidfile and os.path.exists(pidfile):
            os.remove(pidfile)
    except Exception, e:
        print >> sys.stderr, e

def usage(basename):
    print "Usage: %s [OPTIONS]" % basename;
    print ""
    print "Process Fidessa files to generate Bluebox algorithm log files"
    print ""
    print " OPTIONS:"
    print "   -h, --help               \t Display this help message"
    print "   -i, --in=IN_DIRECTORY    \t Input directory"
    print "   -o, --out=OUT_DIRECTORY  \t Output directory for algo logs"
    print "   -d, --date=DATE          \t Date to process, format should"
    print "                            \t be YYYYMMDD"
    print "   -f, --force              \t Should the output directory be erased"

    clean_pid_file()

def main(argv):
    # Set log file
    logfile = os.getenv('BLUEBOXALGOLOG_LOG_NAME')
    if logfile:
        sys.stdout = sys.stderr = open(logfile, 'a')
        print ' ===== %s ====' % (datetime.datetime.now())

    # Argument parsing
    try:
        opts, args = getopt.getopt(argv[1:], 'hi:o:d:f',
                                  ['help', 'in=', 'out=', 'date=', 'force'])
    except getopt.GetoptError:
        usage(argv[0])
        sys.exit(-1)

    input_directory = output_directory = ''
    range = [previous_weekday()]
    force = False

    for opt, val in opts:
        if opt in ("-h", "--help"):
            usage(argv[0])
            sys.exit(0)
        elif opt in ('-f', '--force'):
            force = True
        elif opt in ('-i', '--in'):
            input_directory = val
        elif opt in ('-o', '--out'):
            output_directory = val
        elif opt in ('-d', '--date'):
            if ':' in val:
                start, end = val.split(':')
                start, end = parse_date(start), parse_date(end)
                range = workday_range(start, end)
            else:
                start = parse_date(val)
                range = [start]

    for date in range:
        start = time.time()

        date = date.strftime('%Y%m%d')

        print "Extracting log for", date

        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        # Load fidessa data into the memory sqlite database
        try:
            orderdb.load_fidessa_data(db, date, input_directory)
        except Exception, e:
            traceback.print_exc()
            print e
            continue

        # Process orders
        orders = get_order_ids()
        print "%d orders to process" % len(orders)
        output = open(os.path.join(output_directory, 'audit_%s' % date), "w")
        for (order, version) in orders:
            log(order, version, output)

        output.close()

        print "Computation time: %.0f sec" % (time.time() - start)

if __name__ == '__main__':
    main(sys.argv)

