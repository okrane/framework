'''
Created on Sep 16, 2010

This module load orders and trades psv into a SQLite database.

@author: syarc
'''
from cheuvreux.fidessa.audit_trail import AuditTrail
from cheuvreux.fidessa.fidessa_util import get_base_dir_for_date
from csv import DictReader
import os
import gzip
from cheuvreux import fidessa


def create_schemas(db):
    ''' Create the needed table into the database "db" '''
    # 1. Create order table
    db.createTable('orders', '''
        "ORDER_ID" TEXT NOT NULL,
        "TRADEDATE" TEXT NOT NULL,
        "ID" INTEGER PRIMARY KEY NOT NULL,
        "PRIMARY_TRADER" TEXT NOT NULL,
        "INSTRUMENT_CODE" TEXT NOT NULL,
        "BUY_SELL" TEXT NOT NULL,
        "CUST_XP_DATA_1" TEXT,
        "CUST_XP_DATA_2" TEXT,
        "CUST_XP_DATA_3" TEXT,
        "CUST_XP_DATA_4" TEXT,
        "COUNTERPARTY_CODE" TEXT NOT NULL,
        "ENTERED_DATETIME" TEXT NOT NULL,
        "LIMIT_PRICE" REAL,
        "TRADING_QUANTITY" INTEGER NOT NULL,
        "EXPIRY_DATETIME" TEXT,
        "EXPIRY_TYPE" TEXT,
        "CUST_XP_DATA_6" TEXT,
        "VERSION" INTEGER NOT NULL,
        "PARENT_ORDER_ID" TEXT,
        "ROOT_ORDER_ID" TEXT,
        "VOLUME_DONE" INTEGER,
        "VOLUME_LEFT" INTEGER,
        "ORDER_NOTE_ID" TEXT,
        "ORDER_PRICE_TYPE_QUALIFIER" TEXT,
        "GROSS_PRICE" REAL,
        "MARKET_ID" TEXT,
        "BID_PRICE" TEXT,
        "OFFER_PRICE" REAL
        ''', True)
    # 2. Create order table
    db.createTable('trades', '''
        "ID" INTEGER NOT NULL,
        "TRADE_ID" TEXT NOT NULL,
        "TRADE_PART_INDEX" TEXT NOT NULL,
        "ORDER_ID" TEXT NOT NULL,
        "EXECUTION_ID" TEXT NOT NULL,
        "ENTERED_DATETIME" TEXT NOT NULL,
        "QUANTITY" INTEGER,
        "DEALT_PRICE" REAL,
        "LIQUIDITY_INDICATOR" TEXT,
        "EXECUTION_VENUE" TEXT,
        "VERSION" INTEGER NOT NULL,
        "BID_PRICE" REAL,
        "OFFER_PRICE" REAL
        ''', True)

    # 3. Create bluebox table
    db.createTable('bluebox', '''
            'ORDER_ID' TEXT NOT NULL PRIMARY KEY,
            'ORDER_DATETIME' TEXT NOT NULL,
            'BUY_SELL' TEXT NOT NULL,
            'TRADING_QUANTITY' INTEGER NOT NULL,
            'INSTRUMENT_CODE' TEXT NOT NULL,
            'PRIMARY_MARKET_ID' TEXT NOT NULL,
            'STRATEGY' TEXT NOT NULL,
            'QUANTITY_FILLED' INTEGER,
            'PCT_ADV' REAL,
            'GROSS_FILL_PRICE' REAL,
            'ALGO_FINISH_DATETIME' TEXT,
            'ALGO_START_DATETIME' TEXT,
            'ALGO_END_DATETIME' TEXT
            ''', True)


    # 3. Create index
    db.execute('CREATE INDEX "order_id_index" ON "orders" ("order_id" ASC, "version" ASC)')
    db.execute('CREATE INDEX "parent_order_id" ON "orders" ("parent_order_id" ASC)')
    db.execute('CREATE INDEX "root_order_id" ON "orders" ("root_order_id" ASC)')
    db.execute('CREATE INDEX "trader_order_id" ON "trades" ("order_id" ASC)')
    db.execute('CREATE UNIQUE INDEX "uniq_trade_idx" ON "trades" ("trade_id" ASC,"trade_part_index" ASC, "version" ASC)')
    db.execute('CREATE UNIQUE INDEX "uniq_order" ON "orders" ("order_id" ASC, "version" ASC)')

def load_order_file(db, date, ignore_dma = True, input_directory=None):
    ''' Load the orders file into the database

        @param db: SQLiteBase to use
        @param date: Date to load.
        @param ignore_dma: Ignore DMA order
        @param input_directory: Input directory to look for orders file
    '''

    if not input_directory:
        dir = get_base_dir_for_date(date)
    else:
        dir = input_directory

    if type(date) is not type(''):
        date = date.strftime('%Y%m%d')

    order_filename = os.path.join(dir, 'ORDER.%s.psv' % date)

    if not os.path.exists(order_filename):
        if os.path.exists(order_filename + '.gz'):
            order_filename = order_filename + '.gz'
        else:
            order_filename = fidessa.find_file('ORDER.%s.psv', date)
            if order_filename and not os.path.exists(order_filename):
                raise Exception('%s does not exist!' % order_filename)

    def order_data_generator(date, filename):
        tradedate = date
        if filename.endswith('.gz'):
            order_file = gzip.open(filename, 'rb')
        else:
            order_file = open(filename, 'r')
        reader = DictReader(order_file, delimiter='|')
        for line in reader:
            if not ignore_dma or line['PRIMARY_TRADER'] != 'DMA@CRAG.US':
                yield {'ORDER_ID': line['ORDER_ID'], 'TRADEDATE': tradedate,
                       'ID': line['#'], 'PRIMARY_TRADER': line['PRIMARY_TRADER'],
                       'INSTRUMENT_CODE': line['INSTRUMENT_CODE'], 'BUY_SELL': line['BUY_SELL'],
                       'CUST_XP_DATA_1': line['CUST_XP_DATA_1'], 'COUNTERPARTY_CODE': line['COUNTERPARTY_CODE'],
                       'ENTERED_DATETIME': line['ENTERED_DATETIME'], 'LIMIT_PRICE': line['LIMIT_PRICE'],
                       'TRADING_QUANTITY': line['TRADING_QUANTITY'], 'EXPIRY_DATETIME': line['EXPIRY_DATETIME'],
                       'CUST_XP_DATA_6': line['CUST_XP_DATA_6'], 'VERSION': line['VERSION'],
                       'PARENT_ORDER_ID': line['PARENT_ORDER_ID'], 'ROOT_ORDER_ID': line['ROOT_ORDER_ID'],
                       'CUST_XP_DATA_2': line['CUST_XP_DATA_2'], 'CUST_XP_DATA_3': line['CUST_XP_DATA_3'], 'CUST_XP_DATA_4': line['CUST_XP_DATA_4'],
                       'ORDER_NOTE_ID': line['ORDER_NOTE'], 'VOLUME_DONE': float(line['VOLUME_DONE']),
                       'VOLUME_LEFT': float(line['VOLUME_LEFT']), 'MARKET_ID': line['MARKET_ID'], 'GROSS_PRICE': float(line['GROSS_PRICE']),
                       'ORDER_PRICE_TYPE_QUALIFIER': line['ORDER_PRICE_TYPE_QUALIFIER'],
                       'BID_PRICE': line['BID_PRICE'], 'OFFER_PRICE': line['OFFER_PRICE'],
                       'EXPIRY_TYPE': line['EXPIRY_TYPE']}
        order_file.close()

    #for line in  order_data_generator(date, order_filename):
    #    print line
    stmt = db.prepareInsertStmt('orders',['ORDER_ID', 'TRADEDATE', 'ID', 'PRIMARY_TRADER', 'INSTRUMENT_CODE',
                                   'BUY_SELL', 'CUST_XP_DATA_1', 'COUNTERPARTY_CODE', 'ENTERED_DATETIME',
                                   'LIMIT_PRICE', 'TRADING_QUANTITY', 'EXPIRY_DATETIME', 'CUST_XP_DATA_6',
                                   'VERSION', 'PARENT_ORDER_ID', 'ROOT_ORDER_ID', 'CUST_XP_DATA_2', 'CUST_XP_DATA_3', 'ORDER_NOTE_ID',
                                   'VOLUME_DONE', 'VOLUME_LEFT', 'MARKET_ID', 'GROSS_PRICE', 'ORDER_PRICE_TYPE_QUALIFIER',
                                   'BID_PRICE', 'OFFER_PRICE','EXPIRY_TYPE', 'CUST_XP_DATA_4'])
    db.execManyPreparedInsertStmt(stmt, order_data_generator(date, order_filename))

def load_trade_file(db, date, input_directory=None):
    ''' Load the trades file into the database

        @param db: SQLiteBase to use
        @param date: Date to load.
        @param input_directory: Input directory to look for orders file
    '''

    if not input_directory:
        dir = get_base_dir_for_date(date)
    else:
        dir = input_directory

    if type(date) is not type(''):
        date = date.strftime('%Y%m%d')

    trade_filename = os.path.join(dir, 'TRADE_SUMMARY.%s.psv' % date)
    if not os.path.exists(trade_filename):
        # Try harder
        trade_filename = fidessa.find_file('TRADE_SUMMARY.%s.psv', date)
        if not os.path.exists(trade_filename):
            raise Exception('%s does not exist!' % trade_filename)

    def trade_data_generator(filename):

        if filename.endswith('.gz'):
            order_file = gzip.open(filename, 'rb')
        else:
            order_file = open(filename, 'r')

        reader = DictReader(order_file, delimiter='|')
        for line in reader:
            if line['ENTERED_BY'] != 'DMA@CRAG.US':
                yield {'ORDER_ID': line['ORDER_ID'], 'ID': line['#'], 'TRADE_ID': line['TRADE_ID'],
                       'TRADE_PART_INDEX': line['TRADE_PART_INDEX'], 'EXECUTION_ID': line['EXECUTION_ID'],
                       'ENTERED_DATETIME': line['ENTERED_DATETIME'], 'QUANTITY': line['QUANTITY'],
                       'DEALT_PRICE': line['DEALT_PRICE'], 'LIQUIDITY_INDICATOR': line['LIQUIDITY_INDICATOR'],
                       'EXECUTION_VENUE': line['EXECUTION_VENUE'], 'VERSION': line['VERSION'],
                       'BID_PRICE': line['BID_PRICE'], 'OFFER_PRICE': line['OFFER_PRICE']}
        order_file.close()

    #for line in  order_data_generator(date, order_filename):
    #    print line
    stmt = db.prepareInsertStmt('trades',['ORDER_ID', 'TRADE_ID', 'ID', 'EXECUTION_ID', 'TRADE_PART_INDEX',
                                   'ENTERED_DATETIME', 'QUANTITY', 'DEALT_PRICE', 'LIQUIDITY_INDICATOR',
                                   'EXECUTION_VENUE', 'VERSION', 'BID_PRICE', 'OFFER_PRICE'])
    db.execManyPreparedInsertStmt(stmt, trade_data_generator(trade_filename))

def load_bluebox_file(db, date, input_directory=None):
    ''' Load the bluebox file into the database

        @param db: SQLiteBase to use
        @param date: Date to load.
        @param input_directory: Input directory to look for orders file
    '''

    if not input_directory:
        dir = get_base_dir_for_date(date)
    else:
        dir = input_directory

    if type(date) is not type(''):
        date = date.strftime('%Y%m%d')

    bbox_filename = os.path.join(dir, 'BLUEBOX_ORDER_DETAILS.%s.psv' % date)
    if not os.path.exists(bbox_filename):
        # Try harder
        bbox_filename = fidessa.find_file('BLUEBOX_ORDER_DETAILS.%s.psv', date)
        if not bbox_filename or not os.path.exists(bbox_filename):
            raise Exception('%s does not exist!' % bbox_filename)

    def bbox_data_generator(filename):

        if filename.endswith('.gz'):
            file = gzip.open(filename, 'rb')
        else:
            file = open(filename, 'r')
        reader = DictReader(file, delimiter='|')
        for line in reader:
            if 'ALGO_FINISH_DATETIME' in line:
                algo_finish_datetime = line['ALGO_FINISH_DATETIME']
            else:
                algo_finish_datetime  = ''
            yield {'ORDER_ID': line['ORDER_ID'], 'ORDER_DATETIME': line['ORDER_DATETIME'], 'BUY_SELL': line['BUY_SELL'],
                       'TRADING_QUANTITY': line['TRADING_QUANTITY'], 'INSTRUMENT_CODE': line['INSTRUMENT_CODE'],
                       'PRIMARY_MARKET_ID': line['PRIMARY_MARKET_ID'], 'STRATEGY': line['STRATEGY'],
                       'QUANTITY_FILLED': line['QUANTITY_FILLED'], 'PCT_ADV': line['PCT_ADV'],
                       'GROSS_FILL_PRICE': line['GROSS_FILL_PRICE'], 'ALGO_FINISH_DATETIME': algo_finish_datetime,
                       'ALGO_START_DATETIME': line['ALGO_START_DATETIME'], 'ALGO_END_DATETIME': line['ALGO_END_DATETIME']
                       }
        file.close()

    stmt = db.prepareInsertStmt('bluebox',['ORDER_ID', 'ORDER_DATETIME', 'BUY_SELL', 'TRADING_QUANTITY', 'INSTRUMENT_CODE',
                                   'PRIMARY_MARKET_ID', 'STRATEGY', 'QUANTITY_FILLED', 'PCT_ADV',
                                   'GROSS_FILL_PRICE', 'ALGO_FINISH_DATETIME', 'ALGO_START_DATETIME', 'ALGO_END_DATETIME'])
    db.execManyPreparedInsertStmt(stmt, bbox_data_generator(bbox_filename))


def load_fidessa_data(db, date, input_directory=None):
    ''' Load the fidessa orders and trades file into the database

        @param db: SQLiteBase to use
        @param date: Date to load.
        @param input_directory: If specified, use this directory to look for files
    '''

    create_schemas(db)
    load_order_file(db, date, True, input_directory)
    load_trade_file(db, date, input_directory)
    load_bluebox_file(db, date, input_directory)

#if __name__ == '__main__':
#from cheuvreux.dbtools.sqlite import SQLiteBase
#import time
#import datetime
#    start = time.clock()
#    db = SQLiteBase('fidessa.db')
#    create_schemas(db)
#    load_order_file(db, datetime.date(2010, 9, 15))
#    load_trade_file(db, datetime.date(2010, 9, 15))
#    print time.clock() - start
