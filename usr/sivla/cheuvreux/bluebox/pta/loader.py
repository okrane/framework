'''
Created on Jun 24, 2011

@author: syarc
'''
from cheuvreux import fidessa
from cheuvreux.dbtools import Sqlite
from cheuvreux.dbtools.Sqlite import SQLiteBase
from cheuvreux.dbtools.odbc import ODBC
from cheuvreux.fidessa import rebate, FIDESSA_BASE_DIR, fidessadb
from cheuvreux.fidessa.audit_trail import AuditTrail
from cheuvreux.fidessa.fidessa import FidessaTrade
from cheuvreux.fidessa.fidessadb import FidessaDB
from cheuvreux.utils.date import go_ahead, toMicroseconds
from cheuvreux.utils.decorator import cache
from cheuvreux.utils.statistics import quantile, variance
from collections import defaultdict
import datetime
import math
import os
import time
import csv
import gzip
from cheuvreux.bluebox.pta import opportunity
import traceback

ignore_stocks = ['ZVZZT', 'ZXZZT', 'ZWZZT']
fid = FidessaDB()._backend

client_map = {}

def process_nms_orders(date, db, result_db):
    file = fidessa.find_file('ORDER_EVENTS.%s.psv', date)

    if file.endswith('.gz'):
        fd = gzip.open(file, 'rb')
    else:
        fd = open(file, 'r')

    reader = csv.DictReader(fd, delimiter = '|')


    nms_order_ids = set()
    orders = defaultdict(list)
    omar_to_nms = defaultdict (lambda: [None, None])
    for line in reader:
        if line['EVENT_CODE'] == 'P' and line['EVENT_ID'] == '5':
            if line['ACTION_BY'] == 'BLUEBOX':
                orders[line['NMS_ORDER_ID']].append(line)
                nms_order_ids.add(line['NMS_ORDER_ID'])
            elif not omar_to_nms[line['OMAR_ORDER_ID']][0]:
                omar_to_nms[line['OMAR_ORDER_ID']][0] = AuditTrail.getFastDateTime(line['EVENT_DATETIME'])
        elif line['NMS_ORDER_ID'] in nms_order_ids:
            orders[line['NMS_ORDER_ID']].append(line)
        elif line['EVENT_CODE'] == 'C' and line['OMAR_ORDER_ID'] in omar_to_nms:
            if line['EVENT_ID'] == '1' and not omar_to_nms[line['OMAR_ORDER_ID']][1]:
                omar_to_nms[line['OMAR_ORDER_ID']][1] = AuditTrail.getFastDateTime(line['EVENT_DATETIME'])
            elif  line['EVENT_ID'] == '10' and not omar_to_nms[line['OMAR_ORDER_ID']][1]:
                omar_to_nms[line['OMAR_ORDER_ID']][1] = AuditTrail.getFastDateTime(line['EVENT_DATETIME'])

    def generator(orders):
        for nms_order_id, lines in orders.iteritems():
            trades = []
            start_time = None
            data = {'ORDER_ID': nms_order_id, 'DATE': date, 'INSTRUMENT': '',
                'SIDE': '', 'QUANTITY': '', 'DONE': '', 'LIMIT_PRICE': '',
                'DESTINATION': '', 'BID': '', 'ASK': '', 'NOTES': '',
                'TYPE': '', 'MARKET_ID': '', 'COUNTERPARTY': '',
                'ROOT_ORDER_ID': '', 'GROSS_PRICE': '', 'START_TIME': '', 'END_TIME': '',
                'PRICE_IMPROVEMENT': '', 'COST': '', 'EXPIRY_TYPE': '',
                'ORDER_PRICE_TYPE_QUALIFIER': '', 'PARENT_ORDER_ID': '', 'ACK_LATENCY':''}
            for line in lines:
                if line['EVENT_CODE'] == 'P' and line['EVENT_ID'] == '5' and data['START_TIME'] == '':
                    start_time = AuditTrail.getFastDateTime(line['EVENT_DATETIME'])
                    data['START_TIME'] = start_time.strftime('%H:%M:%S') + '.%06d' % start_time.microsecond if start_time.time() else ''
                    data['DESTINATION'] = 'NMS ' + line['COUNTERPARTY_CODE']
                    data['INSTRUMENT'] = line['INSTRUMENT_CODE']
                    data['PARENT_ORDER_ID'] = line['OMAR_ORDER_ID']
                    data['LIMIT_PRICE'] = float(line['LIMIT_PRICE'])
                    data['QUANTITY'] = float(line['QUANTITY_REMAINING'])
                    if line['TIF'] == 'OO': # On Open
                        data['EXPIRY_TYPE'] = 'GFD'
                        data['ORDER_PRICE_TYPE_QUALIFIER'] = 'OPEN'
                    elif line['TIF'] == 'OC': # On Close
                        data['EXPIRY_TYPE'] = 'GFD'
                        data['ORDER_PRICE_TYPE_QUALIFIER'] = 'CLSE'
                    elif line['TIF'] == '0:00':
                        data['EXPIRY_TYPE'] = 'IOC'
                    elif line['TIF'] == 'D':
                        data['EXPIRY_TYPE'] = 'GFD'

                    order = db.selectOne('SELECT * FROM orders WHERE order_id = ? ORDER BY cast(version as integer) DESC', line['OMAR_ORDER_ID'])
                    if order:
                        data['MARKET_ID'] = order['MARKET_ID']
                        data['SIDE'] = 1 if order['BUY_SELL'] == 'B' else -1
                        data['BID'] = float(order['BID_PRICE']) # TODO when bid in the audit trail
                        data['ASK'] = float(order['OFFER_PRICE']) # TODO when ask in the audit trail
                        data['NOTES'] = '' # TODO
                        data['ORDER_PRICE_TYPE_QUALIFIER'] = '' # TODO
                        data['ROOT_ORDER_ID'] = order['ROOT_ORDER_ID']
                        data['COUNTERPARTY'] = order['COUNTERPARTY_CODE']

                        if data['LIMIT_PRICE'] <= 0.0:
                            data['TYPE'] = 'A'
                        elif data['SIDE'] == 1:
                            if data['LIMIT_PRICE'] >= data['BID']:
                                data['TYPE'] = 'A'
                            elif data['LIMIT_PRICE'] > data['ASK']:
                                data['TYPE'] = 'I'
                            else:
                                data['TYPE'] = 'P'
                        else:
                            if data['LIMIT_PRICE'] >= data['ASK']:
                                data['TYPE'] = 'P'
                            elif data['LIMIT_PRICE'] > data['BID']:
                                data['TYPE'] = 'I'
                            else:
                                data['TYPE'] = 'A'


                elif line['EVENT_CODE'] == 'C' and line['EVENT_ID'] == '1':
                    data['ACK_LATENCY'] = toMicroseconds(AuditTrail.getFastDateTime(line['EVENT_DATETIME']) - start_time)/1000.0

                elif line['EVENT_CODE'] == 'C' and line['EVENT_ID'] == '15':
                    rows = db.select ('SELECT order_id, dealt_price, quantity, execution_venue, liquidity_indicator, buy_sell, bid_price, offer_price, executor_id FROM trades WHERE contributor_ref = ?',
                                      data['DATE'] + line['EXECUTION_ID'])
                    for row in rows:
                        trades.append(FidessaTrade(row))
                elif line['EVENT_CODE'] == 'C' and line['EVENT_ID'] in ('10','8'):
                    end_time = AuditTrail.getFastDateTime(line['EVENT_DATETIME']).time()
                    data['END_TIME'] = end_time.strftime('%H:%M:%S') + '.%06d' % end_time.microsecond if end_time else ''
                    data['DONE'] = float(line['TOTAL_QUANTITY_FILLED'])
                    data['GROSS_PRICE'] = float(line['AVERAGE_PRICE'])

                    # TODO Improve the computation by using each fills
                    if data['SIDE'] == 1:
                        pi = data['ASK'] - data['GROSS_PRICE']
                    else:
                        pi = data['GROSS_PRICE'] - data['BID']
                    data['PRICE_IMPROVEMENT'] = max(0.0, data['DONE'] * pi)


                    data['COST'] = rebate.get_rebate_for_trade(trades)
            yield data

    data = {'ORDER_ID': '',
            'DATE': '',
            'INSTRUMENT': '',
            'SIDE': '',
            'QUANTITY': '',
            'DONE': '',
            'LIMIT_PRICE': '',
            'DESTINATION': '',
            'BID': '', 'ASK': '',
            'NOTES': '',
            'TYPE': '',
            'MARKET_ID': '',
            'COUNTERPARTY': '',
            'ROOT_ORDER_ID': '',
            'GROSS_PRICE': '',
            'START_TIME': '',
            'END_TIME': '',
            'PRICE_IMPROVEMENT': '',
            'COST': '',
            'EXPIRY_TYPE': '',
            'ORDER_PRICE_TYPE_QUALIFIER': '',
            'PARENT_ORDER_ID': '',
            'ACK_LATENCY':''}

    stmt = ('INSERT INTO "orders" ('
                 + ','.join(data.keys()) + ') VALUES ('
                 + ','.join(['?']*len(data)) + ')'
                 )

    data = [g.values() for g in generator(orders)]
    if len(data) > 0:
        result_db.executemany(stmt, data)

    return omar_to_nms


def load_fid_data_in_memory(date):
    db = SQLiteBase(':memory:')

    while (not AuditTrail.is_done_file_present(date)):
        print 'Waiting for done file ...'
        time.sleep(5)

    Sqlite.create_table_from_csv(db, fidessa.find_file('ORDER.%s.psv', date), 'orders', True, '|', ['#'])
    Sqlite.create_table_from_csv(db, fidessa.find_file('TRADE_SUMMARY.%s.psv', date), 'trades', True, '|', ['#'])
    Sqlite.create_table_from_csv(db, fidessa.find_file('BLUEBOX_ORDER_DETAILS.%s.psv', date), 'bluebox', True, '|', ['#'])
    db.createIndex('idx_order_id', 'orders', 'order_id', False)
    db.createIndex('idx_trades_id', 'trades', 'order_id', False)
    db.createIndex('idx_contrib_ref', 'trades', 'contributor_ref', False)

    return db

def parse_algo_name(executor, order, db):
    index = executor.find('_')
    if index > 0:
        if executor[0:index] == 'BETA':
            index = executor.find('_', index+1)
            if index > 0:
                algo = executor[5:index]
            else:
                algo = executor[5:]
        else:
            algo = executor[0:index]
    else:
        algo = executor

    if algo == 'CUSTOM':

        strategy = db.selectOne('SELECT strategy FROM bluebox WHERE order_id = ?', order['order_id'])
        if strategy:
            algo = strategy[0]
        else:
            xp_data_4 = order['CUST_XP_DATA_4']
            if xp_data_4 in ('PASS', 'SMARTEXP'):
                algo = 'SMARTX'
            elif xp_data_4 in ('AGGR', 'MIDPOINT', 'STRIKE'):
                algo = 'CROSSFIRE'
            elif xp_data_4[0:10] == 'FLOAT_TWAP':
                algo = 'TWAPDPL'
            elif xp_data_4[0:9] == 'FLOAT_PEG':
                algo = 'PEGDPL'
            elif xp_data_4 == 'STOP':
                algo = 'STOP'
            elif xp_data_4 == 'FLOAT':
                algo = 'FLOAT'

    return algo

@cache
def get_client_name(name):

    query = ''' SELECT  isnull(a.Name, isnull(cli.client, 'Unknown'))
                FROM KYCDatabase..[Client xref] cli
                LEFT JOIN KYCDatabase..AccountGroups a ON cli.CustomerNumber = a.CustomerNumber
                WHERE cli.HighTouchClient = ?

             '''
    try:
        return fid.select(query, name)[0][0]
    except IndexError:
        return name

def get_start_time(start, order_id, xp2,db):
    bluebox = db.selectOne('SELECT algo_start_datetime FROM bluebox WHERE order_id = ?', order_id)
    if bluebox and len(bluebox['algo_start_datetime']) > 0:
        real_start = max(bluebox['algo_start_datetime'][9:24], start)
    elif xp2 and len(xp2) > 0:
        real_start = max(start,xp2[0:15])
    else:
        real_start = start

    return real_start

def process_algo_generator(date, db, audit):

    for order_id in audit:

        order = None
        algo = ''
        bluebox2 = 0

        start = end = None

        for fields in audit[order_id]:
            if fields[1] in ('ASSN', 'CHLD', 'AGYE') and fields[5] in ('BLUEBOX2','BLUEBOX'):
                order = db.selectOne('SELECT * FROM orders WHERE order_id = ? ORDER BY cast(version as integer) DESC', order_id)

                algo = parse_algo_name(fields[6], order, db)
                if fields[5] == 'BLUEBOX2':
                    bluebox2 = 1
                start = AuditTrail.getFastDateTime(fields[2]).time().strftime('%H:%M:%S.%f')
                start = get_start_time(start, order_id, order['CUST_XP_DATA_2'],db)
            if fields[1] in  ('COMP','ESOR', 'RELE') and start:
                end = AuditTrail.getFastDateTime(fields[2]).time().strftime('%H:%M:%S.%f')

        if order and order['INSTRUMENT_CODE'] not in ignore_stocks:
            parent_order_id = order['parent_order_id']
            if parent_order_id == '':
                parent_order_id = None
            yield {'ORDER_ID': order_id, 'DATE': date, 'ALGO': algo,
                   'COUNTERPARTY': order['counterparty_code'], 'PARENT_ORDER_ID': parent_order_id,
                   'QUANTITY': int(float(order['trading_quantity'])), 'DONE': int(float(order['volume_done'])),'GROSS_PRICE':order['gross_price'],
                   'XP_DATA_6': order['CUST_XP_DATA_6'], 'START_TIME': start, 'END_TIME': end,
                   'ENTERED_BY': order['ENTERED_BY'], 'CLIENT': get_client_name(order['counterparty_code']),
                   'SIDE':  1 if order['BUY_SELL'] == 'B' else -1, 'INSTRUMENT': order['INSTRUMENT_CODE'],
                   'LIMIT_PRICE': order['LIMIT_PRICE'],'MARKET_ID': order['MARKET_ID'], 'BLUEBOX2': bluebox2}

def process_orders_generator(date, db, audit, latency_nms, omar_to_nms):

    omar2nms = open('omar2nms_%s.txt' % date,'w')

    for order_id in audit:
        order = None
        bid, ask = 0.0, 0.0
        destination = None
        start_time, end_time = None, None
        ack_latency = None
        latency_start_time = latency_end_time = None
        use_nms = False
        pi = 0


        for fields in audit[order_id]:
            if bid == 0.0:
                bid = float(fields[12])
            if ask == 0.0:
                ask = float(fields[13])

            if not destination:
                destination = fields[5] + ' ' + fields[6]
                use_nms = fields[5] in ('POST', 'SWEEP')
            if not start_time:
                start_time = AuditTrail.getFastDateTime(fields[2]).time()

            if fields[1] in ('COMP','ESOR'):
                end_time = AuditTrail.getFastDateTime(fields[2]).time()
                if not latency_end_time:
                    latency_end_time = audit.getFastDateTime(fields[2])

            elif fields[1] in ('CHLD') and fields[5] not in ('BLUEBOX', 'BLUEBOX2'):
                latency_start_time = audit.getFastDateTime(fields[2])
                if not order:
                    order = db.selectOne('SELECT * FROM orders WHERE order_id = ? ORDER BY cast(version as integer) DESC', order_id)
            elif fields[1] == 'ESOA':
                evt_time = audit.getFastDateTime(fields[2])
                if not use_nms and not latency_end_time:
                    latency_end_time = evt_time
                elif fields[9] in ('NMS Post Orders accepted order', 'NMS Sweep Orders accepted order') and latency_start_time:
                    accept_time = audit.getFastDateTime(fields[2])
                    if order_id in omar_to_nms:
                        print >> omar2nms, ','.join(map(str,[order_id, latency_start_time, omar_to_nms[order_id][0]]))
                        latency_nms.append(toMicroseconds(omar_to_nms[order_id][0] - latency_start_time) / 1000.0)
                    else:
                        latency_nms.append(toMicroseconds(accept_time - latency_start_time) / 1000.0)
                    latency_start_time = accept_time
            elif fields[1] in ('ESRT','ESOR') and not latency_end_time:
                latency_end_time = audit.getFastDateTime(fields[2])

            elif fields[1] in ('EXEE'):
                try:
                    fill_qty, price, trade_id = AuditTrail.parseFill(fields[9])
                    if  order['expiry_type'] == 'IOC':
                        temp_bid, temp_ask = bid, ask
                    else:
                        temp_bid, temp_ask = float(fields[12]), float(fields[13])

                    if order:
                        if order['BUY_SELL'] == 'B':
                            pi += max(0.0, float(fill_qty) * (temp_ask - float(price)))
                        else:
                            pi += max(0.0, float(fill_qty) * (float(price) - temp_bid))
                except TypeError, e:
                    print e
                    print fields
                    print fields[9]

        if order and bid >= 0.0 and ask >= 0.0 and order['instrument_code'] not in ignore_stocks:
            limit_price = float(order['limit_price'])

            order_type = None

            if 'midpoint peg' in order['ORDER_NOTE'].lower():
                order_type = 'M'
            elif order['buy_sell'] == 'B':
                if limit_price == 0.0 or limit_price >= ask:
                    order_type = 'A'
                elif limit_price <= bid:
                    order_type = 'P'
                else:
                    order_type = 'I'
            else:
                if limit_price == 0.0 or limit_price <= bid:
                    order_type = 'A'
                elif limit_price >= ask:
                    order_type = 'P'
                else:
                    order_type = 'I'

            rows = db.select ('SELECT order_id, dealt_price, quantity, execution_venue, liquidity_indicator, buy_sell, bid_price, offer_price, executor_id FROM trades WHERE order_id = ?', order_id)
            trades = []
            for row in rows:
                trades.append(FidessaTrade(row))

            # For aggressive IOC orders, we take the arrival bid / ask instead of the one
            # computed at trades time
            if float(order['VOLUME_DONE']) > 0 and order_type in ('A') and order['expiry_type'] == 'IOC' :
                if order['BUY_SELL'] == 'B':
                    pi = ask - float(order['GROSS_PRICE'])
                else:
                    pi = float(order['GROSS_PRICE']) - bid

                pi = pi * float(order['VOLUME_DONE'])
            cost = rebate.get_rebate_for_trade(trades)

            if order_id in omar_to_nms:
                entered_time, ack_time = omar_to_nms[order_id][0], omar_to_nms[order_id][1]
                if entered_time and ack_time:
                    ack_latency = toMicroseconds(ack_time - entered_time) / 1000.0
                else:
                    print order_id, ack_time, entered_time

            if not ack_latency and latency_end_time and latency_start_time:
                ack_latency = toMicroseconds(latency_end_time - latency_start_time) / 1000.0

            data = {'ORDER_ID': order_id,
                    'DATE': date,
                    'INSTRUMENT': order['INSTRUMENT_CODE'],
                    'SIDE': 1 if order['BUY_SELL'] == 'B' else -1,
                    'QUANTITY': int(float(order['TRADING_QUANTITY'])),
                    'DONE': int(float(order['VOLUME_DONE'])),
                    'LIMIT_PRICE': limit_price,
                    'DESTINATION': destination.strip(),
                    'BID': bid, 'ASK': ask,
                    'NOTES': order['ORDER_NOTE'],
                    'TYPE': order_type,
                    'MARKET_ID': order['MARKET_ID'],
                    'COUNTERPARTY': order['counterparty_code'],
                    'ROOT_ORDER_ID': order['root_order_id'],
                    'GROSS_PRICE': float(order['GROSS_PRICE']),
                    'START_TIME': start_time.strftime('%H:%M:%S') + '.%06d' % start_time.microsecond if start_time else '',
                    'END_TIME': end_time.strftime('%H:%M:%S') + '.%06d' % end_time.microsecond if end_time else '',
                    'PRICE_IMPROVEMENT': max(0.0, pi),
                    'COST': cost,
                    'EXPIRY_TYPE': order['expiry_type'],
                    'ORDER_PRICE_TYPE_QUALIFIER': order['ORDER_PRICE_TYPE_QUALIFIER'],
                    'PARENT_ORDER_ID': order['PARENT_ORDER_ID'],
                    'ACK_LATENCY': ack_latency}
            yield data

    omar2nms.close()

def is_data_present(date, result_db):
    row = result_db.selectOne('SELECT 1 FROM orders WHERE date = ?', date)

    return (row is not None and row[0] == 1)

def compute_latency(date, result_db):
    def latency_generator():
        rows = result_db.select(''' SELECT destination, ack_latency, order_id FROM orders
                                     WHERE date = '%s' ''' % date)

        latencies = defaultdict(list)
        for row in rows:
            if row[1] is None:
                continue

            service = row[0].split(' ')[0] if row[0].find(' ') > 0 else row[0]
            if row[0].startswith('POST BB_POST') and row[0].count('_') > 2:
                idx = row[0].rfind('_')
                row[0] = row[0][0:idx]
            elif service not in ('POST', 'SWEEP', 'LIMED'):
                row[0] = service


            latencies[row[0]].append(row[1])

        for dest, latency in latencies.iteritems():
            latency.sort()
            yield {'DATE': date,
                   'DESTINATION': dest,
                   'Q25': quantile(latency,.25,True),
                   'Q50': quantile(latency,.5,True),
                   'Q75': quantile(latency,.75,True),
                   'Q99' : quantile(latency,.99,True),
                   'MAX' : latency[-1],
                   'STDEV' : math.sqrt(variance(latency)),
                   'NB_ORDERS': len(latency)}

    data = {'DATE': '', 'DESTINATION': '',
             'Q25': '',  'Q50': '', 'Q75': '',
             'Q99' : '', 'MAX' : '', 'STDEV' : '', 'NB_ORDERS' : '' }

    stmt = ('INSERT INTO "latency" ('
                 + ','.join(data.keys()) + ') VALUES ('
                 + ','.join(['?']*len(data)) + ')'
                 )

    #a = [g.values() for g in latency_generator()]

    result_db.executemany(stmt, [g.values() for g in latency_generator()])


def compute_algo_cost(date, result_db):
    # If done = 0 -> cost = 0
    result_db.run(''' UPDATE algo_orders SET cost = 0
                      WHERE done = 0 AND date = '%s' ''' % date)

    result_db.run('IF OBJECT_ID(\'tempdb..#cost\') IS NOT NULL DROP TABLE #cost')

    # Update cost for root order
    result_db.run(''' SELECT a.order_id, sum(o.cost) as cost INTO #cost
                        FROM algo_orders a
                        JOIN orders o on o.algo_root_order_id = a.order_id
                        WHERE a.date = '%s' AND a.done > 0 AND a.cost IS NULL
                        GROUP BY a.order_id
                  ''' % date)

    result_db.run(''' UPDATE a SET a.cost = t.cost
                        FROM algo_orders a
                        JOIN #cost t on t.order_id = a.order_id
                  ''')

    # Update cost for parent order
    result_db.run('drop table #cost')
    result_db.run(''' SELECT a.order_id, sum(o.cost) as cost INTO #cost
                        FROM algo_orders a
                        JOIN orders o on o.parent_order_id = a.order_id
                        WHERE a.date = '%s' and a.done > 0 AND a.cost IS NULL
                        GROUP BY a.order_id
                  ''' % date)
    result_db.run(''' UPDATE a SET a.cost = t.cost
                        FROM algo_orders a
                        JOIN #cost t on t.order_id = a.order_id
                  ''')


def process_date(date, result_db):

    db = load_fid_data_in_memory(date)

    audit = AuditTrail(date)
    audit.loadData(True)

    # Handle slice sent directly to NMS by an BLUEBOX2
    omar_to_nms = process_nms_orders(date, db, result_db)

    data = {'ORDER_ID': '',
            'DATE': '',
            'INSTRUMENT': '',
            'SIDE': '',
            'QUANTITY': '',
            'DONE': '',
            'LIMIT_PRICE': '',
            'DESTINATION': '',
            'BID': '', 'ASK': '',
            'NOTES': '',
            'TYPE': '',
            'MARKET_ID': '',
            'COUNTERPARTY': '',
            'ROOT_ORDER_ID': '',
            'GROSS_PRICE': '',
            'START_TIME': '',
            'END_TIME': '',
            'PRICE_IMPROVEMENT': '',
            'COST': '',
            'EXPIRY_TYPE': '',
            'ORDER_PRICE_TYPE_QUALIFIER': '',
            'PARENT_ORDER_ID': '',
            'ACK_LATENCY':''}


    stmt = ('INSERT INTO "orders" ('
                 + ','.join(data.keys()) + ') VALUES ('
                 + ','.join(['?']*len(data)) + ')'
                 )

    latency_nms = []
    data = process_orders_generator(date, db, audit, latency_nms, omar_to_nms)
    result_db.executemany(stmt, [g.values() for g in data])
    latency_nms.sort()

    if len(latency_nms) > 0:
        nms_latency_query = ''' INSERT INTO latency (DATE, DESTINATION, Q25, Q50, Q75, Q99, MAX, STDEV, NB_ORDERS)
                                VALUES
                                ('%s', '%s', '%f', '%f', '%f','%f','%f','%f', '%d')
                            ''' % (date, 'NMS Internal', quantile(latency_nms, .25, True), quantile(latency_nms, .5, True),
                                   quantile(latency_nms, .75, True), quantile(latency_nms, .99, True), latency_nms[-1],
                                   math.sqrt(variance(latency_nms)), len(latency_nms))
        result_db.execDirect(nms_latency_query)

    data = {'ORDER_ID': '', 'DATE': '', 'ALGO': '', 'COUNTERPARTY': '', 'PARENT_ORDER_ID':'',
            'QUANTITY':'','DONE':'', 'GROSS_PRICE':'', 'XP_DATA_6': '', 'START_TIME': '', 'END_TIME': '',
            'ENTERED_BY': '', 'CLIENT': '', 'SIDE': '', 'INSTRUMENT': '', 'LIMIT_PRICE': '', 'MARKET_ID': '',
            'BLUEBOX2': ''}

    stmt = ('INSERT INTO "algo_orders" ('
                 + ','.join(data.keys()) + ') VALUES ('
                 + ','.join(['?']*len(data)) + ')'
                 )
    result_db.executemany(stmt, [g.values() for g in process_algo_generator(date, db, audit)])

    # Set algo_root_order_id

    result_db.run(''' UPDATE o SET algo_root_order_id = o.root_order_id
                      FROM orders o
                      JOIN algo_orders p ON o.root_order_id = p.order_id
                      WHERE o.date = '%s'
                   ''' % date)
    result_db.run(''' UPDATE o SET algo_root_order_id = ISNULL(p.parent_order_id, p.order_id)
                      FROM orders o
                      JOIN algo_orders p ON o.parent_order_id = p.order_id
                      WHERE o.date = '%s' AND algo_root_order_id IS NULL
                   ''' % date)

    compute_algo_cost(date, result_db)

    compute_latency(date, result_db)

    # clean up parent order id
    result_db.run(''' UPDATE algo_orders SET parent_order_id = NULL
                          WHERE parent_order_id != '' AND parent_order_id NOT IN
                              (SELECT order_id FROM algo_orders)
                            AND date = '%s'
                      ''' % (date))

def delete_date(date, result_db):
    result_db.run('DELETE FROM algo_orders WHERE date = ?' , date)
    result_db.run('DELETE FROM orders WHERE date = ?' , date)
    result_db.run('DELETE FROM latency WHERE date = ?', date)

def process_range (range, force, output):
    result_db = fidessadb.getODBCConnection(autocommit=False) #ODBC('DRIVER={SQL Server};SERVER=nysql001;DATABASE=quant;UID=syarc;PWD=syarc', autocommit=False)

    for date in range:
        str_date = date.strftime('%Y%m%d')
        if is_data_present(str_date,result_db):
            if force:
                print >> output, 'Deleting %s' % date
                delete_date(str_date, result_db)
            else:
                print >> output, 'Skipping %s' % date
                continue

        start = time.clock()
        process_date(str_date, result_db)
        result_db._conn.commit()
        print >> output, "Computation time for %s: %f" % (date, time.clock() - start)
        
    # Process experiment done by fwk 1
    try:
        opportunity.process_vwap(range)
        opportunity.process_reduce(range)
        opportunity.process(range)
    except Exception, e:
        traceback.print_exc(file=output)

if __name__ == '__main__':
    #load_fid_data_in_memory('20120217')
#    process_nms_orders(datetime.date(2012,2,17), SQLiteBase('temp.db'), ODBC('DRIVER={SQL Server};SERVER=nysql001;DATABASE=quant;UID=syarc;PWD=syarc'))

#    import sys
#    result_db = fidessadb.getODBCConnection(autocommit=False)
#   process_date('20120229', result_db)

#    a, b = '07:00:01.00000 ','08:22:22.2660840'
#    print a,b,max(a,b)
    result_db = fidessadb.getODBCConnection(autocommit=False)
#    compute_algo_cost('20120524', result_db)
#    compute_algo_cost('20120525', result_db)
#    compute_algo_cost('20120526', result_db)
#    compute_algo_cost('20120527', result_db)
    compute_algo_cost('20120105', result_db)
#    compute_algo_cost('20120601', result_db)
#    compute_algo_cost('20120604', result_db)


    #result_db = fidessadb.getODBCConnection(autocommit=False)
    #compute_latency('20111028', result_db)
    result_db._conn.commit()
    #result_db = fidessadb.getODBCConnection(autocommit=False)
    #process_nms_orders('20120228', None, None)
