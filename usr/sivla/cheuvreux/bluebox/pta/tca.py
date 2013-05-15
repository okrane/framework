'''
Created on Feb 4, 2011

@author: syarc
'''
import sys
import csv
import math
from cheuvreux.dbtools.Sqlite import SQLiteBase
from cheuvreux.dbtools.odbc import ODBC
from cheuvreux.dbtools.repository import Repository
from cheuvreux.dbtools.tickdb import TickDB
from cheuvreux.fidessa import SecurityIdCache, fidessadb
from cheuvreux.fidessa.fidessadb import FidessaDB
from cheuvreux.utils.date import workday_range
from collections import defaultdict
import datetime
import os
import traceback



class AlgoPerformance:
    def __init__ (self, turnover, vsot, arrival, exit):
        self.turnover = turnover
        self.vsot = vsot
        self.arrival = arrival
        self.exit = exit

    def get_vsot(self):
        return self.vsot

    def get_arrival(self):
        return self.arrival

    def get_exit(self):
        return self.exit

class AlgoPerfSummary:
    def __init__ (self, turnover=0, perf=0, error=0, count=0):
        self.error = error
        self.count = count
        self.turnover = turnover
        self.perf = perf

    def average(self):
        return self.perf / self.turnover if self.turnover > 0 else 0

    def sd(self):
        return math.sqrt(self.error / self.turnover) if self.turnover > 0 else 0

    def __add__(self, summary):
        return AlgoPerfSummary(self.turnover + summary.turnover, self.perf + summary.perf,
                               self.error + summary.error, self.count + summary.count)

    def __iadd__ (self, summary):
        self.error += summary.error
        self.count += summary.count
        self.turnover += summary.turnover
        self.perf += summary.perf

        return self

    def __str__(self):
        return '%.2f\t[%.2f]' % (self.average(), self.sd())

def compute_perf (perfs, bench_func):
    sum_perf , turnover = 0.0, 0.0
    count = 0

    for perf in perfs:
        if bench_func(perf) and abs(bench_func(perf)) < 150:
            turnover += perf.turnover
            sum_perf += perf.turnover * bench_func(perf)
            count += 1

    if turnover <= 0:
        return AlgoPerfSummary()

    else:
        avg = sum_perf / turnover
        sum_error = 0
        for perf in perfs:
            if bench_func(perf) and abs(bench_func(perf)) < 150:
                sum_error += perf.turnover * (bench_func(perf) - avg)**2

        return AlgoPerfSummary(turnover, sum_perf, sum_error, count)

def report(start, end):

    adv_index = {0: '< 0.25%', 1: '0.25-1%', 2: '1-3%', 3: '3-5%', 4: '5-10%', 5: '> 10%'}
    def get_adv_index (pct_adv):
        if pct_adv < 0.0025:
            return 0
        elif pct_adv < 0.01:
            return 1
        elif pct_adv < 0.03:
            return 2
        elif pct_adv < 0.05:
            return 3
        elif pct_adv < 0.1:
            return 4
        return 5

    def build_algo_data(line):
        side = int (line['side'])
        gross_price = float(line['gross price'])

        vsot, arrival, exit = float(line['vsot']), float(line['arrival']), float(line['exit'])

        perf_vsot = 10000 * side * (vsot - gross_price) / vsot
        perf_arrival = 10000 * side * (arrival - gross_price) / arrival if arrival > 0 else None
        perf_exit = 10000 * side * (exit - gross_price) / exit if exit > 0 else None

        algo_perf = AlgoPerformance(int(line['volume done']) * gross_price, perf_vsot, perf_arrival, perf_exit)

        return {'qty': int(line['quantity']), 'done': int(line['volume done']), 'gross price': gross_price,
                'perf': algo_perf}

    data = defaultdict(lambda : {0: [], 1: [], 2: [], 3: [], 4: [], 5: []})

    db = FidessaDB()
    for date in workday_range(start, end):
        filepath = os.path.join('tca', 'tca_%s.csv' % date.strftime('%Y%m%d'))
        if not os.path.exists(filepath):
            continue
        file = csv.DictReader(open(filepath))
        for line in file:
            order_id = line['order_id']
            order = db.getFidessaOrder(order_id)
        #
        # If you want to filter on a specific counterparty
        #
          #  if order.ctpy == 'LYON':
          #      continue

            if line['pct adv'] != '' and line['vsot'] not in (None, 'None'):
                index = get_adv_index(float(line['pct adv']))
                data[line['algo']][index].append(build_algo_data(line))

    benchmarks = ['arrival', 'vsot', 'exit']

    print 'Algo\tADV Group\tArrival Perf\tArrival Perf (SD)\tVSOT Perf\tVSOT Perf (SD)\tExit Perf\tExit Perf (SD)\tCount'

    for algo in data:

        perf_total = {'arrival': AlgoPerfSummary(), 'vsot': AlgoPerfSummary(), 'exit': AlgoPerfSummary()}

        for adv in data[algo]:
            perfs = [tmp['perf'] for tmp in data[algo][adv]]

            arrival = compute_perf(perfs, AlgoPerformance.get_arrival)
            perf_total['arrival'] += arrival
            vsot = compute_perf(perfs, AlgoPerformance.get_vsot)
            perf_total['vsot'] += vsot
            exit = compute_perf(perfs, AlgoPerformance.get_exit)
            perf_total['exit'] += exit

            print '%s\t%s\t%s\t%d' % (algo, adv_index[adv], '\t'.join(map(str,[arrival, vsot, exit])), vsot.count)

        print '%s\t%s\t%s\t%d' % (algo, 'Total', '\t'.join([str(perf_total[bench]) for bench in benchmarks]), perf_total['vsot'].count)

def perf_generator (start, end, db):
        for date in workday_range(start, end):

            count = db.selectOne('SELECT COUNT(*) FROM tca WHERE "date" = \'%s\'' % date.strftime('%Y%m%d'))
            if count and count[0] > 0:
                print 'Skipping', date
                continue

            filename = os.path.join('tca', 'tca_%s.csv' % date.strftime('%Y%m%d'))

            if not os.path.exists(filename):
                continue

            file = csv.DictReader(open(filename))
            for line in file:
                if line['pct adv'] != '' and line['vsot'] not in(None,'None'):

                    side = int (line['side'])
                    gross_price = float(line['gross price'])

                    try:
                        vsot, arrival, exit = float(line['vsot']), float(line['arrival']), float(line['exit'])
                    except TypeError, e:
                        print line
                        raise e

                    pwp10 = float(line['pwp 10']) if line['pwp 10'] != 'None' else 0
                    pwp15 = float(line['pwp 15']) if line['pwp 15'] != 'None' else 0

                    data = {'date': date.strftime('%Y%m%d'), 'algo': line['algo'], 'order_id' : line['order_id'],
                            'instrument': line['stock'], 'side': side, 'done': int(line['volume done']), 'quantity': int(line['quantity']),
                            'gross_price': gross_price, 'pct_adv': float(line['pct adv'])}

                    data['perf_vwap'] = 10000 * side * (vsot - gross_price) / vsot
                    data['perf_arr'] = 10000 * side * (arrival - gross_price) / arrival if arrival > 0 else None
                    data['perf_exit'] = 10000 * side * (exit - gross_price) / exit if exit > 0 else None
                    data['perf_pwp10'] = 10000 * side * (pwp10 - gross_price) / pwp10 if pwp10 > 0 else None
                    data['perf_pwp15'] = 10000 * side * (pwp15 - gross_price) / pwp15 if pwp15 > 0 else None

                    data['mktcap'] = float(line['mkt cap']) if line['mkt cap'] != '0.0' else None
                    data['spread'] = float(line['spread']) if line['spread'] != 'None' else None
                    data['mkt_id'] = line['mkt id']
                    data['start_time'] = line['start time']
                    data['end_time'] = line['end time']
                    data['mkt_volume'] = float(line['mkt volume']) if line['mkt volume'] != 'None' else None

                    yield [data[k] for k in sorted(data.keys())]



def tca_to_sql(start, end):

    db = fidessadb.getODBCConnection(autocommit=False)

    keys = {'date':'', 'algo':'', 'order_id':'', 'instrument':'', 'side':'', 'done':'', 'quantity':'',
            'gross_price':'', 'pct_adv':'', 'perf_arr':'', 'perf_vwap':'', 'perf_exit':'',
            'perf_pwp10':'', 'perf_pwp15':'', 'mktcap':'', 'spread':'', 'mkt_id':'',
            'mkt_volume': '', 'start_time': '', 'end_time': ''}

    stmt = ('INSERT INTO "tca" ('
        + ','.join(sorted(keys.iterkeys())) + ') VALUES ('
        + ','.join(['?']*len(keys)) + ')')

    data = [p for p in perf_generator(start, end, db)]
    if len(data) > 0:
        db.executemany(stmt, data)
        db._conn.commit()


def tca_to_db(start, end):

    db = SQLiteBase('tca_us.db')
    stmt = db.prepareInsertStmt('tca_us', ['date', 'algo', 'order_id', 'stock', 'side', 'done', 'quantity',
                                    'gross_price', 'pct_adv', 'perf_arr', 'perf_vwap', 'perf_exit',
                                    'perf_pwp10', 'perf_pwp15', 'mktcap', 'spread', 'MKT_ID'])

    db.execManyPreparedInsertStmt(stmt, perf_generator(start, end, db))

def is_data_present_in_db(date):
    db = fidessadb.getODBCConnection(autocommit=False)

    query = 'SELECT count(*) FROM fidessa..[High Touch Orders Cumulative] where tradedate = \'%s\'' % date
    rows = db.selectOne(query) 
    
    return rows[0] > 0


def tca(date, output):
    fidessa = FidessaDB()

    adv = {}
    mkt_caps = {}
    spread = {}

#    path = os.path.join('C:\\algo_log',date.strftime('%Y%m%d'))
#    if not os.path.exists(path):
#        return

    print >> output, ','.join(['date', 'order_id', 'stock', 'algo', 'side', 'quantity', 'limit', 'start time', 'end time',
                    'gross price', 'volume done', 'pct adv', 'mkt cap', 'mkt id', 'pwp 10', 'pwp 15', 'arrival', 'vsot',
                    'exit', 'mkt volume', 'spread'])

    output.flush()

    datas = defaultdict(list)

    for line in open('C:\\algo_log\\audit_%s' % date.strftime('%Y%m%d')):
        fields = line.split('|')
        datas[fields[2]].append(line)

    for data in datas.values():
        try:

            if len(data) < 2:
                continue

            first_line, last_line = data[0], data[len(data) - 1]
            fields = first_line.split('|')
            order_id, stock, algo = fields[2].strip(), fields[6].strip(),fields[10].strip()
            start_time = datetime.datetime.strptime(fields[14].strip(), '%Y%m%d%H%M%S').time()

            if algo not in ('VWAP', 'EVP', 'TWAP', 'CROSSFIRE', 'BUYBACK', 'TWAPDPL', 'PEGDPL', 'ImplementationShortFall', 'TargetClose'):
                continue

            fields = last_line.split('|')
            end_time = datetime.datetime.strptime(fields[31].strip(), '%Y%m%d%H%M%S').time()
            order = fidessa.getFidessaOrder(order_id)

            if order.done == 0:#or not order.market_id.startswith('TOR'):
                continue

            try:
                security = SecurityIdCache.getSecurity(stock, order.market_id)
            except ValueError, e:
                print "Value Error for", stock
                traceback.print_exc()

            if not security:
                print 'skipping %s' % stock
                continue

            security._trading_destination_id = None

            if stock not in adv:
                adv[stock] = TickDB.adv(security, date.strftime('%Y%m%d'))

            if adv[stock]:
                pct_adv = order.quantity / adv[stock]
            else:
                pct_adv = ''

            limit_price = order.limit_price
            if not limit_price:
                limit_price = ''

            arrival_price = TickDB.spread_at_time(security, date.strftime('%Y%m%d'), start_time)
            try:
                arrival_price = (arrival_price[0] + arrival_price[1]) / 2.0
            except TypeError:
                arrival_price = 0

            exit_price = TickDB.spread_at_time(security, date.strftime('%Y%m%d'), end_time)

            try:
                exit_price = (exit_price[0] + exit_price[1]) / 2.0
            except TypeError:

                if end_time.strftime('%H:%M:%S') > '15:59:00':
                    exit_price = TickDB.ohlc(security, date)
                    if exit_price:
                        exit_price = exit_price[3]

                exit_price = 0

            if stock not in mkt_caps:
                mkt_caps[stock] = Repository.outstanding_shares(security.security_id)
                if not mkt_caps[stock]:
                    mkt_caps[stock] = 0
            mkt_cap = arrival_price * mkt_caps[stock]

            if security not in spread:
                spread[security] = TickDB.average_spread(security, date.strftime('%Y%m%d'))

            side = 1 if order.buy_sell is 'B' else -1
            print >> output, ','.join(map(str, [date, order_id, stock, algo, side, order.quantity,
                                     limit_price, start_time, end_time, order.gross_price,
                                     order.done, pct_adv, mkt_cap, order.market_id,
                                     TickDB.pwp(security, date.strftime('%Y%m%d'), start_time, order.done, 0.1),
                                     TickDB.pwp(security, date.strftime('%Y%m%d'), start_time, order.done, 0.15),
                                     arrival_price,
                                     TickDB.vwap(security, date.strftime('%Y%m%d'), start_time, end_time, order.limit_price, side),
                                     exit_price,
                                     TickDB.volume(security, date.strftime('%Y%m%d'), start_time, end_time, all_trades=True),
                                     spread[security]]))
        except ValueError, e:
            print file
            raise e
        except AttributeError, e1:
            # check data is present in the db
            if not is_data_present_in_db(date):
                print 'No data in the database for %s' % date
            else:
                fields = data[0].split('|')
                order_id = fields[2].strip()
                print 'Order ID %s not fount in the database' % order_id
                raise e1


def fix_arrival():
    fidessa = FidessaDB()

    query = ''' SELECT order_id, instrument, date, start_time, end_time, side FROM
                quant..TCA where perf_exit is null
                and end_time >= '15:59:00'
            '''

    for row in fidessa._backend.select(query):
        order = fidessa.getFidessaOrder(row[0])
        stock = row[1]
        date = datetime.datetime.strptime(row[2], '%Y-%m-%d')
        start_time, end_time = row[3][0:8], row[4][0:8]
        side = row[5]
        if not order or order.done == 0:#or not order.market_id.startswith('TOR'):
            continue

        try:
            security = SecurityIdCache.getSecurity(stock, order.market_id)
        except ValueError, e:
            print "Value Error for", stock
            traceback.print_exc()

        if not security:
            continue

#        arrival_price = TickDB.spread_at_time(security, date.strftime('%Y%m%d'), start_time)
#        try:
#            arrival_price = (arrival_price[0] + arrival_price[1]) / 2.0
#        except TypeError:
#            arrival_price = 0

        exit_price = TickDB.ohlc(security, date)

        if exit_price:
            exit_price = exit_price[3]

#        perf_arr = 10000 * side * (arrival_price - order.gross_price) / arrival_price if arrival_price > 0 else 'NULL'
        perf_exit = 10000 * side * (exit_price - order.gross_price) / exit_price if exit_price > 0 else 'NULL'

        fidessa._backend.execDirect(''' UPDATE quant..TCA set perf_exit = %s WHERE order_id = '%s'
                                    ''' % (perf_exit, row[0]))

        print row[0], perf_exit


if __name__ == '__main__':
    is_data_present_in_db(datetime.date(2012,8,10))
    #fix_arrival()
    # 1/ Calcule les perf (tca) sur la date donnee (automatise par windows scheduler)
    #tca(datetime.date(2011,11,25), sys.stdout)
    # 2/ Insere les csv de tca dans la base sqlite tca_us.db
    #tca_to_db(datetime.date(2011,9,1), datetime.date(2011,10,7))
#    tca_to_sql(datetime.date(2011,8,24), datetime.date(2011,8,24))
    # 3/ report des perfs sur la period
    #report(datetime.date(2011,8,15), datetime.date(2011,8,19))


