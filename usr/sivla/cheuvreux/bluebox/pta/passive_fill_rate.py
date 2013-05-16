'''
This module defines function / classes to analyze
post trade execution of passive orders.

Created on Jul 21, 2010

@author: syarc

'''
from cheuvreux.dbtools.repository import Repository, DictionaryId
from cheuvreux.dbtools.Sqlite import SQLiteBase
from cheuvreux.dbtools.temptable import TempTable
from cheuvreux.fidessa import fidessa_util
from cheuvreux.fidessa.audit_trail import AuditTrail
from cheuvreux.fidessa.fidessadb import FidessaDB
from cheuvreux.stdio.html import HtmlTable
from cheuvreux.stdio.mail import Email
from cheuvreux.utils.date import workday_range, go_back
from collections import defaultdict
from datetime import datetime, date
from usr.dev.malas.bluebox import heatmap
import locale
import os
import sys
import time

import matplotlib
import matplotlib.pyplot as plt
from cheuvreux.utils.image import save_to_jpg
from cheuvreux.utils import statistics

dbs = {}

locale.setlocale(locale.LC_ALL, '')

index_query = ''' CREATE INDEX "idx_passive" ON "passive_orders"
                    ("stock" ASC, "destination" ASC) '''

table_query = ''' CREATE TABLE "passive_orders"
                (
                  "DATE" TEXT NOT NULL,
                  "ORDER_ID" TEXT NOT NULL PRIMARY KEY,
                  "ENTERED_TIME" TEXT NOT NULL,
                  "STOCK" TEXT,
                  "SENT" INTEGER,
                  "DONE" INTEGER,
                  "SIDE" TEXT,
                  "DESTINATION" TEXT,
                  "GROSS_PRICE" REAL,
                  "LIMIT_PRICE" REAL,
                  "ARRIVAL_BID" REAL,
                  "ARRIVAL_ASK" REAL,
                  "FINAL_BID" REAL,
                  "FINAL_ASK" REAL,
                  "END_TIME" TEXT,
                  "FIRST_FILL_TIME" TEXT,
                  "LAST_FILL_TIME" TEXT,
                  "DMA" TEXT,
                  "ALGO" TEXT,
                  "PARENT_ORDER_ID" TEXT,
                  "MARKET_ID" TEXT,
                  "ORDER_NOTES" TEXT
                )
        '''


index_query = ''' CREATE INDEX "passive_date_destination" ON "passive_orders" ("date" ASC,"destination" ASC);
                  CREATE INDEX "passive_market" on "passive_orders" ("market_id" ASC);
              '''

ignore_stocks = ['ZVZZT', 'ZXZZT']

service = ['POST', 'SWEEP', 'CES', 'BATS']
executor = ['',
           'BB_POST_FLOW', 'BB_POST_FLOW_HIDDEN',
           'BATS', 'BB_POST_BATS', 'BB_SWEEP_BATS', 'BB_POST_BATS_HIDDEN', 'BB_POST_BATS_100',
           'BB_POST_NSDQ', 'BB_SWEEP_NSDQ',
           'BB_POST_EDGA', 'BB_SWEEP_EDGA', 'BB_POST_EDGA_HIDDEN',
           'BB_POST_EDGX', 'BB_SWEEP_EDGX',
           'BB_POST_ARCX', 'BB_SWEEP_ARCX', 'BB_POST_ARCX_HIDDEN', 'BB_POST_ARCX_100']

fidessa = FidessaDB()

def processPassiveExecution(date):
    auditTrail = AuditTrail(date)

    if not auditTrail.valid():
        return

    auditTrail.loadData(ignore_dma=False)

    for orderId in auditTrail:
        data = auditTrail[orderId]

        if data[0][18] in ignore_stocks:
            continue

        process_order = data[0][5] in service and data[0][6] in executor


        # Check for BLUEBOX fwd orders
        if process_order:
            destination = data[0][5] + ' ' + data[0][6]

        elif data[0][5] == 'BLUEBOX':
            for line in data:
                if line[1] == 'ASSN' and line[5] in service and line[6] in executor:
                    process_order = True
                    destination = line[5] + ' ' + line[6]
                    break

        if process_order:
            order = fidessa.getFidessaOrder(orderId, False, date=date);

            if order is None:
                continue

            # Try to find the first valid bid, ask
            for line in data:
                bid, ask = float(line[12]), float(line[13])
                if bid > 0 and ask > 0:
                    break

            if order.isAggressive(bid, ask):
                continue

            algo = fidessa.getAlgoName(order.parent_order_id)

            end_time = None
            final_bid, final_ask = None, None
            first_fill_time, last_fill_time = None, None
            for line in data:
                if line[1] in ['CEXE', 'EXEE']:
                    if first_fill_time is None:
                        first_fill_time = AuditTrail.getDateTime(line[2])
                    last_fill_time = AuditTrail.getDateTime(line[2])
                elif line[1] in ['RELE', 'ESOR', 'COMP']:
                    end_time = AuditTrail.getDateTime(line[2])
                    final_bid, final_ask = float(line[12]), float(line[13])

            yield {'DATE': date,
                   'ORDER_ID' : orderId,
                   'ENTERED_TIME': AuditTrail.getDateTime(data[0][2]),
                   'STOCK': order.code, 'DONE': order.done,
                   'SENT':order.quantity, 'SIDE': order.buy_sell,
                   'DESTINATION': destination.strip(),
                   'GROSS_PRICE': order.gross_price,
                   'LIMIT_PRICE': order.limit_price,
                   'ARRIVAL_BID': bid, 'ARRIVAL_ASK': ask,
                   'FIRST_FILL_TIME': first_fill_time,
                   'LAST_FILL_TIME': last_fill_time,
                   'FINAL_BID': final_bid, 'FINAL_ASK': final_ask,
                   'END_TIME':end_time,
                   'DMA': data[0][19],
                   'ALGO': algo,
                   "PARENT_ORDER_ID": order.parent_order_id,
                   'MARKET_ID': order.market_id,
                   'ORDER_NOTES': order.note_id}

def buildPassiveDB(start, end, force=False, output=sys.stdout):
    DB_FILE = "passive/passive_%d%.2d.db" % (start.year, start.month)

    if not os.path.exists(DB_FILE):
        base = SQLiteBase(DB_FILE)
        base.execute(table_query)
        base.execute(index_query)
    else:
        base = SQLiteBase(DB_FILE)

    for date in workday_range(start, end):
        start = time.clock()
        date = date.strftime("%Y%m%d")

        if not force:
            rs = base.selectOne("SELECT 1 FROM passive_orders WHERE date = '%s'" % date)
            if rs:
                print "%s skipping" % date
                continue

        base.execute("DELETE FROM passive_orders WHERE date = '%s'" % date)

        stmt = base.prepareInsertStmt('passive_orders',
                                      ['DATE', 'ORDER_ID', 'ENTERED_TIME', 'STOCK', 'DONE',
                                       'SENT', 'SIDE', 'DESTINATION', 'GROSS_PRICE',
                                       'LIMIT_PRICE', 'ARRIVAL_BID', 'ARRIVAL_ASK',
                                       'FIRST_FILL_TIME', 'LAST_FILL_TIME',
                                       'FINAL_BID', 'FINAL_ASK', 'END_TIME', 'DMA',
                                       'ALGO', 'PARENT_ORDER_ID', 'MARKET_ID', 'ORDER_NOTES'])

        base.execManyPreparedInsertStmt(stmt, processPassiveExecution(date))
        print >> output, date, time.clock() - start

    base.close()

def getPassiveFillRate(month, output):
    DB_FILE = "passive/passive_2010%.2d.db" % month
    base = SQLiteBase(DB_FILE)

    universe = base.select(''' SELECT stock FROM passive_orders
                               GROUP BY STOCK ORDER BY count(stock) DESC
                               LIMIT 0,50
                           ''')
    stock = [row[0] for row in universe]

    if 'C' not in stock:
        # Add citigroup
        stock.append('C')

    rs = base.select('''SELECT stock, d.destination, sum(done), sum(sent), sum(done*1.0)/sum(sent), count(*)
                        from passive_orders p
                        join destination d on p.destination = d.service_executor
                        where stock in (%s)
                        group by stock, d.destination
                    ''' % ','.join(map(lambda s: "'%s'" % s, stock)))

    print >> output, 'stock, destination, done, sent, fill_rate, count(*)'
    for line in rs:
        print >> output , '%s,%s,%f,%f,%f,%d' % (line[0], line[1], line[2], line[3], line[4], line[5])

    getHeatmap(stock)

def getHeatmap(codes):
    from simep.funcs.stdio.ssh import SSHConfiguration, SSHException
    output = open ("real_heatmap.csv", "w")
    destinations = ['ARCX', 'BATS', 'EDGA', 'EDGX', 'FLOW', 'NSDQ', 'NYSE']
    print >> output, 'code,', ','.join(destinations)

    pool = SSHConfiguration('prod')
    client = pool.getClient('nybbo001')
    for code in codes:
        sec_id = Repository.dict_id_to_security_id(code, DictionaryId.USCODE)
        if not sec_id:
            print code, ' not found'
            continue
        isin = Repository.security_dict_code(sec_id.keys()[0], DictionaryId.ISIN)

        print code, isin
        try:
            client.get('/var/local/heatmap2/' + isin + '.csv', '.')
            print >> output, "%s," % code + heatmap.Heatmap(isin + '.csv').toString(destinations)
        except SSHException:
            print isin , 'is missing'

def jmpPassiveFillRate(output):
    base_dir = "G:\Research_Quant\SmartPost"
    DB_FILE = "passive_201007.db"
    base = SQLiteBase(DB_FILE)

    sent = defaultdict(int)
    done = defaultdict(int)

    for file in os.listdir(base_dir):
        if file.startswith('smartpost_randomization_'):
            for line in open(os.path.join(base_dir, file)):
                fields = line.split(',')
                order = fidessa.getFidessaOrder(fields[0], False, 1)
                if order is None:
                    continue

                start = datetime.strptime(order.start[0:8], "%H:%M:%S")
                start = start.replace(microsecond=int(order.start[9:]))

                i = 0
                version = 2
                amend_diff_time = ['', '', '', '', '', '', '', '', '', '']
                while True:
                    amend = fidessa.getFidessaOrder(fields[0], False, version)
                    if amend is None:
                        break
                    if amend.start != order.start:
                        amend_time = datetime.strptime(amend.start[0:8], "%H:%M:%S")
                        amend_time = amend_time.replace(microsecond=int(amend.start[9:]))

                        delta = amend_time - start
                        amend_diff_time[i] = str(delta.seconds * 1000000 + delta.microseconds)
                        i += 1

                    version += 1
                    if order is not None:
                        print "%s,%s," % (order.order_id, ','.join(amend_diff_time))
def passive_day_report(date, market=None, output=sys.stdout, html=False):
    DB_FILE = "passive/passive_%d%.2d.db" % (date.year, date.month)
    base = SQLiteBase(DB_FILE)
    fidessa_util.create_destination_map_temp_table(base)

    query = ''' SELECT destination.destination, sum(done) as done, sum(sent) as sent, count(*) as count
                FROM passive_orders
                JOIN destination on destination.service_executor = passive_orders.destination
                WHERE date = '%s' AND DMA = 'N'
            ''' % (date.strftime('%Y%m%d'))
    if market:
        query += " AND market_id in ('%s') " % "','".join(market)

    query += ' GROUP BY destination.destination'

    rows = base.select(query)

    fill_rate = {}

    for row in rows:
        fill_rate[row[0]] = (row[1], row[2], row[3])

    if html:
        table = HtmlTable()
        table.setBorder()
        print >> output, table.header(['Date', 'Destination', 'Done', 'Sent', 'Fill Rate', '# Order'], width=[100])

    for dest, values in sorted(fill_rate.items(), key=lambda (d,v): v, reverse=True):
        if html:
            print >> output, table.line([date, dest,
                                         locale.format("%d", values[0], grouping=True),
                                         locale.format("%d", values[1], grouping=True),
                                         "%.2f" % (100.0 * values[0]/values[1]),
                                         locale.format("%d", values[2], grouping=True)])
        else:
            print >> output, date, dest, values[0], values[1], "%.2f" % (float(values[0])/values[1]), values[2]


    total_sent = total_done = total_order = 0

    for dest in fill_rate:
        total_done += fill_rate[dest][0]
        total_sent += fill_rate[dest][1]
        total_order += fill_rate[dest][2]

    try:
        fill_rate = '%.2f' % (100.0 * total_done / total_sent)
    except ZeroDivisionError:
        fill_rate = '%.2f' % (0.0)

    if html:
        fmt_total = ['', 'Total',
                         locale.format("%d", total_done, grouping=True),
                         locale.format("%d", total_sent, grouping=True),
                         fill_rate,
                         locale.format("%d", total_order, grouping=True)]
        print >> output, table.line(fmt_total, ['style="font-weight:bold;"'])
    else:
        print >> output, '\t'.join(map(str,['','Average', total_done, total_sent, fill_rate, total_order]))


    if html:
        print >> output, table.end()

def passive_historic(start, end, market=None, output=sys.stdout, html=False):

    global dbs

    destinations = set()
    historic = defaultdict(dict)

    base_query = ''' SELECT destination.destination, sum(done), sum(sent)
                    FROM passive_orders
                    JOIN destination on destination.service_executor = passive_orders.destination
                    WHERE date = '%s' AND DMA = 'N'
                 '''
    if market:
        base_query += " AND market_id in ('%s') " % "','".join(market)

    base_query += ' GROUP BY destination.destination'


    for date in workday_range(start, end):
        key = (date.year, date.month)
        if key in dbs:
            db = dbs[key]
        else:
            db = SQLiteBase("passive/passive_%d%.2d.db" % (date.year, date.month))
            fidessa_util.create_destination_map_temp_table(db)
            dbs[key]= db

        query = base_query % (date.strftime('%Y%m%d'))
        rows = db.select(query)
        for row in rows:
            destinations.add(row[0])
            historic[date][row[0]] = (row[1], row[2])

    if html:
        table = HtmlTable()
        table.setBorder()
        print >> output, table.header(['Date'] + sorted(destinations), width=[100])
    else:
        print >> output, 'Date    \t' + '\t'.join(destinations)

    for date in sorted(historic.keys()):
        line = [date]
        for destination in sorted(destinations):
            if destination in historic[date]:
                line.append('%.2f' % (100.0*historic[date][destination][0]/historic[date][destination][1]))
            else:
                line.append('-')
        if html:
            print >> output, table.line(line)
        else:
            print >> output, '\t'.join(map(str,line))

    # Compute average
    avg = ['Average']
    for destination in destinations:
        fill_rates = [historic[date][destination][0]*100.0/historic[date][destination][1] for date in historic if destination in historic[date]]
        avg.append('%.2f' % (sum(fill_rates) / len(fill_rates)))

    if html:
        print >> output, table.line(avg, ['style="font-weight:bold;"'])
    else:
        print >> output, '\t'.join(map(str,avg))

    if html:
        print >> output, table.end()

def histo_average(start, end, market=None, output=sys.stdout, html=False):

    global dbs

    destinations = set()
    historic = defaultdict(lambda: defaultdict(dict))

    base_query = ''' SELECT destination.destination, sum(done), sum(sent), market_id
                    FROM passive_orders
                    JOIN destination on destination.service_executor = passive_orders.destination
                    WHERE date = '%s' AND DMA = 'N'
                 '''
    if market:
        base_query += " AND market_id in ('%s') " % "','".join(market)

    base_query += ' GROUP BY market_id, destination.destination'

    for date in workday_range(start, end):
        key = (date.year, date.month)
        if key in dbs:
            db = dbs[key]
        else:
            db = SQLiteBase("passive/passive_%d%.2d.db" % (date.year, date.month))
            fidessa_util.create_destination_map_temp_table(db)
            dbs[key]= db

        query = base_query % (date.strftime('%Y%m%d'))
        rows = db.select(query)
        for row in rows:
            destinations.add(row[0])
            tape = fidessa_util.get_tape_from_market_id(row[3])

            done, sent = 0.0, 0.0
            if row[0] in historic[tape][date]:
                done, sent = historic[tape][date][row[0]]

            historic[tape][date][row[0]] = (done + row[1], sent + row[2])
            #historic[fidessa_util.get_tape_from_market_id(row[3])][date][row[0]] = (row[1], row[2])

    if html:
        table = HtmlTable()
        table.setBorder()
        print >> output, table.header(['Tape'] + sorted(destinations), width=[100])
    else:
        print >> output, 'Date    \t' + '\t'.join(sorted(destinations))

    # Compute average

    for market in historic:
        avg = [market]
        for destination in sorted(destinations):
            fill_rates = [historic[market][date][destination][0]*100.0/historic[market][date][destination][1] for date in historic[market] if destination in historic[market][date]]

            if len(fill_rates) > 0:
                avg.append('%.2f' % (sum(fill_rates) / len(fill_rates)))
            else:
                avg.append('')

        if html:
            print >> output, table.line(avg)
        else:
            print >> output, '\t'.join(map(str,avg))

    if html:
        print >> output, table.end()





def passive_report(date, histo_length=20, output=sys.stdout, html=False):
    if html:
        print >> output, '<h2>NYSE - Daily Passive Execution </h2>'
    else:
        print >> output, 'NYSE Daily Passive Execution'

    passive_day_report(date, ['NYS-MAIN'], output, html)

    if html:
        print >> output, '<br>&nbsp;<br>'
        print >> output, '<h2>NASDAQ Daily Passive Execution</h2>'
    else:
        print >> output, 'NASDAQ Daily Passive Execution'

    passive_day_report(date, ['NAS-NNM', 'NAS-OBB', 'NAS-SCAP'], output, html)

#    if html:
#        print >> output, '<br>&nbsp;<br>'
#        print >> output, '<h2>NYSE - Monthly Report</h2>'
#    else:
#        print >> output, 'NYSE Monthly Report'

    #passive_historic(go_back(date, histo_length), date, ['NYS-MAIN'], output, html)

#    if html:
#        print >> output, '<br>&nbsp;<br>'
#        print >> output, '<h2>NASDAQ - Monthly Report</h2>'
#    else:
#        print >> output, 'NASDAQ Monthly Report'

    #topassive_historic(go_back(date, histo_length),  date, ['NAS-NNM', 'NAS-OBB', 'NAS-SCAP'],output, html)

    if html:
        print >> output, '<br>&nbsp;<br>'
        print >> output, '<h2>Monthly Average Fill Rate</h2>'
    else:
        print >> output, 'Monthly Average Fill Rate'
    histo_average(go_back(date, histo_length), date, None, output, html)

    if type(output) is Email:
        passive_chart_historic(go_back(date, histo_length), date, None, output)
    else:
        passive_chart_historic(go_back(date, histo_length), date, None)

def tmp_update_market_id(db):
    query = ''' SELECT order_id FROM passive_orders '''

    update_query = ''' UPDATE passive_orders SET market_id = ?
                       WHERE order_id = ? '''

    orders_ids = [(row[0],) for row in db.select(query)]

    temp = TempTable(fidessa._backend, '#order_ids', 'order_id varchar(26)')

    fidessa._backend.runMany("INSERT INTO #order_ids VALUES(?)", orders_ids)

#    for order in orders_ids:
#        fidessa._backend.execDirect("INSERT INTO #order_ids VALUES('%s')" % order)


    print 'Fidessa'

    rows = fidessa._backend.select('''SELECT h.order_id, market_id FROM [High Touch Orders Cumulative] h
                        join #order_ids o on h.order_id = o.order_id
                        where version = 1 ''')

    print 'done'

    def market_generator(rows):
        for row in rows:
            yield (row[1], row[0])

    db.executeMany(update_query, market_generator(rows))


if __name__ == '__main__':

    #tmp_update_market_id(SQLiteBase("passive/passive_201008.db"))

    mail = Email('smtpnotes')
    mail.set_type('html')
    #passive_day_report_market(date(2010,9,21), output=mail, html=True)
    passive_report(date(2010,12,1), output=mail, html=True)
    mail.send('sarchenault')

#    getPassiveFillRate(7, open('passive_fill_rate.txt', 'w'))
#    buildPassiveDB(previous_weekday(), previous_weekday())
#    buildPassiveDB(date(2010, 01, 01), date(2010, 01, 31))
#    buildPassiveDB(date(2010, 02, 01), date(2010, 02, 28))
#    buildPassiveDB(date(2010, 03, 01), date(2010, 03, 31))

#    buildPassiveDB(date(2010, 06, 01), date(2010, 06, 30))
#    getPassiveFillRate("jmp.txt", 7, open('G:\Research_Quant\SmartPost\jmp_fillrate_new.csv', 'w'))
#    jmpPassiveFillRate(sys.stdout)

