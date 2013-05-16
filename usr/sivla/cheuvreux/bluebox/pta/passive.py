'''
Created on Oct 25, 2010

@author: syarc
'''
from cheuvreux.bluebox.execution import PassiveExecution
from cheuvreux.dbtools import dbobject
from cheuvreux.dbtools.Sqlite import SQLiteBase
from cheuvreux.fidessa import orderdb, fidessa, fidessa_util
from cheuvreux.fidessa.audit_trail import AuditTrail
from cheuvreux.fidessa.fidessa import get_fidessa_db
from cheuvreux.stdio.html import HtmlTable, table_style
from cheuvreux.utils import statistics
from cheuvreux.utils.date import parse_date, workday_range, go_back
from cheuvreux.utils.image import save_to_jpg
from collections import defaultdict
import locale
import matplotlib
import matplotlib.pyplot as plt
import os
import sys
import time
from cheuvreux.stdio.mail import Email, HtmlEmail
import pylab

dbs = {}

locale.setlocale(locale.LC_ALL, '')

if not os.path.exists('passive'):
    os.mkdir('passive')

def create_passive_db(file):

    db = SQLiteBase(file)

    db.createTable(PassiveExecution.db_name(), PassiveExecution.db_fields())

    db.createIndex('passive_date_destination', 'passive_orders', 'date, destination', False)
    db.createIndex('passive_stock', 'passive_orders', 'code', False)

    return db

def isDataExist(database, date):
    query = "SELECT 1 FROM passive_orders where date = '%s'" % date

    ans = database.selectOne(query)
    if ans:
        return True
    return False

def update_passive_db(date, db, fidessa_db = None, auditTrail = None, force=False):
    if type(date) is type(''):
        date = parse_date(date)

    # If not force, check if the data already exist
    if not force:
        if db.selectOne('SELECT 1 FROM passive_orders WHERE date = ?', date):
            print 'Skipping ', date
            return
    # Clean database
    db.execute('DELETE FROM passive_orders WHERE date = ?', date.strftime('%Y%m%d'))

    # -------------------- #
    #   Actual process     #
    # -------------------- #

    # 1. Load audit trail data
    if not auditTrail:
        auditTrail = AuditTrail(date)
        if not auditTrail.valid():
            return
        auditTrail.loadData() # No DMA

    # 2. Get fidessa database:
    if not fidessa_db:
        db = SQLiteBase(':memory:')
        orderdb.load_fidessa_data(db, date)

    executions = []
    for order_id in auditTrail:

        if auditTrail[order_id][0][5] in ('BLUEBOX', 'SUMO_OPEN', 'SUMO_CLOSE'):
            # Ignore bluebox order
            continue

        execution = PassiveExecution(order_id)
        try:
            execution.load_execution_data(fidessa_db)
        except Exception:
            # Probably an old order
            continue

        if execution is None or execution._order is None:
            continue

        # Try to find the first valid bid, ask
        for line in auditTrail[order_id]:
            bid, ask = float(line[12]), float(line[13])
            if bid > 0 and ask > 0:
                break

        if not execution._order.isPassive(bid, ask):
            continue

        execution.analyize_audit_trail(auditTrail[order_id])
        executions.append(execution)

    dbobject.save_objects_to_db(db, executions)

def get_db_file(date):
    db_file = "passive/passive_%d%.2d.db" % (date.year, date.month)

    # Create/ open the database
    if not os.path.exists(db_file):
        db = create_passive_db(db_file)
    else:
        db = SQLiteBase(db_file)

    return db

# --------------------------------------------------------------------------- #
#                                                                             #
#                         Report functions                                    #
#                                                                             #
# --------------------------------------------------------------------------- #

def passive_day_report(date, market=None, output=sys.stdout, html=False):
    DB_FILE = "passive/passive_%d%.2d.db" % (date.year, date.month)
    base = SQLiteBase(DB_FILE)
    fidessa_util.create_destination_map_temp_table(base)

    query = ''' SELECT ifnull(destination.destination, passive_orders.destination),
                    sum(done) as done, sum(quantity) as quantity, count(*) as count,
                    ifnull(sum(rebate)/sum(done),0.0)
                FROM passive_orders
                LEFT JOIN destination on destination.service_executor = passive_orders.destination
                WHERE date = '%s'
            ''' % (date.strftime('%Y%m%d'))
    if market:
        query += " AND market_id in ('%s') " % "','".join(market)

    query += ' GROUP BY destination.destination'

    rows = base.select(query)

    fill_rate = {}

    for row in rows:
        fill_rate[row[0]] = (row[1], row[2], row[3], row[4])

    if html:
        table = HtmlTable()
        table.setBorder()
        print >> output, table.header(['Date', 'Destination', 'Done', 'Sent', 'Fill Rate', 'Cost / Share', '# Order'], width=[100])

    for dest, values in sorted(fill_rate.items(), key=lambda (d,v): v, reverse=True):
        if html:
            print >> output, table.line([date, dest,
                                         locale.format("%d", values[0], grouping=True),
                                         locale.format("%d", values[1], grouping=True),
                                         "%.2f%%" % (100.0 * values[0]/values[1]),
                                         '$%.5f' % values[3],
                                         locale.format("%d", values[2], grouping=True)],
                                         ['','',"style='text-align:right'"])
        else:
            print >> output, date, dest, values[0], values[1], "%.2f" % (float(values[0])/values[1]), values[2]


    total_sent = total_done = total_order = total_rebate = 0

    for dest in fill_rate:
        total_done += fill_rate[dest][0]
        total_sent += fill_rate[dest][1]
        total_order += fill_rate[dest][2]
        total_rebate += fill_rate[dest][3]

    fill_rate = '%.2f' % (100.0 * total_done / total_sent)

    if html:
        fmt_total = ['', 'Total',
                         locale.format("%d", total_done, grouping=True),
                         locale.format("%d", total_sent, grouping=True),
                         fill_rate,
                         '$%.5f' % total_rebate,
                         locale.format("%d", total_order, grouping=True)]
        print >> output, table.line(fmt_total, ['','style="font-weight:bold;"','style="font-weight:bold;text-align:right"'])
    else:
        print >> output, '\t'.join(map(str,['','Average', total_done, total_sent, fill_rate, total_rebate, total_order]))


    if html:
        print >> output, table.end()

def passive_historic(start, end, market=None, output=sys.stdout, html=False):

    destinations = set()
    historic = defaultdict(dict)

    base_query = ''' SELECT destination.destination, sum(done), sum(sent)
                    FROM passive_orders
                    JOIN destination on destination.service_executor = passive_orders.destination
                    WHERE date = '%s'
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
#
#    passive_historic(go_back(date, histo_length), date, ['NYS-MAIN'], output, html)
#
#    if html:
#        print >> output, '<br>&nbsp;<br>'
#        print >> output, '<h2>NASDAQ - Monthly Report</h2>'
#    else:
#        print >> output, 'NASDAQ Monthly Report'
#
#    passive_historic(go_back(date, histo_length),  date, ['NAS-NNM', 'NAS-OBB', 'NAS-SCAP'],output, html)
    if html:
        print >> output, '<br>&nbsp;<br>'
        print >> output, '<h2>Monthly Average Fill Rate</h2>'
    else:
        print >> output, 'Monthly Average Fill Rate'
    histo_average(go_back(date, histo_length), date, None, output, html)

    if isinstance(output, Email):
        passive_chart_historic(go_back(date, histo_length), date, None, output)
    else:
        passive_chart_historic(go_back(date, histo_length), date, None)


def main(output, range, fidessa_db=None, force=False, html_output=False):
    '''
        Main method for computing data on passive order

        @param output Output file
        @param range Date range
        @param force if true, it will overwrite previous results
        @param html_output Should the report be HTML formatted
    '''
    start = time.clock()

    last_exception = None
    for date in range:
        sdate = date.strftime("%Y%m%d")

        database = get_db_file(date)
        if force or not isDataExist(database, sdate):
            if fidessa_db is None:
                db = SQLiteBase(':memory:')
                orderdb.load_fidessa_data(db, date)
                tmp_fidessa_db = get_fidessa_db('SQLite', db, date)
            else:
                tmp_fidessa_db = fidessa_db

            update_passive_db(date, database, tmp_fidessa_db, force=True)
        try:
            passive_report(date, 20, output, html_output)
        except Exception, e:
            last_exception = e

    if html_output:
        print >> output, '<br><span style="color: gray; font-size:8pt">Computation time: %f</span>' % (time.clock() - start)
    else:
        print >> output, "Computation time: %f" % (time.clock() - start)

    if last_exception:
        raise last_exception

def histo_average(start, end, market=None, output=sys.stdout, html=False):

    global dbs

    destinations = set()
    historic = defaultdict(lambda: defaultdict(dict))

    base_query = ''' SELECT destination.destination, sum(done), sum(quantity), market_id
                    FROM passive_orders
                    JOIN destination on destination.service_executor = passive_orders.destination
                    WHERE date = '%s'
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



def passive_chart_historic(start, end, market=None, mail=None):
    global dbs

    destinations = set()
    historic = defaultdict(lambda: defaultdict(dict))

    base_query = ''' SELECT destination.destination, sum(done), sum(quantity), market_id
                    FROM passive_orders
                    JOIN destination on destination.service_executor = passive_orders.destination
                    WHERE date = '%s'
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

    fill_rates = defaultdict(dict)
    for market in historic:
        for destination in destinations:
            tmp = [historic[market][date][destination][0]*100.0 / historic[market][date][destination][1]
                   for date in historic[market] if destination in historic[market][date]]

            if len(tmp) > 0:
                fill_rates[market][destination] = tmp

    for market in fill_rates:
        fig = plt.figure()
        matplotlib.rcParams['font.size'] = 8.0
        fig.set_size_inches(4., 3.)
        plt.subplots_adjust(left=0.1, bottom=0.3, right=0.95, top=0.9, wspace=0, hspace=0)

        values, keys = [], []
        for item in sorted(fill_rates[market].items(), key=lambda item: statistics.mean(item[1]), reverse=True):
            values.append(item[1])
            keys.append(item[0])

        plt.boxplot(values)
        locs, labels = plt.xticks(range(1, len(keys) + 1), keys)
        plt.setp(labels, rotation='vertical',fontsize=6)

        ax = plt.gca()
        ax.set_title('Tape %s' % market)

        filename = 'tape_%s.jpg' % market
        save_to_jpg(plt, filename)

        if mail:
            print >> mail, '<img src="cid:%s">' % filename
            mail.attachImage(filename, filename)


if __name__ == '__main__':
    import datetime

    date = datetime.date(2010,12,10)
    #db = SQLiteBase('dev.db')

    mail = HtmlEmail('smtpnotes', style=table_style)
    main(mail, [date], None, False, True);
    mail.send('sarchenault')


