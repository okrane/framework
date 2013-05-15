'''
Created on Oct 20, 2010

@author: syarc
'''

from cheuvreux import fidessa
from cheuvreux.dbtools.Sqlite import SQLiteBase
from cheuvreux.fidessa import fidessa_util, orderdb, SecurityIdCache
from cheuvreux.fidessa.audit_trail import AuditTrail
from cheuvreux.stdio.html import HtmlTable
from collections import defaultdict
import datetime
import locale
import matplotlib
import matplotlib.pyplot as plt
import os
import time
from cheuvreux.fidessa.fidessa import get_fidessa_db
from cheuvreux.fidessa.orderdb import load_fidessa_data
from wx.tools.Editra.src.syntax import sql
from cheuvreux.stdio.mail import Email, HtmlEmail
import traceback
from cheuvreux.dbtools.repository import Repository
from cheuvreux.dbtools.tickdb import TickDB
from cheuvreux.dbtools.securities_tools import SecuritiesBySybase
import pprint
from cheuvreux.utils.date import workday_range
import sys
from cheuvreux.utils.statistics import quantile

'''
Created on 21 avr. 2010

@author: syarc

'''


locale.setlocale(locale.LC_ALL, '')

database = SQLiteBase('aggressive/aggresive_2010.db')
#database = SQLiteBase('citi.db')

ignore_stocks = ['ZVZZT', 'ZXZZT', 'ZWZZT']
event_type = 'CHLD'

# List of darkpools of interest
service_ids = {'GETC':event_type, 'NITED':event_type, 'QANTX':event_type, 'QANTY':event_type,
               'ATDPNG':event_type, 'EBXL':event_type, 'ECUT':event_type, 'POST':event_type,
               'NYFX':event_type, 'NITEM':event_type, 'MILL':event_type, 'BONY':event_type,
               'INET':event_type, 'ARCA':event_type, 'FBCO':event_type, 'NFSC':event_type,
               'AQUA':event_type, 'PDQM':event_type, 'MLXN': event_type, 'BCAP_LX':event_type}

def main(output, range, fidessa_db=None, force=False, html_output=False):
    '''
        Main method for computing price improvement data

        @param output Output file
        @param range Date range
        @param force if true, it will overwrite previous results
        @param html_output Should the report be HTML formatted
    '''
    start = time.clock()

    #maxm = load_maxm_citi_data('citi_maxm.csv')

    last_exception = None
    for date in range:
        sdate = date.strftime("%Y%m%d")
        print sdate

#        if len(maxm[sdate]) <= 0:
#            continue

        if force or not isDataExist(database, sdate):
            if fidessa_db is None:
                db = SQLiteBase(':memory:')
                orderdb.load_fidessa_data(db, date)
                tmp_fidessa_db = get_fidessa_db('SQLite', db, date)
            else:
                tmp_fidessa_db = fidessa_db

            auditTrail = AuditTrail(sdate)
            auditTrail.setMinTime(datetime.datetime(date.year, date.month, date.day, 9, 30, 0))
#            auditTrail.setRootOrderIds(maxm[sdate])
            auditTrail.loadData(True)
            processAuditTrail(auditTrail, database, tmp_fidessa_db)

            # Perform some cleanup
            auditTrail = None
            if fidessa_db is None:
                db.close()
                db = None

        try:
            consolidate(output, database, sdate, html_output)
        except ValueError, e:
            print e
            traceback.print_exc()
            last_exception = e


    print >> output, "Computation time: %f" % (time.clock() - start)

    if last_exception:
        raise last_exception

class Execution(object):
    def __init__(self, pi, done, qty, gross_price, side, cost):
        self.pi = pi
        self.done = done
        self.qty = qty
        self.arrival = None
        self.final = None
        self.side = 1 if side == 'B' else - 1
        self.gross_price = gross_price
        self.cost = cost

    @property
    def improvement(self):
        return self.pi * self.done

    @property
    def left(self):
        return self.qty - self.done

    @property
    def implementationShortfall(self):
        if self.arrival is None or self.final is None:
            return 0

        return self.side * (self.arrival - (self.done * self.gross_price + self.left * self.final) / self.qty)

class PriceImprovements(object):
    def __init__(self):
        self.shortfall = []
        self.improvement = []
        self.done = []
        self.qty = []
        self.cost = []

    def addExecution(self, execution):
        self.improvement.append(execution.improvement)
        self.qty.append(execution.qty)
        self.done.append(execution.done)
        self.shortfall.append(execution.implementationShortfall)
        self.cost.append(execution.cost)

    def getPriceImprovement(self):
        return sum(self.improvement)

    def getDoneQty(self):
        return sum(self.done)

    def getQuantity(self):
        return sum(self.qty)

    @property
    def size(self):
        return len(self.improvement)

    @property
    def implShortfall(self):
        return sum(self.shortfall) / self.size

    @property
    def total_qty(self):
        return sum(self.qty)

    @property
    def total_cost(self):
        return sum(self.cost)

def isDataExist(database, date):
    query = "SELECT 1 FROM price_improvement where date = '%s'" % date

    ans = database.selectOne(query)
    if ans:
        return True
    return False

def processAuditTrail (auditTrail, database, fidessa_db):
    '''
        From the audit trail, this method will compute price improvements
        and other metrics and save them into the database.
    '''

    improvements = defaultdict(PriceImprovements)
    date = auditTrail.filename.split('.')[1]

    # clear data
    database.execute("DELETE FROM price_improvement WHERE date = '%s'" % date)

    # Process the audit trail
    for orderId in auditTrail:
        if auditTrail.stock(orderId) in ignore_stocks:
            continue

        order_time = AuditTrail.getFastDateTime(auditTrail[orderId][0][2])
        if order_time.hour < 9 and order_time.minute < 30:
            #print "skipping", date
            continue

        execution, order, destination = None, None, None

        for line in auditTrail[orderId]:
            if ((line[5] in service_ids and line[1] == service_ids[line[5]]) or
                (line[5] == 'BLUEBOX' and line[6] == 'CROSSFIRE' and line[1] == 'CHLD')):

                # Try to see if the order is aggressive only with the audit trail
                bid, ask = float(line[12]), float(line[13])
                if bid == 0 or ask == 0:
                    # try to find the first valid bid / ask
                    for tmp in auditTrail[orderId]:
                        bid, ask = float(tmp[12]), float(tmp[13])
                        if bid > 0 and ask > 0:
                            break

                try:
                    order = fidessa_db.get_fidessa_order(orderId, False);
                except AttributeError, e:
                    print e
                    print orderId
                    continue


                # Only aggressive price
                if not order.isAggressive(bid, ask) :# or order.isMarketOrder:
                    break

                # Exclude open and close order
                if order.isOpenOrder or order.isCloseOrder:
                    break

                destination = line[5]
                if destination in('POST', 'SWEEP'):
                    destination = fidessa_util.destination_map(destination + ' ' + line[6])
                elif destination == 'BLUEBOX':
                    destination = 'CROSSFIRE'

                if order.done == 0:
                    improvement = 0
                elif order.isBuy:
                    improvement = max(0, ask - order.gross_price)
                else:
                    improvement = max(0, order.gross_price - bid)

                execution = Execution(improvement, order.done, order.quantity,
                                      order.gross_price, order.buy_sell,
                                      fidessa_db.get_cost(order.order_id, date))

                execution.arrival = ask if order.isBuy else bid
            if order and line[1] in ('ESOR'):
                execution.final = ask if order.isBuy else bid

        if execution and destination:
            improvements[(order.code, destination)].addExecution(execution)

    stmt = database.prepareInsertStmt('price_improvement', ['DATE', 'DARKPOOL', 'STOCK', 'PI', 'DONE',
                                                            'QUANTITY', 'COUNT', 'ISHORTFALL', 'COST'])

    def data_generator():
        for key in improvements:
            (code, service) = (key[0], key[1])
            impr = improvements[key]

            yield {'DATE': date, 'DARKPOOL': service, 'STOCK': code,
                   'PI': impr.getPriceImprovement(), 'DONE': impr.getDoneQty(),
                   'QUANTITY': impr.getQuantity(), 'COUNT': impr.size, 'ISHORTFALL': impr.implShortfall,
                   'COST': impr.total_cost}

    database.execManyPreparedInsertStmt(stmt, data_generator())

def consolidate(output, database, date, html_output=False):
    '''
        This method is in charge of loading data from the database
        and format the result for a day.

        @param output Output stream
        @param database Price Improvement database
        @param date Report date
        @param html_output Should the report be formatted in HTML
    '''


    query = ''' select date, darkpool, sum(pi), sum(pi)/sum(done), sum(done),
                      sum(quantity), sum(done*1.0)/sum(quantity), sum(count),
                      ifnull(sum(cost) / sum(done), 0.0)
                from price_improvement
                where date = '%s'
                group by date, darkpool
                order by sum(done) desc
            ''' % date

    rows = database.select(query)

    if not rows:
        raise ValueError("Empty dataset")

    if html_output:
        table = HtmlTable()
        table.setBorder()
        print >> output, table.header(['Date', 'Dark pool', 'PI', 'PI / Share', 'Done', 'Sent', 'Fill Rate', 'Cost / Share', 'Nb Orders'],
                                      width=[100, 150, 100])

    # Sort by Done
    for row in rows:
        if html_output:
            print >> output, table.line([row[0], row[1],
                                         '$' + locale.format("%.2f" , row[2], grouping=True),
                                         "$%.6f" % row[3] if row[3] is not None else 0.0,
                                         locale.format("%d", row[4], grouping=True),
                                         locale.format("%d", row[5], grouping=True),
                                         "%.2f%%" % (100 * row[6]),
                                         '$' + locale.format("%.5f", row[8], grouping=True),
                                         locale.format("%d", row[7], grouping=True)],
                                         ['','',"style='text-align:right'"])
        else:
            print >> output, ','.join(map(str, row))

    if html_output:
        print >> output, table.end()

# ---------------------------------------------------------------------------- #
#                                                                              #
#                              Chart Method                                    #
#                                                                              #
# ---------------------------------------------------------------------------- #


def saveToJpg(figure, filename):
    import PIL

    tmp = 'temp_img.png'
    plt.savefig(tmp)

    output = PIL.Image.open(tmp)
    output.save(filename, 'JPEG')
    os.remove(tmp)

def generate_cost_chart (start, end, darkpool='CROSSFIRE'):

    import matplotlib
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    query = ''' SELECT date, sum(cost) / sum(quantity)
                FROM price_improvement
                WHERE date >= '%s' AND date <= '%s' AND darkpool = '%s'
                GROUP BY date
                ORDER BY date
            ''' % (start, end, darkpool)
    rows = database.select(query)

    dates = []
    cost = []
    for row in rows:
        dates.append(datetime.datetime.strptime(row[0], "%Y%m%d").date())
        cost.append(row[1])

    datesFmt = mdates.DateFormatter("%b-%d")
    matplotlib.rcParams['font.size'] = 8.0

    fig = plt.figure()
    fig.set_size_inches(4., 3.)
    ax = fig.add_subplot(1, 1, 1)

    ax.plot(dates, cost)
    ax.xaxis.set_major_formatter(datesFmt)
    fig.autofmt_xdate()
    ax.set_ylabel("Cost / Share)")
    tmp = 'temp_img.png'
    plt.savefig(tmp)

class piBoxPlot(object):
    def __init__(self, start, end):
        self.start, self.end = start, end

    def generateChart(self, type):

        if type == 'pi':
            self.field = 'sum(pi)/sum(done)'
            self.title = 'Price Improvement per share'
        else:
            self.field = 'sum(done*100.0)/sum(quantity)'
            self.title = 'Fill Rate'


        query = ''' SELECT date, darkpool, %s
                    FROM price_improvement
                    WHERE date >= '%s' and date <= '%s'
                    GROUP BY date, darkpool
                ''' % (self.field, self.start, self.end)
        rows = database.select(query)
        result = defaultdict(list)
        for row in rows:
            if row[2]:
                result[row[1]].append(row[2])

        # generate data for boxplot
        data = []
        darkpools = []
        for darkpool, fillrate in sorted(result.items(), key=lambda item: quantile(item[1], 0.5), reverse=True):
            if type == 'pi' and quantile(fillrate, 0.5) > 0.01:
                continue
            darkpools.append(darkpool)
            data.append(result[darkpool])

        plt.figure()
        matplotlib.rcParams['font.size'] = 8.0

        plt.boxplot(data)
        locs, labels = plt.xticks(range(1, len(darkpools) + 1), darkpools)
        plt.setp(labels, 'rotation', 'vertical')

        ax = plt.gca()
        ax.set_title(self.title)
        if type == 'pi':
            ax.set_ylim(0, 0.01)

        filename = '%s.jpg' % self.title
        saveToJpg(plt, filename)


def getMarketCap(stock, date):
    security = SecurityIdCache.getSecurity(stock)
    if not security:
        return 'Unknown'

    mktcap = security.market_cap(date)
    if mktcap > 100000000000: # $10 billion
        return "Large-cap"
    elif mktcap > 1000000000: # $1 billion
        return "Mid-cap"
    else:
        return "Small-cap"

class DestinationFillRate:
    def __init__(self):
        self.done = defaultdict(lambda: {'A': 0, 'B': 0, 'C': 0})
        self.sent = defaultdict(lambda: {'A': 0, 'B': 0, 'C': 0})

    def addExecution(self, cap, tape, done, sent):
        self.done[cap][tape] += done
        self.sent[cap][tape] += sent

    def to_str(self):
        mktcaps = ['Unknown','Large-cap','Mid-cap','Small-cap']

        str = ''
        for mkt in mktcaps:
            for tape in ['A','B','C']:
                if self.sent[mkt][tape] > 0:
                    try:
                        str += '%.2f\t' % (100.0 * self.done[mkt][tape] / self.sent[mkt][tape])
                    except TypeError:
                        print 'error'
                else:
                    str += '0.0\t'

        return str

def monthly_fillrate(year, month):
    startdate = '%4d%2d01' % (year,month)
    if month == 12:
        month, year = 1, year + 1
    else:
        month = month + 1
    enddate = '%4d%2d01' % (year,month)

    rows = database.select('''
                    SELECT stock, darkpool, sum(done), sum(quantity) FROM price_improvement
                    WHERE date >= '%s' and date < '%s'
                    GROUP BY stock, darkpool
                    ''' % (startdate,enddate))

    destinations = defaultdict(DestinationFillRate)
    mktcap_cache = {}

    for row in rows:

        if row[0] not in mktcap_cache:
            mktcap_cache[row[0]] = getMarketCap(row[0], startdate)

        cap = mktcap_cache[row[0]]
        tape = fidessa_util.get_tape (row[0])

        destinations[row[1]].addExecution(cap, tape, row[2], row[3])


    for dest in destinations:
        print dest, destinations[dest].to_str()
#        print cap
#        destination = set(done[cap]['A'].keys() + done[cap]['B'].keys() + done[cap]['C'].keys())
#        print '\t'.join(destination)
#        for dest in destination:
#            if dest in done[cap]['A']:
#                print done[cap]['A'][dest]*100.0 / sent[cap]['A'][dest],
#            else:
#                print '0',
#        print ''
#        for dest in destination:
#            if dest in done[cap]['B']:
#                print done[cap]['B'][dest]*100.0 / sent[cap]['B'][dest],
#            else:
#                print '0',
#        print ''
#        for dest in destination:
#            if dest in done[cap]['C']:
#                print done[cap]['C'][dest]*100.0 / sent[cap]['C'][dest],
#            else:
#                print '0',
#        print ''


def load_maxm_citi_data(file):
    results = defaultdict(set)

    with open(file,'r') as data:
        for line in data:
            date, order_id = line.strip().split(',')
            results[date].add(order_id)
    return results

if __name__ == '__main__':
    main(sys.stdout, [datetime.date(2011,6,9)], None, True, False)
    #generate_cost_chart('20110302', '20110602')