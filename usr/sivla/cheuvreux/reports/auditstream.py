#!/usr/bin/python

'''
  CACVersion CheuvreuxUSFlow Version 1.1 20101203_1300

  Modification History

  Date    : October 06, 2010
  Author  : Sylvain Archenault
  Changes : Initial script

  Date    : November 02, 2010
  Author  : Sylvain Archenault
  Changes : Add Ratio and Notional columns
            Separate US stocks from other countries

  Date    : November 23, 2010
  Author  : Sylvain Archenault
  Changes : Output flows for unknown sector + detailled flows by stock
            Change FIX field for gross prices
            Improve ratio computation (try to prevent 0 in the ratio)

  Date    : July 21, 2010
  Author  : Sylvain Archenault
  Changes : Support Colombia, Argentina and Chile markets
'''
from cheuvreux.dbtools.repository import Repository
from cheuvreux.stdio.html import HtmlTable, table_style
from cheuvreux.stdio.mail import HtmlEmail
import datetime
import getopt
import re
import sys
import gzip
import os
import locale
from cheuvreux.utils.numbers import rational_approximation
#import matplotlib
#import pylab

try:
    locale.setlocale(locale.LC_ALL, 'en_US')
except locale.Error:
    locale.setlocale(locale.LC_ALL, '')

# FIX Constants
EXEC_TYPE = 150
COUNTERPARTY = 109
ORD_STATUS = 39
EXEC_TRANS_TYPE = 20
AVG_PRICE =6
CUM_QUANTITY = 14
ORDER_QUANTITY = 38
SECURITY_ID =48
ORDER_TYPE = 40
GROSS_PRICE = 31
LAST_SHARES = 32
SIDE = 54
CLI_ORD_ID = 11
ORDER_ID = 37
CURRENCY = 15
TIME = 52
TICKER = 55

# Test stock
ignore_isins = ['US0000000002', 'ZXZZT', 'ZWZZT']

# List of stocks for which the sector is unknown
unknown_stock = []

def parseFIXLine(line):
    ''' Simple FIX line parse
        @return Dictionary of the form { FIX_ID = VALUE }
    '''

    data = {}
    regex = re.compile('([^=]+)=([^\x01]+)\x01')
    for match in regex.finditer(line):
        if match:
            try:
                data[int(match.group(1))] = match.group(2)
            except ValueError, e:
                print >> sys.stderr, e

    return data

class Flow:
    '''
        Helper class to compute flow.
    '''
    def __init__(self, ticker='', buy=0.0, sell=0.0):
        ''' Constructor

            @param ticker: Stock ticker
            @param buy, sell: Initial buy and sell value
        '''
        self.ticker = ticker
        self.buy, self.sell = buy, sell

    def add(self, side, qty, gross_price):
        ''' Add an execution to the flow
            @param side: 1 for buy, anything else for sell
            @param qty: Quantity bought / sold
            @param gross_price: Gross price
        '''
        if side == 1:
            self.buy += qty * gross_price
        else:
            self.sell += qty * gross_price

    def pct_buy(self):
        ''' @return Percentage of buy flow '''
        return 100.0 * self.buy / (self.buy + self.sell)

    def pct_sell(self):
        ''' @return Percentage of sell flow '''
        return 100.0 * self.sell / (self.buy + self.sell)

    def total(self):
        ''' @return Total quantity '''
        return 1.0 * (self.sell + self.buy)

    def imbalance(self):
        ''' Return Flow imbalance.

            It is define by (buy_quantity - sell_quantity) / total
        '''
        return 1.0 * (self.buy - self.sell) / self.total

    def copy(self):
        ''' Copy constructor. '''
        return Flow(self.ticker, self.buy, self.sell)

    def __iadd__(self, other):
        ''' Handles += operator '''
        self.buy += other.buy
        self.sell += other.sell
        return self

    def __add__(self, other):
        ''' Handles + operator '''
        return Flow('',
                   self.buy + other.buy,
                   self.sell + other.sell)

    def __gt__(self, other):
        ''' Compares two flows based on their total quantity '''
        return self.total() > other.total()

    def __lt__(self, other):
        ''' Compares two flows based on their total quantity '''
        return self.total() < other.total()

    def __str__ (self):
        ''' String representation of the form : buy_quantity / sell_quantity (turnover)'''
        return '%.0f \t %.0f \t %.0f' % (self.buy, self.sell, self.total())

    __repr__ = __str__

def get_flows(filename):
    '''
        This method return a dictionary of flow by stock

        @param filename: Fidessa FIX 4.2 log file
        @param currency: If not None, filter on this currency
        @return Dictionary of the form { CCY : {STOCK: Flow object} }
    '''
    flows = {}

    extension = os.path.splitext(filename)[1]
    if extension == '.gz':
        auditstream = gzip.open(filename,'rb')
    else:
        auditstream = open(filename,'r')

    last_trade_time = None

    for line in auditstream:
        fields = parseFIXLine(line)
        if EXEC_TYPE in fields and int(fields[EXEC_TYPE]) in (1,2):
            ccy = fields[CURRENCY]
            if ccy not in flows:
                flows[ccy] = {}

            if fields[SECURITY_ID] not in flows[ccy]:
                flows[ccy][fields[SECURITY_ID]] = Flow(fields[TICKER])

            flows[ccy][fields[SECURITY_ID]].add(int(fields[SIDE]),
                                                int(fields[LAST_SHARES]),
                                                float(fields[GROSS_PRICE]))
            if TIME in fields and fields[TIME] is not '':
                last_trade_time = fields[TIME]

    auditstream.close()

    try:
        last_trade_time = parse_gmt_time(last_trade_time)
    except ValueError, e:
        print e

    return flows, last_trade_time


def sector_flows(flows_by_stock):
    '''
        Aggregate flows by sector.

        @param filename: Fidessa FIX 4.2 log file
        @return A list with the following element [Sector, % Buy, % Sell, % Sector / Total]
    '''

#    tmp_output_file = open('etf_flow.xls','w')

    # Retrieve sector for all stock, use GISC classification (id=25)
    sectors = Repository.get_sectors(flows_by_stock.keys(), 25)
    flows_by_sector = {}
    for stock in flows_by_stock:

        # ignore stocks
        if stock in ignore_isins:
            continue

        # Stock has a sector, aggregate its flow to its sector
        elif stock in sectors:
            if sectors[stock] not in flows_by_sector:
                flows_by_sector[sectors[stock]] = flows_by_stock[stock]
            else:
                flows_by_sector[sectors[stock]] += flows_by_stock[stock]
        else:
            # Sector not found, try another one (id=24)
            sector = Repository.get_sector(stock, 24)
            if sector:
                sectors[stock] = sector
#                if sector[1] == 'ETF':
#                    print >> tmp_output_file, flows_by_stock[stock].ticker, flows_by_stock[stock]

                if sector not in flows_by_sector:
                    flows_by_sector[sector] = flows_by_stock[stock]
                else:
                    flows_by_sector[sector] += flows_by_stock[stock]
            else:
                # Still no sector, log it
                print >> sys.stderr, 'Sector not found for %s : %s' % (stock, flows_by_stock[stock].ticker)
                unknown_stock.append(flows_by_stock[stock])
                if (0,'Unknown') not in flows_by_sector:
                    # We need to do a copy here, because we don't want that the
                    # Flow object to be modified when aggregating flows (see else clause)
                    flows_by_sector[(0,'Unknown')] = flows_by_stock[stock].copy()
                else:
                    flows_by_sector[(0,'Unknown')] += flows_by_stock[stock]

    # Compute total and return results
    total = 0
    for flow in  flows_by_sector.values():
        total += flow.total()
    #total = sum(flow.total() for flow in flows_by_sector.values())

    items = [(v, k) for k, v in flows_by_sector.items()]
    items.sort()
    items.reverse()
    items = [(k, v) for v, k in items]

    data = []
    for sector, flow in items:
        data.append([sector[1], flow.pct_buy(), flow.pct_sell(), 100.0 * flow.total() / total, flow.total()])

#    matplotlib.rcParams['font.size'] = 8.0
#    pylab.figure(1,figsize=(6,4))
#    ax = pylab.axes([0.05,0,0.7,1])
#    pylab.pie([flow.total for flow in flows_by_sector.values()],
#              labels=[key[1] for key in flows_by_sector.keys()],
#              colors=[(0.33,0.19,0.02),(0.55,0.32,0.04),(0.75,0.51,0.18),
#                      (0.87,0.51,0.49),(0.96,0.91,0.76),(0.96,0.96,0.96),
#                      (0.78,0.92,0.9),(0.5,0.8,0.76),(0.21,0.59,0.56),
#                      (0,0.4,0.37),(0,0.24,0.19)], autopct='%1.0f%%')
#
#    pylab.savefig('sector.png')
#
#    pylab.figure(2, figsize=(6,3))
#
#    pos = pylab.arange(len(flows_by_sector)) + 1
#    val = sorted([(sector, flow.imbalance) for sector, flow in flows_by_sector.items()], key=operator.itemgetter(1))
#
#    colors = ['#acf4ac' if v[1] > 0 else '#ffcebe' for v in val]
#    ax = pylab.axes([0.3,0.15,0.65,0.78])
#    ax.vlines(0,0,len(flows_by_sector)+1)
#    pylab.barh(pos, [v[1] for v in val], align='center', color=colors)
#    pylab.yticks(pos, [v[0][1] for v in val])
#    pylab.xlabel("Imbalance")
#    pylab.savefig('imbalance.png')
    #pylab.show()

    return data

def get_ratio(buy, sell):

    if sell > 0:
        # First approximation
        n,d,err = rational_approximation(round(buy / sell,2), 10)

        # Round buy and sell to closest "5"
        round_buy = round(buy * 2, -1) / 2
        round_sell = round(sell * 2, -1) / 2
        if round_sell > 0:
            rn,rd,err = rational_approximation(round_buy / round_sell, 10)
        else:
            rn, rd = n,d

        # Take the fraction with the smallest sum (numerator + denominator)
        if n+d > rn +rd:
            return '%d:%d' % (rn,rd)
        else:
            return '%d:%d' % (n,d)
    return '1:0'

def format_flows(flows, output, html=False):
    '''
        Format flows for ouput

        @param flows as return by the method sector_flows
        @param output Output file (should implement write method)
        @param html If True, will output the result as a HTML table
    '''

    header = ['Sector', '% Buy', '% Sell', '% Total', 'Ratio', 'Notional']
    if html:
        table = HtmlTable()
        table.setBorder()

        pct_buy = pct_sell = notional = 0.0
        print >> output, table.header(header, [200, 100, 100, 100, 100, 100])
        for line in flows:
            print >> output, table.line(['%s' % line[0], '%.0f%%' % line[1],
                                         '%.0f%%' % line[2], '%.0f%%' % line[3],
                                         get_ratio(line[1], line[2]),
                                         '$' + locale.format("%.0f", line[4], grouping=True)],
                                         ['',"style='text-align:right'"])
            pct_buy += line[1] * line[3]
            pct_sell += line[2] * line[3]
            notional += line[4]
        # Total line
        print >> output, table.line(['<b>Overall', '<b>%.0f%%</b>' % (pct_buy / 100.0),
                                    '<b>%.0f%%</b>' % (pct_sell / 100.0), '<b>100%</b>',
                                    '<b>%s</b>' % get_ratio(pct_buy / 100.0, pct_sell / 100.0),
                                    '$' + locale.format("%.0f", notional, grouping=True)],
                                    ['', "style='text-align:right'"])

        print >> output, table.end()

    else:
        print >> output, '%s %10s %10s %10s' % (header[0].center(30), header[1],
                                          header[2], header[3])
        for line in flows:
            print >> output, '%30s %10.2f %10.2f %10.2f' % (line[0], line[1], line[2], line[3])

            pct_buy += line[1] * line[3]
            pct_sell += line[2] * line[3]
            notional += line[4]

        print >> output, '%30s %10.2f %10.2f %10.2f' % ('Overall'.center(30), pct_buy, pct_sell, 100.0)

def summary_flow(flows):
    buy = sell = total = 0.0
    for flow in flows.values():
        buy += flow.buy
        sell += flow.sell
        total += flow.total()

#    if buy + sell <= 0.0:
#        return [0, 0, 0]

    return [100.0 * buy / (buy + sell), 100.0 * sell / (buy + sell), total]



def usage(basename):
    print "usage: %s [OPTIONS] [FILENAME]" % basename;
    print ""
    print "FILENAME: Fidessa FIX 4.2 drop copy log file"
    print ""
    print " OPTIONS:"
    print "   -h, --help          \t Display this help message"
    print "   -e, --email=<DEST>  \t Recipients"

    clean_pid_file()

def clean_pid_file():
    pidfile = os.getenv("PIDFILE")

    try:
        if pidfile and os.path.exists(pidfile):
            os.remove(pidfile)
    except Exception, e:
        print >> sys.stderr, e

def parse_gmt_time(string):
    utc = datetime.datetime(int(string[0:4]), int(string[4:6]), int(string[6:8]),
                             int(string[9:11]), int(string[12:14]), int(string[15:17]))
    utcoffset = datetime.datetime.utcnow() - datetime.datetime.now()

    return utc - utcoffset


def main(argv):

    # Default value
    recipients = 'sarchenault'

    # Set log file
    logfile = os.getenv('CHEUVREUXUSFLOW_LOG_NAME')
    if logfile:
        sys.stdout = sys.stderr = open(logfile, 'a')
        print ' ===== %s ====' % (datetime.datetime.now())


    # Argument parsing
    try:
        opts, args = getopt.getopt(argv[1:], 'he:',
                                  ['help', 'email='])
    except getopt.GetoptError:
        usage(argv[0])
        sys.exit(-1)

    for opt, val in opts:
        if opt in ("-h", "--help"):
            usage(argv[0])
            sys.exit(0)
        elif opt in ('-e', '--email'):
            recipients = val

    if len(args) == 0:
        print >> sys.stderr, 'FILENAME is mandatory'
        usage(argv[0])
        sys.exit(-2)

    # Read Fidessa log file
    try:
        flows, last_trade_time = get_flows(args[0])
    except IOError, e:
        # Handle the case where the file does not exist
        print >> sys.stderr, str(e) + '\n'
        usage(sys.argv[0])
        sys.exit(-3)

    # Format and output flows
    mail = HtmlEmail("smtpnotes", table_style)
    mail.set_sender("Sylvain Archenault <sarchenault@cheuvreux.com>")
    mail.set_subject('Flow %s' % datetime.datetime.now().strftime('%Y-%m-%d %H:%M'))

    if 'USD' in flows:
        print >> mail, '<h2>US Stocks</h2>'
        sector = sector_flows(flows['USD'])
        format_flows(sector, mail, True)

    currencies = {'CAD': 'Canada', 'MXN': 'Mexico', 'BRL': 'Brazil', 'COP': 'Colombia',
                  'ARS': 'Argentina', 'CLP': 'Chile'}

    table = None
    for ccy in currencies:
        if ccy in flows:
            if not table:
                table = HtmlTable()
                table.setBorder()
                print >> mail, '<h2>Other Countries</h2>'
                print >> mail, table.header(['Country', '% Buy', '% Sell', 'Ratio', 'Notional'], [200, 100, 100, 100, 100])
            data = summary_flow(flows[ccy])
            print >> mail, table.line([currencies[ccy], '%.0f%%' % data[0], '%.0f%%' % data[1],
                                       get_ratio(data[0], data[1]),
                                       '$' + locale.format('%.0f', data[2], grouping=True)],
                                      ['', "style='text-align:right'"])

    if table:
        print >> mail, table.end()


    if len(unknown_stock) > 0:
        table = HtmlTable()
        table.setBorder()
        print >> mail, '<h2>Unknown Sector Detail</h2>'
        print >> mail, table.header(['Ticker', 'Buy', 'Sell'], [100])
        for flow in unknown_stock:
            print >> mail, table.line([flow.ticker,
                              '$' + locale.format('%.0f', flow.buy, grouping=True),
                              '$' + locale.format('%.0f', flow.sell, grouping=True)],
                              ['', "style='text-align:right'"])
        print >> mail, table.end()

    if last_trade_time:
        print >> mail, '<br/>&nbsp;<br/><span style="color: gray; font-size:8pt">Last trade at %s</span>' % last_trade_time

    mail.send(recipients)

    if logfile:
        sys.stdout = sys.stderr = open(logfile, 'a')
        print ' ===== %s Done ====' % (datetime.datetime.now())

    clean_pid_file()

def historic_flow():
    output = open('historic_flow.csv','w')

    for file in os.listdir('usflow'):
        date = file.split('_')[1]
        print date
        flow, last_trade_time = get_flows(os.path.join('usflow',file))
        if 'USD' in flow:
            sectors = sector_flows(flow['USD'])
            for sector in sectors:
                print >> output, ','.join(map(str,[date] + sector))

    output.close()

if __name__ == '__main__':
#    historic_flow()
     main(sys.argv)

