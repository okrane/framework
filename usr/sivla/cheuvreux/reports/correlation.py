'''
Module for computing a correlation matrix

Created on Sep 27, 2010

@author: syarc
'''
from cheuvreux.dbtools.repository import Repository
from cheuvreux.dbtools.Sqlite import SQLiteBase
from cheuvreux.dbtools.tickdb import TickDB
from cheuvreux.fidessa import SecurityIdCache
from cheuvreux.utils import statistics
from cheuvreux.utils.date import previous_weekday, go_back, workday_range
from cheuvreux.utils.matrix import Matrix
from cheuvreux.utils.timeseries import TimeSeries, correlation
import cheuvreux.utils.latex as latex
from collections import defaultdict
import datetime
import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.mpl as mpl
import numpy as np
import time
import cPickle
import heapq
import getopt
import sys
import os
import pylab
from cheuvreux.utils.latex import CheuvreuxDocument

CACHE_DIR = 'cache'
CHART_DIR = 'chart'

if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

if not os.path.exists(CHART_DIR):
    os.makedirs(CHART_DIR)

matplotlib.rcParams['font.size'] = 8.0

def most_correlated_stock(stock, correl_matrix, nb=5):
    ''' Returns the 'nb' most correlated stock to the stock 'stock' '''
    correl_vector = correl_matrix[stock]

    max = heapq.nlargest(nb, range(0, len(correl_vector)), key=lambda i: correl_vector[i] if correl_vector[i] != 1 else 0)

    for i in max:
        print correl_matrix.colname(i), correl_matrix.getitem(stock, i)

def correlation_matrix_for_index(indices, lookback=64, date=None):
    '''
        Compute the correlation matrix for the components of the index.
        Each correlation is computed on 'lookback' number of days.

        @param indices: List of index ID
        @param lookback: Number of days used to compute the correlation
        @param date: Use to compute correlation in the past (default is previous working day)
    '''

    if not date:
        date = previous_weekday()

    lookback_date = go_back(date, lookback)

    for index_id in indices:
        # Get Index components
        sec_ids = Repository.index_components(index_id)

        rows = TickDB.getCloses(sec_ids, lookback_date, date)
        mktdata = defaultdict(TimeSeries)
        for row in rows:
            mktdata[row.security_id].add_point(row.date, row.close_prc)

        matrix = Matrix(len(sec_ids), len(sec_ids))

        for i, sec_id in enumerate(sec_ids):
            ts_stockA = mktdata[sec_id]
            codeA = SecurityIdCache.getCodeFromSecurity(sec_id)
            if codeA is None:
                print sec_id

            matrix.setitem(codeA, codeA, 1.0)
            for j in xrange(0,i):
                codeB = SecurityIdCache.getCodeFromSecurity(sec_ids[j])
                if codeB is None:
                    print sec_ids[j]

                ts_stockB = mktdata[sec_ids[j]]
                cor =  correlation(ts_stockA, ts_stockB)
                matrix.setitem(codeA, codeB, cor)
                matrix.setitem(codeB, codeA, cor)

        save_correlation_matrix(matrix, index_id, date)

def correlation_matrix_to_csv(matrix, output):
    print >> output, ';'.join(sorted(matrix.row_iterator()))
    for row in sorted(matrix.row_iterator()):
        for col in sorted(matrix.col_iterator()):
            output.write('%.6f;' % matrix.getitem(row,col))
        output.write('\n')

def correlation_with_index(indices, lookback=64, date=None, db=None):
    ''' Computes the correlation of each component of an index
        with the index itself.

        @param indices: list of index ID
        @param lookback: Number of days used to compute the correlation
        @param date: Use to compute correlation in the past (default is previous working day)
        @param db: If not None will save the correlation into the database
    '''

    if not date:
        date = previous_weekday()

    if not indices:
        indices = []
        rows = db.select('SELECT distinct "index" FROM histo_correlation')
        for row in rows:
            indices.append(Repository.index_id(row[0]))

    lookback_date = go_back(date, lookback)

    for index_id in indices:

        # Get Index components
        sec_ids = Repository.index_components(index_id)

        # Get indices close prices
        indice_closes = TimeSeries()
        for row in TickDB.getIndiceCloses(index_id, lookback_date, date):
            indice_closes[row.date] = row.close_prc

        if len(indice_closes) == 0:
            print 'No data for the indice %d' % index_id
            continue

        # Load market data
        rows = TickDB.getCloses(sec_ids, lookback_date, date)
        mktdata = defaultdict(TimeSeries)
        for row in rows:
            mktdata[row.security_id][row.date] = row.close_prc

        # Compute correlations
        correlations = dict()
        for sec_id in sec_ids:
            ts = mktdata[sec_id]
            code = SecurityIdCache.getCodeFromSecurity(sec_id)
            if not code:
                code = sec_id

            try:
                correlations[code] = correlation(ts, indice_closes)
            except ZeroDivisionError:
                print "No data for ", sec_id, code
                sys.exit(0)

        if db:
            index_name = Repository.index_name(index_id)
            db.execute('INSERT INTO histo_correlation ("index", "date", "avg_correl", "lookback") VALUES (?,?,?,?)',
                       index_name, date.strftime("%Y%m%d"), statistics.mean(correlations.values()), lookback)

def avg_correl_chart(indices, startdate, enddate, db):

    if not indices:
        indices = []
        rows = db.select('SELECT "index" FROM histo_correlation WHERE date = ?', enddate.strftime('%Y%m%d'))
        for row in rows:
            indices.append(row[0])

    for idx in indices:
        if type(idx) is int:
            index_name = Repository.index_name(idx)
        else:
            index_name = idx

        rows = db.select('SELECT date, avg_correl FROM histo_correlation WHERE "index" = ? ORDER BY date',
                         index_name)

        datesFmt = mdates.DateFormatter("%b-%d")

        fig = plt.figure()
        fig.set_size_inches(7., 3.)
        ax = fig.add_subplot(1, 1, 1)
        plt.grid(True)

        dates, correl = [], []
        for row in rows:
            dates.append(datetime.datetime.strptime(row['date'],'%Y%m%d'))
            correl.append(row['avg_correl'])

        ax.plot(dates, correl)
        ax.xaxis.set_major_formatter(datesFmt)
        ax.set_title('%s average correlations' % index_name)
        fig.autofmt_xdate()


        filename = "%s_%s_%s" % (index_name, startdate.strftime("%Y%m%d"), enddate.strftime("%Y%m%d"))
        plt.savefig(os.path.join(CHART_DIR, filename.replace(' ', '_')) , dpi = (300))

def correl_bar_chart(indices, date, db):

    if indices:
        index_names = []
        for index in indices:
            index_names.append(Repository.index_name(index))

        query = ''' SELECT "index", avg_correl FROM histo_correlation
                    WHERE "index" in ('%s') and date = ?
                    ORDER BY avg_correl DESC
                     ''' % ("','".join(index_names))
    else:
        query = ''' SELECT "index", avg_correl FROM histo_correlation
                    WHERE date = ? ORDER BY avg_correl DESC '''

    rows = db.select(query, date.strftime('%Y%m%d'))

    avg_correl = [row[1] for row in rows]

    fig = plt.figure()
    fig.set_size_inches(7., 7.)
    ax = fig.add_subplot(111)
    plt.subplots_adjust(left=0.05, bottom=0.15, right=0.98, top=0.98, wspace=0, hspace=0)
    plt.grid(True)

    pos = np.arange(len(rows)) + 0.5 #Center bars on the Y-axis ticks
    rects = ax.barh(pos, avg_correl, align='center', height=0.5)
    ax.axis([0,1,0,len(rows)])
    pylab.yticks([])

    for i, rect in enumerate(rects):
        width = float(rect.get_width())

        #if width < len(rows[i][0]): # The bars aren't wide enough to print the ranking inside
        #    xloc = width + 1 # Shift the text to the right side of the right edge
        #    clr = 'black' # Black against white background
        #    align = 'left'
        #else:
        xloc = 0.02 * width  # Shift the text to the left side of the right edge
        clr = 'white' # White on magenta
        align='left'

        yloc = rect.get_y() + rect.get_height()/2.0 # Center the the text vertically in the bar
        ax.text(xloc, yloc, rows[i][0], horizontalalignment=align,
                 verticalalignment='center', color = clr, weight='bold')

    pylab.xlabel('Average Correlation')
    plt.autumn()
    plt.savefig(os.path.join(CHART_DIR, 'correl_%s' % date.strftime('%Y%m%d')),dpi=(300))

def generate_report():
    report = CheuvreuxDocument('correl_report.tex')
    for chart in os.listdir(CHART_DIR):
        if chart.endswith('.png'):
            report.add_graphics(CHART_DIR + '/' + chart)
    report.end()
    report.compile()


def load_correlation_matrix(index, date):
    file = os.path.join(CACHE_DIR, 'correl_matrix_%d_%s.p' % (index, date.strftime('%Y%m%d')))
    if not os.path.exists(file):
        raise ValueError("%s doesn't exist" % file)

    return cPickle.load(open(file, 'rb'))

def save_correlation_matrix(matrix, index, date):
    file = os.path.join(CACHE_DIR, 'correl_matrix_%d_%s.p' % (index, date.strftime('%Y%m%d')))
    cPickle.dump(matrix, open(file, 'wb'))

def main():
    db = SQLiteBase('correlation.db')

    try:
        optlist, args = getopt.getopt(sys.argv[1:], 'hn:i:l:d:',
                                      ['help', 'nb=', 'index=', 'lookback=', 'date='])
    except getopt.GetoptError, err:
        print str(err)
        usage(sys.argv[0])
        sys.exit(2)

    nb = 5
    index = None
    start = None
    date = previous_weekday()
    lookback = 64

    for opt, val in optlist:
        if opt in ('-n', '--nb'):
            nb = int(val)
        elif opt in ('-i', '--index'):
            if val == '*':
                index = None
            elif val.isdigit():
                index = [int(val)]
            elif val.find(',') > 0:
                index = map(int,val.split(','))
            else:
                index = [val]
        elif opt in ('-l', '--lookback'):
            lookback = int(val)
        elif opt in ('-d', '--date'):
            if ':' in val:
                (start, date) = val.split(':')
                start = datetime.datetime.strptime(start, '%Y%m%d')
                date  = datetime.datetime.strptime(date, '%Y%m%d')
            else:
                date = datetime.datetime.strptime(val, '%Y%m%d')
        elif opt in ('-h', '--help'):
            usage(sys.argv[0])
            sys.exit(0)

    if len(args) == 0:
        usage(sys.argv[0])
        sys.exit(-1)

    for (i, arg) in enumerate(args):
        if arg == 'correl_matrix':
            correlation_matrix_for_index(index, lookback, date)
        elif arg == 'index_correl':
            if start:
                for d in workday_range(start, date):
                    correlation_with_index(index, lookback, d, db)
            else:
                correlation_with_index(index, lookback, date, db)
        elif arg == 'most_correl_stock':
            correl_matrix = load_correlation_matrix(index, date)
            print nb
            most_correlated_stock(args[i+1], correl_matrix, nb)
        elif arg == 'chart':
            avg_correl_chart(index, start, date, db)
            correl_bar_chart(index, date, db)
        elif arg == 'report':
            generate_report()

def usage(basename):
    print "usage: %s [OPTIONS] [COMMAND]" % basename;
    print ""
    print "COMMANDS:"
    print "  correl_matrix             \t Build the correlation matrix"
    print "  index_correl              \t Compute the index component correlation"
    print "                            \t with the index"
    print "  most_correl_stock <stock> \t Return the 'nb' most correlated stocks"
    print "  chart                     \t Generate chart for the historic average correlation"
    print "  report                    \t Generate PDF report"
    print ""
    print "OPTIONS:"
    print " -n, --nb                   \t Number of stock to return"
    print " -i, --index                \t Index ID or Index name"
    print " -l, --lookback             \t Set the lookback period, default is 64"
    print " -d, --date                 \t Set the date, default is previous working day."
    print "                            \t format is YYYYMMDD"
    print ""
    print " -h, --help                 \t Print this help message"

if __name__ == '__main__':
    main()

# 1,2,3,46,11,12,124,35,472,19,345,346,347
# 6,190,54,9,7,23,68,75

