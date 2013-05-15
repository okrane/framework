'''
Created on Apr 18, 2012

@author: syarc
'''
from cheuvreux.dbtools.odbc import ODBC
from cheuvreux.dbtools.repository import Repository
from cheuvreux.stdio.html import HtmlTable
from cheuvreux.stdio.mail import HtmlEmail
from collections import defaultdict
from matplotlib.dates import DateFormatter, MinuteLocator, HourLocator, date2num
from matplotlib.pyplot import figure, show
from matplotlib.ticker import MultipleLocator, FuncFormatter
import datetime
import matplotlib
import random
import sys

def diff(a):
    return [ x-y for (x,y) in zip(a[1:],a[:-1]) ]

def get_average_curve(stocks, news, db):
    query = ''' SELECT vcv.time, vcv.value FROM volume_curve vc
                JOIN volume_curve_value vcv ON vcv.curve_id = vc.curve_id
                WHERE vc.code = ? and vc.context_id = ?
            '''

    avg = defaultdict(list)
    for stock in stocks:
        # Get the news curve
        rows = db.select(query, stock[0], news)
        curve = diff([row[1] for row in rows])
        for i in range(0,len(curve)):
            avg[i].append(curve[i])

    avg_curve = []
    for i in avg:
        avg_curve.append(sum(avg[i]) / len(avg[i]))

    return avg_curve

def get_curve_diff(stock, news, db):
    # Get the news curve
    query = ''' SELECT vcv.time, vcv.value FROM volume_curve vc
                JOIN volume_curve_value vcv ON vcv.curve_id = vc.curve_id
                WHERE vc.code = ? and vc.context_id = ?
            '''
    rows = db.select(query, stock[0], news[2])
    news_curve = diff([row[1] for row in rows])
    # Get the standard curve
    rows = db.select(query, stock[0], 166)
    usual_day_curve = diff([row[1] for row in rows])

    error = 0
    for i in range(0,len(news_curve)):
        error += (usual_day_curve[i] - news_curve[i])**2

    print stock[0], error
    return error

def pct_format(x, pos=0):
    return '%1.2f' % (100*x)

def report(date, output):

    database = ODBC("DRIVER={PostgreSQL ANSI};Server=65.244.97.57;DATABASE=bluebox;PORT=5435;Uid=python;Pwd=python;AUTO_SERVER=true")
    #database = ODBC("DRIVER={PostgreSQL ANSI};Server=127.0.0.1;DATABASE=bluebox_prod;PORT=5435;Uid=python;Pwd=python;AUTO_SERVER=true")

    query = ''' SELECT ce.event_type, event_name FROM cal_event ce
                JOIN event_type et on et.event_type = ce.event_type
                WHERE event_date = ?
            '''

    rows = database.select(query, date)

    events = [row[0] for row in rows]

    if len(events) <= 0:
        return False

    stock_by_news = defaultdict(list)

    # for each news, get the list of affected stocks
    query = ''' SELECT code, et.event_type, c.context_id, c.security_id, et.event_name
                  FROM (
                        SELECT security_id, min(ranking) as ranking FROM context_ranking cr
                        WHERE (event_type IN (%s) OR event_type IS NULL)
                        GROUP BY security_id
                       ) AS x
                  INNER JOIN context_ranking as c ON c.security_id = x.security_id
                                                 AND c.ranking = x.ranking
                  INNER JOIN volume_curve vc ON vc.security_id = c.security_id
                                             AND vc.context_id = c.context_id
                  INNER JOIN event_type et ON et.event_type = c.event_type
              ''' % (','.join(map(str, events)))

    for row in database.select(query):
        if row[2] != 166: # Make sure the news is not inhibited
            stock_by_news[(row[1], row[4], row[2])].append((row[0],row[3]))

    table = HtmlTable()
    table.setBorder()
    print >> output, table.header(['News', 'Stocks Impacted'],[200,700])
    for news in stock_by_news:
        if len(stock_by_news[news]) > 25:
            sectors = Repository.get_sectors([r[1] for r in stock_by_news[news]])
            count = defaultdict(int)
            for sec_id, sector in sectors.itervalues():
                count[sector] += 1

            news_group = ['%s (%d)' % (sector, c) for (sector, c) in sorted(count.iteritems(), key=lambda x: x[1], reverse=True)]
        else:
            news_group = [r[0] for r in stock_by_news[news]]
        total = len(stock_by_news[news])
        print >> output, table.line(['%s<br/>(%d Stocks)' % (news[1].replace('US-O-',''), total),'&nbsp;&nbsp;&nbsp;&nbsp;'.join(news_group)])

    print >> output, table.end()
    print >> output, '<br/><br/>&nbsp;'

    attachements = []
    for news in stock_by_news:
        print news
        max_diff = -1
        code = None
        for stock in stock_by_news[news]:
            error = get_curve_diff (stock, news, database)
            if error > max_diff:
                max_diff = error
                code = stock[0]

        #code = stock_by_news[news][random.randint(0,len(stock_by_news[news])-1)][0]

        # Get the news curve
        query = ''' SELECT vcv.time, vcv.value FROM volume_curve vc
                    JOIN volume_curve_value vcv ON vcv.curve_id = vc.curve_id
                    WHERE vc.code = ? and vc.context_id = ?
                '''
        rows = database.select(query, code, news[2])

        timestamps = []
        for row in rows:
            if len(timestamps) == 0:
                last = datetime.datetime(date.year, date.month, date.day, 9,25,0)
                timestamps.append(last)
            else:
                last += datetime.timedelta(minutes=5)
                timestamps.append(last)
        timestamps[-1] = datetime.datetime(date.year, date.month, date.day, 16,0,0)
        timestamps.append(datetime.datetime(date.year, date.month, date.day, 16,5,0))

        fig = figure()
        matplotlib.rcParams['font.size'] = 6.0
        ax = fig.add_subplot(111)
        ax.yaxis.set_major_formatter(FuncFormatter(pct_format))
        ax.set_title(code)
        ax.xaxis.set_major_locator(MinuteLocator(interval=30))
        ax.xaxis.set_minor_locator(MinuteLocator(interval=15))
        ax.xaxis.set_major_formatter(DateFormatter('%H:%M:%S'))
        #ax.set_xlim( date2num( datetime.datetime(date.year, date.month, date.day, 9,29,0)),  datetime.datetime(date.year, date.month, date.day, 16,1,0))

        vc = diff([row[1] for row in rows])
        vc.append(0.0)
        ax.plot(timestamps, [0] + vc, label = news[1], )


        #avg_news_curve = get_average_curve(stock_by_news[news], news[2], database)
        #ax.plot(timestamps[1:], avg_news_curve, label = news[1])

        # Get the standard curve
        rows = database.select(query, code, 166)
        vc = diff([row[1] for row in rows])
        vc.append(0.0)
        ax.plot(timestamps, [0] + vc, label = 'Usual Day')

        #avg_curve = get_average_curve(stock_by_news[news], 166, database)
        #ax.plot(timestamps[1:], avg_curve, label = 'Usual Day')

        ax.legend(loc='upper left')
        fig.autofmt_xdate()
        fig.set_size_inches(9., 3.)
        fig.subplots_adjust(left=.05, bottom=.1, right=.99, top=.95)
        ax.grid(True)

        filename = '%s.png' % news[1]
        fig.savefig(filename)

        print >> output, '&nbsp;<h2>%s</h2><br/>' % news[1].replace('US-O-','')
        print >> output, '<img src="cid:%s">' % filename
        print filename
        attachements.append(filename)

    for a in attachements:
        output.attachImage(a, a)

    return True

def get_diff_for_name(stock, date=None):
    if not date:
        date = datetime.datetime.today().date()
        
    database = ODBC("DRIVER={PostgreSQL ANSI};Server=65.244.97.57;DATABASE=bluebox;PORT=5435;Uid=python;Pwd=python;AUTO_SERVER=true")
    
    query = ''' SELECT ce.event_type, event_name FROM cal_event ce
                JOIN event_type et on et.event_type = ce.event_type
                WHERE event_date = ?
            '''

    rows = database.select(query, date)

    events = [row[0] for row in rows]

    if len(events) <= 0:
        return False

    stock_by_news = defaultdict(list)

    # for each news, get the list of affected stocks
    query = ''' SELECT code, et.event_type, c.context_id, c.security_id, et.event_name
                  FROM (
                        SELECT security_id, min(ranking) as ranking FROM context_ranking cr
                        WHERE (event_type IN (%s) OR event_type IS NULL)
                        GROUP BY security_id
                       ) AS x
                  INNER JOIN context_ranking as c ON c.security_id = x.security_id
                                                 AND c.ranking = x.ranking
                  INNER JOIN volume_curve vc ON vc.security_id = c.security_id
                                             AND vc.context_id = c.context_id
                  INNER JOIN event_type et ON et.event_type = c.event_type
                  WHERE code = '%s'
              ''' % (','.join(map(str, events)), stock)

    for row in database.select(query):
        if row[2] != 166: # Make sure the news is not inhibited
            print row
            #stock_by_news[(row[1], row[4], row[2])].append((row[0],row[3]))
    return 
    attachements = []
    for news in stock_by_news:
        print news
        max_diff = -1
        code = None
        for stock in stock_by_news[news]:
            error = get_curve_diff (stock, news, database)
            if error > max_diff:
                max_diff = error
                code = stock[0]

        #code = stock_by_news[news][random.randint(0,len(stock_by_news[news])-1)][0]

        # Get the news curve
        query = ''' SELECT vcv.time, vcv.value FROM volume_curve vc
                    JOIN volume_curve_value vcv ON vcv.curve_id = vc.curve_id
                    WHERE vc.code = ? and vc.context_id = ?
                '''
        rows = database.select(query, code, news[2])

        timestamps = []
        for row in rows:
            if len(timestamps) == 0:
                last = datetime.datetime(date.year, date.month, date.day, 9,25,0)
                timestamps.append(last)
            else:
                last += datetime.timedelta(minutes=5)
                timestamps.append(last)
        timestamps[-1] = datetime.datetime(date.year, date.month, date.day, 16,0,0)
        timestamps.append(datetime.datetime(date.year, date.month, date.day, 16,5,0))

        fig = figure()
        matplotlib.rcParams['font.size'] = 6.0
        ax = fig.add_subplot(111)
        ax.yaxis.set_major_formatter(FuncFormatter(pct_format))
        ax.set_title(code)
        ax.xaxis.set_major_locator(MinuteLocator(interval=30))
        ax.xaxis.set_minor_locator(MinuteLocator(interval=15))
        ax.xaxis.set_major_formatter(DateFormatter('%H:%M:%S'))
        #ax.set_xlim( date2num( datetime.datetime(date.year, date.month, date.day, 9,29,0)),  datetime.datetime(date.year, date.month, date.day, 16,1,0))

        vc = diff([row[1] for row in rows])
        vc.append(0.0)
        ax.plot(timestamps, [0] + vc, label = news[1], )


        #avg_news_curve = get_average_curve(stock_by_news[news], news[2], database)
        #ax.plot(timestamps[1:], avg_news_curve, label = news[1])

        # Get the standard curve
        rows = database.select(query, code, 166)
        vc = diff([row[1] for row in rows])
        vc.append(0.0)
        ax.plot(timestamps, [0] + vc, label = 'Usual Day')

        #avg_curve = get_average_curve(stock_by_news[news], 166, database)
        #ax.plot(timestamps[1:], avg_curve, label = 'Usual Day')

        ax.legend(loc='upper left')
        fig.autofmt_xdate()
        fig.set_size_inches(9., 3.)
        fig.subplots_adjust(left=.05, bottom=.1, right=.99, top=.95)
        ax.grid(True)

        filename = '%s.png' % news[1]
        fig.savefig(filename)

        print >> output, '&nbsp;<h2>%s</h2><br/>' % news[1].replace('US-O-','')
        print >> output, '<img src="cid:%s">' % filename
        print filename
        attachements.append(filename)

    for a in attachements:
        output.attachImage(a, a)

    return True

if __name__ == '__main__':
    #get_diff_for_name('HCN')
    
    #sys.exit()
    email = HtmlEmail('smtpnotes')
    email.set_subject('News impacting volume curves')
    email.set_sender("Sylvain Archenault <sarchenault@cheuvreux.com>")
    email.set_dest("sarchenault@cheuvreux.com")
    #email.set_dest('all.trading.newyork@cheuvreux.com,rburgot,nmayo-ext@cheuvreux.com,mlasnier-ext@cheuvreux.com,ml.aes.newyork')
    #report(datetime.date(2012,6,1), email)
    if report(datetime.datetime.today().date(), email):
        email.flush()
