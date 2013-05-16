'''
Created on Jul 11, 2011

@author: syarc
'''
from cheuvreux.bluebox.pta.report import DB
from cheuvreux.dbtools.odbc import ODBC
from cheuvreux.stdio.html import table_style
from cheuvreux.stdio.mail import HtmlEmail
from cheuvreux.utils.dataset import Dataset, to_html, Dollar, Quantity, \
    Percentage, Float
from cheuvreux.utils.date import workday_range, go_back
from collections import defaultdict
import datetime
import math
import os
from cheuvreux.fidessa import fidessadb
dirname = os.path.dirname

monitor_db = fidessadb.getODBCConnection()

def get_entropy(db, start, date, algo):
    query = ''' select client, avg(turnover) as turnover from (
                  select date, algo, client, sum(gross_price * done) as turnover
                    from algo_orders where date > '%s' and date <= '%s'
                    and parent_order_id is null
            '''  % (start.strftime('%Y%m%d'), date.strftime('%Y%m%d'))
    if algo:
        query += ''' and algo = '%s' ''' % algo
    query += ''' group by date, algo, client
                ) A group by client
            '''

    rows = db.select(query)
    total = sum([row[1] for row in rows])

    entropy = 0.0

    if total > 0:
        for row in rows:
            proba = row[1] / total;
            if proba > 0:
                entropy += - proba * math.log(proba, 2)

    return entropy / math.log(len(rows), 2) if len(rows) > 1 else 0.0

def get_algo_stat(db, date, window):
    start = go_back(date, window)

    query = ''' SELECT algo, count(distinct(client)) FROM algo_orders
                       WHERE date > '%s' AND date <= '%s' AND parent_order_id IS NULL
                  GROUP BY algo
                  '''  % (start.strftime('%Y%m%d'), date.strftime('%Y%m%d'))

    client_algo = {}
    for row in db.select(query):
        client_algo[row[0]] = row[1]

    query = ''' SELECT date, algo, sum(gross_price * done) as turnover, SUM(ISNULL(cost,0)), sum(done) as done
                        FROM algo_orders where date  > '%s' and date <= '%s'
                         AND parent_order_id is null AND done > 0
                        GROUP BY date, algo
          ''' % (start.strftime('%Y%m%d'), date.strftime('%Y%m%d'))

    avg_algo_turnover = defaultdict(int)
    algo_cost = defaultdict(int)
    algo_done = defaultdict(int)

    for row in db.select(query):
        avg_algo_turnover[row[1]] += row[2]
        algo_cost[row[1]] += row[3]
        algo_done[row[1]] += row[4]


    avg_algo= {}
    total = total_cost = total_done = 0

    length = len([d for d in workday_range(start, date)]) - 1

    for algo, turnover in avg_algo_turnover.iteritems():
        entropy = get_entropy(db, start, date, algo)

        avg_algo[algo] = (turnover / length, client_algo[algo], entropy, algo_cost[algo] / algo_done[algo], algo_done[algo] / length)
        total += turnover
        total_cost += algo_cost[algo]
        total_done += algo_done[algo]

    if total == 0:
        raise ValueError('No data')

    query = ''' select count(distinct client) from algo_orders
                where  date > '%s' and date <= '%s'
            ''' % (start.strftime('%Y%m%d'), date.strftime('%Y%m%d'))

    row = db.select(query)
    avg_algo['Total'] = (total / length, row[0][0], get_entropy(db, start, date, None), total_cost / total_done, total_done / length)

    return avg_algo

def data_generator(date):

    query = '''select date, client, algo, sum(quantity), sum(consideration), count(*) as cnt, is_dsa FROM (
                SELECT date, client, algo, quantity, done*gross_price as consideration,
                    case entered_by
                        when 'SMART@CRAG.US' then 0
                        else 1
                    end as is_dsa
                FROM algo_orders
                WHERE parent_order_id IS NULL and date = '%s') A
                GROUP BY date, client, algo, is_dsa
            ''' % (date)

    rows = DB.select (query)
    for row in rows:

        yield {'TRADEDATE': row[0],
               'CLIENT': row[1],
               'ALGO': row[2],
               'VOLUME': row[3],
               'TURNOVER': row[4],
               'COUNT': row[5],
               'IS_DSA': row[6]
               }

def algo_usage(db, date):
    long_avg_algo = get_algo_stat(db, date, 20)
    short_avg_algo = get_algo_stat(db, date, 5)
    last_algo = get_algo_stat(db, date, 1)

    ds = Dataset(['Algorithm','Yesterday Turnover', 'Yesterday Volume', '# Clients', 'Entropy', 'Cost per Share',
                  'Avg Turnover<br/>(20 days)', 'Avg Volume<br/>(20 days)', '# Clients <br/>(20 days)', 'Entropy<br/>(20 days)', 'Cost per Share <br/>(20 days)',
                  'Avg Turnover<br/>(5 days)', 'Avg Volume<br/>(5 days)', '# Clients <br/>(5 days)', 'Entropy<br/>(5 days)', 'Cost per Share <br/>(5 days)',
                  'Change<br/>(5 days)'])
    for algo in long_avg_algo:
        if algo == 'Total':
            continue

        long_avg, long_clients, long_entropy, long_cost, long_done =  long_avg_algo[algo]

        if long_avg == 0:
            continue

        nb_clients = turnover = last_entropy = last_cost = last_done = 0
        if algo in last_algo:
            turnover, nb_clients, last_entropy, last_cost, last_done = last_algo[algo]

        short_clients = short_avg = short_entropy = short_cost = short_done = 0
        if algo in short_avg_algo:
            short_avg, short_clients, short_entropy, short_cost, short_done = short_avg_algo[algo]

        change = 1.0 * (short_avg - long_avg) / long_avg

        ds.append([algo, Dollar(turnover,0), Quantity(last_done), Quantity(nb_clients), Float(last_entropy), Dollar(last_cost, 4),
                         Dollar(long_avg,0), Quantity(long_done), Quantity(long_clients), Float(long_entropy), Dollar(long_cost, 4),
                         Dollar(short_avg,0), Quantity(short_done), Quantity(short_clients), Float(short_entropy), Dollar(short_cost, 4), Percentage(change, True)])

    total_long, cost_long, total_long_done = long_avg_algo['Total'][0], long_avg_algo['Total'][3], long_avg_algo['Total'][4]
    total_short, cost_short, total_short_done = short_avg_algo['Total'][0], short_avg_algo['Total'][3], short_avg_algo['Total'][4]

    ds.add_total_row(['Total',
                      Dollar(last_algo['Total'][0],0), Quantity(last_algo['Total'][4]), Quantity(last_algo['Total'][1]), Float(last_algo['Total'][2]), Dollar(last_algo['Total'][3],4),
                      Dollar(total_long,0), Quantity(total_long_done), Quantity(long_avg_algo['Total'][1]), Float(long_avg_algo['Total'][2]), Dollar(cost_long,4),
                      Dollar(total_short,0), Quantity(total_short_done), Quantity(short_avg_algo['Total'][1]), Float(short_avg_algo['Total'][2]), Dollar(cost_short,4),
                      Percentage(1.0 * (total_short - total_long) / total_long,True)])
    return ds

def client_usage(db, date, nb = 20):

    query = '''
                SET ROWCOUNT %d
                SELECT client,SUM(gross_price * done), COUNT(distinct algo), SUM(cost)/SUM(done), SUM(done)
                FROM algo_orders
                WHERE parent_order_id IS NULL AND date = '%s' AND done > 0
                GROUP BY client
                ORDER BY SUM(gross_price * done) DESC
            ''' % (nb, date.strftime('%Y%m%d'))

    d = Dataset(['Client', 'Turnover', 'Volume', '# Algo', 'Cost per Share'])

    for row in db.select(query):
        d.append([row[0], Dollar(row[1], 0), Quantity(row[4]), Quantity(row[2]), Dollar(row[3], 4)])


    return d

def run_report (date, output, force):
    global monitor_db

    print >> output, '<h2>Algo Usage</h2>'
    ds = algo_usage(monitor_db, date)
    ds.set_col_width( [200,100])
    ds.set_extra_style(['', "style='text-align:right;'"])
    ds.sort('Yesterday Turnover', reverse=True)

    print >> output, to_html(ds)

    print >> output, '<p><br/></p><h2>Client Usage</h2>'
    ds = client_usage(monitor_db, date)
    ds.set_col_width( [400,100])
    ds.set_extra_style(['', "style='text-align:right;'"])
    print >> output, to_html(ds)

def last_date():
    global monitor_db

    row = monitor_db.selectOne('select max(date) from algo_orders')
    if row and row[0]:
        return datetime.datetime.strptime(row[0], '%Y-%m-%d').date()
    return None

if __name__ == '__main__':

    email = HtmlEmail("smtpnotes", style=table_style, font_size='8pt')
    last = previous = datetime.date(2011,11,25)

    for date in workday_range(last, previous):
        print date
        run_report(date, email, True)

    email.set_sender('Sylvain Archenault <sarchenault@cheuvreux.com>')
    email.set_subject('Algo monitoring')

    email.send('sarchenault')
