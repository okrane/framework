'''
Created on Jan 25, 2011

@author: syarc
'''

from cheuvreux.fidessa.fidessadb import FidessaDB
from cheuvreux.stdio.html import table_style, HtmlTable, numberFormat
from cheuvreux.stdio.mail import HtmlEmail
from cheuvreux.utils.date import workday_range, previous_weekday, go_back, \
    nb_working_days, go_ahead
import datetime
import locale
import math
import operator
import sys
from cheuvreux.dbtools.odbc import ODBC

locale.setlocale(locale.LC_ALL, '')

monitor_db = fidessadb.getODBCConnection()

def fid_data_generator(date):
    fid = FidessaDB()._backend

    fid.execDirect("IF OBJECT_ID('tempdb..#tmp_order_id') IS NOT NULL DROP TABLE #tmp_order_id")

    fid.execDirect(''' DECLARE @start datetime

                       SELECT @start = '%s'

                       SELECT tradedate, order_id , root_order_id, parent_order_id, max(version) as version
                        INTO #tmp_order_id
                        FROM (
                            SELECT o.tradedate, o.order_id, root_order_id, max(version) as version, parent_order_id
                                FROM [High Touch Orders Cumulative] o
                                JOIN BLUEBOX_ORDER_DETAILS b ON o.order_id = b.order_id and b.order_date = o.tradedate
                           LEFT JOIN BLUEBOX_ORDER_DETAILS b2 ON b2.order_id = isnull(parent_order_id, '') and b2.order_date = o.tradedate
                                WHERE b2.order_id is null
                                  AND tradedate = @start
                                  AND o.instrument_code NOT IN ('ZVZZT','ZXZZT', 'ZWZZT')
                                GROUP BY tradedate, o.order_id, root_order_id, parent_order_id
                            UNION
                              SELECT tradedate, o.order_id, root_order_id, max(version) as version, parent_order_id
                                FROM [High Touch Orders Cumulative] o
                           LEFT JOIN BLUEBOX_ORDER_DETAILS b on b.order_id = isnull(parent_order_id,'') and b.order_date = o.tradedate
                                WHERE cust_xp_data_1 IS NOT NULL -- AND cust_xp_data_0 = 'BLUEBOX'
                                  AND cust_xp_data_1 NOT IN ('0','NONE','CS')
                                  AND b.order_id is null
                                  AND tradedate = @start
                                  AND o.instrument_code NOT IN ('ZVZZT','ZXZZT', 'ZWZZT')
                                GROUP BY tradedate, o.order_id, root_order_id, parent_order_id
                          ) A
                        GROUP BY  tradedate, order_id , root_order_id, parent_order_id
                   ''' % (date.strftime('%Y%m%d')))

    fid.execDirect('''
                   DELETE #tmp_order_id
                    FROM [High Touch Orders Cumulative] o
                    JOIN #tmp_order_id t ON t.order_id = o.order_id AND o.version = t.version
                     LEFT JOIN BLUEBOX_ORDER_DETAILS b ON b.order_id = o.order_id
                    WHERE cust_xp_data_4 = 'CARE' and isnull(strategy, cust_xp_data_1)  = 'CUSTOM'

                    DELETE FROM #tmp_order_id
                     WHERE root_order_id <> order_id AND root_order_id  IN (SELECT order_id FROM #tmp_order_id)

                    DELETE FROM #tmp_order_id
                     WHERE root_order_id <> order_id AND parent_order_id  IN (SELECT order_id FROM #tmp_order_id)

                    DELETE tmp FROM #tmp_order_id AS tmp
                      JOIN [High Touch Orders Cumulative] o ON o.order_id = tmp.root_order_id
                     WHERE o.tradedate != tmp.tradedate
                   ''')

    fid.execDirect("IF OBJECT_ID('tempdb..#tmp_work') IS NOT NULL DROP TABLE #tmp_work;")

    fid.execDirect('''SELECT o.tradedate,
                            isnull(strategy, cust_xp_data_1) AS algo,
                            cust_xp_data_1,
                            cust_xp_data_4,
                            volume_done,
                            o.trading_quantity,
                            volume_done*gross_price AS turnover,
                            counterparty_code,
                            t.order_id,
                            o.entered_by,
                            0 as is_dsa
                        INTO #tmp_work
                        FROM [High Touch Orders Cumulative] o
                        JOIN #tmp_order_id t ON t.order_id = o.order_id AND o.version = t.version
                        LEFT JOIN BLUEBOX_ORDER_DETAILS b ON b.order_id = o.order_id

                update #tmp_work
                   set cust_xp_data_4 = o.cust_xp_data_4
                FROM #tmp_work t
                JOIN [High Touch Orders Cumulative] o on t.order_id = o.order_id and version = 1
                WHERE t.cust_xp_data_4 is null and o.cust_xp_data_4 is not null
                ''')

    fid.execDirect('''
                    update #tmp_work
                        set algo = substring(algo, 6, 50)
                        where substring(algo, 0, 6) = 'BETA_'

                    update #tmp_work
                        set algo = substring(algo, 6, 50)
                        where substring(algo, 0, 6) = 'TEST_'

                    update #tmp_work
                        set algo = cust_xp_data_1
                        where algo like '%@CRAG.US'

                    update #tmp_work
                    set algo = case(charindex('_',algo))
                                when 0 then algo
                                else substring(algo,0,charindex('_',algo))
                                end

                    update #tmp_work
                    set algo = 'TWAPDPL'
                    where algo = 'CUSTOM' and substring(cust_xp_data_4,0,11) = 'FLOAT_TWAP'

                    update #tmp_work
                    set algo = case(charindex('_',cust_xp_data_4))
                                when 0 then cust_xp_data_4
                                else substring(cust_xp_data_4,0,charindex('_',cust_xp_data_4))
                               end
                    where algo = 'CUSTOM'

                    update #tmp_work
                    set algo = 'CROSSFIRE'
                    where algo = 'AGGR'

                    update #tmp_work
                    set algo = 'SMARTX'
                    where algo in ('PASS', 'SMARTEXP')

                    update #tmp_work
                    set algo = 'CROSSFIRE'
                    where algo = 'SPOTLIGHT'

                    update #tmp_work
                    set algo = 'TWAP'
                    where algo = 'STEALTH'

                    update #tmp_work
                    set algo = 'CROSSFIRE'
                    where algo in ('STRIKE','MIDPOINT')

                    update #tmp_work
                    set counterparty_code ='HOUSE'
                    where counterparty_code is NULL

                    update #tmp_work
                    set is_dsa = 1
                    where entered_by = 'SMART@CRAG.US'

                    ''')



    rows = fid.select(''' SELECT tmp.tradedate, isnull(a.Name, isnull(cli.client, 'Unknown')), upper(tmp.algo), sum(tmp.volume_done) as volume,
                                 sum(tmp.turnover) as turnover, count(tmp.order_id) as count,
                                 is_dsa
                          FROM #tmp_work tmp
                           LEFT JOIN KYCDatabase..[Client xref] cli ON counterparty_code = cli.HighTouchClient
                           LEFT JOIN KYCDatabase..AccountGroups a ON cli.CustomerNumber = a.CustomerNumber
                          WHERE tmp.algo IS NOT NULL
                          GROUP BY tradedate, isnull(a.Name, isnull(cli.client, 'Unknown')), tmp.algo, is_dsa
                      ''')

    print len(rows)
    for row in rows:
        yield {'TRADEDATE': row[0].strftime('%Y-%m-%d'), 'CLIENT': row[1], 'ALGO': row[2], 'VOLUME': row[3],
               'TURNOVER': row[4], 'COUNT': row[5], 'IS_DSA': row[6] }

    fid.close()

def is_data_available(db, date):

    ans = db.selectOne('SELECT count(*) FROM client_monitor WHERE tradedate = \'%s\'' % date.strftime('%Y%m%d'))
    return ans and ans[0] > 0

def get_avg_turnover_by_client (db, end_date, window):

    start = go_back(end_date, window)

    query = '''
                select client, avg(turnover) as turnover, count(tradedate) as dates, max(tradedate) as last_date from (
                    select client, tradedate, sum(turnover) as turnover from client_monitor
                    where tradedate > '%s' AND tradedate <= '%s'
                    group by tradedate, client
                ) A group by client
            ''' % (start.strftime('%Y%m%d'), end_date.strftime('%Y%m%d'))
    avg_client = {}
    for row in db.select(query):
        avg_client[row[0]] = (row[1], row[2], row[3])

    return avg_client

def volume_by_client_alert(db, date):

    long_avg_client = get_avg_turnover_by_client(db, date, 20)
    short_avg_client = get_avg_turnover_by_client(db, date, 5)


    query =  '''
                select client, tradedate, sum(turnover) as turnover, count(algo) as nb_algo from client_monitor
                    where tradedate = '%s'
                    group by tradedate, client
            ''' % (date.strftime('%Y-%m-%d'))

    last_turnovers = {}
    for row in db.select(query):
        last_turnovers[row[0]] = (row[2], row[3])

    data = []#StructuredData(['Client', 'Avg Turnover', 'Last Turnover', 'Change'])

    for client in long_avg_client:

        long_avg, nb_dates, last_trade =  long_avg_client[client]
        if long_avg == 0:
            continue

        turnover, nb_algo = 0.0, 0
        if client in last_turnovers:
            turnover, nb_algo = last_turnovers[client]

        short_avg = 0
        if client in short_avg_client:
            short_avg = short_avg_client[client][0]

        change = 100.0 * (short_avg - long_avg) / long_avg
        trade_probability = nb_dates / 20.0

        last_trade = datetime.datetime.strptime(last_trade,'%Y-%m-%d').date()
        no_trade = nb_working_days(last_trade, date) - 1

        likelihood = 1
        if no_trade > 0:
            likelihood = math.pow(1-trade_probability, no_trade)

        data.append([client, turnover, nb_algo, long_avg, short_avg, change,
                     no_trade, trade_probability,
                     likelihood])

    return data

def get_entropy(db, start, date, algo):
    query = ''' select client, avg(turnover) as turnover from (
                  select tradedate, algo, client, sum(turnover) as turnover
                    from client_monitor where tradedate > '%s' and tradedate <= '%s'
            '''  % (start.strftime('%Y%m%d'), date.strftime('%Y-%m-%d'))
    if algo:
        query += ''' and algo = '%s' ''' % algo
    query += ''' group by tradedate, algo, client
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

    query = ''' SELECT algo, count(distinct(client)) FROM client_monitor
                       WHERE tradedate > '%s' and tradedate <= '%s'
                   GROUP BY algo
                   '''  % (start.strftime('%Y%m%d'), date.strftime('%Y%m%d'))

    client_algo = {}
    for row in db.select(query):
        client_algo[row[0]] = row[1]

    query = ''' select algo, avg(turnover) as turnover from (
                  select tradedate, algo, sum(turnover) as turnover
                    from client_monitor where tradedate > '%s' and tradedate <= '%s'
                group by tradedate, algo
                ) A group by algo
            ''' % (start.strftime('%Y%m%d'), date.strftime('%Y%m%d'))

    avg_algo= {}
    total = 0
    for row in db.select(query):
        entropy = get_entropy(db, start, date, row[0])

        avg_algo[row[0]] = (row[1], client_algo[row[0]], entropy)
        total += row[1]


    query = ''' select count(distinct client) from client_monitor
                where  tradedate > '%s' and tradedate <= '%s'
            ''' % (start.strftime('%Y%m%d'), date.strftime('%Y%m%d'))

    row = db.select(query)
    avg_algo['Total'] = (total, row[0][0], get_entropy(db, start, date, None))

    return avg_algo

def algo_usage(db, date):
    long_avg_algo = get_algo_stat(db, date, 20)
    short_avg_algo = get_algo_stat(db, date, 5)
    last_algo = get_algo_stat(db, date, 1)

    data = []
    for algo in long_avg_algo:

        long_avg, long_clients, long_entropy =  long_avg_algo[algo]

        if long_avg == 0:
            continue

        nb_clients = turnover = last_entropy = 0
        if algo in last_algo:
            turnover, nb_clients, last_entropy = last_algo[algo]

        short_clients = short_avg = short_entropy = 0
        if algo in short_avg_algo:
            short_avg, short_clients, short_entropy = short_avg_algo[algo]

        change = 100.0 * (short_avg - long_avg) / long_avg

        data.append([algo, turnover, nb_clients, long_avg, long_clients, short_avg, short_clients, change, short_entropy, long_entropy, last_entropy])

    return data

def format_algo(data, output):

    use_html = isinstance(output, HtmlEmail)

    if use_html:
        table = HtmlTable()
        print >> output, table.header(['Algorithm','Yesterday Turnover', '# Clients', 'Entropy', 'Avg Turnover<br>(20 days)',
                                       '# Clients <br> (over 20 days)', 'Entropy<br> (over 20 days)', 'Avg Turnover<br> (5 days)', '# Clients <br> (over 5 days)',
                                       'Entropy<br> (over 5 days)', 'Change<br> (over 5 days)'
                                       ], [300,100])

    for row in sorted(data, key=operator.itemgetter(1), reverse=True):
        if row[0] == 'Total':
            continue
        if use_html:
            print >> output, table.line([row[0],
                                        '$' + locale.format("%d", row[1], grouping=True),
                                        row[2],
                                        locale.format('%.2f',row[10]),
                                        '$' + locale.format("%d", row[3], grouping=True),
                                        row[4],
                                        locale.format('%.2f',row[9]),
                                        '$' + locale.format("%d", row[5], grouping=True),
                                        row[6],
                                        locale.format('%.2f',row[8]),
                                        numberFormat('%.2f%%',row[7])],
                                        ['',"style='text-align:right'"])
        else:
            print >> output, '%50s\t%10.0f\t%3d\t%10d\t%10d\t%4.2f' % (row[0], row[1], row[2], row[3], row[4], row[5])

    for row in data:
        if row[0] == 'Total':
            if use_html:
                print >> output, table.line(['<b>Total</b>','$' + locale.format("%d", row[1], grouping=True),
                                            row[2],
                                             locale.format('%.2f',row[10]),
                                            '$' + locale.format("%d", row[3], grouping=True),
                                            row[4],
                                             locale.format('%.2f',row[9]),
                                            '$' + locale.format("%d", row[5], grouping=True),
                                            row[6],
                                             locale.format('%.2f',row[8]),
                                            numberFormat('%.2f%%',row[7])],
                                            ['',"style='text-align:right; font-weight:bold'"])
            else:
                print >> output, '%50s\t%10.0f\t%3d\t%10d\t%10d\t%4.2f' % (row[0], row[1], row[2], row[3], row[4], row[5])


    if use_html:
        print >> output, table.end()

def format_client_volume(data, output):
    use_html = isinstance(output, HtmlEmail)

    if use_html:
        table = HtmlTable()
        print >> output, table.header(['Client','Yesterday Turnover', '# Algo', 'Avg Turnover<br>(20 days)',
                                       'Avg Turnover<br> (5 days)', 'Change<br> (over 5 days)', 'Last Trade<br>(Nb working days)',
                                       'Trade Probability', 'Likelihood'], [300,100])
#        setattr(data,'formatter', {'Avg Turnover': MoneyFormatter(), 'Last Turnover': MoneyFormatter(), 'Change': PercentageFormatter()})
#        print >> output, make_html_table(data,widths=[500,100],extra=['',"style='text-align:right'"], sort_key='Change')
#        table = HtmlTable()
#        print >> output, table.header(['Client', 'Avg Turnover', 'Last Turnover', 'Change'], [500,100])

    for row in sorted(data, key=operator.itemgetter(5)):
        if use_html:
            print >> output, table.line([row[0],
                                        '$' + locale.format("%d", row[1], grouping=True),
                                        row[2],
                                        '$' + locale.format("%d", row[3], grouping=True),
                                        '$' + locale.format("%d", row[4], grouping=True),
                                        numberFormat('%.2f%%',row[5]),
                                        row[6],
                                        '%.2f%%' % (100*row[7]),
                                        '%.2f%%' % (100*row[8])
                                        ],
                                        ['',"style='text-align:right'"])
        else:
            print >> output, '%50s\t%10.0f\t%2d\t%10d\t%10d\t%4.2f\t%s' % (row[0], row[1], row[2], row[3], row[4], row[5], row[6])

    if use_html:
        print >> output, table.end()

def client_monitor(date, output=sys.stdout, force=False):
    global monitor_db

    if force or not is_data_available(monitor_db, date):
        monitor_db.run("DELETE FROM client_monitor WHERE tradedate = '%s'" % date.strftime('%Y%m%d'))
        cols = {'TRADEDATE': '', 'CLIENT': '', 'ALGO': '', 'VOLUME': '', 'TURNOVER': '', 'COUNT': '', 'IS_DSA': ''}
        stmt = ('INSERT INTO "client_monitor" ('
                 + ','.join(cols.keys()) + ') VALUES ('
                 + ','.join(['?']*len(cols)) + ')'
                 )
        monitor_db.executemany(stmt, [g.values() for g in fid_data_generator(date)])

    format_algo(algo_usage(monitor_db, date), output)
    format_client_volume(volume_by_client_alert(monitor_db, date), output)

def last_date():
    global monitor_db

    row = monitor_db.selectOne('select max(tradedate) from client_monitor')
    if row and row[0]:
        return datetime.datetime.strptime(row[0], '%Y-%m-%d').date()
    return None

if __name__ == '__main__':

    email = HtmlEmail("smtpnotes", style=table_style)
    last = last_date() + datetime.timedelta(days=1)
    previous = previous_weekday()

    if last > previous:
        last = previous

    for date in workday_range(last, previous):
        print date
        client_monitor(date, email, False)

    email.set_sender('Sylvain Archenault <sarchenault@cheuvreux.com>')
    email.set_subject('Algo monitoring')

    email.send('sarchenault')
