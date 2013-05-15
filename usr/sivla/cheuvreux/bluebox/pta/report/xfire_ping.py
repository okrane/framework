'''
Created on Jun 29, 2011

@author: syarc
'''
import sys
from cheuvreux.stdio.ssh import SSHConfiguration, SSHException
import os
from cheuvreux.bluebox.pta.report import DB
from cheuvreux.utils.dataset import Dollar, Quantity, Percentage, Dataset,\
    to_html, Float
from cheuvreux.stdio.mail import HtmlEmail
from cheuvreux.utils.date import toMicroseconds, workday_range, go_back
from datetime import datetime, date
from collections import defaultdict
from cheuvreux.utils.statistics import quantile, variance
import math

def parse_time(str):
    return datetime(1900,1,1,int(str[0:2]), int(str[3:5]), int(str[6:8]), int(str[9:15]))

def algo_count(algo):
    query = ''' SELECT start_time, end_time FROM algo_orders a
                  JOIN #XFIRE_ID x on x.order_id = a.order_id
                   AND ALGO = '%s'
            ''' % algo

    return len(DB.select(query))

def historic_latency(range, algo=None):

    query = ''' SELECT a.start_time, isnull(o.start_time, a.end_time),a.order_id,o.start_time FROM algo_orders a
                JOIN XFIRE_ID x on x.order_id = a.order_id
                LEFT JOIN orders o on o.parent_order_id = x.order_id AND expiry_type = 'GFD'
                WHERE a.date = '%s'
            '''

    if algo:
        query += " AND ALGO = '%s'" % algo


    for date in range:
        rows = DB.select(query % date.strftime('%Y%m%d'))
        xfire_latency = []
        for row in rows:
            try:
                xfire_latency.append(toMicroseconds(parse_time(row[1]) - parse_time(row[0]))/1000.0)
            except ValueError:
                print row[0], row[1]

        xfire_latency.sort()
        if len(xfire_latency) > 0:
            print '\t'.join(map(str,[date,
                                     quantile(xfire_latency, 0.25, issorted=True),
                                     quantile(xfire_latency, 0.5, issorted=True),
                                     quantile(xfire_latency, 0.75, issorted=True),
                                     quantile(xfire_latency, 0.99, issorted=True),
                                     xfire_latency[-1],
                                     math.sqrt(variance(xfire_latency))]))

def latency_report(algo=None):

    query = ''' SELECT isnull(dm.name, destination), o.start_time, o.end_time, o.order_id
                  FROM algo_orders a
                JOIN #XFIRE_ID x on x.order_id = a.order_id
                JOIN orders o on o.parent_order_id = a.order_id
                LEFT JOIN destination_map dm on dm.svcexec = destination
                WHERE o.end_time is not null
            '''

    if algo:
        query += " AND ALGO ='%s'" % algo

    latencies = defaultdict(list)

    for row in DB.select(query):
        end = row[2]
        if end == 'None':
            end = row[1]
        #print row[0], row[1], row[2]
        latencies[row[0]].append(toMicroseconds(parse_time(end) - parse_time(row[1]))/1000.0)

    if len(latencies) == 0:
        return None

    ds = Dataset(['Destination', 'Q25','Median','Q75','Q99','Max', 'StdDev'])
    for dest in latencies:
        sequence = sorted(latencies[dest])
        ds.append([dest, Float(quantile(sequence,0.25,issorted=True)),
                         Float(quantile(sequence,0.5,issorted=True)),
                         Float(quantile(sequence,0.75,issorted=True)),
                         Float(quantile(sequence,0.99,issorted=True)),
                         Float(sequence[-1]),
                         Float(math.sqrt(variance(sequence)))] )





    query = ''' SELECT a.start_time, isnull(o.start_time, a.end_time),a.order_id,o.start_time FROM algo_orders a
                JOIN #XFIRE_ID x on x.order_id = a.order_id
                LEFT JOIN orders o on o.parent_order_id = x.order_id AND expiry_type = 'GFD'
            '''

    if algo:
        query += "WHERE ALGO = '%s'" % algo

    xfire_latency = []

    for row in DB.select(query):
        if row[1] == 'None':
            continue

        try:
            xfire_latency.append(toMicroseconds(parse_time(row[1]) - parse_time(row[0]))/1000.0)
        except ValueError:
            print row[0], row[1]

    xfire_latency.sort()
    ds.append(['Crossfire', Float(quantile(xfire_latency, 0.25, issorted=True)),
                         Float(quantile(xfire_latency, 0.5, issorted=True)),
                         Float(quantile(xfire_latency, 0.75, issorted=True)),
                         Float(quantile(xfire_latency, 0.99, issorted=True)),
                         Float(xfire_latency[-1]),
                         Float(math.sqrt(variance(xfire_latency)))])


    ds.sort('Median')
    ds.set_col_width([150, 100])
    ds.set_extra_style(['', "style='text-align:right;'"])

    return ds

def process_file (client, filename, order_ids):
    try:
        client.get('/ext1/RFWK/runtime/parameters/%s' % filename, '.')
        for line in open(filename, 'r'):
            order_ids.add(line.split(',')[0])
        os.remove(filename)
    except SSHException:
        pass

def process_file_old (client, filename, order_ids):
    try:
        client.get('/ext1/RFWK/runtime/parameters/%s' % filename, '.')
        for line in open(filename, 'r'):
            order_id, params = line.split(',')[0:2]
            if len(params.split(';')) >= 9:
                order_ids.add(order_id)
            #order_ids.add(line.split(',')[0])
        os.remove(filename)
    except SSHException:
        pass

def process_file_new (client, filename, order_ids):
    tmp_order_id = set()
    try:
        client.get('/ext1/RFWK/runtime/parameters/%s' % filename, '.')
        for line in open(filename, 'r'):
            order_id, params = line.split(',')[0:2]
            if order_id in tmp_order_id:
                continue
            tmp_order_id.add(order_id)
            l = len(params.split(';'))
            if l > 2:
                order_ids.add( (order_id, l < 9))
        os.remove(filename)
    except SSHException:
        pass

def load_order_ids(range, process_file_function=process_file):
    pool = SSHConfiguration('prod')
    client = pool.getClient('65.244.97.57')

    order_ids = set()

    for date in range:
        sdate = date.strftime('%Y%m%d')
        filename = 'parameters_CASE_PROD_FWK_6_%s.txt' % sdate
        process_file_function(client, filename, order_ids)
        filename = 'parameters_ALGO_MODELS_FWK_1_%s.txt' % sdate
        process_file_function(client, filename, order_ids)
        filename = 'parameters_ALGO_MODELS_FWK_2_%s.txt' % sdate
        process_file_function(client, filename, order_ids)
        filename = 'parameters_ALGO_MODELS_FWK_3_%s.txt' % sdate
        process_file_function(client, filename, order_ids)

    return order_ids

def generate_seq(order_ids):
    
    records = []
    
    query = ''' select parent_order_id, o.destination, o.order_id from quant..orders o
                where parent_order_id in ('%s')
                order by o.parent_order_id, o.order_id 
            ''' % ('\',\''.join([o[0] for o in order_ids]))
            
            
    seq = 1
    last_parent_order_id = last_destination = None
    for row in DB.select(query):
        if not last_parent_order_id or row[0] != last_parent_order_id:
            seq = 1
            last_destination = None
        else:
            seq += 1
            
        records.append([row[2], row[0], seq, row[1], last_destination])
        last_parent_order_id = row[0]
        last_destination = row[1]
        
        
    DB.executemany(''' INSERT INTO XFIRE_SEQ 
                          (ORDER_ID, PARENT_ORDER_ID, SEQ, DESTINATION, LAST_DESTINATION)
                       VALUES
                          (?,?,?,?,?)
                    ''', [r for r in records])

def report(range):
    DB.execDirect('CREATE TABLE #XFIRE_ID (ORDER_ID VARCHAR(16) PRIMARY KEY, NEW INT)')

    order_ids = load_order_ids(range, process_file_new)
    
    # Insert in a temp table just for this report
    DB.executemany('INSERT INTO #XFIRE_ID VALUES (?, ?)', [g for g in order_ids])

    # We also want to insert them in the "real" table and compute sequence number
    try:
        DB.executemany('INSERT INTO XFIRE_ID VALUES (?, ?)', [g for g in order_ids])
        generate_seq(order_ids)
    except:
        pass

    query = ''' SELECT sum(done), sum(quantity), count(*) FROM algo_orders a
                JOIN #XFIRE_ID x on x.order_id = a.order_id
            '''
    xfire = DB.selectOne(query)

    query = ''' select isnull(dm.name, destination), sum(price_improvement),
                        sum(o.done), sum(o.quantity), sum(o.cost), count(*) from algo_orders a
                JOIN #XFIRE_ID x on x.order_id = a.order_id
                JOIN orders o on o.parent_order_id = a.order_id
                LEFT JOIN destination_map dm on dm.svcexec = destination
                GROUP BY isnull(dm.name, destination)
                ORDER BY sum(o.done) DESC
            '''

    rows = DB.select(query)

    if len(rows) == 0:
        raise ValueError('Empty report')

    data = []
    data.append(['Crossfire', 0, 0, Quantity(xfire[0]), Quantity(xfire[1]),
                 Percentage(float(xfire[0]) / xfire[1]), 0, Quantity(xfire[2])])


    for row in rows:
        data[0][1] += row[1] # PI
        data[0][6] += row[4] # Cost
        data.append([row[0], Dollar(row[1],2), Dollar(row[1]/row[2] if row[2] > 0 else 0,6),
                     Quantity(row[2]), Quantity(row[3]),
                     Percentage(float(row[2])/row[3]), Dollar(row[4]/row[2] if row[2] > 0 else 0,4),
                     Quantity(row[5])])

    data[0][1] = Dollar(data[0][1], 2) # PI
    data[0][2] = Dollar(data[0][1].value / data[0][3].value,6)   # PI / share
    data[0][6] = Dollar(data[0][6] / data[0][3].value,4)   # Cost / Share

    ds = Dataset(['Destination', 'PI', 'PI / share', 'Done', 'Total', 'Fill Rate', 'Cost / Share', 'Nb Orders'], data)

    ds.set_extra_style(['', "style='text-align:right;'"])
    ds.set_col_width([150, 100])

    return ds

def run_report(range, output=sys.stdout, html=False):

    range = [r for r in range]

    ds = report(range)

    if html:
        print >> output, to_html(ds)
    else:
        print >> output, str(ds)

    ds = getco_smart_router_liquidity(range)

    print >> output, '<h2>Getco Smart Router</h2>'
    if html:
        print >> output, to_html(ds)
    else:
        print >> output, ds


    ds = latency_report('CROSSFIRE')

    if ds:
        count = algo_count('CROSSFIRE')
        print >> output, '<h2>CROSSIRE latency (%d orders)</h2>' % count

        if html:
            print >> output, to_html(ds)
        else:
            print >> output, str(ds)

    ds = latency_report('XFIRE')

    if ds:
        count = algo_count('XFIRE')
        print >> output, '<h2>XFIRE latency (%d orders)</h2>' % count

        if html:
            print >> output, to_html(ds)
        else:
            print >> output, str(ds)

    ds = getco_ats_details(range)

    if ds:
        print >> output, '<h2>Getco ATS Details</h2>'
        if html:
            print >> output, to_html(ds)
        else:
            print >> output, str(ds)

def getco_ats_details(range):
    rows = DB.select('''
                SELECT liquidity_indicator, t.QUANTITY, t.DEALT_PRICE, t.OFFER_PRICE, t.BID_PRICE, t.buy_sell
                FROM  algo_orders a
                JOIN #XFIRE_ID x on x.order_id = a.order_id
                JOIN orders o on o.parent_order_id = a.order_id
                JOIN Fidessa..[High Touch Trade Summary Cumulative] t on t.order_id = o.order_id
                WHERE destination = 'POST BB_POST_GFLO'
                     ''' )

    data = {'R': [0,0,0], 'S': [0,0,0], '': [0,0,0]}
    for row in rows:
        pi = (row[3] - row[2]) if row[5] == 'B' else (row[2] - row[4])

        key = row[0].strip() if row[0] else ''
        data[key][0] += row[1] # Quantity done
        data[key][1] += max(0.0, row[1]*pi)# PI
        data[key][2] += 1 # Count

    datalist = []

    datalist.append(['Getco', Dollar(data['R'][1],2), Dollar(data['R'][1] / data['R'][0] if data['R'][0] > 0 else 0,6),
                     Quantity(data['R'][0]), Quantity(data['R'][2]) ])
    datalist.append(['JPM', Dollar(data['S'][1],2), Dollar(data['S'][1] / data['S'][0] if data['S'][0] > 0 else 0, 6),
                     Quantity(data['S'][0]), Quantity(data['S'][2]) ])
    datalist.append(['Unknown', Dollar(data[''][1],2), Dollar(data[''][1] / data[''][0] if data[''][0] > 0 else 0, 6),
                     Quantity(data[''][0]), Quantity(data[''][2]) ])

    ds = Dataset(['Destination', 'PI', 'PI / share', 'Done', 'Nb Orders'], datalist)
    ds.set_extra_style(['', "style='text-align:right;'"])

    return ds

def getco_smart_router_liquidity(range):

    mapping = {'QR' : 'Nasdaq',
               'QA' : 'Nasdaq',
               'QO' : 'Nasdaq',
               'ZR' : 'BATS',
               'NR' : 'NYSE',
               'GR' : 'Getco ATS',
               'PR' : 'ARCA',
               'PA' : 'ARCA',
               'PO' : 'ARCA',
               'KR' : 'EDGX',
               'KO' : 'EDGX',
               'FR' : 'CSFB',
               'JR' : 'EDGA',
               'JO' : 'EDGA',
               'ER' : 'Knight Match',
               'XR' : 'Philadelphia Stock Exchange',
               'IR' : 'ITG',
               'SR' : 'Sigma-X',
               'HR' : 'Barclays ATS',
               'YR' : 'BATS-Y',
               'MR' : 'Chicago Stock Exchange',
               'MO' : 'Chicago Stock Exchange',
               'VR' : 'Level ATS',
               'BR' : 'Nasdaq-BX',
               'CR' : 'National Stock Exchange',
               'LR' : 'Knight Link',
               'AR' : 'AMEX',
               'UR' : 'UBS',
               'WR' : 'Chicago Board of Options Exchange',
               'WO' : 'Chicago Board of Options Exchange',
               'DR' : 'Deutsche-Bank ATS',
               'DO' : 'Deutsche-Bank ATS',
               'DA' : 'Deutsche-Bank ATS',
               'OR' : 'JP Morgan ATS',
               'RR' : 'CSFB Light Pool',
               'RO' : 'CSFB Light Pool'  }


    rows = DB.select(''' SELECT liquidity_indicator, sum(quantity) FROM Fidessa..[High Touch Trade Summary Cumulative]
                         WHERE execution_venue = 'GESX' AND tradedate >= '%s' AND tradedate <= '%s'
                         GROUP BY LIQUIDITY_INDICATOR
                     ''' % (range[0].strftime('%Y%m%d'), range[-1].strftime('%Y%m%d')))

    aggr_qty = defaultdict(int)
    for row in rows:
        if row[0]:
            destination = mapping[row[0].strip()] if row[0].strip() in mapping else row[0]
            aggr_qty[destination] += row[1]

    ds = Dataset(['Destination', 'Quantity', 'Pct Total'])
    total = sum([row[1] for row in rows])
    for dest, qty in aggr_qty.iteritems():
        ds.append([dest, Quantity(qty), Percentage(float(qty) / total)])

    ds.sort('Quantity', True)
    ds.set_extra_style(['',"style='text-align:right;'"])
    ds.set_col_width([200,100])
    return ds


def historic_value ():
    rows = DB.select(''' select date, destination, datediff(ms,start_time, end_time)
                  from orders where destination = 'POST BB_POST_GETRTD'
                  --and parent_order_id is not null
              ''')

    result = {#'CROSSFIRE': defaultdict(list),
              'CROSSFIRE v2': defaultdict(list)}

#    rows = DB.select(''' select parent.date, parent.algo, sum(child.cost)/child.done)--, datediff(ms,child.start_time, child.end_time)
#                        from algo_orders child
#                        join algo_orders parent on child.parent_order_id = parent.order_id
#                        where child.algo in ('CROSSFIRE', 'CROSSFIRE v2')
#                        and parent.algo in ('VWAP','POV') and child.done > 0
#                        and child.parent_order_id is not null
#                        group by parent.date, parent.algo
#                    ''')


    result = {}
    for r in rows:
        if r[0] not in result:
            result[r[0]] = {}
            result[r[0]][r[1]] = [r[2]]
        elif r[1] not in result[r[0]]:
            result[r[0]][r[1]] = [r[2]]
        else:
            result[r[0]][r[1]].append(r[2])

    algos = set()
    for date in result:
        for k in result[date].keys():
            algos.add(k)

    print '\t'.join([date] + sorted(algos))
    for date in sorted(result.keys()):
        line = [date]
        for a in sorted(algos):
            if a not in result[date]:
                line.append(0)
                #line.append(0)
            else:
                line.append(quantile(result[date][a], 0.5))
                #line.append(len(result[date][a]))

        print '\t'.join(map(str, line))

if __name__ == '__main__':
    #range = [date(2012, 2, 17)]

    #DB.execDirect('CREATE TABLE XFIRE_ID (ORDER_ID VARCHAR(16) PRIMARY KEY, NEW INT)')
    #DB.executemany('INSERT INTO XFIRE_ID VALUES (?, ?)', [g for g in load_order_ids(workday_range(date(2012,3,1), date(2012,5,22)), process_file_new)])
    #DB._conn.commit()
    #generate_seq()

    #historic_latency(workday_range(date(2012,3,1), date(2012,5,22)), 'XFIRE v2')
    #sys.exit()
    email = HtmlEmail('smtpnotes')
    email.set_subject('Crossfire Ping')
    email.set_dest('sarchenault@cheuvreux.com')
#
    range = workday_range(date(2012,8,8), date(2012,8,8))
    ds = run_report(range, email, True)
    #print >> email, to_html(ds)
    #ds = report(range)

    #print >> email, to_html(ds)
#  ds = latency_report(range, 'CROSSFIRE v2')

    #ds = latency_report(range, 'CROSSFIRE')

    #print >> email, to_html(ds)

#    ds = latency_report(range, 'CROSSFIRE v2')

#    print >> email, to_html(ds)

    #latency_report(range)

    email.flush()
