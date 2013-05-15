'''
Created on Jul 10, 2012

@author: syarc
'''
import getopt
import sys
from cheuvreux.stdio.mail import HtmlEmail
import datetime
from cheuvreux.utils.dataset import Dataset, Quantity, Percentage, Dollar,\
    to_html, Float
from cheuvreux.dbtools.odbc import ODBC
import locale
from collections import defaultdict
from cheuvreux.utils.statistics import weighted_mean, weighted_std

locale.setlocale(locale.LC_ALL, '')

def get_avg_perf (DB, start, end):
    query = ''' select algo, done, perf_vwap from quant..tca
                where date >= '%s' and date < '%s' 
                and abs(perf_vwap) < 200 and done > 500
            ''' % (start, end)
            
    algos_perf = defaultdict(list)
    algos_volume = defaultdict(list)
    for row in DB.select(query):
        algo = row[0]
        if algo == 'EVP':
            algo = 'POV'
        elif algo == 'ImplementationShortFall':
            algo = 'IS'
        elif algo == 'TargetClose':
            algo = 'EXIT'
        algos_perf[algo].append(row[2])
        algos_volume[algo].append(row[1])
        
    
    summary = {}
        
    for algo in algos_perf:
        avg = weighted_mean(algos_perf[algo], algos_volume[algo])
        std = weighted_std(algos_perf[algo], algos_volume[algo])
        summary[algo] = (avg,std)
        
    return summary

def get_algo_analysis(DB, start, end):
    
    perf = get_avg_perf(DB, start, end)
    
    query = '''
                select algo, sum(done), sum(done*gross_price), count(distinct counterparty), sum(cost), count(*)
                from quant..algo_orders
                where date >= '%s' and date < '%s' and parent_order_id is null
                group by algo
            ''' % (start, end)
            
    ds = Dataset(['Algo', 'Done', 'Turnover', '# Orders', '# Clients', 'Cost Per Share', 'Avg Slippage', 'Std Slippage'])
    ds.set_extra_style(['', "style='text-align:right;'"])
    done = turnover = count = cost = 0
    for row in DB.select(query):
        done += row[1]
        turnover += row[2]
        count += row[5]
        cost += row[4]
        
        if row[0] in perf:
            mean_perf = Float(perf[row[0]][0], precision=2)
            std_perf =  Float(perf[row[0]][1], precision=2)
        else:
            mean_perf = std_perf = ''
        ds.append([row[0], Quantity(row[1]), Dollar(row[2], precision=0), Quantity(row[5]), Quantity(row[3]), 
                   Dollar(float(row[4]) / row[1] if row[1] > 0 else 0.0, precision = 4),
                   mean_perf, std_perf])
        
    total_clients = DB.select(''' SELECT count ( distinct counterparty) 
                                   FROM quant..algo_orders 
                                   WHERE date >= '%s' AND date < '%s' ''' % (start, end))[0][0]
    ds.add_total_row(['Total', Quantity(done), Dollar(turnover, precision = 0), 
                      Quantity(count), total_clients, Dollar(cost / done, precision = 4), '', ''])
    return ds

def get_venue_analysis(DB, start, end):
    query = '''
                select isnull(dm.exchange, destination), sum(done), sum(quantity), count(*), sum(cost), sum(price_improvement)
                from quant..orders
                join quant..destination_map dm on dm.svcexec = destination
                where date >= '%s' and date < '%s'
                group by isnull(dm.exchange, destination)
            ''' % (start, end)
            
    ds = Dataset(['Destination', 'Done', 'Sent', 'Fill Rate', '# Orders', 'Cost / share'])
    ds.set_extra_style(['', "style='text-align:right;'"])
    
    done = sent = cost = nb_orders = cost = pi = 0
    
    for row in DB.select(query):
        done += row[1]
        sent += row[2]
        nb_orders += row[3]
        cost += row[4]
        #pi += row[5]
        
        ds.append([row[0], Quantity(row[1]), Quantity(row[2]), 
                   Percentage(float(row[1]) / row[2] if row[2] > 0 else 0.0) ,
                   Quantity(row[3]), 
                   Dollar(row[4] / row[1] if row[1] > 0 else 0.0, precision = 4)]) 
                    #Dollar(row[5] / row[1] if row[1] > 0 else 0.0, precision = 4)])
        
    ds.add_total_row(['Total', Quantity(done), Quantity(sent), Percentage(float(done) / sent),
                      Quantity(nb_orders), Dollar(cost / done, precision = 4)])
                    #Dollar(pi / done, precision = 4)])
    return ds

def usage(basename):
    print "Usage: %s [OPTIONS]" % basename;
    print ""
    print "Produce a report of the activity"
    print ""
    print " OPTIONS:"
    print "   -h, --help               \t Display this help message"
    print "   -d, --date=DATE          \t Range to process, format should "
    print "                            \t be YYYYMMDD:YYYYMMDD"
    print "                            \t If not specified, the 3 month are used"
    print "   -e, --email=DESTS        \t Send the result by email to a list"
    print "                            \t of persons by email"
    
def report(argv):
    try:
        opts, args = getopt.getopt(argv[1:], 'hd:e:',
                                  ['help', 'date=', 'email='])
    except getopt.GetoptError:
        usage(argv[0])
        sys.exit(-1)

    start = end = None
    output = sys.stdout
    
    for opt, val in opts:
        if opt in ('-h', '--help'):
            usage(argv[0])
            sys.exit(0)
        elif opt in ('-d', '--date'):
            try:
                start, end = val.split(':')
            except:
                print 'Invalid date range (%s)' % val
                usage(argv[0])
                sys.exit(-2)
        elif opt in ('-e', '--email'):
            output = HtmlEmail('smtpnotes')
            output.set_sender('Sylvain Archenault <sarchenault@cheuvreux.com>')
            output.set_dest(val)

    if not start or not end:
        today = datetime.datetime.today()
    
        year = today.year
        if today.month <= 3:
            year -= 1
            month = 12 - (3 - today.month)
        else:
            month = today.month - 3
        start = datetime.date(year, month, 1)
        end = datetime.date(today.year, today.month, 1)

    DB = ODBC('DRIVER={SQL Server};SERVER=nysql001;DATABASE=Fidessa;UID=syarc;PWD=syarc')
    
    ds = get_algo_analysis(DB, start, end)
    ds.sort('Done', reverse=True)
    if isinstance(output, HtmlEmail):
        print >> output, '<h2>Algo Analysis</h2>' 
        print >> output, to_html(ds)
    else:
        print >> output, str(ds)
        
    ds = get_venue_analysis(DB, start, end)
    ds.sort('Done', reverse=True)
    if isinstance(output, HtmlEmail):
        print >> output, '<h2>Venue Analysis</h2>' 
        print >> output, to_html(ds)
    else:
        print >> output, str(ds)
    
        
    if  isinstance(output, HtmlEmail):
        output.set_subject('Overal Analysis from %s to %s' % (start, end - datetime.timedelta(days=1)))
        output.flush()

if __name__ == '__main__':
    report(sys.argv)
