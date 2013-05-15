'''
Created on Jul 10, 2012

@author: syarc
'''

import sys
from cheuvreux.dbtools.odbc import ODBC
from cheuvreux.utils.dataset import Dataset, Quantity, Dollar, to_html
from cheuvreux.stdio.mail import HtmlEmail
import locale
import getopt
import datetime

locale.setlocale(locale.LC_ALL, '')

def usage(basename):
    print "Usage: %s [OPTIONS]" % basename;
    print ""
    print "Compute cost on a given period (default being last month)"
    print ""
    print " OPTIONS:"
    print "   -h, --help               \t Display this help message"
    print "   -d, --date=DATE          \t Range to process, format should "
    print "                            \t be YYYYMMDD:YYYYMMDD"
    print "                            \t If not specified, the last month is used"
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
            output.set_dest(val)

    if not start or not end:
        today = datetime.datetime.today()
    
        year = today.year
        if today.month == 1:
            year -= 1
            month = 12
        else:
            month = today.month - 1
        start = datetime.date(year, month, 1)
        end = datetime.date(today.year, today.month, 1)
                
    
    DB = ODBC('DRIVER={SQL Server};SERVER=nysql001;DATABASE=Fidessa;UID=syarc;PWD=syarc')
    
    ds = Dataset(['Destination', 'Volume', 'Cost / Rebate'])
    
    # First query does most of the work
    # Second query after the UNION is for destination that needs the executor id 
    # to find the cost (Lime for the moment)
    query = '''
                select isnull(name,t.execution_venue), sum(quantity), sum(cost) FROM
                (
                    select execution_venue, quantity, quantity * isnull(rate,0.0) as cost
                    from [High Touch Trade Summary Cumulative] t
                    left join KYCDatabase..[RebateChargeRate Table] r on exchange = t.execution_venue 
                                                             and UPPER(liquidity) = UPPER(isnull(liquidity_indicator,'')) 
                                                             AND tradedate BETWEEN startdate and ISNULL(ENDDATE,'30000101')
                    where tradedate >= '%s' and tradedate < '%s'
                    and execution_venue not in ('LIFC')
                ) t
                join quant..execution_venue_map m on m.execution_venue = t.execution_venue
                group by  isnull(name,t.execution_venue)
                union
                select isnull(name,t.execution_venue), sum(quantity), sum(cost) FROM
                (
                    select execution_venue, quantity, quantity * isnull(rate,0.0) as cost
                    from [High Touch Trade Summary Cumulative] t
                    left join KYCDatabase..[RebateChargeRate Table] r on exchange = t.execution_venue 
                                                             AND UPPER(liquidity) = UPPER(isnull(liquidity_indicator,'')) 
                                                             AND UPPER(r.executor_id) = UPPER(t.executor_id)
                                                             AND tradedate BETWEEN startdate and ISNULL(ENDDATE,'30000101')
                    where tradedate >= '%s' and tradedate < '%s'
                    and execution_venue in ('LIFC')
                ) t
                join quant..execution_venue_map m on m.execution_venue = t.execution_venue
                group by  isnull(name,t.execution_venue)
            ''' % (start, end, start, end)
            
    rows = DB.select(query)
    total_volume = total_cost = 0
    for row in rows:
        ds.append([row[0],Quantity(row[1]), Dollar(row[2], color=True, precision = 0)])
        total_volume += row[1]
        total_cost += row[2]
        
    if len(ds) == 0:
        # No Data
        return
        
    ds.add_total_row(['Total', Quantity(total_volume), Dollar(total_cost, color=True, precision = 0)])
    ds.sort('Volume',reverse=True)
    ds.set_extra_style(['', "style='text-align:right;'"])
                   
    if isinstance(output, HtmlEmail): 
        print >> output, to_html(ds)
        output.set_sender('Sylvain Archenault <sarchenault@cheuvreux.com>')
        output.set_subject('Market cost / rebate from %s to %s' % (start, end - datetime.timedelta(days=1)))
        output.flush()
    else:
        print >> output, str(ds)
    

if __name__ == '__main__':
    report(sys.argv)