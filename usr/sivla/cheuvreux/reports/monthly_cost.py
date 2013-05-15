'''
Created on Dec 9, 2010

@author: syarc
'''

from cheuvreux.dbtools.odbc import ODBC
from cheuvreux.stdio.html import HtmlTable
from cheuvreux.stdio.mail import Email
from cheuvreux.utils.date import parse_date
import getopt
import locale
import sys
import traceback

locale.setlocale(locale.LC_ALL, '')


def usage(basename):
    print "usage: %s [OPTIONS]" % basename;
    print ""
    print "OPTIONS:"
    print "  -d, --date=<DATE>  \t Specify the date (default is last working day)"
    print "                     \t could be a range START:END (format is YYYYMMDD)"
    print "  -e, --email=<DEST> \t Send the result via email to <DEST> (comma separated)"


def monthly_cost(start, end, output):
    fidessadb =  ODBC('DRIVER={SQL Server};SERVER=nysql001;DATABASE=Fidessa;UID=syarc;PWD=toto')

    query = '''SELECT execution_venue, sum(quantity) as quantity, count(*) as "count", sum(rate*quantity)
                FROM [High Touch Trade Summary Cumulative] trade
                LEFT JOIN KYCDatabase..[RebateChargeRate Table] rebate on rebate.exchange = trade.execution_venue
                                                                       and enddate is null
                                                                       and RTRIM(LTRIM(rebate.liquidity)) = isnull(trade.liquidity_indicator,'')
                WHERE tradedate between '%s' and '%s' and version = 1 and trade_part_index = 1
                GROUP BY execution_venue
                ORDER BY quantity DESC
            ''' % (start, end)

    rows = fidessadb.select(query)

    html = type(output) is Email

    if html:
        table = HtmlTable()
        table.setBorder()
        print >> output, "<h2>Cost / Rebate report for %s to %s</h2>" % (start, end)
        print >> output, table.header(['Destination', 'Done', '# Order', 'Cost / Rebate'], width=[100])
    else:
        print >> output, "Cost / Rebate report for %s to %s" % (start, end)

    total_qty = total_count = total_cost = 0

    for row in rows:
        total_qty += row[1]
        total_count += row[2]
        if row[3] is not None:
            total_cost += row[3]
        if html:
            if row[3] is not None:
                cost = '$' + locale.format("%d", row[3], grouping=True)
            else:
                cost = 'N/A'

            print >> output, table.line([row[0],
                                         locale.format("%d", row[1], grouping=True),
                                         locale.format("%d", row[2], grouping=True),
                                         cost],
                                         ['','style="text-align:right;"'])
        else:
            print >> output, '%s\t%d\t%d\%d' % (row[0], row[1], row[2],row[3])

    if html:
        print >> output, table.line(['Total',
                                     locale.format("%d", total_qty, grouping=True),
                                         locale.format("%d", total_count, grouping=True),
                                         '$' + locale.format("%d", total_cost, grouping=True)],
                                         ['style="font-weight:bold"','style="text-align:right;font-weight:bold"'])
    else:
        print >> output, 'Total\t%d\t%d\%d' % (total_qty, total_count, total_cost)


    if html:
        print >> output, table.end()

if __name__ == '__main__':

    start = end = None
    output, error = sys.stdout, sys.stderr

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hd:e:',
                                  ['help', 'date=', 'email='])
    except getopt.GetoptError:
        usage(sys.argv[0])
        sys.exit(2)

    for opt, val in opts:
        if opt in ("-h", "--help"):
            usage(sys.argv[0])
            sys.exit(0)
        elif opt in ('-f', '--force'):
            force = True
        elif opt in ('-d', '--date'):
            if ':' in val:
                start, end = val.split(':')
                start, end = parse_date(start), parse_date(end)
            else:
                start = end = parse_date(val)

        elif opt in ('-e', '--email'):
            output, error = Email("smtpnotes"), Email("smtpnotes")
            output.set_sender("Sylvain Archenault <sarchenault@cheuvreux.com>")
            output.set_type('html')
            output.set_dest(val)


            error.set_sender("Sylvain Archenault <sarchenault@cheuvreux.com>")
            error.set_type('html')
            error.set_dest('sarchenault')

            print >> error, "<pre>"

    if not start or not end:
        usage(sys.argv[0])
        sys.exit(-2)

    if type(output) is Email:
        output.set_subject("Cost Rebate Report %s to %s" % (start, end))
        error.set_subject("Cost Rebate Report %s to %s [Error]" % (start, end))

    try:
        monthly_cost(start, end, output)
    except Exception:
        traceback.print_exc(None, error)
        if type(error) is Email:
            print >> error, "</pre>"

        error.flush()
        sys.exit(-1)

    output.flush()

