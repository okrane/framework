'''
Created on Oct 31, 2011

@author: syarc
'''

from cheuvreux.bluebox.pta.report import DB
from cheuvreux.dbtools.odbc import ODBC
from cheuvreux.stdio.mail import HtmlEmail
from cheuvreux.utils.dataset import Dollar, Percentage, Quantity, Dataset, \
    to_html, Integer, ComposedValue
from datetime import date
import datetime
import sys
from cheuvreux.fidessa import fidessadb

def get_avg_latency(db, start, end):
    rows = db.select(''' SELECT destination, avg(q50), avg(q99), avg(stdev)
                   FROM latency WHERE date between '%s' and '%s'
                  GROUP BY destination
              ''' % (start, end))

    result = {}
    for row in rows:
        result[row[0]] = (row[1], row[2], row[3])
    return result


def run_report(range, output, html=False):

    result_db = fidessadb.getODBCConnection()
    rows = result_db.select(''' SELECT destination, avg(q50), avg(q99), avg(stdev), sum(nb_orders)
                                  FROM latency WHERE date between'%s' and '%s'
                                  GROUP BY destination
                            ''' % (range[0], range[-1]))


    if len(rows) == 0:
        raise ValueError("Empty Report")

    avg_latency = None
    if len(range) == 1:
        avg_latency = get_avg_latency(result_db, range[0] - datetime.timedelta(30), range[0])

    ds = Dataset(['Destination','Median','Q99','Std Dev.', 'Count'])
    for row in rows:
        avg_med = avg_stdev = avg_q99 = 0
        if avg_latency and row[0] in avg_latency:
            avg_med = Integer(avg_latency[row[0]][0])
            avg_q99 = Integer(avg_latency[row[0]][1])
            avg_stdev = Integer(avg_latency[row[0]][2])
        ds.append([row[0],
                   ComposedValue(Integer(row[1]), avg_med, inversed=True, threshold = 0.05),
                   ComposedValue(Integer(row[2]), avg_q99, inversed=True, threshold = 0.25),
                   ComposedValue(Integer(row[3]), avg_stdev, inversed=True, threshold = 0.25),
                   Quantity(row[4])])

    ds.sort('Median')
    ds.set_col_width([150, 100])
    ds.set_extra_style(['', "style='text-align:right;'"])

    if html:
        print >> output, to_html(ds)
    else:
        print >> output, str(ds)


if __name__ == '__main__':
    mail = HtmlEmail("smtpnotes")
    mail.set_dest('sarchenault@cheuvreux.com')
    mail.set_type('html')
    mail.set_subject('Latency')

    run_report([datetime.date(2011,12,9)], mail, True)

    mail.flush()