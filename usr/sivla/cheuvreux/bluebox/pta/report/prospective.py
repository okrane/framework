'''
Created on Apr 26, 2012

@author: syarc
'''

import datetime
from cheuvreux.bluebox.pta.report import DB
from cheuvreux.utils.dataset import Dataset, Dollar, Quantity, Percentage,\
    to_html
from cheuvreux.stdio.mail import HtmlEmail
from collections import defaultdict

def summary(range):

    query = ''' SELECT isnull(dm.name, o.destination), sum(price_improvement), sum(o.done), sum(o.quantity),
                       sum(o.cost), count(*), avg(1.0*done/quantity)
                FROM quant..orders o
                LEFT JOIN quant..destination_map dm ON o.destination = dm.svcexec
                WHERE date BETWEEN '%s' and '%s' AND notes LIKE '%%prospective%%'
                GROUP BY isnull(dm.name, o.destination)
                ORDER BY sum(o.done) DESC
            ''' % (range[0].strftime('%Y%m%d'), range[-1].strftime('%Y%m%d'))

    rows = DB.select(query)

    if len(rows) == 0:
        raise ValueError('Empty report')

    data = []
    for row in rows:
        data.append([row[0], # Name
                     #Dollar(row[1],2), # PI
                     #Dollar(row[1]/row[2] if row[2] > 0 else 0,6), # PI per share
                     Quantity(row[2]), Quantity(row[3]), # Qty Done , Qty sent
                     Percentage(float(row[2])/row[3]), Percentage(float(row[6])), # Fill Rate, Avg Fill Rate
                     Dollar(row[4]/row[2] if row[2] > 0 else 0,4), # Cost per share
                     Quantity(row[5])]) # Count

    ds = Dataset(['Destination', 'Done', 'Total', 'Fill Rate', 'Avg Fill Rate', 'Cost / Share', 'Nb Orders'], data)

    ds.set_extra_style(['', "style='text-align:right;'"])
    ds.set_col_width([150, 100])

    return ds

def venue_breakdown(range, destination):

    query = ''' SELECT isnull(map.name,"last-mkt"), sum(t.quantity)
                  FROM Fidessa..[High Touch Trade Summary Cumulative] t
                  JOIN quant..orders o on t.order_id = o.order_id
                LEFT JOIN LAST_MKT_MAP map on map.last_mkt = "last-mkt"
                WHERE date between '%s' AND '%s' AND notes LIKE '%%prospective%%'  and destination like '%s'
                GROUP BY isnull(map.name,"last-mkt")
                ORDER BY sum(t.quantity) DESC
            ''' % (range[0].strftime('%Y%m%d'), range[-1].strftime('%Y%m%d'), destination)

    rows = DB.select(query)

    if len(rows) == 0:
        return None

    aggr_qty = defaultdict(int)
    for row in rows:
        if row[0]:
            aggr_qty[row[0].strip()] += row[1]

    ds = Dataset(['Destination', 'Quantity', 'Pct Total'])
    total = sum([row[1] for row in rows])
    for dest, qty in aggr_qty.iteritems():
        ds.append([dest, Quantity(qty), Percentage(float(qty) / total)])

    ds.sort('Quantity', True)
    ds.set_extra_style(['',"style='text-align:right;'"])
    ds.set_col_width([200,100])
    return ds


def report(range, output):
    dates = [r for r in range]
    ds = summary(dates)
    if ds:
        print >> output, to_html(ds)

    ds = venue_breakdown(dates, 'GETRTD%')
    if ds:
        print >> output, '<h2>GetAlpha Venue Breakdown</h2>'
        print >> output, to_html(ds)

    ds = venue_breakdown(dates, 'CTDLA MERCURY%')
    if ds:
        print >> output, '<h2>Mercury Venue Breakdown</h2>'
        print >> output, to_html(ds)

if __name__ == '__main__':
    range = [datetime.date(2012,5,1), datetime.date(2012, 5, 31)]

    email = HtmlEmail('smtpnotes')
    email.set_subject('Prospective slice')
    email.set_dest('sarchenault@cheuvreux.com')
    report(range, email)
    email.flush()