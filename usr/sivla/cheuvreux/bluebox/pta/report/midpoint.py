'''
Created on Jun 27, 2011

@author: syarc
'''
from cheuvreux.bluebox.pta.report import DB
from cheuvreux.utils.dataset import Dataset, Percentage, Dollar, Quantity,\
    to_html
from cheuvreux.stdio.mail import HtmlEmail
from collections import defaultdict
import datetime


def midpoint_report(start_date, end_date):


    query = ''' SELECT isnull(dm.name, orders.destination), replace(lower(notes),'midpoint peg',''), sum(done), sum(quantity), sum(done)*1.0/sum(quantity),
                       sum(price_improvement) / sum(done), sum(cost)/sum(done), count(*)
                FROM orders
                LEFT JOIN destination_map dm on dm.svcexec = destination
                WHERE date between ? and ? and type = 'M'
                GROUP BY isnull(dm.name, orders.destination), notes having sum(done) > 0
                ORDER by sum(done) DESC
            '''

    rows = DB.select(query, start_date, end_date)

    if len(rows) == 0:
        raise ValueError('Empty report')

    data = []
    for row in rows:

        data.append([row[0], row[1], Quantity(row[2]), Quantity(row[3]), Percentage(row[4]),
                     Dollar(row[5],4), Dollar(row[6],4), Quantity(row[7])])


    ds = Dataset(['Destination', 'Notes', 'Done', 'Total', 'Fill Rate', 'PI / Share', 'Cost / Share', 'Count'], data)

    ds.set_extra_style(['', '', "style='text-align:right;'"])

    return ds

def midpoint_alert(start_date, end_date):

    query = ''' SELECT order_id, destination, start_time, end_time
                FROM orders
                WHERE date between ? and ? AND type = 'M' AND done > 0
                 AND price_improvement = 0.0 AND bid < ask
            '''
    rows = DB.select(query, start_date, end_date)

    alerts = defaultdict(list)
    for row in rows:
        start, end = datetime.datetime.strptime(row[2][0:8],'%H:%M:%S'), datetime.datetime.strptime(row[3][0:8],'%H:%M:%S')
        if end - start <= datetime.timedelta(seconds=1):
            alerts[row[1]].append(row[0])

    ds = Dataset(['Destination', 'Count', 'Order Id'])
    for dest in alerts:
        if len(alerts[dest]) >= 10:
            ds.append([dest, len(alerts[dest]), ' '.join(alerts[dest])])

    return ds

def run_report(range, output, html=False):

    process_method = to_html
    if not html:
        process_method = Dataset.__str__

    start, end = range[0].strftime('%Y%m%d'), range[-1].strftime('%Y%m%d')
    print >> output, process_method(midpoint_report(start, end))
    alerts = midpoint_alert(start, end)
    if alerts:
        alerts.set_col_width([150, 100, 500])
        if html:
            print >> output, '<h2>MidPoint Fill Alerts</h2>'
        else:
            print >> output, 'MidPoint Fill Alerts'

        print >> output, process_method(alerts)

if __name__ == '__main__':
    email = HtmlEmail('smtpnotes')
    email.set_subject('Midpoint Summary')
    email.set_dest('sarchenault@cheuvreux.com')

    run_report([datetime.date(2011,10,12)], email, True)

    email.flush()