'''
Created on Oct 28, 2011

@author: syarc
'''
import sys
from datetime import date
from cheuvreux.bluebox.pta.report import DB
from cheuvreux.utils.dataset import Dollar, Percentage, Quantity, Dataset,\
    to_html, ComposedFloat
from cheuvreux.stdio.mail import HtmlEmail
import datetime

def get_avg_fill_rate(start, end):
    query = ''' SELECT isnull(dm.name, destination), t.alias, sum(o.done), sum(o.quantity), sum(price_improvement)
                  FROM algo_orders a
             JOIN orders o on o.parent_order_id = a.order_id
             JOIN #TYPE_GRP t on t.type = o.type
             LEFT JOIN destination_map dm on dm.svcexec = destination
                 WHERE a.parent_order_id IS NULL
                   AND a.date between '%s' and '%s'
                   AND a.ALGO = 'CROSSFIRE'
             GROUP BY isnull(dm.name, destination), t.alias
            ''' % (start, end)

    result = {}
    for row in DB.select(query):
        if row[1] not in result:
            result[row[1]] = {}
        result[row[1]][row[0]] = row[2], row[3], row[4]

    return result

def run_report(range, output=sys.stdout, html=False):

    DB.execDirect('CREATE TABLE #TYPE_GRP (type char, alias char)')

    DB.run (''' INSERT INTO #TYPE_GRP VALUES('A','A');
                INSERT INTO #TYPE_GRP VALUES('I','P');
                INSERT INTO #TYPE_GRP VALUES('P','P');
                INSERT INTO #TYPE_GRP VALUES('M','M');
            ''')
    query = ''' select isnull(dm.name, destination), t.alias, sum(price_improvement),
                    sum(o.done), sum(o.quantity), sum(o.cost), count(*) from algo_orders a
            JOIN orders o on o.parent_order_id = a.order_id
            JOIN #TYPE_GRP t on t.type = o.type
            LEFT JOIN destination_map dm on dm.svcexec = destination
            WHERE a.parent_order_id IS NULL
              AND a.date between '%s' and '%s'
              AND a.ALGO = 'CROSSFIRE'
            GROUP BY isnull(dm.name, destination), t.alias
            ORDER BY sum(o.done) DESC
        '''

    rows = DB.select(query % (range[0].strftime('%Y%m%d'), range[-1].strftime('%Y%m%d')))

    if len(rows) == 0:
        raise ValueError('Empty report')

    avgFillRate = None

    if len(range) == 1:
        avgFillRate = get_avg_fill_rate(range[0] - datetime.timedelta(30), range[0])

    ds_price_stgy = {}

    total_price_stgy = {} #    total_pi = total_done = total = cost = nb_orders = 0
    for row in rows:
        if row[1] not in ds_price_stgy:
            ds = Dataset(['Destination', 'PI', 'PI / share', 'Done', 'Fill Rate (Avg)', 'Cost / Share', '# Orders'])
            ds.set_extra_style(['', "style='text-align:right;'"])
            ds.set_col_width([150, 100])

            ds_price_stgy[row[1]] = ds
            total_price_stgy[row[1]] = [0,0,0,0,0]


        avg_fill_rate = None
        if avgFillRate:
            avg = avgFillRate[row[1]][row[0]]
            avg_fill_rate = Percentage(float(avg[0]) / avg[1])
            avg_pi = Dollar(avg[2] / avg[0] if avg[0] > 0 else 0,4)

        ds_price_stgy[row[1]].append([row[0], Dollar(row[2],2),
                      Dollar(row[2]/row[3] if row[3] > 0 else 0,4),
                     #ComposedFloat(,avg_pi),
                     Quantity(row[3]),
                     ComposedFloat(Percentage(float(row[3])/row[4]), avg_fill_rate),
                     Dollar(row[5]/row[3] if row[3] > 0 else 0,4),
                     Quantity(row[6])])


        total_price_stgy[row[1]][0] += row[2] # Total PI
        total_price_stgy[row[1]][1] += row[3] # Total done
        total_price_stgy[row[1]][2] += row[4] # Total sent
        total_price_stgy[row[1]][3] += row[5] # Total cost
        total_price_stgy[row[1]][4] += row[6] # Total count

    for stgy in ds_price_stgy:
        values = total_price_stgy[stgy]
        total_pi, total_done, total, cost, nb_orders = values[0], values[1], values[2], values[3], values[4]

        ds_price_stgy[stgy].add_total_row(['Total', Dollar(total_pi, 2), Dollar(total_pi / total_done if total_done > 0 else 0, 4),
                      Quantity(total_done),
                      Percentage(float(total_done) / total), Dollar(cost / total_done, 4), Quantity(nb_orders)])

    stgy_map = {'I': 'Inside', 'M': 'Midpoint', 'P': 'Passive', 'A': 'Aggressive'}

    if html:
        for stgy in sorted(ds_price_stgy.keys()):
            ds_price_stgy[stgy].sort('Fill Rate (Avg)', True)
            print >> output, '<h2>%s</h2>' % stgy_map[stgy]
            print >> output, to_html(ds_price_stgy[stgy])
    else:
        for stgy, ds in ds_price_stgy.iteritems():
            print >> output, stgy_map[stgy]
            print >> output, to_html(ds)
            print >> output, str(ds)

if __name__ == '__main__':
    range = [date(2011,11,11)]

    email = HtmlEmail('smtpnotes')
    email.set_subject('Crossfire')
    email.set_dest('sarchenault@cheuvreux.com')

    run_report(range, email, True)

    email.flush()