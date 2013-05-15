'''
Created on May 21, 2012

@author: syarc
'''
import datetime
from cheuvreux.bluebox.pta.report import DB
from cheuvreux.utils.dataset import Dataset, Quantity, Percentage, Dollar,\
    to_html, BPs
from cheuvreux.stdio.mail import HtmlEmail
from cheuvreux.utils.date import previous_weekday

def get_aggr_price(order_id, end_time):
    query = ''' SELECT gross_price, done FROM quant..algo_orders WHERE parent_order_id = '%s'
                   AND start_time > '%s' AND algo = 'CROSSFIRE v2'
                   AND datediff(ss, '%s', start_time) < 5
            ''' % (order_id, end_time, end_time)

    row = DB.selectOne(query)
    if row:
        return round(row[0],2), row[1]

    return None

def perf_improvement(order_id):
    improvement = 0

    query = ''' SELECT done, quantity, gross_price, bid, ask, end_time, side, order_id
                FROM quant..orders WHERE type = 'M'
                AND parent_order_id = '%s'
            ''' % (order_id)

    rows = DB.select(query)
    for row in rows:
        impr = 0.0
        side = row[6]
        # Some quantity has been done
        if row[0] > 0:
            if side == 1: #buy
                impr += row[0] * (row[4] - row[2])
            else: # Sell
                impr += row[0] * (row[2] - row[3])

        # There is some remaining qty
        if row[1] - row[0] > 0:
            aggr_price = get_aggr_price(order_id, row[5])
            if aggr_price:
                if side == 1: #Buy
                    impr += aggr_price[1] * (round(row[4],2) - max(aggr_price[0], round(row[4],2)))
                else: #Sell
                    impr += aggr_price[1] * (min(aggr_price[0], round(row[3],2)) - round(row[3],2))
            else:
                print "can't find aggressive order for %s %s %s" % (order_id, row[7], row[5])

        improvement += impr
        
    return improvement


def report(range, output, html=False):

    query = ''' select o.destination, count(*), sum(o.done), sum(o.quantity),
                       avg(1.0*o.done/o.quantity), sum(o.cost)
                from quant..algo_orders a
                join quant..orders o on o.parent_order_id = a.order_id and type = 'M'
                where a.date between '%s' and '%s' and algo in ('TWAP','VWAP')
                group by o.destination
                order by count(*) desc
            ''' % (range[0].strftime('%Y%m%d'), range[-1].strftime('%Y%m%d'))

    rows = DB.select(query)
    ds = Dataset(['Destination', '#','Done','Fill Rate', 'Avg Fill Rate', 'Cost'])
    ds.set_extra_style(['', "style='text-align:right;'"])


    total = total_done = total_sent = total_cost = 0
    for row in rows:

        total += row[1]
        total_done += row[2]
        total_sent += row[3]
        total_cost += row[5]

        ds.append([row[0], Quantity(row[1]), Quantity(row[2]),
                   Percentage(float(row[2])/row[3]),
                   Percentage(row[4]),
                   Dollar(row[5]/row[2] if row[2] > 0 else 0,4)
                   ])

    query = ''' select avg(1.0*o.done / o.quantity)
                FROM quant..algo_orders a
                JOIN quant..orders o ON o.parent_order_id = a.order_id and type = 'M'
                WHERE a.date between '%s' and '%s' and algo in ('TWAP','VWAP')
            ''' % (range[0].strftime('%Y%m%d'), range[-1].strftime('%Y%m%d'))

    avg = DB.selectOne(query)
    ds.add_total_row(['Total',Quantity(total),
                      Quantity(total_done),
                      Percentage(float(total_done)/total_sent),
                      Percentage(avg[0]),
                      Dollar(total_cost / total_done if total_done > 0 else 0,4)])
    if html:
        print >> output, '<h2>Per Destination</h2>'
        print >> output, to_html(ds)
    else:
        print >> output, ds


    query = '''select a.order_id, count(*), min(a.done), min(a.gross_price), sum(o.done), sum(o.quantity),
                      avg(1.0*o.done/o.quantity), min(a.side), min(a.instrument)
                from quant..algo_orders a
                join quant..orders o on o.parent_order_id = a.order_id and type = 'M'
                where a.date between '%s' and '%s' and algo in ('TWAP','VWAP') and a.done > 0 
                group by a.order_id
                order by count(*) desc
            ''' % (range[0].strftime('%Y%m%d'), range[-1].strftime('%Y%m%d'))

    rows = DB.select(query)
    ds = Dataset(['Order Id', 'Ticker', '#', 'Parent Qty', 'Done', 'Fill Rate', 'Avg Fill Rate', 'Perf Change'])
    ds.set_extra_style(['', "style='text-align:right;'"])


    sum_count = sum_qty = sum_done = sum_parent = sum_diff = den = 0
    for row in rows:
        side = row[7]
        old_gross_price = (row[2] * row[3] + side * perf_improvement(row[0])) / row[2]
        impr = side * (old_gross_price - row[3]) / row[3]
        sum_diff += row[2] * side * (old_gross_price - row[3])
        den += row[2] * row[3]

        sum_count += row[1]
        sum_qty += row[5]
        sum_done += row[4]
        sum_parent += row[2]

        ds.append([row[0], row[8], Quantity(row[1]), Quantity(row[2]),
                   Quantity(row[4]), Percentage(float(row[4])/row[5]),
                   Percentage(row[6]),
                   BPs(impr, True)
                   ])

    ds.add_total_row(['Total','',Quantity(sum_count),
                      Quantity(sum_parent),
                      Quantity(sum_done),
                      Percentage(float(sum_done)/sum_qty),
                      Percentage(avg[0]),
                      BPs(sum_diff / den, True)
                      ])

    if html:
        print >> output, '<h2>Per Order Id</h2>'
        print >> output, to_html(ds)
    else:
        print >> output, ds



if __name__ == '__main__':

    email = HtmlEmail('smtpnotes')
    email.set_subject('VWAP / TWAP Midpoint posting')
    email.set_dest('sarchenault@cheuvreux.com')

    #report([previous_weekday()], email, True)
    report([datetime.date(2012,6,29), datetime.date(2012,6,29)], email, True)

    email.flush()