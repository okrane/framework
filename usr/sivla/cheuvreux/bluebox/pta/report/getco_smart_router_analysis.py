'''
Created on Mar 23, 2012

@author: syarc
'''

from cheuvreux.bluebox.pta.report import DB
from collections import defaultdict
from cheuvreux.utils.dataset import Dataset, Percentage, Quantity
from cheuvreux.utils.statistics import quantile, summary

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
               'RR' : 'CSFB Light Pool'   }


start, end = '20120201', '20120329'

def midpoint_liquidity():

    rows = DB.select(''' SELECT liquidity_indicator, sum(t.quantity) FROM Fidessa..[High Touch Trade Summary Cumulative] t
                         join quant..orders o on o.order_id = t.order_id and o.destination = 'POST BB_POST_GETRTD_U1'
                         WHERE date >= '%s' AND date <= '%s'
                         GROUP BY LIQUIDITY_INDICATOR
                     ''' % (start, end))

    aggr_qty = defaultdict(int)
    for row in rows:
        destination = mapping[row[0].strip()] if row[0].strip() in mapping else row[0]
        aggr_qty[destination] += row[1]

    ds = Dataset(['Destination', 'Quantity', 'Pct Total'])
    total = sum([row[1] for row in rows])
    for dest, qty in aggr_qty.iteritems():
        ds.append([dest, Quantity(qty), Percentage(float(qty) / total)])

    ds.sort('Quantity', True)
    ds.set_extra_style(['',"style='text-align:right;'"])
    ds.set_col_width([200,100])

    print ds


def toto():
    rows = DB.select('''
    select new, o.price_improvement/o.done from quant..orders o
        join XFIRE_ID x on x.order_id = o.parent_order_id
        where destination = 'POST BB_POST_GETRTD' and o.done > 0

    ''')

    new = {1 : [], 0 : []}
    for row in rows:
        new[row[0]].append(row[1])

    for n in new:
        print '\t'.join(map(str, [n] + map(lambda x : '%.5f' % x, summary(new[n], [0.5,0.6,0.7,0.8,0.9,0.99]))))

def price_improvement():

    rows = DB.select(''' SELECT o.order_id, liquidity_indicator, t.quantity, t.gross_price, t.buy_sell,
                                o.bid, o.ask, x.new, o.price_improvement
                         FROM Fidessa..[High Touch Trade Summary Cumulative] t
                         join quant..orders o on o.order_id = t.order_id and o.destination = 'POST BB_POST_GETRTD'
                         join XFIRE_ID x on x.order_id = o.parent_order_id
                         WHERE date >= '%s' AND date <= '%s' and o.start_time < '15:59:00' and o.done > 0
                         --and o.parent_order_id not in (select parent_order_id from RANDOM_PARENT_ORDER_ID)
                     ''' % (start, end))

    pis = defaultdict(list)
    qty = defaultdict(int)

    pi_order_id = defaultdict(lambda: [0,0])

    for row in rows:
        quantity, gross_price = float(row[2]), float(row[3])
        buy = row[4] == 'B'
        bid, ask = float(row[5]), float(row[6])

        #if gross_price < bid or gross_price > ask:
        #   continue

        if row[0] not in pi_order_id:
            pi_order_id[row[0]][0] = row[8]

        if buy:
            pi = max(0,ask - gross_price)
        else:
            pi = max(0,gross_price - bid)

#        if pi < 0:
#            print pi, bid, ask, gross_price
#        elif pi > 1:
#            print pi, buy, bid, ask, gross_price

        pis[(mapping[row[1].strip()],row[7])].append(pi * quantity)
        qty[(mapping[row[1].strip()],row[7])] += quantity

        pi_order_id[row[0]][1] += pi * quantity

    for exc in pis:
        print '\t'.join(map(str,[exc[0], exc[1], sum(pis[exc]) / qty[exc], qty[exc], len(pis[exc])]))

    for order_id in pi_order_id:
        if abs(pi_order_id[order_id][0] - pi_order_id[order_id][1]) > 0.01:
            print order_id, '%.2f, %.2f' % (pi_order_id[order_id][0], pi_order_id[order_id][1])

#    print sum(all_pis[0]) / len(all_pis[0])
#    print sum(all_pis[1]) / len(all_pis[1])

price_improvement()
