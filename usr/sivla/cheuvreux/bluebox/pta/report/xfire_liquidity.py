'''
Created on Jul 21, 2011

@author: syarc
'''
from cheuvreux.bluebox.pta.report import DB
from operator import itemgetter
from collections import defaultdict

def liquidity(ctpy, date):

    query= '''select name, is_dark, sum(done)
            from orders o
            left join destination_map d on d.svcexec = o.destination
            where counterparty = ? and date = ? and done > 0
            group by name, is_dark
           '''

    rows = DB.select(query, ctpy, date)

    total = sum ([r[2] for r in rows])

    ans = [ (d[0], d[2], 100.0 * d[2] / total) for d in sorted(rows, key=itemgetter(2), reverse=True) ]
    for d in ans:
        print '%s\t%d\t%.1f%%' % (d[0], d[1], d[2])

    print ""

    data = defaultdict(float)
    for row in rows:
        if row[1] == 1:
            data['Dark Pool'] += row[2]
        else:
            data[row[0]] += row[2]

    res = [ (a, b, 100.0 * b / total) for a,b in data.iteritems() ]
    for r in sorted(res, key=itemgetter(1), reverse=True):
        print '%s\t%d\t%.1f%%' % (r[0], r[1], r[2])

if __name__ == '__main__':
    liquidity('DRW','20110720')