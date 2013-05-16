'''
Created on Apr 4, 2011

@author: syarc
'''
import datetime
from cheuvreux.fidessa.fidessadb import FidessaDB
from cheuvreux.dbtools.tickdb import TickDB
from cheuvreux.fidessa import SecurityIdCache
from collections import defaultdict
import pickle

is_dark = {'AMEX':False, 'AQUA':True, 'ARCA':False, 'ARCX':False,
'AUTO':True, 'BATS':False, 'BCAP':True, 'BYXX':False, 'EBXL':True, 'ECUT':True, 'EDGA':False, 'EDGX':False,
'FBCO':True, 'FLOW':False, 'GESX':False, 'GFLO':True, 'GOTO':True, 'INET':False, 'LATS':True, 'MAXM':False, 'MLCO':True,
'MSCO':True, 'NFSC':True, 'NITE':True, 'NQBX':False, 'NSDQ':False, 'NYFX':True, 'NYSE':False, 'PDQM':True,
'PULX':True, 'QANTX':True, 'QANTY':True, 'TRAC':True, 'TRIM':True, 'UBSS':True}


def aggregate():
    stock = 'intc'
    results = pickle.load(open('results_%s_2011.cache' % stock, 'rb'))
    desc = pickle.load(open('desc_%s_2011.cache' % stock, 'rb'))

    agg = dict()
    agg['Dark'] = [0,0,0,list()]
    for venue in desc:
        if venue in is_dark and is_dark[venue]:
            agg['Dark'][0] += desc[venue][0]
            agg['Dark'][1] += desc[venue][1]
            agg['Dark'][2] += desc[venue][2]
            agg['Dark'][3] += agg['Dark'][3] + results[venue]
        else:
            agg[venue] = [desc[venue][0], desc[venue][1], desc[venue][2], results[venue]]

    for venue in agg:
        print '%s\t%.2f\t%d\t%d\t%.2f' % (venue, 10000 * sum(agg[venue][3]) / len(agg[venue][3]),
                                          agg[venue][0], agg[venue][1], agg[venue][2])

def toxicity_report(start, end):

    timedelta = 30

    results = defaultdict(list)
    desc = defaultdict(lambda: [0,0,0,list()])

    db = FidessaDB()

    stock = 'HPQ'

    query = ''' SELECT timestamp, buy_sell, instrument_code, market_id, execution_venue, gross_price, quantity, tradedate,
                        offer_price, bid_price
                FROM %s
                WHERE tradedate >= '%s' and tradedate <= '%s' and instrument_code = '%s'
            ''' % (FidessaDB.TRADE_TABLE, start.strftime('%Y%m%d'), end.strftime('%Y%m%d'), stock)
    rows = db._backend.select(query)
    for row in rows:
        security = SecurityIdCache.getSecurity(row[2], row[3])
        if not security:
            #print row[2], row[3], 'not found'
            continue
        time = datetime.datetime.strptime(row[0][9:17],'%H:%M:%S') + datetime.timedelta(seconds=timedelta)
        spread = TickDB.spread_at_time(security, row[7].strftime('%Y%m%d'), time.time().strftime('%H:%M:%S'))
        if not spread or not spread[0]: #Unable to find price
            continue

        trade_mid_price = (row[8] + row[9] ) /2.0
        mid_price = (spread[0] + spread[1]) / 2.0
        sign = 1.0 if row[1] == 'B' else -1.0

        reversion = sign * (mid_price - row[5]) / row[5]

        results[row[4]].append(reversion)
        desc[row[4]][0] += 1
        desc[row[4]][1] += row[6]
        desc[row[4]][2] += row[6] * row[5]
        desc[row[4]][3].append(sign * (mid_price - trade_mid_price) / trade_mid_price)


    for venue in results:
        print '%s\t%.2f\t%d\t%d\t%.2f' % (venue, 10000 * sum(results[venue]) / len(results[venue]),
                                          desc[venue][0], desc[venue][1], desc[venue][2])

    pickle.dump(results, open('results30_%s_2011.cache' % stock, 'wb'))
    pickle.dump(dict(desc.items()), open('desc30_%s_2011.cache' % stock, 'wb'))

if __name__ == '__main__':
#    aggregate()
    toxicity_report(datetime.date(2011,1,1),datetime.date(2011,04,6))
