'''
Created on Nov 8, 2010

@author: syarc
'''

import datetime
import time
from cheuvreux.fidessa.audit_trail import AuditTrail
from collections import defaultdict
from cheuvreux.utils.statistics import variance
import math
from cheuvreux.fidessa.fidessadb import FidessaDB
from cheuvreux.dbtools.tickdb import TickDB
from cheuvreux.fidessa import SecurityIdCache
import traceback



ignore_stocks = ['ZVZZT', 'ZXZZT', 'ZWZZT']
event_type = 'CHLD'

service_ids = {'GETC':event_type, 'NITED':event_type, 'QANTX':event_type, 'QANTY':event_type,
               'ATDPNG':event_type, 'EBXL':event_type, 'ECUT':event_type,
               'NYFX':event_type, 'NITEM':event_type, 'MILL':event_type, 'BONY':event_type,
               'FBCO':event_type, 'NFSC':event_type,
               'AQUA':event_type, 'PDQM':event_type, 'MLXN': event_type, 'BCAP_LX':event_type}

def main(date):
    auditTrail = AuditTrail(date)
    auditTrail.loadData()
    fidessa = FidessaDB()

    results = defaultdict(list)

    sdate = date.strftime('%Y%m%d')

    for order_id in auditTrail:
        try:
            for line in auditTrail[order_id]:
                if line[5] in service_ids and line[1] == service_ids[line[5]]:
                    order = fidessa.getFidessaOrder(order_id)
                    if order is None or order.gross_price == 0.0:
                        continue

                    t = AuditTrail.getFastDateTime(line[2])
                    start, end = t - datetime.timedelta(seconds=30),  t + datetime.timedelta(seconds=30)
                    sec_id = SecurityIdCache.getSecurityId(line[18])[0]
                    if sec_id is None:
                        break
                    clock = time.clock()
                    avg_price = TickDB.average_deal_price(sec_id, sdate, start.strftime('%H:%M:%S'), end.strftime('%H:%M:%S'))
                    print time.clock() - clock

                    if avg_price is None:
                        break

                    if order.isBuy:
                        perf = order.gross_price - avg_price
                    else:
                        perf = avg_price - order.gross_price

                    #print order_id, line[18], SecurityIdCache.getSecurityId(line[18]), start, time, end, order.gross_price, avg_price
                    results[line[5]].append(perf)

                    # go to the next order
                    break
        except Exception, e:
            traceback.print_exc()
            continue


    for service in results:
        print service, sum(results[service])/len(results[service]), len(results[service]), math.sqrt(variance(results[service]))



if __name__ == '__main__':
    date = datetime.date(2010,11,3)
    print date
    #main(date)
    date = datetime.date(2010,11,4)
    print date
    main(date)
    date = datetime.date(2010,11,5)
    print date
    main(date)
