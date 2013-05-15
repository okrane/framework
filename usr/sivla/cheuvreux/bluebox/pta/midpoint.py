'''
Created on Feb 14, 2011

@author: syarc
'''
import datetime
from cheuvreux.fidessa.audit_trail import AuditTrail
import re
from cheuvreux.fidessa.fidessadb import FidessaDB
import sys
from cheuvreux.fidessa import SecurityIdCache
from cheuvreux.dbtools.tickdb import TickDB
from cheuvreux.utils.date import workday_range

midpoint_destination = ('BB_POST_INET_HMID', 'BB_POST_BATS_HMID', 'BB_POST_EDGX_HMID',
                        'BB_POST_FLOW_HMID', 'CROSS_STREAM_MID', 'BB_POST_EDGA_HMID',
                        'RESTING_ORDR_MID ', 'BB_POST_ARCX_HMID', 'PEG2MIDPOINT',
                        'LX_MID','BB_POST_BX_HMID', 'MIDPOINT_MIN100')

def get_deal(security, time, after):
    time = time + datetime.timedelta(minutes = after)
    deal =TickDB.get_first_deal(security, time.date().strftime('%Y%m%d'), time.time())
    if deal:
        return deal[0][0]
    else:
        return None

def mid_point(date):

    midpoint_orders = open('midpoint_orders_%s.txt' % date.strftime('%Y%m%d'), 'w')
    midpoint_fills = open('midpoint_fills_%s.txt' % date.strftime('%Y%m%d'), 'w')

    fidessa = FidessaDB()

    audit_trail = AuditTrail(date)
    audit_trail.loadData()

    header = ['order_id', 'parent_order_id', 'entered', 'stock', 'side', 'destination',
              'qty', 'qty fill', 'gross price', 'bid', 'ask']
    for order_id in audit_trail:
        first_line =  audit_trail[order_id][0]
        if first_line[6] not in midpoint_destination:
            continue

        destination = first_line[5] + '|' + first_line[6]

        order = fidessa.getFidessaOrder(order_id)
        if not order:
            continue

        security = SecurityIdCache.getSecurity(order.code, order.market_id)
        if not security:
            continue

        time = datetime.datetime.strptime(order.date + ' ' + order.entered[0:8], '%Y%m%d %H:%M:%S')

        print >> midpoint_orders, ','.join(map(str, [order_id, order.parent_order_id, order.entered, order.code,
                                  order.buy_sell, destination, order.quantity, order.done, order.gross_price,
                                  first_line[12], first_line[13], get_deal(security, time, 1), get_deal(security, time, 5),
                                 get_deal(security, time, 10)]))

        for line in audit_trail[order_id]:
            if line[1] == 'EXEE':
                trade_id = re.split('[^\[]*\[([^\]]*)\]', line[9])[1]
                trade = fidessa.getFidessaTrade(order_id, trade_id)
                if trade:

                    time = datetime.datetime.strptime(trade.timestamp[0:17], '%Y%m%d %H:%M:%S')

                    print >> midpoint_fills, ','.join(map(str,
                                [order_id, order.parent_order_id, trade.timestamp, order.code, order.buy_sell,
                                 destination, order.quantity, trade.quantity, trade.gross_price, trade.bid_price,
                                 trade.offer_price, get_deal(security, time, 1), get_deal(security, time, 5),
                                 get_deal(security, time, 10)]))


if __name__ == '__main__':
    for date in workday_range(datetime.date(2011,2,1), datetime.date(2011,2,11)):
        mid_point(date)

