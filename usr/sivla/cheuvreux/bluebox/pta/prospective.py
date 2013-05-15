'''
Created on Oct 28, 2010

@author: syarc
'''
from cheuvreux.dbtools.Sqlite import SQLiteBase
from cheuvreux.fidessa.audit_trail import AuditTrail
from cheuvreux.fidessa.fidessadb import FidessaDB
from cheuvreux.utils.date import workday_range
import datetime

db = FidessaDB()
prospective_db = SQLiteBase('prospective.db')

def main(date):

    query = ''' select order_id, parent_order_id, instrument_code, market_id,
                       volume_done, volume_left, gross_price, buy_sell
                from [High Touch Orders Cumulative]
                where lower(order_note_id) like 'prospective%%'
                  and cust_xp_data_1 <> 'CUSTOM' and counterparty_code <> 'JSSF' and version = 1
                  and tradedate = '%s'
            ''' % (date)

    rows = db._backend.select(query)

    order_ids = set([row[0] for row in rows])

    def generator(order_ids):

        auditTrail = AuditTrail(date)
        auditTrail.orderIds = order_ids;
        auditTrail.loadData()


        gross_prices = {}

        for row in rows:
            for line in auditTrail[row[0]]:
                if line[1] == 'CHLD':
                    destination = line[5] + '|' + line[6]
                    if row[1] not in gross_prices:
                        parent_order = db.getFidessaOrder(row[1], False, 1, date)
                        gross_prices[row[1]] = parent_order.gross_price

                    yield {'ORDER_ID': row[0], 'PARENT_ORDER_ID': row[1], 'STOCK': row[2],
                           'MARKET_ID': row[3], 'DESTINATION': destination,
                           'DONE': row[4], 'SENT': row[5],
                           'GROSS_PRICE': row[6], 'PARENT_GROSS_PRICE': gross_prices[row[1]], 'DATE': date,
                           'BUY_SELL': row[7]}

    stmt = prospective_db.prepareInsertStmt('prospective',
                                            ['ORDER_ID', 'PARENT_ORDER_ID', 'STOCK',
                                             'MARKET_ID', 'DESTINATION','DONE', 'SENT',
                                             'GROSS_PRICE', 'PARENT_GROSS_PRICE', 'DATE', 'BUY_SELL'])
    prospective_db.execManyPreparedInsertStmt(stmt, generator(order_ids))
#    partition = defaultdict(set)

#    for order_id in orderIds:
#        for line in auditTrail[order_id]:
#            if line[1] == 'CHLD':
#                partition[line[5] + '|' + line[6]].add(order_id)

if __name__ == '__main__':
    for date in workday_range(datetime.date(2010, 12, 1), datetime.date(2010,12,31)):
        print date
        main(date.strftime('%Y%m%d'))
