'''
Created on Oct 7, 2010

@author: syarc

'''
import sys

rebate_in_memory = True

# Is null function is different from SQL Server / SQLiteBase
ISNULL_FUNCTION = 'ISNULL'
REBATE_TABLE = '[RebateChargeRate Table]'

# "_" prefix function do the real work and take a SQLiteBase object as first argument
def _create_table(db):
    db.createTable('rebate',
                   '''
                   "EXCHANGE" varchar(50) NOT NULL,
                   "LIQUIDITY" varchar(50) NOT NULL,
                   "EXECUTOR_ID" varchar(50) NOT NULL,
                   "RATE" real NOT NULL DEFAULT 0.0,
                   "STARTDATE" TEXT,
                   "ENDDATE" TEXT,
                   "DESCRIPTION" varchar(20)
                   ''')

def _load_rebates(db):
    #

    def rebate_generator():
        from cheuvreux.dbtools.odbc import ODBC
        base = ODBC('DRIVER={SQL Server};SERVER=nysql001;DATABASE=KYCDatabase;UID=syarc;PWD=syarc')
        rows = base.select(''' SELECT exchange, isnull(liquidity,''), rate, convert(varchar, StartDate, 112),
                                      convert(varchar, EndDate, 112), EXECUTOR_ID
                               FROM [RebateChargeRate Table]''')

        for row in rows:
            if row[2] is None:
                row[2] = 0.0
            if row[1] == ' ':
                row[1] = ''
            yield row

    stmt = db.prepareInsertStmt('rebate', ['EXCHANGE', 'LIQUIDITY', 'RATE', 'STARTDATE', 'ENDDATE', 'EXECUTOR_ID'])
    db.execManyPreparedInsertStmt(stmt, rebate_generator())

def _get_rebate(db, exchange, liquidity, exec_id):

    row = db.selectOne(''' SELECT rate FROM %s WHERE exchange = ? AND upper(liquidity) = upper(?)
                               AND upper(executor_id) = upper(?)
                               AND enddate IS NULL''' % REBATE_TABLE,
                       exchange, liquidity, str(exec_id))

    if not row:
        row = db.selectOne(''' SELECT rate FROM %s WHERE exchange = ? AND upper(liquidity) = upper(?)
                               AND enddate IS NULL''' % REBATE_TABLE,
                       exchange, liquidity)
    if row:
        return row[0]
    return None

def _get_rebate_date(db, exchange, liquidity, exec_id, date):
    row = db.selectOne(''' SELECT rate FROM %s
                                WHERE exchange = ?
                                  AND UPPER(liquidity) = UPPER(?)
                                  AND UPPER(executor_id) = UPPER(?)
                           AND ? BETWEEN startdate AND %s(enddate,'30000101')
                       ''' % (REBATE_TABLE, ISNULL_FUNCTION), exchange, liquidity, exec_id, date)
    if not row:
        row = db.selectOne(''' SELECT rate FROM %s
                                WHERE exchange = ?
                                  AND UPPER(liquidity) = UPPER(?)
                                  AND ? BETWEEN startdate AND %s(enddate,'30000101')
                           ''' % (REBATE_TABLE, ISNULL_FUNCTION), exchange, liquidity, date)
    if row:
        return row[0]
    return None

def get_rebate(exchange, liquidity, exec_id, date=None):
    if date:
        return _get_rebate_date(rebate_db, exchange, liquidity, exec_id, date)
    else:
        return _get_rebate(rebate_db, exchange, liquidity, exec_id)

def get_rebate_for_trade(trades, date=None):
    '''
        Compute the total rebate for a list of trades
    '''

    rebate = 0.0

    for trade in trades:
        r = get_rebate(trade.exchange, trade.liquidity, trade.executor_id, date)

        if r is None:
            print >> sys.stderr, "Rebate not found for ", trade.exchange, trade.liquidity, trade.order_id
        elif abs(r) > 0:
            rebate += trade.quantity * r


    return rebate

if rebate_in_memory:
    from cheuvreux.dbtools.Sqlite import SQLiteBase
    ISNULL_FUNCTION = 'IFNULL'
    REBATE_TABLE = 'rebate'
    rebate_db = SQLiteBase(':memory:')
    _create_table(rebate_db)
    _load_rebates(rebate_db)
else:
    from cheuvreux.dbtools.odbc import ODBC
    rebate_db = ODBC('DRIVER={SQL Server};SERVER=nysql001;DATABASE=KYCDatabase;UID=syarc;PWD=syarc')

if __name__ == '__main__':
    from cheuvreux.fidessa.fidessadb import FidessaDB
    from cheuvreux.fidessa.fidessa import FidessaTrade

    db = FidessaDB()._backend
    rows = db.select (''' SELECT order_id, gross_price, quantity, execution_venue, liquidity_indicator,
                        buy_sell, bid_price, offer_price, executor_id FROM [High Touch Trade Summary Cumulative] WHERE order_id = ?''', '00070904399ORCG0')
    trades = []
    for row in rows:
        r = dict()
        r['order_id'] = row[0]
        r['dealt_price'] = row[1]
        r['quantity'] = row[2]
        r['execution_venue'] = row[3]
        r['liquidity_indicator'] = row[4].strip()
        r['bid_price'] = row[7]
        r['offer_price'] = row[6]
        r['buy_sell'] = row[5]
        r['executor_id'] = row[8]
        trades.append(FidessaTrade(r))

        print row

    print get_rebate_for_trade(trades)




#    import cProfile, time
#
#    order_id = '00036447154ORCG0'
#    fdb = get_fidessa_db('SQLite', SQLiteBase('../bluebox/pta/dev.db'), '20101019')
#
#    start = time.time()
#    trades = fdb.get_trades(order_id)
#    print time.time() - start
#    for trade in trades:
#        print trade



    #print get_rebate('FBCO', ' ', '20091001')



