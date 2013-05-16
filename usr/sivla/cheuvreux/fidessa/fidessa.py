'''
Created on Oct 7, 2010

@author: syarc
'''
from cheuvreux.dbtools.Sqlite import SQLiteBase
from cheuvreux.fidessa import orderdb, fidessa_util, rebate

class FidessaTrade:

    def __init__(self, row):
        self.order_id = row['order_id']
        self.price = float(row['dealt_price'])
        self.quantity = float(row['quantity'])
        self.exchange = row['execution_venue']
        self.liquidity = row['liquidity_indicator'].strip()
        self.executor_id = row['executor_id']

        self.pi = 0
        try:
            if row['bid_price'] > 0 and row['offer_price'] > 0:
                if row['buy_sell'] == 'B':
                    pi = float(row['offer_price']) - self.price
                else:
                    pi = self.price - float(row['bid_price'])

                self.pi = self.quantity * max(0, pi)
        except IndexError:
            pass

    def __str__(self):
        return "%s %d @ %f on %s (%s)" % (self.order_id, self.quantity,
                                         self.price, self.exchange, self.liquidity)

class FidessaOrder:
    '''
        Fidessa order object
    '''

    def __init__(self, row):
        ''' Constructor
        '''

        self.date, self.entered, dummy = row['o.entered_datetime'].strip().split(' ')

        (self.order_id, self.ctpy, self.code) = (row['o.order_id'], row['o.counterparty_code'], row['o.instrument_code'])
        self.algo = fidessa_util.parse_algo_name(row['algo'])
        if row['startTime'] is not None and row['startTime'] is not '':
            self.start = row['startTime']
        else:
            self.start = self.entered
        self.end = row['endTime'] if row['endTime'] is not None else '16:00:00'
        self.done = row['o.volume_done']
        self.left = row['o.volume_left']
        self.note_id = row['o.order_note_id']
        self.buy_sell = row['o.buy_sell']
        self.gross_price = row['o.gross_price']
        self.parent_order_id = row['o.parent_order_id']
        self.order_price_type = row['o.order_price_type_qualifier']
        self.market_id = row['o.market_id']
        self.nb_child = row['nb_child']

        if row['o.limit_price'] is None or row['o.limit_price'] == 0:
            self.limit_price = None
        else:
            self.limit_price = row['o.limit_price']

    def __str__(self):
        return ("%s, %s, %s, %s, %s, %s, %s, %s, %s, %d, %d, %f"
                % (self.order_id, self.buy_sell, self.ctpy, self.code, self.date, self.entered,
                   self.algo, self.start, self.end, self.done, self.left, self.gross_price))

    @property
    def quantity(self):
        ''' Returns global order quantity (done and left) '''
        return self.done + self.left

    @property
    def isBuy(self):
        ''' Is buy order '''
        return self.buy_sell == 'B'

    def isAggressive(self, bid, ask):
        if self.limit_price is None:
            return True

        if self.isBuy:
            return self.limit_price >= ask
        else:
            return self.limit_price <= bid

    def isPassive(self, bid, ask):
        if self.limit_price is None:
            return False

        if self.isBuy:
            return self.limit_price <= bid

        return self.limit_price >= ask

    @property
    def isMarketOrder(self):
        return self.limit_price is None

    @property
    def isOpenOrder(self):
        return self.order_price_type == 'OPEN'

    @property
    def isCloseOrder(self):
        return self.order_price_type == 'CLSE'

class FidessaDB:

    def __init__(self):
        pass

    def get_fidessa_order(self, order_id):
        raise Exception('Not Implemented')

    def get_nb_child(self, order_id):
        raise Exception('Not Implemented')

class FidessaDB_SQLite(FidessaDB):

    def __init__(self, sqlite_db, date):
        if not sqlite_db:
            sqlite_db = SQLiteBase(':memory:')
            orderdb.load_fidessa_data(sqlite_db, date)

        self._backend = sqlite_db

    def get_nb_child(self, order_id):
        ''' Returns the number of slice '''

        row = self._backend.selectOne('SELECT count(*) FROM orders WHERE parent_order_id = ?',
                                      order_id)
        if row:
            return row[0]

        return None

    def getBlueboxOrderIds (self, date, strategy):
        '''
            Returns the bluebox order ids for a given date and strategy
        '''
        rows = self._backend.select(''' select order_id from bluebox
                                       where strategy in ('%s')
                                    ''' % "','".join(strategy))
        return set([row[0] for row in rows])


    def get_fidessa_order(self, order_id, version=None, date=None):
        ''' Returns a Fidessa Order object based on a 'order_id'

            @param order_id: Order id
            @param version: Version number you want to retrieve (accept "last"
                            for last version.
            @param date: Limit search for the specify date
        '''

        if version == 'last':
            row = self._backend.selectOne("SELECT max(version) FROM orders " +
                                          "WHERE order_id = ?", order_id)
            if row is not None and len(row) > 0:
                version = row[0]

        if not version:
            version = 1


        query = ''' select o.order_id, o.counterparty_code, o.instrument_code, o.entered_datetime,
                    o.cust_xp_data_1 as algo, o.cust_xp_data_2 as startTime, o.cust_xp_data_3 as endTime,
                    o.volume_done, o.volume_left, o.order_note_id, o.buy_sell, o.gross_price, o.limit_price,
                    o.parent_order_id, o.order_price_type_qualifier, o.market_id, count(c.order_id) as nb_child
                    from orders o
                    left join orders c on c.parent_order_id = o.order_id and c.version = 1
                    where o.order_id = ?
                      and o.version = ?
                '''
        if date is not None:
            query += " AND tradedate = '%s' " % date

        rows = self._backend.select(query, order_id, version)
        if len(rows) > 0:
            return FidessaOrder(rows[0])
        else:
            return None

    def get_trades(self, order_id):

        query = ''' SELECT t.order_id as order_id, dealt_price, quantity, execution_venue, liquidity_indicator, executor_id
                    FROM trades t, orders o
                    WHERE o.version = 1 and o.version = t.version and o.order_id = t.order_id
                    and (o.order_id = ?)
                    UNION ALL
                    SELECT t.order_id, t.dealt_price, t.quantity, t.execution_venue, t.liquidity_indicator
                    FROM trades t, orders o
                    WHERE o.version = 1 and o.version = t.version and o.order_id = t.order_id
                    and (o.parent_order_id = ?)
                '''
        rows = self._backend.select(query, order_id, order_id)

        trades = []
        for row in rows:
            trades.append(FidessaTrade(row))

        return trades

    def get_cost(self, order_id, date):
        '''
            Compute the cost of the order

            Negative values mean that the exchange gave us some money
        '''
        trades = self.get_trades(order_id)

        return rebate.get_rebate_for_trade(trades, date)


def get_fidessa_db(mode = 'ODBC', sqlite_db = None, date = None):
    if mode == 'SQLite':
        return FidessaDB_SQLite(sqlite_db, date)
    else:
        from cheuvreux.fidessa.fidessadb_odbc import FidessaDB_ODBC
        return FidessaDB_ODBC()
