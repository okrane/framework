'''
Created on 23 mars 2010

@author: syarc
'''
from cheuvreux.dbtools.odbc import ODBC
from cheuvreux.dbtools.temptable import TempTable
from collections import defaultdict
import time
from cheuvreux.fidessa import rebate


def getODBCConnection(db=None, autocommit=True):
    if db:
        return ODBC('DRIVER={SQL Server};SERVER=nysql001;DATABASE=%s;UID=syarc;PWD=syarc' % db, autocommit = autocommit)
    else:
        return ODBC('DRIVER={SQL Server};SERVER=nysql001;DATABASE=quant;UID=syarc;PWD=syarc', autocommit = autocommit)

class FidessaDB:
    '''
        This class offers methods to access the Fidessa database
        and help PTA computation
    '''

    # Constant for Fidessa Tables
    ORDER_TABLE = '[High Touch Orders Cumulative]'
    TRADE_TABLE = '[High Touch Trade Summary Cumulative]'
    ENTERED_BY_FILTER = 'DMA@CRAG.US'

    def __init__(self, backend='ODBC'):
        '''
        Constructor
        '''
        self._backend = ODBC('DRIVER={SQL Server};SERVER=nysql001;DATABASE=Fidessa;UID=syarc;PWD=syarc')
        #self._createTempTable()

    @property
    def backend(self):
        return self._backend

    @staticmethod
    def parseAlgoName(algo):
        if algo is None:
            return algo

        idx = algo.find('_')
        if idx > 0:
            tmp = algo.split('_')
            if tmp[0] in ('BETA', 'TEST'):
                algo = tmp[0] + "_" + tmp[1]
            else:
                algo = tmp[0]
        return algo

    def _createTempTable(self):
        self._temp_work = TempTable(self.backend, '#tmp_work_orders',
            '''     tradedate datetime, order_id varchar(255), parent_order_id varchar(255),
                    root_order_id varchar(255), counterparty_code varchar(255),
                    instrument_code varchar(255), entered_datetime varchar(50),
                    algo varchar(255), startTime varchar(255), endTime varchar(255),
                    volume_done int, volume_left int, order_note_id varchar(255),
                    buy_sell varchar(255), gross_price float ''')
        self._temp_work.addIndex('ix1', 'order_id', clustered=True)
        self._temp_work.addIndex('ix2', 'parent_order_id')
        self._temp_work.addIndex('ix3', 'root_order_id')

        self._temp_orders = TempTable(self._backend, '#tmp_orders',
                                      self._temp_work.fields + ', nb_child int')
        self._temp_orders.addIndex('ix3', 'order_id', clustered=True)
        self._temp_orders.addIndex('ix4', 'parent_order_id')
        self._temp_orders.addIndex('ix5', 'root_order_id')


        self._temp_trades = TempTable(self.backend, "#tmp_work",
                                      ''' parent_order_id varchar(255), order_id varchar(255),
                                          quantity int, liquidity_indicator char(3), execution_venue varchar(255)
                                      ''')
        self._temp_trades.addIndex('ix6', 'parent_order_id', clustered=True)
        self._temp_trades.addIndex('ix7', 'order_id')

        self._temp_childs = TempTable(self.backend, '#tmp_childs', self._temp_trades.fields)
#        self._temp_analytics = TempTable(self._backend, '#tmp_analytics',
#                                         'order_id varchar(255),quantity int,passive_quantity int')

    def has_order_been_amended(self, order_id):

        query = '''SELECT distinct CASE limit_price
                            WHEN 0 THEN trading_quantity
                            ELSE trading_quantity * limit_price
                            END
                    FROM [High Touch Orders Cumulative]
                    WHERE order_id = ?
                '''

        row = self._backend.select(query, order_id);

        return len(row) > 1

    def getFidessaTrade(self, order_id, trade_id):

        query = ''' SELECT timestamp, buy_sell, gross_price, quantity, bid_price, offer_price
                     FROM [High Touch Trade Summary Cumulative]
                     WHERE order_id = ? AND trade_id = ? '''
        rows = self._backend.select(query, order_id, trade_id)
        if rows and len(rows) > 0:
            return rows[0]
        return None

    def getFidessaOrder(self, order_id, computeChild=False, version=1, date=None):
        ''' Returns a Fidessa Order object based on a 'order_id'

            @param order_id: Order id
            @param computeChild: True to retrieve information about child order
            @param version: Version number you want to retrieve (accept "last"
                            for last version.
            @param date: Limit search for the specify date
        '''

        if version == 'last':
            row = self._backend.selectOne("SELECT max(version) from [High Touch Orders Cumulative]" +
                                          "where order_id = ?",
                                          order_id)
            if row is not None and len(row) > 0:
                version = row[0]

        if computeChild:
            query = ''' select order_id, counterparty_code, instrument_code, entered_datetime,
                        cust_xp_data_1 as algo, cust_xp_data_2 as startTime, cust_xp_data_3 as endTime,
                        volume_done, volume_left, order_note_id, buy_sell, gross_price, limit_price,
                        parent_order_id, order_price_type_qualifier, market_id, count(child.order_id) as nb_child
                        from %s o
                        join [High Touch Orders Cumulative] child on child.tradedate = o.tradedate
                                               and child.parent_order_id = o.order_id
                                               and child.version = 1
                        where order_id = ?
                          and version = ?
                    ''' % FidessaDB.ORDER_TABLE
        else:
            query = ''' select order_id, counterparty_code, instrument_code, entered_datetime,
                        cust_xp_data_1 as algo, cust_xp_data_2 as startTime, cust_xp_data_3 as endTime,
                        volume_done, volume_left, order_note_id, buy_sell, gross_price, limit_price,
                        parent_order_id, order_price_type_qualifier, market_id, 0 as nb_child
                        from %s o
                        where order_id = ?
                          and version = ?
                    ''' % FidessaDB.ORDER_TABLE
        if date is not None:
            query += " AND tradedate = '%s' " % date

        rows = self._backend.select(query, order_id, version)
        if len(rows) > 0:
            return FidessaOrder(rows[0])
        else:
            return None

    get_fidessa_order = getFidessaOrder

    def get_cost(self, order_id, date):
        '''
            Compute the cost of the order

            Negative values mean that the exchange gave us some money
        '''
        trades = self.get_trades(order_id)

    def getFidessaOrders(self, orderIds, date=None):
        query = ''' select order_id, counterparty_code, instrument_code, entered_datetime,
                        cust_xp_data_1 as algo, cust_xp_data_2 as startTime, cust_xp_data_3 as endTime,
                        volume_done, volume_left, order_note_id, buy_sell, gross_price, limit_price,
                        parent_order_id, 0 as nb_child
                        from %s o
                        where order_id in ('%s')
                          and version = 1
                    ''' % (FidessaDB.ORDER_TABLE, "','".join(orderIds))
        if date is not None:
            query += " AND tradedate = '%s' " % date

        rows = self._backend.select(query)

        orders = {}
        for row in rows:
            orders[rows[0][0]] = FidessaOrder[row[0]]

    def getDistinctClient(self, date):
        query = ''' select distinct (counterparty_code) as client from %s
                    where tradedate = ? and entered_by <> '%s'
                ''' % (FidessaDB.ORDER_TABLE, FidessaDB.ENTERED_BY_FILTER)

        return self._backend.select(query, date)

    def _clean(self):
        ''' Clear all temporary tables. '''
        self._temp_orders.reset()
        self._temp_childs.reset()
        self._temp_work.reset()
        self._temp_trades.reset()

    def _prepareWorkTable(self, client, date):
        ''' Fill the temporary work table '''

        t0 = time.clock()
        query = ''' insert %s
                    select tradedate, order_id, parent_order_id, root_order_id, counterparty_code, instrument_code,
                    entered_datetime, cust_xp_data_1 as algo, cust_xp_data_2 as startTime, cust_xp_data_3 as endTime,
                    volume_done, volume_left, order_note_id, buy_sell, gross_price
                    from %s o
                    where counterparty_code = ? and tradedate = ? and version = 1
                    and entered_by <> '%s'
                ''' % (self._temp_work.name, FidessaDB.ORDER_TABLE, FidessaDB.ENTERED_BY_FILTER)

        self.backend.run(query, client, date)#, " rows inserted in ", time.clock() - t0, " sec! [1]"

        t0 = time.clock()
        query = ''' insert %s
                    select parent_order_id, order_id, quantity,
                    liquidity_indicator, execution_venue
                    from %s t
                    where real_counterparty_code = ? and version = 1 and tradedate = ?
                    and entered_by <> '%s'
                ''' % (self._temp_trades.name, FidessaDB.TRADE_TABLE, FidessaDB.ENTERED_BY_FILTER)

        self.backend.run(query, client, date)#, " rows inserted in ", time.clock() - t0, " sec! [2]"

    def getMainOrdersByClientAndDate(self, client, date, note_id=None):
        '''
            Returns a list of orders (FidessaOrder) done by a client
            on a specified date.

            @param client Client code
            @param date Date (format should be YYYYMMDD)
            @param temp If True, stores data into a temporary table, you need
                that
            @param note_id filter on order_note_id
        '''

        self._clean()
        self._prepareWorkTable(client, date)

        # Base query - It write the data in to a temp table
        query = ''' insert #tmp_orders
                    select o.tradedate, o.order_id, o.parent_order_id, o.root_order_id,
                        o.counterparty_code, o.instrument_code, o.entered_datetime,
                        o.algo, o.startTime, o.endTime, o.volume_done, o.volume_left,
                        o.order_note_id, o.buy_sell, o.gross_price, count(child.order_id) as nb_child
                    from %s o
                    left join %s child on child.tradedate = o.tradedate
                                and child.parent_order_id = o.order_id
                    where o.counterparty_code = ?
                       and o.tradedate = ?
                       and o.parent_order_id is null
                ''' % (self._temp_work.name, self._temp_work.name)
        # Add filter if any
        if note_id is not None:
            query += ' and order_note_id = ?'

        # group by final clause
        query += ''' group by o.tradedate, o.parent_order_id, o.order_id, o.counterparty_code,
                     o.instrument_code, o.entered_datetime, o.algo, o.startTime, o.endTime,
                     o.volume_done, o.volume_left, o.order_note_id, o.buy_sell,
                     o.gross_price, o.root_order_id
                 '''

        if note_id is not None:
            rows = self.backend.run(query, client, date, note_id)
        else:
            rows = self.backend.run(query, client, date)

        rows = self.backend.select('select * from #tmp_orders')

        orders = dict()
        for row in rows:
            orders[row.order_id] = FidessaOrder(row)
        return orders


    def computeChilds(self):
        '''
            This method save all child order of order found in
            #temp_orders into the temporary table #tmp_childs.

            You can then call methods to compute some statistics
        '''

        self._temp_childs.reset()

        # first we compute child for alone order (i.e no children
        query = ''' insert #tmp_childs
                    select t.order_id, t.order_id, t.quantity, t.liquidity_indicator, t.execution_venue
                    from %s t
                    join %s tmp on tmp.order_id = t.order_id
                ''' % (self._temp_trades.name, self._temp_orders.name)
        self.backend.run(query)

        # then Direct child order
        query = ''' insert %s
                    select t.parent_order_id, t.order_id, t.quantity, t.liquidity_indicator, t.execution_venue
                    from %s t
                    join %s tmp on tmp.order_id = t.parent_order_id
                ''' % (self._temp_childs.name, self._temp_trades.name, self._temp_orders.name)
        self.backend.run(query)

        # Finally second level child
        query = ''' insert %s
                    select o.root_order_id, t.order_id, t.quantity, t.liquidity_indicator, t.execution_venue
                    from %s tmp
                    join %s o on o.root_order_id = tmp.order_id and tmp.order_id <> o.parent_order_id
                             and tmp.tradedate = o.tradedate
                             and tmp.counterparty_code = o.counterparty_code
                    join %s t on o.order_id = t.order_id
                ''' % (self._temp_childs.name, self._temp_orders.name ,
                       self._temp_work.name, self._temp_trades.name)
        self.backend.run(query)

    def getPassiveFillRate(self, orders):
        query = ''' select parent_order_id, sum(quantity) as quantity from %s tmp
                            join KYCDatabase..[RebateChargeRate Table] rebate
                                on rebate.liquidity = tmp.liquidity_indicator
                                and rebate.exchange = tmp.execution_venue
                                and rebate.EndDate is NULL and rebate.rate < 0
                            group by parent_order_id
                ''' % self._temp_childs.name
        rows = self.backend.select(query)
        for row in rows:
            order = orders[row.parent_order_id]
            orders[row.parent_order_id].analytics['PassiveFillRate'] = float(row.quantity) / order.quantity

    def getSingleOrderPassiveFillRate(self, order):
        '''
            This method compute the passive fill rate for a single order
            (i.e does not have child). If not sure, please use
            <code> getOrderPassiveFillRate </code> , it will call the appropriate
            method

            @param order: FidessaOrder object
        '''
        query = ''' select sum(quantity) as quantity from % s trade
                join KYCDatabase..[RebateChargeRate Table] rebate
                            on rebate.liquidity = trade.liquidity_indicator
                            and rebate.exchange = trade.execution_venue
                where tradedate = ? and order_id = ? and version = 1
                ''' % FidessaDB.TRADE_TABLE
        ans = self._backend.select(query, order.date, order.order_id)[0]

        if ans[0] is not None:
            return float(ans.quantity) / order.done
        return 0.0

    def getUniverse(self, start, end, client):
        ''' Returns a set of stocks traded by a client on a specified date range

            @param start: Start range (format YYYMMDD, or anything accept by SQL Server)
            @param end: Start range (format YYYMMDD, or anything accept by SQL Server)
            @param client: Counterparty code
        '''

        rows = self._backend.select(""" select distinct instrument_code from [High Touch Orders Cumulative]
                        where COUNTERPARTY_CODE = ?
                        and tradedate between ? and ?
                        and version = 1
                        and parent_order_id is null
                       """, client, start, end)


        ans = set()
        for row in rows:
            ans.add(row.instrument_code)

        return ans

    def getPassiveFillRateForClient(self, date, client):
        ''' Returns the passive fill rate for all execution of a client
            on a given day

            @param date: Date, format YYYYMMDD (or anything accept by SQL Server)
            @param client: Counterparty code
        '''

        # Get whole quantity for the day.
        row = self._backend.select(""" select sum(quantity) as quantity from [High Touch Trade Summary Cumulative] trade
                            where trade.TradeDate = ?
                                and REAL_COUNTERPARTY_CODE = ? and version = 1
                                """
                                , date, client)[0]
        qty = row.quantity

        row = self._backend.select(""" select sum(quantity) as quantity from [High Touch Trade Summary Cumulative] trade
                        join KYCDatabase..[RebateChargeRate Table] rebate
                            on rebate.liquidity = trade.liquidity_indicator and rebate.exchange = trade.execution_venue
                        where trade.TradeDate = ? and REAL_COUNTERPARTY_CODE = ?
                          and rebate.EndDate is NULL and rebate.rate < 0
                          and version = 1
                        """, date, client)[0]

        passive = row.quantity

        try:
            return (passive * 1.0 / qty, qty)
        except TypeError:
            return (0.0, 0)

    def getBlueboxOrderIds (self, date, strategy):
        '''
            Returns the bluebox order ids for a given date and strategy
        '''
        rows = self._backend.select(''' select order_id from BLUEBOX_ORDER_DETAILS
                                       where order_date = ?
                                         and strategy in ('%s')
                                    ''' % "','".join(strategy), date)
        return set([row.order_id for row in rows])

    def getAlgoName (self, order_id):
        '''
            Returns the algo name for an order id
        '''

        row = self._backend.selectOne(''' SELECT cust_xp_data_1
                                        FROM [High Touch Orders Cumulative]
                                        WHERE order_id = ? and version = 1
                                   ''', order_id)

        # Try to look in BLUEBOX table
        if row is None or row[0] is None:
            row = self._backend.selectOne(''' SELECT strategy FROM BLUEBOX_ORDER_DETAILS
                                            WHERE order_ID = ? ''', order_id)


        if row is not None and len(row) > 0:
            return FidessaDB.parseAlgoName(row[0])
        else:
            return None

    def getMarketId(self, order_id):
        ''' Returns the market id for an order id '''

        row = self._backend.selectOne(''' SELECT market_id FROM %s
                                          WHERE order_id = ? and version = 1
                                      ''' % FidessaDB.ORDER_TABLE, order_id)

        if row:
            return row[0]

        return None

    def getPrimaryMarket(self, stock, date = None):
        ''' Returns the market-id (or primary market) for a stock '''

        query = ''' SELECT top 1 market_id FROM %s
                    WHERE instrument_code = ? ORDER BY tradedate DESC
                ''' % FidessaDB.ORDER_TABLE

        if date:
            query = ''' SELECT top 1 market_id FROM %s
                        WHERE instrument_code = ? AND tradedate='%s'
                        ORDER BY tradedate DESC
                    ''' % (FidessaDB.ORDER_TABLE, date)

        row = self._backend.selectOne(query, stock)

        if row:
            return row[0]

        return None

    def getRootOrderId(self, order_id):

        query = 'SELECT root_order_id FROM %s WHERE version = 1 and order_id = ?' % FidessaDB.ORDER_TABLE

        row = self._backend.selectOne(query, order_id)
        if row:
            return row[0]

        return None

class FidessaTrade(object):
    def __init__(self, row):
        self.entered = row.timestamp

class FidessaOrder(object):
    '''
        Fidessa order object
    '''

    def __init__(self, row):
        ''' Constructor
        '''
        (self.date, self.entered, dummy) = row.entered_datetime.split(' ')
        (self.order_id, self.ctpy, self.code) = (row.order_id, row.counterparty_code, row.instrument_code)
        self.algo = row.algo

        if row.startTime is not None:
            self.start = row.startTime
        else:
            self.start = self.entered

        if row.endTime is not None:
            self.end = row.endTime
        else:
            self.end = '16:00:00'

        self.done = int(row.volume_done)
        self.left = int(row.volume_left)
        self.nbChild = row.nb_child
        self.note_id = row.order_note_id
        self.buy_sell = row.buy_sell
        self.gross_price = float(row.gross_price)
        self.analytics = defaultdict(float)
        self.parent_order_id = row.parent_order_id
        self.order_price_type = row.order_price_type_qualifier
        self.market_id = row.market_id

        if row.limit_price is None or row.limit_price == 0:
            self.limit_price = None
        else:
            self.limit_price = row.limit_price

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

    @property
    def isMarketOrder(self):
        return self.limit_price is None

    @property
    def isOpenOrder(self):
        return self.order_price_type == 'OPEN'

    @property
    def isCloseOrder(self):
        return self.order_price_type == 'CLSE'




