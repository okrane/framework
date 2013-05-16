'''
Created on Oct 7, 2010

@author: syarc
'''

from cheuvreux.dbtools.odbc import ODBC
from cheuvreux.fidessa.fidessa import FidessaDB, FidessaOrder


class FidessaDB_ODBC(FidessaDB):
    '''
        This class offers methods to access the Fidessa database
        and help PTA computation
    '''

    # Constant for Fidessa Tables
    ORDER_TABLE = '[High Touch Orders Cumulative]'
    TRADE_TABLE = '[High Touch Trade Summary Cumulative]'
    ENTERED_BY_FILTER = 'DMA@CRAG.US'

    def __init__(self):
        '''
        Constructor
        '''
        self._backend = ODBC('DRIVER={SQL Server};SERVER=nysql001;DATABASE=Fidessa;UID=syarc;PWD=toto')
        self._createTempTable()

    def get_fidessa_order(self, order_id, version=1, date=None):
        ''' Returns a Fidessa Order object based on a 'order_id'

            @param order_id: Order id
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


        query = ''' select order_id, counterparty_code, instrument_code, entered_datetime,
                    cust_xp_data_1 as algo, cust_xp_data_2 as startTime, cust_xp_data_3 as endTime,
                    volume_done, volume_left, order_note_id, buy_sell, gross_price, limit_price,
                    parent_order_id, order_price_type_qualifier, market_id
                    from %s o
                    where order_id = ?
                      and version = ?
                ''' % FidessaDB_ODBC.ORDER_TABLE
        if date is not None:
            query += " AND tradedate = '%s' " % date

        rows = self._backend.select(query, order_id, version)
        if len(rows) > 0:
            return FidessaOrder(rows[0])
        else:
            return None
