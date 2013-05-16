'''
Created on 20 avr. 2010

@author: syarc
'''
from cheuvreux.dbtools import is_pyodbc_available
from cheuvreux.dbtools.connections import Connections
from cheuvreux.dbtools.repository import Repository
from cheuvreux.utils.date import go_back
from datetime import datetime
import datetime
import math
#from dev.usr.sivla.funcs.data.pyData import convertStr, pyDataIntraday, pyData

class TickDB(object):

    cursor = Connections.getCursor('BSIRIUS')
    cursor.arraysize = 1024

    if is_pyodbc_available:
        # ODBC is faster on WinXp client than python-sybase module
        from cheuvreux.dbtools.odbc import ODBC
        try:
            backend = ODBC('DRIVER={Adaptive Server Enterprise};SERVER=SIRIUS;PORT=9250;BASE=repository;UID=batch_tick;PWD=batick;UseCursor=1')
        except:
            backend = None
            is_pyodbc_available = False

    @staticmethod
    def deal_schema(date):
        ''' Returns deal database schema from a date '''
        month = date[4:6]
        year = date[0:4]
        return 'tick_db_%sd_%s' % (month, year)

    @staticmethod
    def order_schema(date):
        ''' Returns order database schema from a date '''
        month = date[4:6]
        year = date[0:4]
        return 'tick_db_%so_%s' % (month, year)

    @staticmethod
    def trade_list(security, date, start=None, end=None):
        '''
            full trade list
        '''
        sec_id = security.security_id
        if security.trading_destination_id is None:
            td_id = 'null'
        else:
            td_id = str(security.trading_destination_id)


        if end is None:
            end = '23:59:59'
        if start is None:
            start = '00:00:00'

        query = ''' select time, microseconds, price, size, bid, bid_size, ask, ask_size
                    from %s..deal_archive%s
                    where date = '%s' and security_id = %s
                    and trading_destination_id = %s
                    and time >= '%s' and time <= '%s'
                ''' % (TickDB.deal_schema(date), security.suffix, date, sec_id,
                       td_id, start, end)
        Connections.change_connections('production')
        result = Connections.exec_sql('QUANT', query)

        if result:
        #convert to pyData for structure:
        #transpose result:
            columns = []
            for idx in range(2, len(result[0])):
                columns.append([x[idx] for x in result])

            #dt = []
            #for x in result:
            #    dt.append(datetime(convertStr(date[0:4]), convertStr(date[4:6]), convertStr(date[6:8]), x[0].hour, x[0].minute, x[0].second, x[1]))

            #return pyDataIntraday('init', date=dt, value=columns,
            #          colnames=['price', 'volume', 'bid', 'bid_size', 'ask', 'ask_size'])
        #return pyData('init', date=[], value={})
    @staticmethod
    def current_volume(security_id, suffix):
        ''' Returns 'real-time' volume for a security id
            by trading destination
        '''
        query = ''' select security_id, trading_destination_id, volume
                    from tick_db..deal_day%s
                    where security_id = @security_id
                    group by trading_destination_id
                    having deal_id = max(deal_id)
                ''' % (suffix)

        TickDB.cursor.execute(query, {'@security_id':security_id})

        volume = TickDB.cursor.fetchall();
        return volume

    @staticmethod
    def daily_volume(security, start, end):
        sec_id = security.security_id
        if security.trading_destination_id is None:
            td_id = 'null'
        else:
            td_id = str(security.trading_destination_id)

        query = ''' select sum(volume) from tick_db..trading_daily%s
                        where date between '%s' and '%s'
                        and security_id = %s and trading_destination_id = %s
                    ''' % (security.suffix, start, end, sec_id, td_id)

        TickDB.cursor.execute(query)
        volume = TickDB.cursor.fetchone()
        if volume:
            return volume[0]
        return None

    @staticmethod
    def volume(security, date, start=None, end=None, all_trades=False):
        ''' Returns the volume for a (security_id, trading_destination_id
            on a given date (and, if specified, a time range).
        '''

        sec_id = security.security_id
        if security.trading_destination_id is None:
            td_id = 'null'
        else:
            td_id = str(security.trading_destination_id)

        if start is None and end is None:
            query = ''' select volume from tick_db..trading_daily%s
                        where date = '%s' and security_id = %s
                    ''' % (security.suffix, date, sec_id)
            if not all_trades:
                query += ' and trading_destination_id = %s' % td_id

        else: # end or start is not none
            if end is None:
                end = '23:59:59'
            if start is None:
                start = '00:00:00'

            query = ''' select sum(size) from %s..deal_archive%s
                        where date = '%s' and security_id = %s
                        and time >= '%s' and time <= '%s'
                    ''' % (TickDB.deal_schema(date), security.suffix, date, sec_id,
                           start, end)

            if not all_trades:
                query += ' and trading_destination_id = %s' % td_id

        TickDB.cursor.execute(query)
        volume = TickDB.cursor.fetchone()
        if volume:
            return volume[0]
        return None

    @staticmethod
    def ohlc(security, date):
        ''' Returns open, high, low, close for a stock on a day '''
        if security.trading_destination_id is None:
            td_id = 'null'
        else:
            td_id = str(security.trading_destination_id)

        query = ''' select open_prc, high_prc, low_prc, close_prc from  tick_db..trading_daily%s
                    where date = '%s' and security_id = %s and trading_destination_id = %s
                ''' % (security.suffix, date, security.security_id, td_id)

        TickDB.cursor.execute(query)
        result = TickDB.cursor.fetchone()
        return result

    def adv(security, date, window=20, trading_destination_id = None):
        ''' Returns the ADV for a stock

            @param security Security ID
            @param date End date
            @param window Number of date to use.
        '''

        if not trading_destination_id:
            td_id = 'null'
        else:
            td_id = str(trading_destination_id)

        start_date = go_back(datetime.datetime.strptime(date, '%Y%m%d'), window)

        if type(security) is type(0):
            sec_id = security
        else:
            sec_id = security.security_id

        query = ''' SELECT avg(volume) FROM tick_db..trading_daily_ameri
                    WHERE date >= '%s' and date <= '%s'
                      AND trading_destination_id = %s
                      AND security_id = %d
                ''' % (start_date, date, td_id, sec_id)

        TickDB.cursor.execute(query)
        ans = TickDB.cursor.fetchone()
        if not ans:
            return None
        return ans[0]

    adv = staticmethod(adv)

    @staticmethod
    def getHistoricClose5min(securities, start, end, td_id):

        if hasattr(securities, '__iter__'):
            # Object is iterable
            tmp = ','.join(map(str, set(securities)))
        else:
            tmp = '%s' % (securities)

        query = ''' SELECT security_id, date, begin_time, (high + low) / 2 as close_prc
                    FROM tick_db..trading_intraday_volume_ameri
                    WHERE date BETWEEN '%s' and '%s'
                      AND security_id in (%s) AND phase = 'C'
                      AND trading_destination_id = %d
                ''' % (start, end, tmp, td_id)
        TickDB.cursor.execute(query)
        result = TickDB.cursor.fetchall()
        return result

    @staticmethod
    def average_spread(security, start, end=None):
        ''' Compute average spread on a range

            If end is None, it is set to start.
        '''

        if security.trading_destination_id is None:
            td_id = 'null'
        else:
            td_id = str(security.trading_destination_id)

        if end is None:
            end = start

        query = ''' select sum(average_spread_numer)/sum(average_spread_denom) as avg_spread from tick_db..trading_daily%s
                    where security_id = %s and trading_destination_id = %s
                    and date between '%s' and '%s'
                ''' % (security.suffix, security.security_id, td_id, start, end)
        TickDB.cursor.execute(query)
        spread = TickDB.cursor.fetchone()
        if spread:
            return spread[0]
        return None

    def average_deal_price(security, date, start, end):
        query = ''' select avg(price)
                    from %s..deal_archive_ameri
                    where security_id = %s and date = '%s' and time between '%s' and '%s'
                ''' % (TickDB.deal_schema(date), security, date, start, end)

        TickDB.cursor.execute(query)
        price = TickDB.cursor.fetchone()
        if price:
            return price[0]
        return None

    average_deal_price = staticmethod(average_deal_price)

    def pwp(security, date, start, order_size, participation):
        ''' Compute the Participation Weighted Price of an order.

            For example, it the participation rate is 10% and order_size is
            5,000, the PWP for this order is the VWAP computed over 50,000
            shares traded in the market after the order start time.

            @param security Security object
            @param date Order date
            @param start Order start time
            @param order_size Order size
            @param participation Participation benchmark (in %)
        '''

        schema = TickDB.deal_schema(date)

        # 1. Get the market volume at algo start time.
        query = ''' SELECT TOP 1 deal_id, volume FROM %s..deal_archive%s
                    WHERE security_id = %d AND date = '%s' AND time >= '%s'
                    ORDER BY date, time, microseconds
                ''' % (schema, security.suffix, security.security_id, date, start)

        TickDB.cursor.execute(query)
        ans = TickDB.cursor.fetchone()
        if not ans:
            return None # Something went wrong
        first_deal, start_volume = ans[0], ans[1]

        end_volume = start_volume + order_size / participation


        # 2. Get the end time (look for the first trade where market volume is greater than
        # our target)
        query = ''' SELECT TOP 1 deal_id FROM %s..deal_archive%s
                    WHERE security_id = %d AND date = '%s' AND time > '%s'
                      AND volume > %d
                    ORDER BY volume ASC
                ''' % (schema, security.suffix, security.security_id, date, start, end_volume)

        TickDB.cursor.execute(query)
        ans = TickDB.cursor.fetchone()
        if not ans:
            # Something went wrong - or the volume was not done during the time frame
            # can't compute the PWP.
            return None

        last_deal = ans[0]

        # 3. Compute the VWAP over the period
        query = ''' SELECT sum(size * price) / sum(size) FROM %s..deal_archive%s
                    WHERE security_id = %d AND date = '%s'
                      AND deal_id >= %.0f AND deal_id < %.0f
                ''' % (schema, security.suffix, security.security_id, date, first_deal, last_deal)

        TickDB.cursor.execute(query)
        ans = TickDB.cursor.fetchone()
        if not ans:
            return None # Something went wrong

        return ans[0]

    pwp = staticmethod(pwp)

    def day_vwap(security_id, start, end,limit=None, side=None):
        '''
            Returns the VWAP for the security over the period

            @param security Security object
            @param date Date
            @param start Start time
            @param end End time
        '''

        query = ''' SELECT sum(size * price) / sum(size) FROM tick_db..deal_day_ameri
                    WHERE security_id = %d
                      AND time BETWEEN '%s' and '%s'
                ''' % (security_id, start, end)

        if limit and side:
            if abs(side) != 1:
                raise ValueError ('Invalid side value - should be 1 or -1')
            query += ' AND %d*price <= %d*%f ' % (side,side,limit)
        TickDB.cursor.execute(query)
        ans = TickDB.cursor.fetchone()
        if not ans:
            return None # Something went wrong
        return ans[0]
    day_vwap = staticmethod(day_vwap)

    def vwap(security, date, start, end,limit=None, side=None):
        '''
            Returns the VWAP for the security over the period

            @param security Security object
            @param date Date
            @param start Start time
            @param end End time
        '''



        query = ''' SELECT sum(size * price) / sum(size) FROM %s..deal_archive%s
                    WHERE security_id = %d AND date = '%s'
                      AND time BETWEEN '%s' and '%s'
                ''' % (TickDB.deal_schema(date), security.suffix, security.security_id, date, start, end)

        if limit and side:
            if abs(side) != 1:
                raise ValueError ('Invalid side value - should be 1 or -1')
            query += ' AND %d*price <= %d*%f ' % (side,side,limit)
        TickDB.cursor.execute(query)
        ans = TickDB.cursor.fetchone()
        if not ans:
            return None # Something went wrong
        return ans[0]
    vwap = staticmethod(vwap)

    @staticmethod
    def spread_at_time(security, date, time):
        '''
            Returns bid / ask at a specific time
        '''

        sec_id = security.security_id
        td_id = None
        if security.trading_destination_id:
            td_id = str(security.trading_destination_id)

        query = ''' select top 1 bid, ask from %s..deal_archive%s
                    where date = '%s' and time >= '%s'
                      and security_id = %d
                ''' % (TickDB.deal_schema(date), security.suffix, date, time,
                           sec_id)
        if td_id:
            query += ''' and trading_destination_id = %s ''' % td_id

        query += 'order by date, time'


        #Connections.change_connections('production')
        result = Connections.exec_sql('QUANT', query)
        if result:
            return result[0]

        else:
            query = ''' select top 1 bid, ask from %s..deal_archive%s
                         where date = '%s' and time >= '%s' and security_id = %d
                   ''' % (TickDB.deal_schema(date), security.suffix, date, time, sec_id)
            query += 'order by date, time'

            result = Connections.exec_sql('QUANT', query)
            if result:
                return result[0]
            else:
                return (None, None)

    @staticmethod
    def get_first_deal(security, date, time):
        ''' Returns the first deal occured after the specific time '''

        query = ''' select top 1 price, size from %s..deal_archive%s
                     where date = '%s' and time >= '%s'
                       and security_id = %d
                    order by date, time, microseconds
                ''' % (TickDB.deal_schema(date), security.suffix, date, time, security.security_id)

        return TickDB.backend.select(query)

    @staticmethod
    def getIndiceCloses(indices, startdate, enddate=None):
        if not enddate:
            enddate = startdate

        if hasattr(indices, '__iter__'):
            # Object is iterable
            tmp = ','.join(map(str, set(indices)))
            suffix = Repository.table_suffix_indice_id(int(indices.__iter__().next()))
        else:
            tmp = '%s' % (indices)
            suffix = Repository.table_suffix_indice_id(int(indices))

        query = '''SELECT indice_id, date, close_prc
                   FROM tick_db..indice_daily%s
                   WHERE date between '%s' and '%s'
                   AND indice_id in (%s)
                   order by indice_id, date
                ''' % (suffix, startdate, enddate, tmp)
        if is_pyodbc_available:
            return TickDB.backend.select(query)
        else:
            TickDB.cursor.execute(query)
            return TickDB.cursor.fetchall()

    @staticmethod
    def getCloses(securities, startdate, enddate=None):
        if not enddate:
            enddate = startdate

        if hasattr(securities, '__iter__'):
            # Object is iterable
            tmp = ','.join(map(str, set(securities)))
            suffix = Repository.table_suffix_sec_id(int(securities.__iter__().next()))
        else:
            tmp = '%s' % (securities)
            suffix = Repository.table_suffix_sec_id(int(securities))

        query = '''SELECT security_id, date, close_prc
                   FROM tick_db..trading_daily%s
                   WHERE date between '%s' and '%s'
                   AND trading_destination_id is null
                   AND security_id in (%s)
                   order by security_id, date
                ''' % (suffix, startdate, enddate, tmp)
        if is_pyodbc_available:
            return TickDB.backend.select(query)
        else:
            TickDB.cursor.execute(query)
            return TickDB.cursor.fetchall()

    @staticmethod
    def get_close_volumes (security, start, end):

        query = ''' SELECT date, close_volume FROM tick_db..trading_daily%s
                    WHERE date BETWEEN '%s' and '%s'
                     AND security_id = %d AND trading_destination_id = %d
                ''' % (security.suffix, start, end,
                       security.security_id, security.trading_destination_id)

        TickDB.cursor.execute(query)
        return TickDB.cursor.fetchall()

    @staticmethod
    def getClose(securities, date, td_id):
        query = '''SELECT security_id, close_prc
                   FROM tick_db..trading_daily_ameri
                   WHERE date = '%s' AND trading_destination_id = %d
                   AND security_id in (%s)
                ''' % (date, td_id, ','.join(securities))
        TickDB.cursor.execute(query)
        result = TickDB.cursor.fetchall()
        d = dict()
        for row in result:
            d[row[0]] = row[1]
        return d

if __name__ == '__main__':
    from cheuvreux.dbtools.security import Security
#    (sec_id, td_id) = Repository.us_security_id('EURX', True)
#    print sec_id, td_id
#    sec = Security(sec_id, td_id,)
#    print TickDB.spread_at_time(sec, '20100715', '12:00:00')
#
#    (sec_id, td_id) = Repository.us_security_id('A', True)
#    print sec_id, td_id
#    print TickDB.getClose([str(sec_id)], '20100210', td_id)
#    print TickDB.getClose([str(sec_id)], '20100215', td_id)
#    print TickDB.vwap(Security(159175, None), '20100915', '14:43:27', '14:43:30', 80,-1)
#    print TickDB.vwap(Security(159175, None), '20100915', '14:43:27', '14:43:30')
    print TickDB.pwp(Security(374225, None), '20101001', '11:48:50', 2000, 0.15)
#    print TickDB.pwp(Security(160061, None), '20100812', '09:16:05', 2500, 0.15)
    #print TickDB.adv(285381, '20100813')



    #sec = Security()

#    ids = Repository.index_components(6)
#
#    start = time.clock()
#    for id in ids:
#        print TickDB.current_volume(id, '_ameri')
#    print time.clock() - start



#
#    sec = Security (11679, None)
#    print TickDB.volume(sec, '20100419')
#    sec = Security (11679, 25)
#
#    print sec.market_cap('20100426')
#
#    print TickDB.volume(sec, '20100419')
#    print TickDB.volume(sec, '20100419', '12:00:00', '13:00:00')
#    print TickDB.ohlc(sec, '20100419')
#    print TickDB.average_spread(sec, '20100419')
