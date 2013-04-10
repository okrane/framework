'''
Created on 20 avr. 2010

@author: syarc
'''
from simep.funcs.dbtools.connections import Connections
from usr.dev.sivla.funcs.data.pyData import convertStr, pyDataIntraday, pyData
from datetime import datetime
from simep.funcs.dbtools.odbc import ODBC

class TickDB(object):

    cursor = Connections.getCursor('BSIRIUS')
    backend = ODBC('DRIVER={Adaptive Server Enterprise};SERVER=SIRIUS;PORT=9250;BASE=repository;UID=batch_tick;PWD=batick;UseCursor=1')

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

            dt = []
            for x in result:
                dt.append(datetime(convertStr(date[0:4]), convertStr(date[4:6]), convertStr(date[6:8]), x[0].hour, x[0].minute, x[0].second, x[1]))

            return pyDataIntraday('init', date=dt, value=columns,
                      colnames=['price', 'volume', 'bid', 'bid_size', 'ask', 'ask_size'])
        return pyData('init', date=[], value={})
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
        return volume[0] if volume is not None else None

    @staticmethod
    def volume(security, date, start=None, end=None):
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
                        and trading_destination_id = %s
                    ''' % (security.suffix, date, sec_id, td_id)
        else: # end or start is not none
            if end is None:
                end = '23:59:59'
            if start is None:
                start = '00:00:00'

            query = ''' select sum(size) from %s..deal_archive%s
                        where date = '%s' and security_id = %s
                        and trading_destination_id = %s
                        and time >= '%s' and time <= '%s'
                    ''' % (TickDB.deal_schema(date), security.suffix, date, sec_id,
                           td_id, start, end)

        TickDB.cursor.execute(query)
        volume = TickDB.cursor.fetchone()
        return volume[0] if volume is not None else None

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
        return spread[0] if spread is not None else None

    @staticmethod
    def spread_at_time(security, date, time):
        '''
            Returns bid / ask at a specific time
        '''

        sec_id = security.security_id
        if security.trading_destination_id is None:
            td_id = 'null'
        else:
            td_id = str(security.trading_destination_id)

        query = ''' select top 1 bid, ask from %s..deal_archive%s
                    where date = '%s' and time >= '%s'
                      and security_id = %d and trading_destination_id = %s
                    order by date, time
                ''' % (TickDB.deal_schema(date), security.suffix, date, time,
                           sec_id, td_id)

        Connections.change_connections('production')
        result = Connections.exec_sql('QUANT', query)
        return result[0] if result else (None, None)

    @staticmethod
    def getCloses(securities, startdate, enddate=None):
        if not enddate:
            enddate = startdate

        if hasattr(securities, '__iter__'):
            # Object is iterable
            tmp = ','.join(map(str, set(securities)))
        else:
            tmp = '%s' % (securities)

        query = '''SELECT security_id, date, close_prc
                   FROM tick_db..trading_daily_ameri
                   WHERE date between '%s' and '%s'
                   AND trading_destination_id is null
                   AND security_id in (%s)
                   order by security_id, date
                ''' % (startdate, enddate, tmp)
        return TickDB.backend.select(query)

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
    from usr.dev.sivla.funcs.DBTools.security import Security

    from usr.dev.sivla.funcs.DBTools.repository import Repository
    import time

    (sec_id, td_id) = Repository.us_security_id('EURX', True)
    print sec_id, td_id
    sec = Security(sec_id, td_id,)
    print TickDB.spread_at_time(sec, '20100715', '12:00:00')

    (sec_id, td_id) = Repository.us_security_id('A', True)
    print sec_id, td_id
    print TickDB.getClose([str(sec_id)], '20100210', td_id)
    print TickDB.getClose([str(sec_id)], '20100215', td_id)

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
