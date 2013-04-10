'''
Created on 20 avr. 2010

@author: syarc
'''
from simep.funcs.dbtools.connections import Connections
import datetime

class Repository(object):

    cursor = Connections.getCursor('BSIRIUS')

    @staticmethod
    def fetchall(sql, params={}, select=None):
        Repository.cursor.execute(sql, params, select)
        return Repository.cursor.fetchall()

    @staticmethod
    def fetchone(sql, params={}, select=None):
        Repository.cursor.execute(sql, params, select)
        return Repository.cursor.fetchone()

    @staticmethod
    def security_id(code, source=None):
        ''' Returns a security id for a given code (and source) '''

        ans = Repository.trading_destination(code, source)
        if len(ans) > 1:
            raise Exception("REPOSITORY: the code has not exactly one security id")
        return ans[0][1]

    @staticmethod
    def security_dict_code(security_id, dict_id):
        '''
            Returns the reference code for a given security id
            for a specified dictionary id

            dict_id: (See Dictionnary class)
                5 is for ISIN
                271 is used to match US code to security id
        '''
        query = ''' select reference from repository..security_dictionary
                    where security_id = %d and dictionary_id = %d
                      and end_date is null
                ''' % (security_id, dict_id)
        Repository.cursor.execute(query)
        code = Repository.cursor.fetchone()
        return code[0] if code is not None else None

    @staticmethod
    def dict_id_to_security_id(code, dict_id):

        query = ''' select sd.reference, sd.security_id from repository..security_dictionary sd
                        join repository..security_historic sh on sh.security_id = sd.security_id
                        join repository..security s on s.security_id = sd.security_id and s.place_id = s.primary_place_id
                        where sd.end_date is null and sd.dictionary_id = %d and sh.end_date is null
                ''' % (dict_id)
        # If several code are given, look for all of them
        # Since len of a string > 0, need to check it is not
        if len(code) and not isinstance(code, str):
            query += " and sd.reference in ('%s')" % ("','".join(code))
        else:
            query += " and sd.reference = '%s'" % (code)

        Repository.cursor.execute(query)
        rows = Repository.cursor.fetchall()

        if not rows: #Nothing found, try another query
            #W/o the constraint on place_id = primary_place_id
            query = ''' select sd.reference, sd.security_id from repository..security_dictionary sd
                        join repository..security_historic sh on sh.security_id = sd.security_id
                        where sd.end_date is null and sd.dictionary_id = %d and sh.end_date is null
                ''' % (dict_id)
            if len(code) and not isinstance(code, str):
                query += " and sd.reference in ('%s')" % ("','".join(code))
            else:
                query += " and sd.reference = '%s'" % (code)

            Repository.cursor.execute(query)
            rows = Repository.cursor.fetchall()

        if not rows: #Nothing found, try another query
            #W/o the constraint on place_id = primary_place_id
            query = ''' select sd.reference, sd.security_id from repository..security_dictionary sd
                        where sd.end_date is null and sd.dictionary_id = %d
                ''' % (dict_id)
            if len(code) and not isinstance(code, str):
                query += " and sd.reference in ('%s')" % ("','".join(code))
            else:
                query += " and sd.reference = '%s'" % (code)

            Repository.cursor.execute(query)
            rows = Repository.cursor.fetchall()

        return dict((row[1], row[0]) for row in rows)

    @staticmethod
    def us_code_to_isin(us_code):
        query = ''' select o.reference, o.security_id
                        from repository..security_dictionary o,repository..security_dictionary t
                        where t.reference in ('%s') and o.dictionary_id = 5 and t.dictionary_id = 271
                        and o.security_id=t.security_id and o.end_date is null
                    ''' % (us_code)
        Repository.cursor.execute(query)
        row = Repository.cursor.fetchone()
        return row

    @staticmethod
    def isin_to_security_id(isin):
        if type(isin) is not type('') and len(isin):
            query = ''' select o.reference, o.security_id
                        from repository..security_dictionary o,repository..security_dictionary t
                        where o.reference in ('%s') and o.dictionary_id = 5 and t.dictionary_id = 271
                        and o.security_id=t.security_id
                    ''' % ("','".join(isin))
        else:
            query = ''' select o.reference, o.security_id
                        from repository..security_dictionary o,repository..security_dictionary t
                        where o.reference in ('%s') and o.dictionary_id = 5 and t.dictionary_id = 271
                        and o.security_id=t.security_id
                    ''' % (isin)

        Repository.cursor.execute(query)
        rows = Repository.cursor.fetchall()

        return dict((row[1], row[0]) for row in rows)

    @staticmethod
    def exec_id_to_trading_destination(exec_ids):
        query = ''' select trading_destination_id, execution_market_id
                    from repository..trading_destination
                    where execution_market_id in (%s)
                ''' % (','.join(map(str, exec_ids)))

        Repository.cursor.execute(query)
        rows = Repository.cursor.fetchall()

        return dict((row[0], row[1]) for row in rows)

    @staticmethod
    def outstanding_shares(security_id, date=None):
        ''' Returns the number of outstanding shares for a Security

            This number could be used to compute market capitalization
        '''

        if date is None:
            date = datetime.date.today().strftime("%Y%m%d")

        query = ''' select nb_shares_outstanding
                    from repository..security_historic
                    where security_id = %d
                    and begin_date < '%s' and isnull(end_date,'21000101') > '%s'
                ''' % (security_id, date, date)

        Repository.cursor.execute(query)
        nb = Repository.cursor.fetchone()
        return nb[0] if nb is not None else None

    @staticmethod
    def get_sector(security, classification_id=25):
        '''
            Returns the sector of a security.

            @param security: Accepts both ISIN (is security is a string) and security_id (integer
                             in that case.

            @param classification_id: Type of classification wanted (25 is the default and
                                      stands for 'Global Industry Classification Standard (GICS)'

            @return tuple of the form (short_name, name)
        '''

        if type(security) is int:
            query = ''' SELECT short_name, name
                        FROM repository..security_classification sc
                        JOIN repository..category c on c.category_id = sc.category_id
                        WHERE sc.classification_id = %d
                          AND sc.security_id = %d
                    ''' % (classification_id, security)
        else:
            #Consider security is a ISIN
            query =  ''' SELECT short_name, name
                         FROM repository..security_classification sc
                         JOIN repository..category c on c.category_id = sc.category_id
                         JOIN repository..security_dictionary sd ON sd.security_id = sc.security_id
                                                              AND sd.dictionary_id = 5
                                                              AND sd.end_date is null
                         WHERE sc.classification_id = %d
                         AND sd.reference = '%s'
                    ''' % (classification_id, security)

        Repository.cursor.execute(query)
        return Repository.cursor.fetchone()

    @staticmethod
    def get_sectors(security, classification_id=25):
        if type(security[0]) is int:
            query = ''' SELECT security_id, short_name, name
                        FROM repository..security_classification sc
                        JOIN repository..category c on c.category_id = sc.category_id
                        WHERE sc.classification_id = %d
                          AND sc.security_id in (%s)
                    ''' % (classification_id, ','.join(map(str,security)))
        else:
            #Consider security is a ISIN
            query =  ''' SELECT sd.reference, short_name, name
                         FROM repository..security_classification sc
                         JOIN repository..category c on c.category_id = sc.category_id
                         JOIN repository..security_dictionary sd ON sd.security_id = sc.security_id
                                                              AND sd.dictionary_id = 5
                                                              AND sd.end_date is null
                         WHERE sc.classification_id = %d
                         AND sd.reference in ('%s')
                    ''' % (classification_id, "','".join(security))
        Repository.cursor.execute(query)
        result = {}
        for row in Repository.cursor.fetchall():
            result[row[0]] = (row[1],row[2])
        return result

    @staticmethod
    def us_security_id(uscode, first_only=False):
        ''' Returns a security id for a US security

            This method uses a intern dictionary to map US code to security id.
            Some securities could be missing.
        '''

        query = ''' select sd.security_id, trading_destination_id
                    from repository..security_dictionary sd
                    join repository..security_historic sh on sh.security_id = sd.security_id
                    join repository..security s on s.security_id = sd.security_id and s.place_id = s.primary_place_id
                    left join repository..security_market sm on sm.security_id = sd.security_id
                    where reference = '%s'
                    and dictionary_id = %d and sd.end_date is null and sh.end_date is null
                    order by ranking
                ''' % (uscode, DictionaryId.USCODE)
        Repository.cursor.execute(query)
        if Repository.cursor.rowcount <= 0:
            query = ''' select sd.security_id, trading_destination_id
                        from repository..security_dictionary sd
                        join repository..security s on s.security_id = sd.security_id
                        left join repository..security_market sm on sm.security_id = sd.security_id
                        where reference = '%s'
                        and dictionary_id = %d and sd.end_date is null and trading_destination_id is not null
                        order by ranking
                    ''' % (uscode, DictionaryId.USCODE)
            Repository.cursor.execute(query)

        if first_only:
            ans = Repository.cursor.fetchone()
        else:
            ans = Repository.cursor.fetchall()

        if ans is None:
            ans = (None, None)

        return ans

    @staticmethod
    def index_id(index_name):
        '''
            Returns index_id for an index_name
        '''
        query = ''' select indice_id from repository..indice where name = '%s' ''' % (index_name)
        Repository.cursor.execute(query)
        id = Repository.cursor.fetchone()
        return id[0] if id is not None else None

    @staticmethod
    def index_components(index_id):
        '''
            Returns the components (security_id) of an index
        '''
        query = ''' select security_id from repository..indice_component where indice_id = %d ''' % (index_id)
        Repository.cursor.execute(query)
        ids = Repository.cursor.fetchall()
        return [id[0] for id in ids] if ids is not None else None

    @staticmethod
    def trading_destination(id, source=None):
        ''' Returns the trading destinations available for a security id '''
        query = None

        if type(id) is type(1):
            # id is an integer
            query = ''' select trading_destination_id from repository..security_market
                        where security_id = %d
                        order by ranking, trading_destination_id asc ''' % (id)
        elif type(id) == type(''):
            #id is a string
            if source is None:
                query = ''' select sm.trading_destination_id, src.name, td.name, sm.security_id, sec.name from repository..security_market sm, repository..security sec,
                            repository..source src, repository..security_source ss , repository..trading_destination td
                            where sm.security_id=ss.security_id
                            and   sec.security_id = sm.security_id
                            and   src.source_id=ss.source_id
                            and  sm.trading_destination_id = td.trading_destination_id
                            and ss.reference = '%s'
                            order by ranking, td.trading_destination_id asc
                        ''' % (id)
            else:
                query = ''' select ss.trading_destination_id, ss.security_id from
                            repository..source src, repository..security_source ss, repository..security_market sm
                            where src.source_id=ss.source_id
                            and ss.reference = '%s'
                            and src.name = '%s'
                            and sm.security_id = ss.security_id
                            and sm.trading_destination_id = ss.trading_destination_id
                            order by ranking, trading_destination_id asc
                        ''' % (id, source.upper())

        if query is not None:
            ans = Repository.fetchall(query)

        return ans

    @staticmethod
    def table_suffix(id):
        ''' Returns the suffix needed for tick data

            This suffix could be:
                '' for europe
                'ameri' for US stocks
                'asia' for asia stocks
            @param id: Security ID
        '''

        #TODO: handle different country_id
        #syarc: Is there a table with country_id ???

        # Ameri suffix
        query = ''' select 1 from repository..security where security_id = %d
                    and primary_place_id in (3)
                ''' % (id)
        if Repository.fetchone(query) == (1,):
            return 'ameri'

        # Asia suffix
        # TODO:

        #Default: nothing
        return ''


class DictionaryId:
    ISIN = 5
    USCODE = 271

if __name__ == '__main__':
#    print Repository.get_sector('US0017651060')
#    print Repository.get_sector(10673)
    print Repository.us_security_id('UFS')
#    print Repository.trading_destination(110)
#    print Repository.trading_destination('FTE.PA')
#    print Repository.trading_destination('FTE.PA', 'idn_selectfeed')
#    print Repository.security_id('FTE.PA', 'idn_selectfeed')
#    print Repository.us_security_id('C')
#    print Repository.us_security_id('C', True)
#    print Repository.table_suffix(11679)
#    print Repository.security_dict_code(10673, 5)
#    print Repository.outstanding_shares(10673, None)


