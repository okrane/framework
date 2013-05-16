'''
Created on Jun 30, 2010

@author: syarc
'''
from cheuvreux.dbtools.Sqlite import SQLiteBase
from cheuvreux.fidessa import FIDESSA_BASE_DIR

import datetime
import os

fidessa_util_db = SQLiteBase(os.path.join(os.path.split(__file__)[0],
                                           'fidessa_util.db'))

import sys
if sys.version_info[0] == 2 and sys.version_info[1] >= 6:
    from cheuvreux.fidessa.fidessadb import FidessaDB
    fidessa = FidessaDB()


market_tape_map = {'NYS-MAIN': 'A',
                   'ASE-MAIN': 'A',
                   'PSE-MAIN': 'B',
                   'NAS-NNM' : 'C',
                   'NAS-SCAP': 'C',
                   'NAS-OBB' : 'C'
                  }

#class DestinationMap(object):
#    def map(self, service_executor):
#        return destination_map(service_executor)


def destination_map(service_executor):
    rs = fidessa_util_db.selectOne(''' SELECT destination
                        FROM destination
                        WHERE service_executor = ?
                        ''', service_executor)
    if rs is not None:
        return rs[0]
    else:
        return service_executor

def create_destination_map_temp_table(db):
    # Table already exist, do nothing
    if db.isTableExist('destination'):
        return

    # Create temp table
    db.execute('''CREATE TABLE IF NOT EXISTS destination
                        (SERVICE_EXECUTOR TEXT PRIMARY KEY,DESTINATION TEXT)''')

    # Populate it!
    for row in fidessa_util_db.select('SELECT service_executor, destination FROM destination'):
        db.execute('INSERT INTO destination VALUES (?,?)', row[0], row[1])

def get_tape(stock):
    row = fidessa_util_db.selectOne('SELECT tape FROM tape WHERE stock=?', stock)
    if not row:
        # Try to find it !
        market_id = fidessa._backend.selectOne('''
                    SELECT top 1 market_id FROM [High Touch Orders Cumulative]
                    WHERE instrument_code = '%s'
                    ORDER BY tradedate DESC''' % stock)

        if market_id:
            tape = get_tape_from_market_id(market_id[0])
            fidessa_util_db.execute('INSERT INTO tape VALUES ("%s","%s")' % (stock, tape))
            return tape
        return None
    return row[0]

def is_nyse_stock(stock):
    is_nyse = False
    rs = fidessa_util_db.selectOne(''' SELECT is_nyse FROM nyse_stock where code = ? ''', stock)
    if rs is None:
        # Search for the stock and update the database
        primaryMarket = fidessa.getPrimaryMarket(stock)
        is_nyse = primaryMarket == ('NYS-MAIN')

        fidessa_util_db.execute('INSERT INTO nyse_stock (code, is_nyse) VALUES (?,?)', stock, is_nyse)

        pass
    else:
        if rs[0] == 1:
            is_nyse = True
        else:
            is_nyse = False

    return is_nyse

def searchAuditFile(date):
    '''
        Returns the audit trail file for a date.
    '''
    for dir in os.listdir(FIDESSA_BASE_DIR):
        fullpath = os.path.join(FIDESSA_BASE_DIR, dir)
        if os.path.isfile(fullpath):
            continue

        for file in os.listdir(fullpath):
                if date is not None and file.find(date) == -1:
                        continue
                if file.find('AUDIT_TRAIL') > -1:
                    return os.path.join(fullpath, file)

def get_base_dir_for_date(date):
    '''
        Returns the fidessa directory for a date
    '''
    # Fidessa audit file for date d are stored under
    # a folder named "d+1"
    if type(date) is type(''):
        tmp_date = datetime.datetime.strptime(date, "%Y%m%d")
    else:
        tmp_date = date

    tmp_date = (tmp_date + datetime.timedelta(1)).strftime("%Y%m%d")
    return os.path.join(FIDESSA_BASE_DIR, tmp_date)

def parse_algo_name(algo):
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

def get_tape_from_market_id(market_id):
    if market_id in market_tape_map:
        return market_tape_map[market_id]

    if market_id.startswith('NAS'):
        return 'C'
    elif market_id.startswith('NYS'):
        return 'A'
    else:
        return 'B'



#if __name__ == '__main__':
#print is_nyse_stock('INT')
