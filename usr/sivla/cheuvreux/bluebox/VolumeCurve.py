'''
Created on 30 avr. 2010

@author: syarc
'''

from cheuvreux.dbtools import tickdb
from cheuvreux.dbtools.SE import get_se_estimator_by_run_id, \
    get_runs_by_perimeters, get_runs_by_perimeters_for_date,\
    get_context_runs_by_perimeter
from cheuvreux.dbtools.odbc import ODBC
from cheuvreux.dbtools.repository import DictionaryId, Repository
from cheuvreux.dbtools.security import Security
from cheuvreux.dbtools.tickdb import TickDB
from cheuvreux.utils.date import previous_weekday
from subprocess import Popen
import datetime
import pyodbc
import sys
import time
import traceback
import atexit
from cheuvreux.dbtools.connections import Connections

EPSILON = 0.00001


cal_event = '''  CREATE CACHED TABLE PUBLIC.CAL_EVENT(
                        EVENT_ID INTEGER PRIMARY KEY AUTO INCREMENT,
                        EVENT_TYPE INTEGER,
                        SECURITY_ID INTEGER,
                        EVENT_DATE DATE
                    )
            '''

cal_event_idx = 'CREATE INDEX IDX_DATA ON CAL_EVENT (EVENT_DATE)'

context_ranking = ''' CREATE CACHED TABLE PUBLIC.CONTEXT_RANKING(
                        SECURITY_ID INTEGER,
                        CONTEXT_ID INTEGER,
                        EVENT_TYPE INTEGER,
                        RANKING INTEGER
                    )
                '''

cr_idx = 'CREATE INDEX IDX_CR ON CONTEXT_RANKING (SECURITY_ID, CONTEXT_ID)'

event_type = ''' CREATE CACHED TABLE PUBLIC.EVENT_TYPE(
                    EVENT_TYPE INTEGER NOT NULL PRIMARY KEY,
                    EVENT_NAME VARCHAR(64)
                )
            '''


event_type = '''
                INSERT INTO EVENT_TYPE VALUES (10,'derivative_expiry');
                INSERT INTO EVENT_TYPE VALUES (12,'london_closed');
                INSERT INTO EVENT_TYPE VALUES (15,'monthly_derivative_expiry');
                INSERT INTO EVENT_TYPE VALUES (20,'earnings_conf_call');
                INSERT INTO EVENT_TYPE VALUES (21,'earnings');
                INSERT INTO EVENT_TYPE VALUES (88890,'US-O-EIA Petroleum Status Report');
                INSERT INTO EVENT_TYPE VALUES (88893,'US-O-GDP');
                INSERT INTO EVENT_TYPE VALUES (88894,'US-O-Chairman Press Conference');
                INSERT INTO EVENT_TYPE VALUES (88903,'US-O-New Home Sales');
                INSERT INTO EVENT_TYPE VALUES (88906,'US-O-ADP Employment Report');
                INSERT INTO EVENT_TYPE VALUES (88912,'US-O-FOMC Minutes');
                INSERT INTO EVENT_TYPE VALUES (88918,'US-O-Consumer Sentiment (p)');
                INSERT INTO EVENT_TYPE VALUES (88926,'US-O-Pending Home Sales Index');
                INSERT INTO EVENT_TYPE VALUES (88934,'US-O-S&P Case-Shiller HPI');
                INSERT INTO EVENT_TYPE VALUES (88938,'US-O-ISM Non-Mfg Index');
                INSERT INTO EVENT_TYPE VALUES (88939,'US-O-Consumer Confidence');
                INSERT INTO EVENT_TYPE VALUES (88940,'US-O-Consumer Price Index');
                INSERT INTO EVENT_TYPE VALUES (88943,'US-O-Import and Export Prices');
                INSERT INTO EVENT_TYPE VALUES (88948,'US-O-ISM Mfg Index');
                INSERT INTO EVENT_TYPE VALUES (88955,'US-O-Housing Market Index');
                INSERT INTO EVENT_TYPE VALUES (88958,'US-O-Existing Home Sales');
                INSERT INTO EVENT_TYPE VALUES (88959,'US-O-Construction Spending');
                INSERT INTO EVENT_TYPE VALUES (88970,'US-O-Durable Goods Orders');
                INSERT INTO EVENT_TYPE VALUES (88973,'US-O-Personal Income and Outlays');
                INSERT INTO EVENT_TYPE VALUES (88988,'US-O-Chicago PMI');
                INSERT INTO EVENT_TYPE VALUES (88993,'US-O-International Trade');
                INSERT INTO EVENT_TYPE VALUES (88998,'US-O-Employment Cost Index');
                INSERT INTO EVENT_TYPE VALUES (88999,'US-O-Jobless Claims');
                INSERT INTO EVENT_TYPE VALUES (89018,'US-O-Treasury Budget');
                INSERT INTO EVENT_TYPE VALUES (89021,'US-O-Motor Vehicle Sales');
                INSERT INTO EVENT_TYPE VALUES (89026,'US-O-Producer Price Index');
                INSERT INTO EVENT_TYPE VALUES (89047,'US-O-Housing Starts');
                INSERT INTO EVENT_TYPE VALUES (89053,'US-O-Industrial Production');
                INSERT INTO EVENT_TYPE VALUES (89056,'US-O-FOMC Meeting Announcement');
                INSERT INTO EVENT_TYPE VALUES (89060,'US-O-Employment Situation');
                INSERT INTO EVENT_TYPE VALUES (89064,'US-O-Retail Sales');
                INSERT INTO EVENT_TYPE VALUES (89065,'US-O-Productivity and Costs');
                INSERT INTO EVENT_TYPE VALUES (89070,'US-O-Philadelphia Fed Survey');
                INSERT INTO EVENT_TYPE VALUES (89072,'US-O-Factory Orders');
                INSERT INTO EVENT_TYPE VALUES (89085,'US-O-Beige Book');
'''
class VolumeCurve(object):

    ''' Internal SE id'''
    context = 'Usual day'

    def __init__(self, curve_id, run_id, run_date, se_indicator, sec_id, estimator_id, db):
        ''' Constructor
            @param SE indicator for volume curve
        '''
        self.curve_id, self.sec_id, self.estimator_id = curve_id, sec_id, estimator_id
        self.run_id, self.run_date = run_id, datetime.date(run_date.year, run_date.month + 1, run_date.day)
        self.volume = []
        self.computeCumulative(se_indicator, db)

    @staticmethod
    def lastRunId(db, sec_id, context_id):
        result = db.selectOne("select curve_id, run_id from volume_curve where security_id = ? and context_id = ?", sec_id, context_id)
        if result is None:
            result = (None, None)
        return result

    @staticmethod
    def createCurve(db, sec_id, context_id):

        row = db.selectOne('SELECT SEDL, ISIN, CODE FROM VOLUME_CURVE WHERE security_id = ?', sec_id)
        if row:
            sedl, isin, code = row[0], row[1], row[2]
        else:
            start = time.clock()
            isin = Repository.security_dict_code(sec_id, DictionaryId.ISIN)
            #print 'mkt ', time.clock() - start
            start = time.clock()
            sedl = Repository.security_dict_code(sec_id, DictionaryId.SEDL)
            #print 'mkt ', time.clock() - start
            start = time.clock()
            code = Repository.market_code (sec_id)
            #print 'mkt ', time.clock() - start

        #code = Repository.security_dict_code(sec_id, DictionaryId.USCODE)

        if not code:
            short_name = Repository.short_name(sec_id)
            if short_name.startswith('US'):
                code = short_name[2:]
                print "US Code not found for %d, %s has been deduced" % (sec_id, code)
            else:
                print "US Code not found for %d" % sec_id

        start = time.clock()
        db.run("INSERT INTO volume_curve (security_id, code, isin, sedl, context_id) VALUES (?, ?, ?, ?, ?)", sec_id, code, isin, sedl, context_id)
        curveId = db.selectOne("select curve_id from volume_curve where security_id = ? and context_id = ?", sec_id, context_id)
        return curveId[0]

    @staticmethod
    def deleteCurve(db, sec_id):
        curveId = db.selectOne("select curve_id from volume_curve where security_id = ?", sec_id)
        if curveId is not None:
            db.run("DELETE FROM volume_curve_value WHERE curve_id = ?", curveId[0])
        db.run("DELETE FROM volume_curve WHERE security_id = ?", sec_id)

    @staticmethod
    def getSecIdsAvailable(db):
        return db.select("SELECT security_id FROM volume_curve")

    def computeCumulative(self, se_indicator, db=None):
        ''' Compute cumulative volume curve '''
        cumVolume = 0.0

        values = dict(zip(se_indicator.date, se_indicator[VolumeCurve.context]))

        if self.estimator_id == 2:
            if 'alpha' in values and 'beta' in values:
                self.alpha, self.beta = values['alpha'], values['beta']
            else:
                self.alpha, self.beta = 0, 0
        else:
            temp = db.selectOne('SELECT alpha, beta FROM volume_curve WHERE security_id = ? AND context_id = ?',
                               self.sec_id, 2)
            if not temp:
                print 'no alpha / beta found for ', self.sec_id
                self.alpha, self.beta = values['alpha'], values['beta']
            else:
                self.alpha, self.beta = temp[0], temp[1]


        if 'auction_open' not in values or values['auction_open'] == 0.0:
            td_ids = Repository.trading_destination(self.sec_id)
            # no open estimation for canada (30 = TSX)
            raise ValueError("No open estimation for %d\n" % self.curve_id)
            #    values['auction_open'] = 0.0
            #else:

        cumVolume += values['auction_open']
        self.volume.append(cumVolume)

        i = 1
        try:
            while True:
                cumVolume += values['slice%03d' % i]
                if cumVolume > 1.0:
                    if cumVolume - 1.0 > 10*EPSILON:
                        sys.stderr.write("Cumulative volume is greater than 1 for %s\n" % self.curve_id)
                    cumVolume = 1.0
                self.volume.append(cumVolume)
                i += 1
        except KeyError:
            pass

        if 'auction_close' not in values or values['auction_close'] == 0.0:
            td_ids = Repository.trading_destination(self.sec_id)
            if td_ids and td_ids[0][0] == 99:
                print "No close estimation for ETF %d" % self.curve_id
            else:
                raise ValueError("No close estimation for %d\n" % self.curve_id)

        cumVolume += values['auction_close']

        self.volume.append(cumVolume)

    def save2file(self, out):
        ''' Write volume curve to a file

            The format is:
                alpha, self.alpha
                beta, self.beta
            Then we output the "volume curve"
                [time, cumulative volume]

            This time is normalized between 0 and 1
            (0 = open auction, 1 = close auction)

            Time between volume curve point is constant and is currently
            equals to 5 minutes.
        '''
        #out.write("%s,%s\n" % ('date', self.date))s
        out.write("%s,%f\n" % ('alpha', self.alpha))
        out.write("%s,%f\n" % ('beta', self.beta))

        # First point is (0,0)
        out.write("%f,%f\n" % (0.0, 0.0))
        # Open auction, auction time is set to 0 + epsilon
        out.write("%f,%f\n" % (0.000001, self.volume[0]))

        size = len(self.volume)
        divisor = size - 2 # minus @ to remove the two auctions
        for i in range(1, size - 2):
            out.write("%f,%f\n" % (1.0 * i / divisor, self.volume[i]))

        # The last point of the SE indicator is equal to the market close
        # To integrate close auction, we set its time to 1 - epsilon (0.00001).
        # Hence we have 1 = close auction
        out.write("%f,%f\n" % (0.999999, self.volume[size - 2]))
        # Last line should 1.0000, 1.0000
        if abs(1.0 - self.volume[size - 1]) > EPSILON:
            sys.stderr.write("Consistency Check: %d (%s) sum of volume does not equal to 1, %f\n"
                             % (self.sec_id, self.isin, 1.0 - self.volume[size - 1]));


        out.write("%f,%f\n" % (1.0, 1.0))

    def save2db(self, db):

        # Clear curve
        db.run('DELETE FROM volume_curve_value WHERE curve_id = ?', self.curve_id)

        # First point is (0,0)
        db.run('INSERT INTO volume_curve_value VALUES (?,?,?)', self.curve_id, 0.0, 0.0)
        # Open auction, auction time is set to 0 + epsilon
        db.run('INSERT INTO volume_curve_value VALUES (?,?,?)', self.curve_id, 0.000001, self.volume[0])

        size = len(self.volume)
        divisor = size - 2 # minus @ to remove the two auctions

        for i in range(1, size - 2):
            db.run('INSERT INTO volume_curve_value VALUES (?,?,?)', self.curve_id, 1.0 * i / divisor, self.volume[i])

        # The last point of the SE indicator is equal to the market close
        # To integrate close auction, we set its time to 1 - epsilon (0.00001).
        # Hence we have 1 = close auction
        db.run('INSERT INTO volume_curve_value VALUES (?,?,?)', self.curve_id, 0.999999, self.volume[size - 2])

        if abs(1.0 - self.volume[size - 1]) > EPSILON:
            sys.stderr.write("Consistency Check: %d sum of volume does not equal to 1, %f\n"
                             % (self.sec_id, 1.0 - self.volume[size - 1]));

        db.run('INSERT INTO volume_curve_value VALUES (?,?,?)', self.curve_id, 1.0, 1.0)

        db.run("UPDATE volume_curve SET date = ?, run_id = ?, alpha = ?, beta = ? where curve_id = ?",
               self.run_date, self.run_id, self.alpha, self.beta, self.curve_id)


    def save2db_new(self, db):

        # Clear curve
        db.run('DELETE FROM volume_curve_value WHERE curve_id = %s', (self.curve_id))

        def generator():
            # First point is (0,0)
            #db.run('INSERT INTO volume_curve_value VALUES (?,?,?)', self.curve_id, 0.0, 0.0)
            yield (self.curve_id, 0.0, 0.0)
            # Open auction, auction time is set to 0 + epsilon
            #db.run('INSERT INTO volume_curve_value VALUES (?,?,?)', self.curve_id, 0.000001, self.volume[0])
            yield (self.curve_id, 0.000001, self.volume[0])

            size = len(self.volume)
            divisor = size - 2 # minus @ to remove the two auctions

            for i in range(1, size - 2):
                yield (self.curve_id, 1.0 * i / divisor, self.volume[i])
                #db.run('INSERT INTO volume_curve_value VALUES (?,?,?)', self.curve_id, 1.0 * i / divisor, self.volume[i])

            # The last point of the SE indicator is equal to the market close
            # To integrate close auction, we set its time to 1 - epsilon (0.00001).
            # Hence we have 1 = close auction
            #db.run('INSERT INTO volume_curve_value VALUES (?,?,?)', self.curve_id, 0.999999, self.volume[size - 2])
            yield (self.curve_id, 0.999999, self.volume[size - 2])

            if abs(1.0 - self.volume[size - 1]) > EPSILON:
                sys.stderr.write("Consistency Check: %d sum of volume does not equal to 1, %f\n"
                                 % (self.sec_id, 1.0 - self.volume[size - 1]));


            #db.run('INSERT INTO volume_curve_value VALUES (?,?,?)', self.curve_id, 1.0, 1.0)
            yield (self.curve_id, 1.0, 1.0)


        db.executemany('INSERT INTO volume_curve_value VALUES (%s,%s,%s)',generator())

        db.run("UPDATE volume_curve SET date = %s, run_id = %s, alpha = %s, beta = %s where curve_id = %s",
               (self.run_date, self.run_id, self.alpha, self.beta, self.curve_id))


def volume_curve(database, date=None):
    '''
        Update the standard volume curve
    '''
    start = time.clock()
    context_id = 2
    estimator_id = 2
    universe = get_runs_by_perimeters([14,15,16,17], estimator_id, context_id)
#    universe2 = get_runs_by_perimeters_for_date([16], 2, 2, '20110629') # 17


#    for line in database.select('SELECT security_id, code, date FROM volume_curve'):
#        found = False
#        for sec_id, lastRunDate, lastRunId, run_quality in universe:
#            if sec_id == line[0]:
#                found = True
#                break
#
#        if not found:
#            print '\t'.join(map(str,[line[0], line[1], line[2], TickDB.adv(line[0], '20120301', 30)]))
#    return

    if date is None:
        date = datetime.date.today().strftime("%d/%m/%Y")

    for sec_id, lastRunDate, lastRunId, run_quality in universe:
        if run_quality < 0.25:
            continue

        #start2 = time.clock()
        curveId, runId = VolumeCurve.lastRunId(database, sec_id, context_id)
        #print 'lastRunId', time.clock() - start2

        if runId is None or lastRunId > runId:

            if curveId is None:
                try:
                    curveId = VolumeCurve.createCurve(database, sec_id, context_id)
                except pyodbc.IntegrityError:
                    traceback.print_exc()
                    continue

            try :
                print "Processing %d (run date: %s) curveId: %d" % (sec_id, lastRunDate, curveId)
                #start2 = time.clock()
                indicator = get_se_estimator_by_run_id (lastRunId, estimator_id, VolumeCurve.context)
                #print 'get_se_estimator_by_run_id', time.clock() - start2
                curve = VolumeCurve(curveId, lastRunId, lastRunDate, indicator, sec_id, estimator_id, database)
                #start2 = time.clock()
                curve.save2db(database)
                #print 'save2db', time.clock() - start2
            except ValueError as e:
                sys.stderr.write('%s: %s' % (sec_id, e))

    # Delete invalid curve
    database.execDirect('delete from volume_curve where run_id is null')

    # Need to check if some volume curves has been deleted
#    se_sec_ids = [elmt[0] for elmt in universe if elmt[3] >= 0.25]
#    sec_ids = VolumeCurve.getSecIdsAvailable(database)
#    for sec_id in sec_ids:
#        if sec_id[0] not in se_sec_ids:
#            print "Deleting security %d" % sec_id[0]
#            VolumeCurve.deleteCurve(database, sec_id[0])

    print "Run time", time.clock() - start

def check_corruption(database):
    for row in database.select("select curve_id from volume_curve"):
        database.select ('select * from volume_curve_value where curve_id = ?', row[0])

    database.select ('select * from restricted_stocks')
    database.select ('select * from crag_rules')
    database.select ('select * from crag_params')
    database.select ('select * from ticker_map')
    database.select ('select * from extra_destinations')
    database.select ('select * from DARKPOOLS_ATTRIBUTES ')



#def update_marketcap(database):
#    for isin in database.select('SELECT isin, MARKETCAP FROM MARKETCAPITALIZATION'):
#        try:
#            security_id = SecurityIdCache.getSecurityIdFromIsin(isin[0])
#        except IndexError:
#            continue
#        if security_id and security_id[0]:
#            print Repository.short_name(security_id[0])[2:], isin[0], isin[1]
#            continue
#
#
#            shares = Repository.outstanding_shares(security_id[0])
#            if shares:
#                security = Security(security_id[0], security_id[1])
#                ohlc = TickDB.ohlc(security, previous_weekday())
#                if ohlc:
#                    print isin[0], shares * ohlc[0], isin[1], 100 * (isin[1] -  shares * ohlc[0]) / isin[1]

def update_marketcap(database, csvfile):
    for line in open(csvfile):
        isin, mktcap = line.strip().split(',')
        print isin, mktcap

        database.run('UPDATE MARKETCAPITALIZATION SET MARKETCAP = ? WHERE ISIN = ?', int(float(mktcap)), isin)


def last_value(db):
    for stock in ['DIA','MDY','IWM','EFA','EEM','LQD','AGG','EMB']:
        id = db.select('SELECT curve_id FROM volume_curve where code = \'%s\'' % stock)
        print stock, id

        db.select('select * from volume_curve_value where curve_id = %d and time > 0.93' % id)

def cleanup(java, database):
    print 'Clean up'
    if database:
        try:
            database.close()
            # We need to wait for the connection to be correctly closed.
            time.sleep(1)
        except Exception, e:
            print e
            pass
    if java:
        java.kill()


def load_context_ranking(database):

    event_types =[row[0] for row in database.select('SELECT event_type FROM EVENT_TYPE')]

    database.run('DELETE FROM CONTEXT_RANKING')

    cursor = Connections.getCursor('QUANT')

    # Load context ranking for the events we're interested in
    # the volume curve context and the S&P 500
    query = ''' select security_id, context_id, event_type, ranking
                from quant..context_ranking
                where estimator_id = 15 and event_type in (null,%s)
                and security_id in (SELECT security_id FROM repository..indice_component where indice_id = 46)
            ''' % ','.join(map(str,event_types))

    cursor.execute(query)
    rows = []
    for row in cursor.fetchall():
        #if row[1] != 166 or not row[2]:
        rows.append(row)

    query = 'INSERT INTO CONTEXT_RANKING values (?,?,?,?)'

    database.executemany(query, rows)




def load_calendar(database, date):

    event_types =[row[0] for row in database.select('SELECT event_type FROM EVENT_TYPE')]
    
    database.run('DELETE FROM CAL_EVENT WHERE event_date >= \'%s\'' % date)
    
    database.run('DELETE FROM CAL_EVENT WHERE event_date < TODAY')

    def gen_event():
        cursor = Connections.getCursor('BSIRIUS')
        # Handle event for global scope
        query = ''' select c.event_type, null, c.event_date from repository..cal_event c
                            where event_date > '%s' and scope_type = 12
                            and event_type in (%s) ''' % (date, ','.join(map(str,event_types)))
        print query
        cursor.execute(query)

        for row in cursor.fetchall():
            yield [row[0], row[1], datetime.date(row[2].year, row[2].month + 1, row[2].day)]

        # Handle event for NY place
        query = ''' select c.event_type, null, c.event_date from repository..cal_event c
                            where event_date > '%s' and scope_type = 2 and scope_id = 3
                            and event_type in (%s) ''' % (date, ','.join(map(str,event_types)))
        cursor.execute(query)
        for row in cursor.fetchall():
            yield [row[0], row[1], datetime.date(row[2].year, row[2].month + 1, row[2].day)]
            #yield [row[0], row[1], date.strptime('%Y-%row[2])
    #

    # Handle event for scope security

    query = 'INSERT INTO CAL_EVENT (EVENT_TYPE, SECURITY_ID, EVENT_DATE) VALUES (?,?,?)'
    database.executemany(query, [g for g in gen_event()])


def update_alpha_beta(database):
    '''
        Update the curve which have null value for alpha and/or beta.
    '''
    rows = database.select('SELECT curve_id, run_id, context_id, security_id FROM volume_curve WHERE alpha is null OR beta is null')
    for row in rows:
        if row[2] == 2:
            indicator = get_se_estimator_by_run_id (row[1], 2, row[2])
            values = dict(zip(indicator.date, indicator[row[2]]))

            database.run('UPDATE volume_curve SET alpha = ?, beta = ? WHERE curve_id = ?', values['alpha'], values['beta'], row[0])
            print row, values['alpha'], values['beta']
        else:
            values = database.selectOne('SELECT alpha, beta FROM volume_curve where security_id = ? and context_id = 2', row[3])
            database.run('UPDATE volume_curve SET alpha = ?, beta = ? WHERE security_id = ? and context_id = ?', values[0], values[1], row[3], row[2])

def context_volume_curve(database):
    '''
        Update the contextualized volume curve
    '''

    estimator_id = 15
    ctx = database.select('SELECT distinct context_id FROM context_ranking')

    universe = get_context_runs_by_perimeter([14],estimator_id, [c[0] for c in ctx])
    for sec_id, date, last_run_id, context_id in universe:

        row = database.selectOne('SELECT * FROM context_ranking WHERE security_id = ? AND context_id = ?', sec_id, context_id)
        if not row:
            #print 'Skipping %d for context %d' % (sec_id, context_id)
            continue
        #start = time.clock()
        curveId, run_id = VolumeCurve.lastRunId(database, sec_id, context_id)
        #print 'lastRunId', time.clock() - start

        if not run_id or last_run_id > run_id:
            if not curveId:
                #start = time.clock()
                try:
                    curveId = VolumeCurve.createCurve(database, sec_id, context_id)
                except pyodbc.IntegrityError:
                    traceback.print_exc()
                    continue
                #print 'createCurve: ', (time.clock() - start)

            try :
                print "Processing %d (run date: %s) curveId: %d contextId: %d runId: %d" % (sec_id, date, curveId, context_id, last_run_id)
                #start2 = time.clock()
                indicator = get_se_estimator_by_run_id (last_run_id, estimator_id, VolumeCurve.context)
                #print 'get_se_estimator_by_run_id', time.clock() - start2
                #start2 = time.clock()
                curve = VolumeCurve(curveId, last_run_id, date, indicator, sec_id,estimator_id, database)
                #print 'VolumeCurve', time.clock() - start2
                #start2 = time.clock()
                curve.save2db(database)
                #print 'save2db', time.clock() - start2
            except ValueError as e:
                sys.stderr.write('%s: %s' % (sec_id, e))

def run_checks(database):
    '''
        Check database for incorrect state.
    '''
    check_corruption(database)

    query = 'SELECT COUNT(*) FROM volume_curve WHERE alpha is null OR beta is null'
    row = database.selectOne(query)
    if row[0] > 0:
        print >> sys.stderr, '%d curves have a null alpha / beta' % row[0]

    query = ''' SELECT curve_id FROM volume_curve
                WHERE curve_id NOT IN (SELECT distinct curve_id FROM volume_curve_value)
                ORDER BY curve_id
            '''
    rows = database.select(query)
    for row in rows:
        print >> sys.stderr, 'Curve ID %d doesn\'t have values' % row[0]

    query = ''' SELECT distinct curve_id FROM volume_curve_value
                WHERE curve_id NOT IN (SELECT distinct curve_id FROM volume_curve)
                ORDER BY curve_id
            '''

    rows = database.select(query)
    for row in rows:
        print >> sys.stderr, 'Curve ID %d doesn\'t exist in volume_curve' % row[0]


def extract_volume_curves(db, stocks):
    query = ''' select time, value from volume_curve_value vcv
            join volume_curve vc on vc.curve_id = vcv.curve_id
            where code = '%s' and context_id = 2
            '''
    
    today = datetime.datetime.today().date()
    open_time = datetime.datetime(today.year, today.month, today.day,9,30)
    close = datetime.datetime(today.year, today.month, today.day, 16)
    sdate =  today.strftime('%Y%m%d') 
    diff = (close - open_time).seconds
    
    for stock in stocks:
        rows = db.select(query % stock)
        if len(rows):
            with open('volume_curve/%s_%s.csv' % (stock.replace('/','-'), sdate),'w') as file:
                for row in rows:
                    if row[0] == 0.0:
                        time = '9:29:59'
                    elif row[0] == 1e-6:
                        time = '9:30:00'
                    elif row[0] == 1 - 1e-6:
                        time = '15:59:59'
                    elif row[0] == 1:
                        time = '16:00:00'
                    else:
                        time = (open_time + datetime.timedelta(seconds=diff * row[0])).time()
                    print >> file, '%s,%.6f' % (time, row[1])
        

if __name__ == '__main__':
    java = database = None
    try:
        ###
        ### There is multiple way to connect to the database
        ###
        
        ### First way is to start a java process - it's better to start the h2 server from the command line (h2.cmd)
        #java = Popen("java -cp C:\\Apps\\Java\\lib\\h2-1.3.164.jar org.h2.tools.Server -ifExists -pg -pgPort 54435 -baseDir C:\\Eclipse\\bluebox\\db")

        # Establish the connection using the Postgre driver: 
        database = ODBC("DRIVER={PostgreSQL ANSI};Server=localhost;DATABASE=bluebox_prod;PORT=5435;Uid=python;Pwd=python;AUTO_SERVER=true", autocommit = False)
        
        ####
        # It's also possible to establish the connection to the server running on the fidessa server, but it's pretty slow
        ####
        
        # -- UAT 
        #database = ODBC("DRIVER={PostgreSQL ANSI};Server=12.192.234.57;DATABASE=bluebox;Uid=python;Pwd=python;PORT=5435")#,autocommit = False)
        # -- STANDBY
        #database = ODBC("DRIVER={PostgreSQL ANSI};Server=12.182.174.57;DATABASE=bluebox;Uid=python;Pwd=python;PORT=5435")#,autocommit = False)
        # -- PROD
        #database = ODBC("DRIVER={PostgreSQL ANSI};Server=65.244.97.57;DATABASE=bluebox;Uid=python;Pwd=python;PORT=5435")#,autocommit = False)

        atexit.register(cleanup, java, database)

        # Update regular volume curve
        volume_curve(database)

        # Update alpha / beta in case some of them are null
        #update_alpha_beta(database)
        
        # Load calendar event
        #load_calendar(database, '2012-07-24')
        
        # Load ranking for context volume curve
        #load_context_ranking(database)
        
        # Load the context volume curve
        #context_volume_curve(database)
        
        # Run checks to make sure the database is in a correct state
        #run_checks(database)

        #extract_volume_curves(database, ['AAPL', 'XOM', 'MSFT', 'IBM', 'WMT', 'CVX', 'GE', 'BRK/B', 'GOOG', 'JNJ', 'T', 'PG'])
        #database.commit()
    finally:
        if database:
            try:
                database.close()
                database = None
                # We need to wait for the connection to be correctly closed.
                time.sleep(1)
            except Exception, e:
                print e
                pass
        if java:
            java.kill()
            java = None
