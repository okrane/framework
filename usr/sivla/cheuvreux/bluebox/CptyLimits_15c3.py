'''
Created on Jul 18, 2012

@author: syarc
'''

from cheuvreux import fidessa
from cheuvreux.fidessa import fidessadb
from cheuvreux.stdio.mail import HtmlEmail, Email
from cheuvreux.utils.dataset import Dataset, to_html, Float
from collections import defaultdict
import csv
import getopt
import gzip
import locale
import sys
from cheuvreux.utils.date import parse_date, previous_weekday, workday_range,\
    next_weekday
import datetime


locale.setlocale(locale.LC_ALL, '')
DB = fidessadb.getODBCConnection()

class Limit(Float):
    
    def __init__(self, value, change):
        Float.__init__(self, value, 0)
        self.change = change
        
    def __str__(self):
        return '$' + Float.__str__(self)

    def html(self):
        s = locale.format(('%%.%df' % self.precision), self.value, True)
        if self.change > 0:
            return '<span style="color: #008000"><b>$%s</b></span>' % (s)
        elif self.change < 0:
                return '<span style="color: #FF0000"><b>$%s</b></span>' % (s)
        else:
            return '$%s' % (s)
        
def load_file_for_date(date):
    '''
        Read counterparty limits from a file and puts
        all the version in a dictionnary : (COUNTERPARTY => list of different version)
    '''
    filename = fidessa.find_file('COUNTERPARTY_CONTROL.%s.psv', date)
    
    if not filename:
        print >> sys.stderr, 'No file name found for %s' % date
        sys.exit(-3)
    elif filename.endswith('.gz'):
        fd = gzip.open(filename, 'rb')
    else:
        fd = open(filename,'r')

    data = defaultdict(list)
    for line in csv.DictReader(fd, delimiter='|'):
        data[line['COUNTERPARTY_CODE']].append(line)
    
    fd.close()
    
    return data

def get_last_version_from_db(db):
    
    query = ''' SELECT COUNTERPARTY, VERSION FROM cpty_limits
                WHERE IS_LAST_VERSION = 1
            '''
    
    res = {}
    for row in db.select(query):
        res[row[0]] = row[1]
        
    return res

def get_last_version_from_data(data):
    '''
        From a dictionnary (data) as returned by load_file_for_date, this method 
        returns a new dictionnary with only the last version of each counterparty limit. 
    '''
    last_versions = {}
    for ctpy, versions in data.iteritems():
        max_version = None
        for version in versions:
            if not max_version or int(version['VERSION']) > int(max_version['VERSION']):
                max_version = version
        last_versions[ctpy] = max_version
        
    return last_versions


def init_table(date):
    '''
        Init the ctpy_limits database with the limits from the specified date.
        
        This method erased the content of the table, and populates it with
        the last version of each counterparty limits.
    
    '''
    
    # Clear everything
    DB.run('DELETE FROM cpty_limits')
    
    data = load_file_for_date(date)
    
    def data_generator(data):
        # extract last version
        max_versions = get_last_version_from_data(data)
        for ctpy, max_version in max_versions.iteritems():
            yield {'COUNTERPARTY': ctpy, 'ENTERED_BY': max_version['ENTERED_BY'],
                   'STRAIGHT_ORDER_CONS_LIMIT':  max_version['STRAIGHT_ORDER_CONSIDERATION_LIMIT'], 
                   'STRAIGHT_DAILY_CONS_LIMIT': max_version['STRAIGHT_DAILY_CONSIDERATION_LIMIT'], 
                   'DISC_ORDER_CONS_LIMIT': max_version['DISCRETIONARY_ORDER_CONSIDERATION_LIMIT'], 
                   'DISC_DAILY_CONST_LIMIT': max_version['DISCRETIONARY_DAILY_CONSIDERATION_LIMIT'],
                   'COMBINED_ORDER_CONS_LIMIT': max_version['COMBINED_ORDER_CONSIDERATION_LIMIT'], 
                   'COMBINED_DAILY_CONS_LIMIT': max_version['COMBINED_DAILY_CONSIDERATION_LIMIT'], 
                   'VERSION': int(max_version['VERSION']),
                   'IS_LAST_VERSION' : 1, 
                   'STATUS': max_version['STATUS'],
                   'VERSION_DATE': date }
            
    cols = {'COUNTERPARTY': '', 'ENTERED_BY': '', 'STRAIGHT_ORDER_CONS_LIMIT': '', 
           'STRAIGHT_DAILY_CONS_LIMIT': '', 'DISC_ORDER_CONS_LIMIT': '', 'DISC_DAILY_CONST_LIMIT': '',
           'COMBINED_ORDER_CONS_LIMIT': '', 'COMBINED_DAILY_CONS_LIMIT': '', 'VERSION': '',
           'IS_LAST_VERSION' : '', 'STATUS': '', 'VERSION_DATE': '' }
    stmt = ('INSERT INTO "cpty_limits" (' + ','.join(cols.keys()) + ') VALUES ('
                 + ','.join(['?']*len(cols)) + ')' )
    DB.executemany(stmt, [d.values() for d in data_generator(data)])
    
def insert_last_version(db, version, date):
    '''
        Helper function to insert a new record in the database
        
        The new record is automatically set as the last verion
    '''
    
    if not version:
        raise ValueError('No Data')
    
    record = {'COUNTERPARTY': version['COUNTERPARTY_CODE'], 'ENTERED_BY': version['ENTERED_BY'],
                   'STRAIGHT_ORDER_CONS_LIMIT':  version['STRAIGHT_ORDER_CONSIDERATION_LIMIT'], 
                   'STRAIGHT_DAILY_CONS_LIMIT': version['STRAIGHT_DAILY_CONSIDERATION_LIMIT'], 
                   'DISC_ORDER_CONS_LIMIT': version['DISCRETIONARY_ORDER_CONSIDERATION_LIMIT'], 
                   'DISC_DAILY_CONST_LIMIT': version['DISCRETIONARY_DAILY_CONSIDERATION_LIMIT'],
                   'COMBINED_ORDER_CONS_LIMIT': version['COMBINED_ORDER_CONSIDERATION_LIMIT'], 
                   'COMBINED_DAILY_CONS_LIMIT': version['COMBINED_DAILY_CONSIDERATION_LIMIT'], 
                   'VERSION': int(version['VERSION']),
                   'IS_LAST_VERSION' : 1, 
                   'STATUS' : version['STATUS'],
                   'VERSION_DATE': date }
    stmt = ('INSERT INTO "cpty_limits" (' + ','.join(record.keys()) + ') VALUES ('
                 + ','.join(['?']*len(record)) + ')' )
    
    db.executemany(stmt, [record.values()])
      
def fix_last_version():
    '''
        Fix counterparty for which there is no last version
        
        It will make the higher version the last version
    '''
    
    query = ''' select distinct counterparty from cpty_limits 
               where counterparty not in (
                   select counterparty from cpty_limits where is_last_version = 1)
            '''
    
    last_version_query = ''' update cpty_limits
                             set is_last_version = 1
                             where version = (SELECT max(version) FROM cpty_limits WHERE counterparty = '%s')
                             and counterparty = '%s'
                         '''
    
    for row in DB.select(query):
        DB.run(last_version_query % (row[0], row[0]))
        

def get_version_for_cpty(data, cpty, version):
    for versions in data[cpty]:
        if int(versions['VERSION']) == version:
            return versions
        
    return None

def update_limits(date, force=False):
    '''
        Update the cpty_limits database.
        
        For each new counterparty, a new record is inserted.
        For counterparty which have a newer version, a new record is inserted 
        and the old is invalidated
        
        If 'force' is True, it will erase all version from that date
        
        
    '''
       
    print 'Process %s' % date
       
    if force:
        DB.run("DELETE FROM cpty_limits WHERE version_date = '%s' " % date)
    
    fix_last_version()
        
    db_version = get_last_version_from_db(DB)
    
    all_data = load_file_for_date(date)
    data = get_last_version_from_data(all_data)
    
    for cpty, last_version_details in data.iteritems():
        if cpty not in db_version:
            print 'New counterparty %s entered by %s on %s' % (cpty, last_version_details['ENTERED_BY'], date)
            version = 1
            last_version = int(last_version_details['VERSION'])
            while version <= last_version:
                try:
                    insert_last_version(DB, get_version_for_cpty(all_data, cpty, version), date)
                except ValueError, e:
                    print e, cpty, version
                version += 1
                
        elif int(last_version_details['VERSION']) > db_version[cpty]:
            DB.run("UPDATE cpty_limits SET IS_LAST_VERSION = 0 WHERE COUNTERPARTY = '%s' AND IS_LAST_VERSION = 1" % (cpty))
            
            # Process each version between db_last_version and the file last version
            version = db_version[cpty] + 1
            last_version = int(last_version_details['VERSION'])
            while version <= last_version:
                version_details = get_version_for_cpty(all_data, cpty, version)
                if not version_details:
                    print 'Warning: no data found for %s version: %d' % (cpty, version)
                if version_details['ENTERED_BY'] != 'OMAR':
                    print 'New version for %s by %s on %s (%d -> %d)' % (cpty, version_details['ENTERED_BY'], date, 
                                                                         db_version[cpty], int(version_details['VERSION']))
                #print 'Process %d for %s' % (version, cpty)
                try:
                    insert_last_version(DB, version_details, date)
                except ValueError, e:
                    print e, cpty, version
                version += 1
            
def find_previous_version(cpty, version):
    query = ''' select * from cpty_limits
                where counterparty = '%s' and version < %d
                order by version desc
            ''' % (cpty, version)
    
    return DB.selectOne(query)

def compute_changes(previous, last):
    if not previous:
        # new record - nothing to do
        return [last[0], 
                Limit(last[2],last[2] > 0), Limit(last[3],last[3] > 0), 
                Limit(last[4],last[4] > 0), Limit(last[5],last[5] > 0), 
                Limit(last[6],last[6] > 0), Limit(last[7],last[7] > 0), 
                'Entered by ' + last[1].replace('@CRAG.US',''), last[10]]
        
    # We need to compute differences:
    
    directed_order_change = 1.0 * (last[2] - previous[2]) / previous[2] if previous[2] > 0 else last[2] > 0
    directed_daily_change = 1.0 * (last[3] - previous[3]) / previous[3] if previous[3] > 0 else last[3] > 0
    
    care_order_change = 1.0 * (last[4] - previous[4]) / previous[4] if previous[4] > 0 else last[4] > 0
    care_daily_change = 1.0 * (last[5] - previous[5]) / previous[5] if previous[5] > 0 else last[5] > 0
    
    combined_order_change = 1.0 * (last[6] - previous[6]) / previous[6] if previous[6] > 0 else last[6] > 0 
    combined_daily_change = 1.0 * (last[7] - previous[7]) / previous[7] if previous[7] > 0 else last[7] > 0
        
    if (abs(directed_order_change) > 0 or
       abs(directed_daily_change) > 0 or
       abs(care_order_change) > 0 or
       abs(care_daily_change) > 0 or
       abs(combined_order_change) > 0 or
       abs(combined_daily_change) > 0) :
        return [last[0],
                Limit(last[2],directed_order_change),
                Limit(last[3],directed_daily_change),
                Limit(last[4],care_order_change),
                Limit(last[5],care_daily_change),
                Limit(last[6],combined_order_change),
                Limit(last[7],combined_daily_change), 
                'Updated by ' + last[1].replace('@CRAG.US',''), last[10]]
        
    return None

def report_changes(start, end, output, counterparty=None):
    ''' Send an email with the changes in counterparty limits '''
    
    
    # First part is changes or new:
    query = ''' SELECT * FROM cpty_limits 
                WHERE version_date >= '%s' AND version_date <= '%s'
                  AND entered_by != 'OMAR'
            ''' % (start, end)
    
    if counterparty:
        query += " AND counterparty = '%s'" % counterparty
            
    ds = Dataset(['Client', 'Directed Order', 'Directed Daily', 'Care Order', 'Care Daily', 'Combined Order', 'Combined Daily', 'Entered by', 'Date'])
    for row in DB.select(query):
        previous_version = find_previous_version(row[0], row[8])
        change = compute_changes(previous_version, row)
        if change:
            ds.append(change)
    ds.set_extra_style(['', "style='text-align:right;'"])
    ds.sort('Client')
    
    # Second part is deletion 
    query = ''' SELECT * FROM cpty_limits
                WHERE version_date >= '%s' AND version_date <= '%s'
                 AND entered_by != 'OMAR' AND STATUS = 'C'
            ''' % (start, end)
            
    if counterparty:
        query += " AND counterparty = '%s'" % counterparty
            
    rows = DB.select(query)
    deletion = Dataset(['Client', 'Entered by', 'Date'])
    deletion.set_extra_style(['', "style='text-align:right;'"])

    if len(rows) > 0:
        for row in rows:
            deletion.append([row[0], row[1].replace('@CRAG.US',''), row[10]])
            
        deletion.sort('Client')
    
    if len(ds) <= 0 and len(rows) <= 0:
        subject = 'No Credit limit changes %s' % start
            
        if end != start:
            subject += ' - %s' % end
            
        if counterparty:
            subject += ' for ' + counterparty
            
        if isinstance(output, HtmlEmail):
            output.set_subject(subject)
        else:
            print >> output, subject
            
        return
    
    if isinstance(output, HtmlEmail):
        subject = 'Credit limit Changes %s' % start
        
        if end != start:
            subject += ' - %s' % end
            
        if counterparty:
            subject += ' for ' + counterparty
            
        output.set_subject(subject)
        if len(ds) > 0:
            print >> output, to_html(ds)
            
        if len(deletion) > 0:
            print >> output, '<h2>Counterparties deleted</h2>'
            print >> output, to_html(deletion)
    else:
        if len(ds) > 0:
            print >> output, str(ds)
        if len(deletion) > 0:
            print >> output, 'Counterparties deleted'
            print >> output, str(deletion)
        
def usage(basename):
    print "usage: %s [OPTIONS] [COMMANDS]" % basename;
    print ""
    print "COMMANDS:"
    print "  -h, --help         \t Print this help message"
    print "  -r, --report       \t Send a report to show changes over a period"
    print "  -i, --init         \t Init the database with limit from a specific date"
    print "  -u, --update       \t Process credit limit changes for a period"
    print "  -x, --fix          \t Fix last version column"
    print ""
    print "OPTIONS:"
    print "  -d, --date=<DATE>  \t Specify the date (default is last working day)"
    print "                     \t could be a range START:END (format is YYYYMMDD)"
    print "  -f, --force        \t Force the computation (erase previous data)"
    print "  -e, --email=<DEST> \t Send report by email"
    print "  -c, --ctpy=<CPTY>  \t Filter on a counterparty"

def get_last_database_date():
    row = DB.select('select max(version_date) from cpty_limits')
    if row:
        return datetime.datetime.strptime(row[0][0],'%Y-%m-%d').date()
    else:
        return previous_weekday()

def main(argv):
    try:
        opts, args = getopt.getopt(argv[1:], 'hd:fe:iruxc:',
                                  ['help', 'date=', 'force',
                                   'email=', 'report', 'init','update',
                                   'fix','cpty'])
    except getopt.GetoptError:
        usage(argv[0])
        sys.exit(-1)
        
    # Different modes:
    init = report = update = fix_last = False
    force = False
    output = sys.stdout
    start = end = None
    cpty = None
        
    for opt, val in opts:
        if opt in ("-h", "--help"):
            usage(argv[0])
            sys.exit(0)
        elif opt in ('-f', '--force'):
            force = True
        elif opt in ('-e', '--email'):
            output = HtmlEmail("smtpnotes",font_size='8pt')
            output.set_sender("Lionel Massoulard <lmassoulard-ext@cheuvreux.com>")
            output.set_dest(val)
        elif opt in ('-d', '--date'):
            if ':' in val:
                start, end = val.split(':')
                start, end = parse_date(start), parse_date(end)
            else:
                start = end = parse_date(val)
              
                
        elif opt in ('-i', '--init'):
            init = True
        elif opt in('-r', '--report'):
            report = True
        elif opt in ('-u', '--update'):
            update = True
        elif opt in ('-x', '--fix'):
            fix_last = True
        elif opt in ('-c', '--cpty'):
            cpty = val
            
    if not init and not report and not update and not fix_last:
        usage(argv[0])
        sys.exit(-2)
        
    if fix_last:
        print >> output, "Fixing last version"
        fix_last_version()
        
    if init:
        if not start:
            start = previous_weekday()
        init_table(start.strftime('%Y%m%d'))
        
    if update:
        if not start:
            start = next_weekday(get_last_database_date())
            end = previous_weekday()
            
            start = min(end, start)
            
        if start != end:
            for date in workday_range(start, end):
                update_limits(date.strftime('%Y%m%d'), force)
        else:
            update_limits(start.strftime('%Y%m%d'), force)
        
    if report:
        report_changes(start, end, output, counterparty = cpty)
        
        if isinstance(output, HtmlEmail):
            output.flush()

    
if __name__ == '__main__':
    main(sys.argv)
#    date = '20120718'
#    output = HtmlEmail('smtpnotes')
#    output.set_dest('sarchenault@cheuvreux.com')
#    output.set_type('html')
#    output.set_subject('Credit limit changes')
#    #update_limits('20120718',force=True)
#    report_changes(date, output)
