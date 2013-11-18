# -*- coding: utf-8 -*-

from lib.dbtools.connections import Connections
from lib.dbtools.read_dataset import ftickdb
from lib.dbtools.get_repository import *
from datetime import datetime, timedelta
from CSymData import flids
#import curses

def export_level1_market_data(filename, security_id, date):
    separator = "|"
    symbol = 'FTE.PA'
    exchg = exchangeinfo(security_id = security_id)
    main_destination = exchg[exchg['EXCHANGETYPE'] == 'M']['EXCHGID'][0]    
    isin = convert_symbol(value = security_id, source = 'security_id', dest = 'isin', exchgid = main_destination)    
    ccy = currency(security_id = security_id)
    datestr = date.strftime('%d/%m/%Y') if isinstance(date, datetime) else date
    data = ftickdb(security_id = security_id, date = datestr)
    f = open(filename, 'w')
    for i in range(len(data.index)):
        # static part        
        fix_message = ''
        fix_message += '8=FIX.4.2' + separator
        fix_message += '9=0' + separator
        fix_message += '35=W'+ separator
        fix_message += '49=FIXMD_SRC'+ separator
        fix_message += '56=FIXMD_DEST'+ separator
        fix_message += '52=' + data.index[i].strftime("%Y%m%d-%H:%M:%S") + separator
        fix_message += '55=' + symbol + separator
        fix_message += '207=SEPA'+ separator
        fix_message += '262=28875920180194' + separator     
        fix_message += '70=' + ccy + separator
        fix_message += '88=' + isin + separator
        
        # dynamic fields
        fix_message += '268=3' + separator
        
        # bid        
        fix_message += '269=0' + separator
        fix_message += '270=' + str(data.ix[i]['bid']) + separator
        fix_message += '15=EUR' + separator
        fix_message += '271=' + str(data.ix[i]['bid_size']) + separator
        fix_message += '273=' + data.index[i].strftime("%H:%M:%S") + separator
        fix_message += '110=1' + separator
        fix_message += '290=1' + separator
        
        # ask        
        fix_message += '269=1' + separator
        fix_message += '270='  + str(data.ix[i]['ask']) + separator
        fix_message += '15=EUR' + separator
        fix_message += '271=' + str(data.ix[i]['ask_size']) + separator
        fix_message += '273=' + data.index[i].strftime("%H:%M:%S") + separator
        fix_message += '110=1' + separator
        fix_message += '290=1' + separator
        
        # price        
        fix_message += '269=2' + separator
        fix_message += '270=' + str(data.ix[i]['price'])  + separator
        fix_message += '15=EUR' + separator
        fix_message += '271=' + str(data.ix[i]['volume']) + separator
        fix_message += '272=' + data.index[i].strftime("%Y%m%d")  + separator
        fix_message += '273=' + data.index[i].strftime("%H:%M:%S") + separator
        fix_message += '110=1' + separator
        fix_message += '290=1' + separator

        fix_message += '10=0' + separator
        
        f.writelines(fix_message + '\n')
        
    f.close()

def convert_to_fix_format(source, destination):
   # print flids.keys()      
    separator = "|"    
    g = open(source, 'r')    
    #f = open(destination, 'w')
    unique_keys = []
    for line in g.readlines():
        #print line
        tokens = line.split('|')       
        symbol = tokens[0].split(":")[1]
        
        values = {} 
        for e in tokens[1:-1]:
            tk    = e.split("=")
            if tk[0] != "RECORD_TIME" and int(tk[0]) not in unique_keys : unique_keys.append(int(tk[0]))
            values[tk[0]] = tk[1]
        
        seconds = int(values['RECORD_TIME']) / 1000
        milliseconds = int(values['RECORD_TIME']) % 1000        
        d = datetime.fromtimestamp(seconds) + timedelta(microseconds = 1000 * milliseconds)
        print d    
        
        # bid
        #if values.has_key()
        
        #fix_message += '%d=%s' % (key, value) + separator
        
        #f.writelines(fix_message + '\n')
    
    #f.close()
    print unique_keys
    for k in unique_keys:
        if k in flids.keys():
            print flids[k]
        else:
            print k, "inexistant"
    g.close()
        
def extract_close_auction(source, destination):
     
    g = open(source, 'r')    
    f = open(destination, 'w')
    
    for line in g.readlines():        
        start = False
        tokens = line.split('|')
        symbol = tokens[0].split(":")[1]
        
        values = {} 
        for e in tokens[1:-1]:
            tk    = e.split("=")
            values[tk[0]] = tk[1]
        
        seconds = int(values['RECORD_TIME']) / 1000        
        d = datetime.fromtimestamp(seconds)        
        print d
        
        if (d.hour >= 17 and d.minute >= 30) and not start:
            print "Starting Close Auction by time"
            start = True
            
        if d.hour >= 17 and values.has_key('1308') and not start:
            print "Starting Close Auction by feed"
            start = True
            
        if start:
            f.writelines(line)
    
    g.close()
    f.close()
        
    
        
if __name__ == "__main__":
#     Connections.change_connections('production')
    #export_level1_market_data("C:/Downloads/exports.fmd", 110, "13/05/2013")
   # print 6/0
    query = "select top 10 * from SECURITY"
    from lib.dbtools.connections import Connections
    Connections.change_connections('production')
    result = Connections.exec_sql('KGR', query)
    print result
    #convert_to_fix_format('data_sym_record.26920', 'close_auction.07.10.13')       
