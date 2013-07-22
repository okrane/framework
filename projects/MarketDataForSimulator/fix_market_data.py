# -*- coding: utf-8 -*-

from lib.dbtools.connections import Connections
from lib.dbtools.read_dataset import ftickdb
from lib.dbtools.get_repository import *
from datetime import datetime, timedelta
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
        
if __name__ == "__main__":
#     Connections.change_connections('production')
    export_level1_market_data("C:/Downloads/exports.fmd", 110, "13/05/2013")       
