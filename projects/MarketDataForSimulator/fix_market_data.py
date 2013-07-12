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
        fix_message += '52=%s' + separator % data.index[i].strftime("%Y%m%d-%H:%M:%S")
        fix_message += '55=%s' + separator % symbol
        fix_message += '207=SEPA'+ separator
        fix_message += '262=28875920180194' + separator     
        fix_message += '70=%s' + separator % ccy
        fix_message += '88=%s' + separator % isin
        
        # dynamic fields
        fix_message += '268=3' + separator
        
        # bid        
        fix_message += '269=0' + separator
        fix_message += '270=%f' + separator % data.ix[i]['bid']
        fix_message += '15=EUR' + separator
        fix_message += '271=%d' + separator % data.ix[i]['bid_size']
        fix_message += '273=%s' + separator % data.index[i].strftime("%H:%M:%S")
        fix_message += '110=1' + separator
        fix_message += '290=1' + separator
        
        # ask        
        fix_message += '269=1' + separator
        fix_message += '270=%f'  + separator % data.ix[i]['ask']
        fix_message += '15=EUR' + separator
        fix_message += '271=%d'  + separator% data.ix[i]['ask_size']
        fix_message += '273=%s'  + separator% data.index[i].strftime("%H:%M:%S")
        fix_message += '110=1' + separator
        fix_message += '290=1' + separator
        
        # price        
        fix_message += '269=2' + separator
        fix_message += '270=%f'  + separator % data.ix[i]['price']
        fix_message += '15=EUR' + separator
        fix_message += '271=%d' + separator % data.ix[i]['volume']
        fix_message += '272=%s'  + separator% data.index[i].strftime("%Y%m%d")
        fix_message += '273=%s' + separator % data.index[i].strftime("%H:%M:%S")
        fix_message += '110=1' + separator
        fix_message += '290=1' + separator

        fix_message += '10=0' + separator
        
        f.writelines(fix_message + '\n')
        
    f.close()
        
if __name__ == "__main__":
    export_level1_market_data("C:/Downloads/exports.fmd", 110, "13/05/2013")       
