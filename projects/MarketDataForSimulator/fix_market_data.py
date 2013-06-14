# -*- coding: utf-8 -*-

from lib.dbtools.connections import Connections
from lib.dbtools.read_dataset import ftickdb
from datetime import datetime, timedelta

def export_level1_market_data(filename, security_id, date):
    datestr = date.strftime('%d/%m/%Y') if isinstance(date, datetime) else date
    data = ftickdb(security_id = security_id, date = datestr)
    f = open(filename, 'w')
    for i in range(len(data.index)):
        # static part        
        fix_message = ''
        fix_message += '8=FIX.4.2|'
        fix_message += '35=W|'
        fix_message += '49=FIXMD_SRC|'
        fix_message += '56=FIXMD_DEST|'
        fix_message += '52=%s|' % data.index[i].strftime("%Y%m%d-%H:%M:%S")
        fix_message += '55=TRU|'
        fix_message += '207=JSE|'
        fix_message += '262=28875920180194|'        
        
        # dynamic fields
        fix_message += '268=3|'
        # bid        
        fix_message += '269=0|'
        fix_message += '270=%f|' % data.ix[i]['bid']
        fix_message += '15=EUR|'
        fix_message += '271=%d|' % data.ix[i]['bid_size']
        fix_message += '273=%s|' % data.index[i].strftime("%H:%M:%S")
        fix_message += '110=1|'
        fix_message += '290=1|'
        
        # ask        
        fix_message += '269=1|'
        fix_message += '270=%f|' % data.ix[i]['ask']
        fix_message += '15=EUR|'
        fix_message += '271=%d|' % data.ix[i]['ask_size']
        fix_message += '273=%s|' % data.index[i].strftime("%H:%M:%S")
        fix_message += '110=1|'
        fix_message += '290=1|'
        
        # price        
        fix_message += '269=2|'
        fix_message += '270=%f|' % data.ix[i]['price']
        fix_message += '15=EUR|'
        fix_message += '271=%d|' % data.ix[i]['volume']
        fix_message += '273=%s|' % data.index[i].strftime("%H:%M:%S")
        fix_message += '110=1|'
        fix_message += '290=1|'
        
        
    