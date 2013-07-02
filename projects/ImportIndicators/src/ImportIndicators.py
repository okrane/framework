'''
Created on 1 Jul 2013

@author: gpons
'''


from lib.dbtools.connections import Connections 
from datetime import datetime

def ImportIndicators(DataBase, l_indicator, perimeter, path):
    
    
    outfile = open('%s/security_indicator_dump.txt' %path,'w')
    Connections.change_connections('production')
    
    cursor, cxn = Connections.getCursor(DataBase)
    
    if perimeter == 'all':
        for indicator_id in l_indicator:
            query = "select ci.indicator_id, cii.name, ci.indicator_value, ci.security_id, ci.trading_destination_id ,erc.EXCHGID " \
            "from MARKET_DATA..ci_security_indicator ci " \
            "left join KGR..EXCHANGEREFCOMPL erc on (erc.EXCHANGE = ci.trading_destination_id) " \
            "left join MARKET_DATA..ci_indicator cii on (cii.indicator_id = ci.indicator_id) " \
            "where (erc.GLOBALZONEID = 1 or erc.GLOBALZONEID is NULL) and ci.indicator_id in (%s) " \
            "and (ci.trading_destination_id > 0 or ci.trading_destination_id is NULL) " \
            "order by ci.indicator_id, ci.security_id, ci.trading_destination_id" %(indicator_id)
    
            cursor.execute(query)
            result = cursor.fetchall()
            
            for line in result:
                if line[5] is None:
                    line[5] = 'AGGR'
                str_line  = ''
                for item in line:
                    str_line = "%s%s;" %(str_line, item)
                str_line = '%s\n' %str_line[:-1]
                outfile.write(str_line)
    
    outfile.close()

if __name__ == '__main__':
    
    db = 'MARKET_DATA'
    day = '../%s/security_indicator_dump.txt' %datetime.today().strftime('%Y%m%d')
    
#     l_indicator = ['1', '2', '3', '21', '22', '24', '25', '27', '29', '31', '33', '35', '73', '75', '37', '38', '39', '40']
    l_indicator = ['1', '2']
    
    perimeter = 'all'
    
    res = ImportIndicators(db, l_indicator, perimeter, path)
    