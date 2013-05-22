# -*- coding: utf-8 -*-
"""
Created on Wed Apr 03 10:50:05 2013

@author: nijos
"""

""" 
-------------------------------------------------------------------------------
IMPORT
-------------------------------------------------------------------------------
""" 
from simep.funcs.dbtools.connections import Connections
import numpy as np
import datetime as dt
import time as time

""" 
-------------------------------------------------------------------------------
params
-------------------------------------------------------------------------------
""" 

Connections.change_connections('production_copy')


#--input
from_date='20/03/2012'
to_date='28/03/2012'
secids_array=np.array([2,110])


#--transform
from_dt=dt.datetime.strptime(from_date, '%d/%m/%Y')
to_dt=dt.datetime.strptime(to_date, '%d/%m/%Y')

dateList =[ from_dt + dt.timedelta(days=x) for x in range(0,1+(to_dt-from_dt).days) ]
weekdateList=[x for x in dateList if x.weekday() not in [5,6]]




#--transform
date=weekdateList[1]

"""
#  orderbook
"""

"""
infos_select=[('date','date'),
            ('time','convert(varchar,time,108)'),
            ('microseconds','microseconds'),
            ('side','side'),
            ('depth','depth'),
            ('price','price'),
            ('size','size'),
            ('mmk_number','mmk_number'),
            ('image','image')]

connect_str='BSIRIUS'
select_str=''.join([x[1]+',' for x in infos_select])
select_str=select_str[0:-1]

if date.month>9:
    tmp_month=str(date.month)
else:
    tmp_month='0'+str(date.month)

server_str='tick_db_'+tmp_month+'o_'+str(date.year)
del tmp_month

db_str='orderbook_update'

end_req = ' where date = "%s" and security_id=%d order by date,time,microseconds' % (dt.datetime.strftime(date, '%Y%m%d'),secids_array[1])
"""
  
"""
# stock deals
"""

"""
connect_str='BSIRIUS'

infos_select=[('date','date'),
            ('time','convert(varchar,time,108)'),
            ('microseconds','microseconds'),
            ('price','price'),
            ('trading_destination_id','trading_destination_id'),
            ('volume','size'),
            ('sell','overbid'),
            ('buy','overask'),
            ('bid','bid'),
            ('ask','ask'),
            ('bid_size','bid_size'),
            ('ask_size','ask_size'),
            ('cross','cross'),
            ('auction','auction'),
            ('opening_auction','opening_auction'),
            ('closing_auction','closing_auction'),
            ('trading_at_last','trading_at_last'),
            ('trading_after_hours','trading_after_hours'),
            ('interpolated','interpolated'),
            ('deal_id','deal_id')]

select_str=''.join([x[1]+',' for x in infos_select])
select_str=select_str[0:-1]

if date.month>9:
    tmp_month=str(date.month)
else:
    tmp_month='0'+str(date.month)

server_str='tick_db_'+tmp_month+'d_'+str(date.year)
del tmp_month

db_str='deal_archive'

end_req = ' where date = "%s" and security_id=%d order by date,time,microseconds' % (dt.datetime.strftime(date, '%Y%m%d'),secids_array[1])
""" 
  
  
"""
# index price
"""

connect_str='BSIRIUS'

infos_select=[('date','date'),
            ('time','convert(varchar,time,108)'),
            ('price','price'),
            ('tickprice_id','tickprice_id')]

select_str=''.join([x[1]+',' for x in infos_select])
select_str=select_str[0:-1]

if date.month>9:
    tmp_month=str(date.month)
else:
    tmp_month='0'+str(date.month)

server_str='tick_db_'+tmp_month+'d_'+str(date.year)
del tmp_month

db_str='indice'

end_req = ' where date = "%s" and indice_id=%d order by date,time' % (dt.datetime.strftime(date, '%Y%m%d'),1)
  
  


"""
# trading daily
"""

""" 
connect_str='BSIRIUS'
select_str=' * '
server_str='tick_db'
db_str='trading_daily' 
end_req = ' where date = "%s" and security_id=%d order by date,trading_destination_id' % (dt.datetime.strftime(date, '%Y%m%d'),secids_array[1])
""" 

               
query= 'select %s from %s..%s %s' % (select_str,server_str,db_str,end_req)


t0=time.clock()
data = Connections.exec_sql(connect_str, query)
print 'Request duration : %3.2f (sec)' % (time.clock()-t0)


db = Sybase.connect('BSIRIUS',u'batch_tick',u'batick',u'temp_works')

""" 
-------------------------------------------------------------------------------
MAIN
-------------------------------------------------------------------------------
""" 

if __name__=='__main__':
    print 'test1'
