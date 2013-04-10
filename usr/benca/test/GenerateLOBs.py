'''
Created on 19 juil. 2010

@author: benca
'''

from simep.utils import generateLOBFile
from simep.funcs.dbtools.securities_tools import SecuritiesTools 


# PARAMETERS
dates = ['20100524']
secids = [2]
tradis = [81]

# BUILDING STUFF
for tradid in tradis:
    for date in dates:
        for secid in secids:
            String = "CREATING LOB(%s, % 6d, % 2d)" %(date,secid, tradid)
            print String
            try:
                times = SecuritiesTools.get_trading_hours('sqlite', secid, tradid, date)
                generateLOBFile(date, secid, tradid, 0.00001, times[0], times[1], 1)
            except:
                String = "COULDN'T CREATE LOB(%s,%06d)" %(date,secid)
                print String
