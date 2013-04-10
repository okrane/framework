from simep.funcs.data.pyData import pyData, convertStr
from usr.dev.sivla.agents.LobRecorder import LobRecorder
from datetime import datetime


def split_list(strlst):
    return [float(x) for x in strlst[1:-1].split(',')]
    
x = pyData('csv', filename = 'C:/st_sim/usr/dev/sivla/scenarii/SBB/LobRecorder/ACCP_PA_004/DAY_20100104/01a1f71dcfd6f9e21671a55bc121818b.csv')
x.date = [datetime(year   = convertStr(a[0:4]),  
                   month  = convertStr(a[5:7]), 
                   day    = convertStr(a[8:10]), 
                   hour   = convertStr(a[11:13]),
                   minute = convertStr(a[14:16]),
                   second = convertStr(a[17:19]), 
                   microsecond = 0) for a in x.date]

x['ask_price'] = [split_list(a) for a in x['ask_price']]
x['bid_price'] = [split_list(a) for a in x['bid_price']]
x['ask_size'] = [split_list(a) for a in x['ask_size']]
x['bid_size'] = [split_list(a) for a in x['bid_size']]

print x

params = {'start': datetime(2010, 01, 04, 9, 30, 00), 
          'end':   datetime(2010, 01, 04, 10, 00, 00)}

data_interval = x.interval(params['start'], params['end'])
print data_interval
LobRecorder.run_visualization(data_interval, title = 'Model: ROB ')


a = '[20, 30]'
print split_list(a)


    