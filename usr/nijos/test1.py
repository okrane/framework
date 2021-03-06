# -*- coding: utf-8 -*-
"""
Spyder Editor

This temporary script file is located here:
C:\Documents and Settings\nijos\.spyder2\.temp.py
"""

""" 
-------------------------------------------------------------------------------
IMPORT
-------------------------------------------------------------------------------
""" 

import numpy as np
import scipy 
import matplotlib.pyplot as plt
from datetime import *
import time
from scipy.io  import matlab
import pandas as pd
from lib.data.matlabutils import *
import lib.data.st_data as st_data
import lib.dbtools.get_repository as get_repository
import lib.dbtools.read_dataset as read_dataset
from lib.data.ui.Explorer import Explorer
import lib.dbtools.get_algodata as get_algodata
import lib.tca.get_algostats as get_algostats
from lib.dbtools.connections import Connections

# data_seq=pd.read_csv('C:/test.csv')

""" 
-------------------------------------------------------------------------------
USE OF MATLABUTILS
-------------------------------------------------------------------------------
""" 
#data=read_dataset.ftickdb(security_id=110,date='17/05/2013')
#data=read_dataset.bic(security_id=110,date='17/05/2013')



data=read_dataset.trading_daily(start_date='10/07/2013',end_date='15/07/2013')




# uniquexte
#data=pd.DataFrame({'A' : np.array([1,2,1,3,2,3,2]), 'B' :np.array([2,2,2,3,2,3,2])})
#res1drow=uniqueext(data['A'].values,return_index=True,return_inverse=True)
#res1dcol=uniqueext(data[['A']].values,return_index=True,return_inverse=True)
#res2d=uniqueext(data[['A','B']].values,return_index=True,return_inverse=True,rows=True)
#    
## ismember
#data=pd.DataFrame({'A' : np.array([1,2,1,3,2,3,2]), 'B' :np.array([2,2,2,3,2,3,2])})
#res1d=ismember(data['A'].values,data['B'].values)
#data['A'].values[res1d[1][res1d[0]]]

""" 
-------------------------------------------------------------------------------
USE OF get repositrory
-------------------------------------------------------------------------------
""" 
# locall_tz_from
get_repository.local_tz_from(security_id=[0,110])
get_repository.exchangeid2tz(exchange_id=get_repository.tdidch2exchangeid(td_id=[1,4,61,4]))


""" 
-------------------------------------------------------------------------------
LOAD OF market data
-------------------------------------------------------------------------------
""" 
data=read_dataset.ft(security_id=10735,date='11/03/2013')
data=read_dataset.ft(security_id=110,date='11/03/2013')
data=read_dataset.histocurrencypair(start_date='01/01/2013',end_date='01/05/2013')
data=read_dataset.histocurrencypair(start_date='01/05/2013',end_date='10/05/2013',currency=['GBX','SEK'])
data=read_dataset.lastrate2ref(currency='GBX',date='01/05/2013')
""" 
-------------------------------------------------------------------------------
STRING
-------------------------------------------------------------------------------
""" 
x = '%d:%5.2f - %s' % (1, 3.14,'e')
    
""" 
-------------------------------------------------------------------------------
DATE
-------------------------------------------------------------------------------
""" 
day1 = dt.datetime(2006, 12, 1)
day2 = dt.datetime(2007, 3, 23)
delta = day2 - day1 # timedelta object
print delta.days
  
# datestr
dt.datetime.strftime(day1, '%Y%m%d')
dt.datetime.strftime(day1, '%d/%m/%Y')
  
#datenum
date_str=dt.datetime.strftime(day1, '%Y%m%d')
dt.datetime.strptime(date_str, '%Y%m%d') 
  
# handling timezone
# SEE http://docs.python.org/2/library/datetime.html
  
  
""" 
-------------------------------------------------------------------------------
ARRAY = matrix
-------------------------------------------------------------------------------
""" 
# -- create
x = np.random.rand(10,2)
y = np.random.rand(10,2)
size(x)
ndim(x)
shape(x)

#-operation
x*y
x.T
sum(x, 1)

#----get
x[0,0]
x[:,1]
x[-1,0]


""" 
-------------------------------------------------------------------------------
LIST = cell
-------------------------------------------------------------------------------
""" 
#-create
x = [0, 1, 2, 3.0, 'baba']
y = [4, 5, x]

#----get
y[2][2]
y[2][4][3]
len(y)

#--- operation
x.append('ab')


xlist = [0, 1, 2, 3]
ylist = [x*2 for x in xlist]


""" 
-------------------------------------------------------------------------------
TUPLES = read only list
-------------------------------------------------------------------------------
""" 
x1 = (1, 2, 3)


""" 
-------------------------------------------------------------------------------
DICTIONNARY = struct
-------------------------------------------------------------------------------
""" 
xdict = {1:'a', 'b':23,'c':{53:'x', 12:0}}
xdict.items()
xdict.keys()

#----get
xdict['b']
xdict['c'][53]


""" 
-------------------------------------------------------------------------------
Boucle if
-------------------------------------------------------------------------------
""" 
x=3
if x < 2:
    print 'x is small'
elif x < 4:
    print 'x is not so small'
else:
    print 'x is large'
# break/ continue/ pass

""" 
-------------------------------------------------------------------------------
Boucle for
-------------------------------------------------------------------------------
""" 
# avec des int
for i in range(0,12):
    print i
# avec des enumerate
items = ['aa', 'bb', 'cc']
for i, item in enumerate(items):
    print i, item
 
# avec des tupples  
xlist = r_[0:4]
ylist = ['a', 'b', 'c', 'd']
for x, y in zip(xlist, ylist):
    print x, y  
    
# avec des list
xlist = [0, 1, 2, 3]
ylist = [x*2 for x in xlist]


""" 
-------------------------------------------------------------------------------
PLOT
-------------------------------------------------------------------------------
""" 

plt.figure()
x = np.r_[0:2*np.pi:0.01]
plt.plot(x, np.sin(x))




""" 
-------------------------------------------------------------------------------
CLASS
-------------------------------------------------------------------------------
""" 
# basic
class MyObject(object):
    def __init__(self):
        self.x = 3
    def print_x(self):
            print self.x
    def add_to_x(self, y):
        self.x += y
        
myobj = MyObject()
myobj.print_x()
myobj.add_to_x(5)
myobj.print_x()

# static
class myobjstatic:
    x=3
    @staticmethod
    def print_x():
            print myobjstatic.x
    @staticmethod
    def add_to_x(y):
        myobjstatic.x += y    
  
myobjstatic.print_x()
myobjstatic.add_to_x(2)
myobjstatic.print_x()  


""" 
-------------------------------------------------------------------------------
FUNCTION
-------------------------------------------------------------------------------
""" 
def multiply_by_two(x, and_add=0):
    xnew = x*2.
    xnew += and_add

    return xnew
    
y = multiply_by_two(3)    


""" 
-------------------------------------------------------------------------------
LOAD FILES
-------------------------------------------------------------------------------
""" 
# matlab
matfile='C:\st_repository\get_orderbook_limit\orderbook_by_destination\depth_max=1\FTE.PA_4\2012_08_14.mat'
matfile='C:\data_to_load.mat'
mat = matlab.loadmat(matfile)


""" 
-------------------------------------------------------------------------------
PANDAS
-------------------------------------------------------------------------------
"""

""" 
index from dataframe with nan
"""
np.nonzero(map(lambda x : (not isinstance(x,basestring)) and (np.isnan(x)),var_occ.values))[0]

""" 
CREATE
"""
# from a dictionnary of series
data = pd.DataFrame({'a' : pd.Series([1., 2., 3.,7.2]),
     'b' : pd.Series([1., 2., 3., 4.])})
     

# from a dictionnary of series (2) in a tuimeseries
timedata = pd.date_range('17/3/2013 10:00', '17/3/2013 11:00', freq='60s')
tmp=np.random.rand(len(timedata))
data=pd.DataFrame({'bid' : 10+tmp, 'ask' : 10+tmp+np.abs(tmp)},index=timedata)

# from a mongodb
def create_dict(dtime):
   return {'datetime': dtime, 'quantity': np.random.randint(1, 10), 'price': np.random.randint(1,10)}

documents = [create_dict(i) for i in pd.date_range('17/3/2013 10:00', '17/3/2013 11:00', freq='5Min')]
data = pd.DataFrame.from_records(documents, columns=['datetime', 'quantity', 'price'], index='datetime')

""" 
VIEW
"""
data.to_excel('C:/viewdf.xlsx')

""" 
TRANSFORM
"""
timedata = pd.date_range('17/3/2013 10:00', '17/3/2013 11:00', freq='60s')
tmp=np.random.rand(len(timedata))
data=pd.DataFrame({'bid' : 10+tmp, 'ask' : 10+tmp+np.abs(tmp)},index=timedata)



# add
data["mid"]=0.5*(data["bid"]+data["ask"])

# apply formula (1) + add
def tmp_appf(x):
    mid=0.5*(x["bid"]+x["ask"])
    mid3=0.5*(x["bid"]+x["ask"])
    return pd.Series(dict(mid2=mid,mid3=mid3))

data=data.join(data.apply(tmp_appf,axis=1))

# filtering (1) : by a Dataframe boolean
dataf1=data[(data['bid']>10.5) & (data['mid']<11)]

# rename colnames
data = data.rename(columns={'ask': 'ask1'})
data = data.rename(columns={'ask1': 'ask'})

# drop colnames
data["mid"]=0.5*(data["bid"]+data["ask"])
data["mid2"]=0.5*(data["bid"]+data["ask"])
data=data.drop(['mid','mid2'],axis=1)

# get the values in a numpy array
data2=data.ix[0]
a=data2[['bid','ask']].values
b=data[['bid']].values

uniqueext(a,return_index=False,return_inverse=False,rows=False)

""" 
GEt elements
"""
data = pd.DataFrame({'a' : pd.Series([1., 2., 3.,7.2]),
     'b' : pd.Series([1., 2., 3., 4.])})
# columns
data['a']    
data['a'] [1] 
data[['a','b']]   
# rows vy index  
data.ix[0]
# colnames ibn a list
data.columns.tolist()
# index
data.index


"""
rolling
"""
data=read_dataset.ft(security_id=110,date='13/03/2013')
datevals=np.array([x.to_datetime() for x in data.index])
vals_rolling_mean=[np.mean(data['volume'].values[(datevals>=x) & (datevals<(x + timedelta(seconds=60)))]) for x in datevals]

    
"""
Aggregation dataframe
"""
data=read_dataset.ft(security_id=110,date='13/03/2013')

convertTime(data.index[0],step_sec=60,out_mode='grid')
a=gridTime(date=data.index[0:1000],step_sec=600,out_mode='grid')

a=gridTime(start_time=datetime.strptime('10/03/2013 10:03:02', '%d/%m/%Y %H:%M:%S'),
           end_time=datetime.strptime('10/03/2013 10:08:10', '%d/%m/%Y %H:%M:%S'),step_sec=600,out_mode='grid')
a=gridTime(start_time=datetime.strptime('10/03/2013 10:03:02', '%d/%m/%Y %H:%M:%S'),
           end_time=datetime.strptime('10/03/2013 10:28:10', '%d/%m/%Y %H:%M:%S'),step_sec=600,out_mode='ceil',tz='Europe/London')
a=gridTime(start_time=data.index[0].to_datetime(),
           end_time=data.index[1000].to_datetime(),step_sec=600,out_mode='ceil')


grouped=data.groupby([gridTime(date=data.index,step_sec=600,out_mode='ceil'),'auction'])
#grouped=data.groupby([,'auction'])
data_agg=pd.DataFrame([{'date': k[0],
                        'auction': k[1],
                        'date_close' :v.index.max(),
                        'volume': v.volume.sum()}
                        for k,v in grouped])

a=np.array([gridTime(x,step_sec=360,out_mode='ceil') for x in data.index[0:5]])


# import example
#file_path='../data/'
file_path='C:/st_sim/usr/nijos/data/'
file_name='test1.txt'
data=pd.read_csv(file_path+file_name)

# aggregate by variables
grouped=data.groupby(['time'])
for k,v in grouped:
    print k
    print v
# 1/
data_agg_1=pd.DataFrame([{'time': k,
                        'vwap': (v.price * v.volume).sum() / v.volume.sum(),
                        'vwap2': np.sum(v.price * v.volume) / np.sum(v.volume),
                        'volume': v.volume.mean()}
                        for k,v in grouped])
data_agg_1=pd.DataFrame([{'time': k,
                        'volume': v.volume.mean()}
                        for k,v in grouped])                        
                        
# aggregate by same time !
grouped=data.groupby(['time','contract'])
# 1/
data_agg_2=pd.DataFrame([{'time': k[0],
                        'contract': k[1],
                        'vwap': (v.price * v.volume).sum() / v.volume.sum(),
                        'vwap2': np.sum(v.price * v.volume) / np.sum(v.volume),
                        'volume': v.volume.mean()}
                        for k,v in grouped])
                        
# aggregate by same timeframe !
grouped=data.groupby(pd.TimeGrouper(freq='5Min'))

# 1/
data_agg_3=pd.DataFrame([{'time': k[0],
                        'contract': k[1],
                        'vwap': (v.price * v.volume).sum() / v.volume.sum(),
                        'vwap2': np.sum(v.price * v.volume) / np.sum(v.volume),
                        'volume': v.volume.mean()}
                        for k,v in grouped])

data.groupby(data.index.map(lambda t: t.minute))
data.groupby(np.array([k for k,v in data.groupby(pd.TimeGrouper(freq='5Min'))]))

"""
Aggregation dataframeas a time series
"""
file_path='C:/st_sim/usr/nijos/data/'
file_name='test1.txt'
data=pd.read_csv(file_path+file_name,index_col=0,parse_dates=True)



""" 
HANDLE TIMEZONE
"""
# create Datetime localized in UTC
timedata = pd.date_range('17/3/2013 10:00', '17/3/2013 11:00', freq='600s')
timedata1=timedata.tz_localize('UTC')
# tranform in another Timezone paris
timedata2=timedata1.tz_convert('Europe/Berlin')
tmp=np.random.rand(len(timedata))

timedata3 = pd.date_range('17/3/2013 11:00', '17/3/2013 12:00', freq='600s',tz='UTC')
data=pd.DataFrame({'bid' : 10+tmp, 'ask' : 10+tmp+np.abs(tmp)},index=timedata)
data1=pd.DataFrame({'bid' : 10+tmp, 'ask' : 10+tmp+np.abs(tmp)},index=timedata1)
data2=pd.DataFrame({'bid' : 10+tmp, 'ask' : 10+tmp+np.abs(tmp)},index=timedata2)
data3=pd.DataFrame({'bid' : 10+tmp, 'ask' : 10+tmp+np.abs(tmp)},index=timedata3)


""" 
HANDLE TIMEZONE
"""
import lib.plots.intraday as intrplot
data=read_dataset.ftickdb(security_id=110,date='17/03/2013')
intrplot.plot_intraday(data,exclude_auction=[0,0,0,0],step_sec=360)




""" 
Create a volume curve
"""
data=read_dataset.bic(security_id=110,step_sec=60,start_date='01/05/2013',end_date='05/06/2013')

grouped=data.groupby([[x.to_datetime() for x in data.index]-st_data.gridTime(date=data.index,step_sec=60*60*24,out_mode='floor'),
                       'opening_auction','intraday_auction','closing_auction'])
grouped_data=pd.DataFrame([{'date':k[0],
                'opening_auction':k[1],'intraday_auction':k[2],'closing_auction':k[3],
                'nb_day':np.size(v.volume),
                'volume': np.sum(v.volume),
                'vwap': np.sum(v.vwap * v.volume) / np.sum(v.volume)} for k,v in grouped])
                
grouped_data['volume'].plot()                
data.to_excel('C:/view_df.xlsx')

# only started if uit is this script 
if __name__=='__main__':
    print 'test1'
