from lib.data.st_data import * 
 
#data = from_mat_file("Q:/dev_repository/get_tick/ft/FTE/2013_05_01.mat")
"""
grouped=data.groupby(pd.TimeGrouper(freq='5Min')) 
for k, v in grouped:
    print v["bid"]
    """
    
"""
data=pd.DataFrame({'A' : np.array([1,2,1,3,2,3,2]), 'B' :np.array([2,2,2,3,2,3,2])})
a=data[['A','B']].values
a.view([('', a.dtype)]*a.shape[1])
"""
from pandas import *
import numpy as np
import datetime

# - Generate 
s_time1 = datetime.datetime(2013, 04, 23, 10, 00, 00)
s_time2 = datetime.datetime(2013, 04, 23, 10, 51, 01)
N = 20
df1 = DataFrame(np.random.randn(N,3), index = date_range(s_time1, periods = N, freq = '5T'))
df1.columns = ['ret_a', 'ret_b', 'ret_c'] 

#     exp.Explorer(df1)

N = 5
df2 = DataFrame(np.random.randn(N,2), index = date_range(s_time2, periods = N, freq = '5T'))
df2.columns = ['ret_b', 'ret_c']

print df1
print df2


c_df = concat([df1, df2], ignore_index =False)
print c_df

# - merge avec union
c_df = merge(df1, df2, how = 'outer', left_index = True, right_index = True, sort = True)
print c_df
#     exp.Explorer(c_df)

# - merge avec intersection avec les indices du DF de gauche
c_df = merge(df1, df2, how = 'right', left_index = True, right_index = True, sort = True)
print c_df
#     exp.Explorer(c_df)
