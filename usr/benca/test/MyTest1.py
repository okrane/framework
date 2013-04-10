'''
Created on 16 juil. 2010

@author: benca
'''

import scipy.io
import numpy
from simep.funcs.data.pyData import pyData




if True:
    print dir(scipy.io.matlab)
    print dir(scipy.io.matlab.mio5)
    
    help(scipy.io.matlab.savemat)
    
    D = {'date':['09:00', '10:00', '11:00', '12:00', '13:00', '14:00', '15:00'], 
         'value':numpy.array([[0.02,1],[0.03,2],[0.04,3],[0.07,4],[0.09,5],[0.03,6],[0.04,7]], float)}
    
    scipy.io.matlab.savemat('C:\\test702.mat', {'pydata':D}, appendmat=True, format='5', do_compression=False)
else:
    x = pyData('xls', filename='C:/ACCP_4_20100520.xls', colnames = ['Date', 
                                                                     'PI-(t)', 
                                                                     'ExecPrice', 
                                                                     'min_perMarketVolume', 
                                                                     'IsPI(t)',
                                                                     'ExecQty',
                                                                     'max_perMarketVolume',
                                                                     'MarketVolume',
                                                                     'PI(t)',
                                                                     'PI+(t)'])
    x.to_mat('my_mat_pydata', 'C:\\test702.mat')

