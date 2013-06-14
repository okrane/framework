#!python2.7
'''
Created on 23 Apr 2013

@author: gpons
'''

import lib.dbtools.read_dataset as read_dataset
import lib.plots.intraday as intrplot
import time
import lib.data.ui.Explorer as exp
import pandas
import matplotlib.pyplot as plt

if __name__ == '__main__':
    
    l_date = ['01/02/2013','04/03/2013','05/03/2013','06/03/2013','07/03/2013','08/03/2013','11/03/2013','12/03/2013','13/03/2013','14/03/2013','15/03/2013','18/03/2013','19/03/2013','20/03/2013','21/03/2013','22/03/2013','25/03/2013','26/03/2013','27/03/2013','28/03/2013']
    l_day_volume = []
    
    t0 = time.clock()
    for day in l_date: 
        data=read_dataset.ftickdb(security_id=110,date=day, remote=True)
        l_day_volume.append(data.volume.sum())
    
    print time.clock() - t0
    
    plt.plot(l_day_volume)
    plt.show()