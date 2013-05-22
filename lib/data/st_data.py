# -*- coding: utf-8 -*-

import pandas as pd
import scipy.io
from datetime import *

def to_dataframe(data):    
    """ 
    This is my first function
    """        
    value = data[0][0].value
    colnames = [x[0] for x in data[0][0].colnames[0]]
    dates = [x[0] for x in data[0][0].date]
    #title = data[0][0].title[0]
    
    # TODO: timezones
    timedata =[datetime.fromordinal(int(matlab_datenum)) + timedelta(days=matlab_datenum%1) - timedelta(days = 366) for matlab_datenum in dates]
    frame = {}
    for i in range(len(colnames)):
        frame[colnames[i]] = [x[i] for x in value]    
    
    dataframe = pd.DataFrame(frame, index = timedata)
    return dataframe
    # TODO: rownames and codebook

def from_mat_file(filename, variable = 'data'):
    """ 
    This is my second function
    """    
    mat = scipy.io.loadmat(filename, struct_as_record  = False)
    return to_dataframe(mat[variable])


if __name__ == "__main__":
    data = from_mat_file("Q:/dev_repository/get_tick/ft/FTE/2013_05_01.mat")
    spread = 10000 * (data['ask'] - data['bid']) / data['price']
    import matplotlib.pyplot as plt
    spread.plot()        
    plt.show()
    
    print spread

