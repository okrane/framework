# -*- coding: utf-8 -*-

import pandas as pd
import scipy.io
from datetime import *
# from lib.dbtools.connections import Connections
from lib.dbtools.get_repository import *
from lib.data.matlabutils import *

<<<<<<< HEAD

def to_dataframe(data,timezone=False):
    if not data:
        return []
    
=======
def to_dataframe(data):    
    """ 
    This is my first function
    """        
>>>>>>> 39bf89d8f888632b5db880aad5b3d1e143430180
    value = data[0][0].value
    colnames = [x[0] for x in data[0][0].colnames[0]]
    dates = [x[0] for x in data[0][0].date]
    #title = data[0][0].title[0]
    
    timedata =[datetime.fromordinal(int(matlab_datenum)) + timedelta(days=matlab_datenum%1) - timedelta(days = 366) for matlab_datenum in dates]

    frame = {}
    for i in range(len(colnames)):
        frame[colnames[i]] = [x[i] for x in value]    
    
    dataframe = pd.DataFrame(frame, index = timedata)
    
    # TODO: rownames and codebook
    
    if timezone:
        # TODO: better with place_id of td_info, wait for Romain and referentiel to be clean)
        if (('info' in data[0][0]._fieldnames) and 
            all([x in data[0][0].info[0][0]._fieldnames for x in ['localtime','security_id']]) and 
            data[0][0].info[0][0].localtime[0][0]):
                stz=get_repository("local_tz_from",security_id=data[0][0].info[0][0].security_id[0][0])
                if  not (not stz[0]):
                   dataframe=dataframe.tz_localize(stz[0])
                else:
                    raise NameError('to_dataframe:timezone - No timezone find in database')
        else:
            raise NameError('to_dataframe:timezone - No information on the loaded matfile')
               
    return dataframe
    
    
def from_mat_file(filename, variable = 'data'):
    """ 
    This is my second function
    """    
    mat = scipy.io.loadmat(filename, struct_as_record  = False)
    return to_dataframe(mat[variable])


if __name__ == "__main__":
<<<<<<< HEAD
    filename="Q:/kc_repository/get_tick/ft/110/2013_05_20.mat"
    data = from_mat_file(filename)
=======
    data = from_mat_file("Q:/dev_repository/get_tick/ft/FTE.PA/2013_05_02.mat")
>>>>>>> 39bf89d8f888632b5db880aad5b3d1e143430180
    spread = 10000 * (data['ask'] - data['bid']) / data['price']
    import matplotlib.pyplot as plt
    spread.plot()        
    plt.show()
    
    print spread

