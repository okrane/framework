# -*- coding: utf-8 -*-

import pandas as pd
import scipy.io
from datetime import *
# from lib.dbtools.connections import Connections
from lib.dbtools.get_repository import *
from lib.data.matlabutils import *

def to_dataframe(data,timezone=False):
    
    if not data:
        return
    #--------------------------------------------------------------------------
    # EXTRACT ST_DATA needed info
    #--------------------------------------------------------------------------
    value = data[0][0].value
    colnames = [x[0] for x in data[0][0].colnames[0]]
    dates = [x[0] for x in data[0][0].date]
    #title = data[0][0].title[0]
    
    frame = {}
    for i in range(len(colnames)):
        frame[colnames[i]] = [x[i] for x in value] 
    
    frame['date']=[datetime.fromordinal(int(matlab_datenum)) + timedelta(days=matlab_datenum%1) - timedelta(days = 366) for matlab_datenum in dates]
    
    # TODO: rownames and codebook
    
    dataframe=pd.DataFrame.from_records(frame,columns=colnames.append('date'),index = ['date'])    
    
    # test if it is old cheuvreux data info
    is_old_data=False
    if (('info' in data[0][0]._fieldnames) and
        (('version' not in data[0][0].info[0][0]._fieldnames) or
        (data[0][0].info[0][0].version[0][0] is not 'kepche_1'))):
            is_old_data=True
            
    #--------------------------------------------------------------------------
    # Mapping des destinations de trading : Cheuvreux -> Kepler
    #--------------------------------------------------------------------------
    if (is_old_data and ('trading_destination_id' in colnames)):
        if ('security_id' not in data[0][0].info[0][0]._fieldnames):
                raise NameError('to_dataframe:security_id - Security_id info is missing !')      
        td_newvals=get_repository("tdidch2exchangeid",
                                  td_id=dataframe['trading_destination_id'].values,
                                    security_id=data[0][0].info[0][0].security_id[0][0])
        del dataframe['trading_destination_id']
        dataframe.insert(0,'trading_destination_id',td_newvals)    
    
    #--------------------------------------------------------------------------
    # TIMEZONE
    #--------------------------------------------------------------------------
    if timezone:
        if (('info' in data[0][0]._fieldnames) and 
            all([x in data[0][0].info[0][0]._fieldnames for x in ['localtime','td_info']])):
            if (not data[0][0].info[0][0].localtime[0][0]):
                stz=['GMT']
            else:
                if is_old_data:
                    exchid=get_repository("tdidch2exchangeid",td_id=data[0][0].info[0][0].td_info[0][0].trading_destination_id[0][0])
                else:
                    exchid=[data[0][0].info[0][0].td_info[0][0].trading_destination_id[0][0]]  
                stz=get_repository("exchangeid2tz",exchange_id=exchid)
                
            if  not (not stz[0]):
                dataframe=dataframe.tz_localize(stz[0])
            else:
                raise NameError('to_dataframe:timezone - No timezone found in database')
        else:
            raise NameError('to_dataframe:timezone - Timezone can"t be found in the input matfile')
       
    return dataframe
    
    
def from_mat_file(filename, variable = 'data'):
    """ 
    This is my second function
    """    
    mat = scipy.io.loadmat(filename, struct_as_record  = False)
    return to_dataframe(mat[variable])


if __name__ == "__main__":
    filename="Q:/kc_repository/get_tick/ft/110/2013_05_20.mat"
    data = from_mat_file(filename)
    spread = 10000 * (data['ask'] - data['bid']) / data['price']
    import matplotlib.pyplot as plt
    spread.plot()        
    plt.show()    
    print spread

