# -*- coding: utf-8 -*-
"""
Created on Thu Apr 04 11:33:15 2013

@author: nijos
"""

""" 
-------------------------------------------------------------------------------
IMPORT
-------------------------------------------------------------------------------
""" 

import numpy as np
import matplotlib.pyplot as plt
import datetime as dt
import pandas as pd
import tempfile as tempfile
import subprocess as subprocess
import os as os
from pymongo import MongoClient


""" 
-------------------------------------------------------------------------------
FUNCS NEEDED
-------------------------------------------------------------------------------
"""         

def open_in_excel(df, index=True, excel_path="excel.exe", tmp_path='.'):
    """Open dataframe df in excel.

    excel_path - path to your copy of excel
    index=True - export the index of the dataframe as the first columns
    tmp_path    - directory to save the file in


    This creates a temporary file name, exports the dataframe to a csv of that file name,
    and then tells excel to open the file (in read only mode). (It uses df.to_csv instead
    of to_excel because if you don't have excel, you still get the csv.)

    Note - this does NOT delete the file when you exit. 
    """
    f=tempfile.NamedTemporaryFile(delete=False, dir=tmp_path, suffix='.csv', prefix='tmp_')
    tmp_name=f.name
    f.close()
    df.to_csv(tmp_name, index=index)
    cmd=[excel_path, '/r', '/e', tmp_name]
    try:
        ret_val=subprocess.Popen(cmd).pid
    except:
        print "open_in_excel(): failed to open excel"
        print "filename = ", tmp_name
        print "command line = ", cmd
    return ret_val


def parser_timestamp(x):
    return dt.datetime.strptime(x, '%d/%m/%Y %H:%M:%S %f') 


def slicer_ob(side_vals,depth_vals,x,side,depth):
    ret_vals=np.nan
    idx=(side==side_vals) & (depth==depth_vals) & (np.isfinite(x))
    if np.any(idx):
        ret_vals=x.values[idx][-1]
    return ret_vals    



def load_csv_ob(sec_id,td_id,date,root_path="Y:\\st_repository\\obdata\\"):
    
    """ test """
    if ((not isinstance(sec_id,int)) | 
    (not isinstance(td_id,int)) |
    (not isinstance(date,str))|
    (not isinstance(root_path,str))):
        raise Exception("load_csv_ob: Bad input") 
    
    """ read csv file """
    file_name=dt.datetime.strftime(dt.datetime.strptime(date, '%d/%m/%Y') , '%Y%m%d')+'.csv'
    file_path=os.path.join(root_path,'security_id=%d'%(sec_id),'td_id=%d'%(td_id),file_name)
    data=pd.read_csv(file_path, 
                 parse_dates = {'datetime' : ['date', 'time','microseconds']},
                 date_parser=parser_timestamp)
    """ reshape """
    #data=data.head(1000)              
    grouped=data.groupby(['datetime'])
    
    data=pd.DataFrame([{'datetime': k,
                        'security_id': np.min(v.security_id),
'trading_destination_id': np.min(v.trading_destination_id),
'bid_price_1': slicer_ob(0,0,v.price,v.side,v.depth),
'bid_size_1': slicer_ob(0,0,v.size,v.side,v.depth),
'bid_nb_1': slicer_ob(0,0,v.mmk_number,v.side,v.depth),
'ask_price_1': slicer_ob(1,0,v.price,v.side,v.depth),
'ask_size_1': slicer_ob(1,0,v.size,v.side,v.depth),
'ask_nb_1': slicer_ob(1,0,v.mmk_number,v.side,v.depth),
'bid_price_2': slicer_ob(0,1,v.price,v.side,v.depth),
'bid_size_2': slicer_ob(0,1,v.size,v.side,v.depth),
'bid_nb_2': slicer_ob(0,1,v.mmk_number,v.side,v.depth),
'ask_price_2': slicer_ob(1,1,v.price,v.side,v.depth),
'ask_size_2': slicer_ob(1,1,v.size,v.side,v.depth),
'ask_nb_2': slicer_ob(1,1,v.mmk_number,v.side,v.depth),
'bid_price_3': slicer_ob(0,2,v.price,v.side,v.depth),
'bid_size_3': slicer_ob(0,2,v.size,v.side,v.depth),
'bid_nb_3': slicer_ob(0,2,v.mmk_number,v.side,v.depth),
'ask_price_3': slicer_ob(1,2,v.price,v.side,v.depth),
'ask_size_3': slicer_ob(1,2,v.size,v.side,v.depth),
'ask_nb_3': slicer_ob(1,2,v.mmk_number,v.side,v.depth),
'bid_price_4': slicer_ob(0,3,v.price,v.side,v.depth),
'bid_size_4': slicer_ob(0,3,v.size,v.side,v.depth),
'bid_nb_4': slicer_ob(0,3,v.mmk_number,v.side,v.depth),
'ask_price_4': slicer_ob(1,3,v.price,v.side,v.depth),
'ask_size_4': slicer_ob(1,3,v.size,v.side,v.depth),
'ask_nb_4': slicer_ob(1,3,v.mmk_number,v.side,v.depth),
'bid_price_5': slicer_ob(0,4,v.price,v.side,v.depth),
'bid_size_5': slicer_ob(0,4,v.size,v.side,v.depth),
'bid_nb_5': slicer_ob(0,4,v.mmk_number,v.side,v.depth),
'ask_price_5': slicer_ob(1,4,v.price,v.side,v.depth),
'ask_size_5': slicer_ob(1,4,v.size,v.side,v.depth),
'ask_nb_5': slicer_ob(1,4,v.mmk_number,v.side,v.depth)} for k,v in grouped],
index=[k for k,v in grouped]).fillna(method='ffill')
                              
    return data

   
def add_dfts_to_collection(df,db_name,collection_name,server="localhost",port=27017):
    client = MongoClient(server,port)
    # creat databse
    db = client[db_name]
    # create collection
    collection = db[collection_name]
    # drop datetime if in the dataset
    if 'datetime' in df.columns.tolist():
        df=df.drop(['datetime'],axis=1)
    
    for i in range(0,len(df)-1):
        doc=df.ix[i].to_dict()
        doc.update({'datetime':df.index[i].to_datetime()})
        collection.insert(doc)
        
    client.close();




""" 
-------------------------------------------------------------------------------
MAIN
-------------------------------------------------------------------------------
""" 

if __name__=='__main__':
    print 'test1'
    
    #--- load data infos
    sec_id_list=[110]
    td_id_list=[4,61,81,89]
    date='06/03/2013'

    #--- save into a mongodb
    db_name='tickdb'
    collection_name='orderbook_update'
    server="localhost"
    port=27017
    
    for sec_id in sec_id_list:
        for td_id in td_id_list:
            data=load_csv_ob(sec_id,td_id,date)
            add_dfts_to_collection(data,db_name,collection_name,server=server,port=port)
    
    #--- plot (1)
    start_time='10:01:00'
    end_time='16:20:00'
    
    id_subset=((data['datetime']>=dt.datetime.strptime(date+' '+start_time,'%d/%m/%Y %H:%M:%S')) & 
    (data['datetime']<=dt.datetime.strptime(date+' '+end_time,'%d/%m/%Y %H:%M:%S')))
    
    fig, axes = plt.subplots(nrows=2, ncols=1)
    data[id_subset][['ask_price_1','bid_price_1']].plot(ax=axes[0])
    # subplot 2
    def tmp_appf(x):
        return pd.Series(dict(ask_price_1=x['ask_size_1'], bid_size_1=-1*x['bid_size_1']))
        
    data[id_subset].apply(tmp_appf,axis=1).plot(ax=axes[1])

    #--- plot (2) : aggregated by 5 minutes
    grouped=data.groupby(pd.TimeGrouper(freq='5Min'))
    data_agg=pd.DataFrame([{'datetime': k,
                            'median_ask_size_1': np.median(v['ask_size_1']),
'median_bid_size_1': np.median(v['bid_size_1'])}
for k,v in grouped],
index=[k for k,v in grouped])

    id_subset=((data_agg['datetime']>=dt.datetime.strptime(date+' '+start_time,'%d/%m/%Y %H:%M:%S')) & 
    (data_agg['datetime']<=dt.datetime.strptime(date+' '+end_time,'%d/%m/%Y %H:%M:%S')))
    
    data_agg[id_subset][['median_ask_size_1','median_bid_size_1']].plot(kind='bar')



               
