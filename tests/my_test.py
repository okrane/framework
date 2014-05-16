import lib.data.dataframe_tools as dft
import numpy as np

import lib.dbtools.read_dataset as read_dataset
from lib.data.ui.Explorer import Explorer
import lib.stats as stats
# 
data=read_dataset.ftickdb(security_id=276,date='06/01/2014')

agg_exch=dft.agg(data,
                 group_vars=['exchange_id'],
                 
                stats={'nb_deal': lambda df : np.size(df.volume),
                       'volume': lambda df : np.sum(df.volume),
                       'open' : lambda df :df.price.values[0],
                       'high' : lambda df :np.max(df.price.values),
                       'low' : lambda df :np.min(df.price.values),
                       'close' : lambda df :df.price.values[-1],
                       'vwas' : lambda df :stats.slicer.vwas(df.bid.values,df.ask.values,df.price.values,df.volume.values,df.auction.values),
                       'vwap2' : lambda df : np.sum(df.price.values * df.price.values) / np.sum(df.price.values)})

agg_slicer= dft.agg(data[(data['dark'] == 0 ) & (data['auction'] == 0 )], 
                    step_sec = 60, 
                    group_vars = [],
                    stats={'nb_deal': lambda df : np.size(df.volume),
                       'volume': lambda df : np.sum(df.volume),
                       'open' : lambda df :df.price.values[0],
                       'high' : lambda df :np.max(df.price.values),
                       'low' : lambda df :np.min(df.price.values),
                       'close' : lambda df :df.price.values[-1],
                       'vwas' : lambda df :stats.slicer.vwas(df.bid.values,df.ask.values,df.price.values,df.volume.values,df.auction.values),
                       'vwap2' : lambda df : np.sum(df.price.values * df.price.values) / np.sum(df.price.values)})


# agg_slicer2 = dft.agg(data, 
#                     step_sec = 60,
#                     stats = {'nb_deal': lambda df : np.size(df.volume),
#            'volume': lambda df : np.sum(df.volume)})


print agg_exch


agg_slicer.to_csv('C:\\total.csv')
# print agg_slicer2










import sys
sys.exit()
# import paramiko
# 
# if __main__ == '__name__':
#     ssh = paramiko.SSHClient()
#     ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#     ssh.connect(self.server['ip_addr'],
#                 username = self.server['list_users']['flexapp']['username'], 
#                 password = self.server['list_users']['flexapp']['passwd'])



# -*- coding: utf-8 -*-

import pandas as pd
from pandas.io.parsers import *
from lib.dbtools.connections import Connections
from lib.data.pyData import convertStr
import numpy as np

def upload_file(filename = 'C:\st_sim\projects\FixedIncomeReferential\Datas.xlsx'):
    """ This is the doc of my function 
    @param filename: the file name 
    """ 
#     xls = pd.ExcelFile(filename)
#     data = xls.parse('Paris', header = 0 )
    Connections.change_connections('dev')
    client = Connections.getClient('FixedIncome')
    
    # get mapping
    mapping = client['FixedIncome']['FieldMapping'] 
    mapping_dictionary = {}
    for m in mapping.find():
        mapping_dictionary[m['field']] = m
    
    #print mapping_dictionary
    
    collection = client['FixedIncome']['Referential']
    collection.remove()
    
    for i in range(len(data.index)): 
        row = {}
        for k in data.ix[i].keys():
            if k in mapping_dictionary.keys():
                row[k] = mapping_dictionary[k][convertStr(data.ix[i][k], '%d/%m/%Y')]
            else:
                row[k] = convertStr(data.ix[i][k], '%d/%m/%Y')
            
        #row = dict((k, convertStr(data.ix[i][k], '%d/%m/%Y')) for k in data.ix[i].keys() if k not in mapping_dictionary.keys() else (k, convertStr(data.ix[i][k], '%d/%m/%Y')) ) 
        collection.insert(row)
        
        print "--------------------------------------------"
        ''' 
        for doc in collection.find():
        print doc
        
        print " "
        ''' 
def extract_unique(field):
    client = Connections.getClient('MARS')
    collection = client['FixedIncome']['Referential']
    
    result = collection.aggregate([{'$project': {field: 1, '_id': 0}}])
    l = np.unique(np.array([str(x[field]) for x in result['result']]))
    print l
        
def upload_mapping_table(filename, field):
    csv = pd.read_csv(filename, ';') 
    client = Connections.getClient('MARS')
    collection = client['FixedIncome']['FieldMapping']
    
    collection.remove()
    
    d = {'field': field}
    for i in range(len(csv.index)): 
        print csv['Value'][i], csv['Mapped Value'][i]
        #print np.isnan(str(csv['Mapped Value'][i])
        d[str(csv['Value'][i])] = str(csv['Mapped Value'][i]) if str(csv['Mapped Value'][i]) != 'nan' else ''
    
    collection.insert(D)
            
def download_to_csv(filename):
    pass
            
def bloom_code():
    import win32com.client
    
    # Get Dispatch interface from blpdatax.dll
    w = win32com.client.Dispatch('Bloomberg.Data.1')
    
    # Use BlpSubscribe method to request price
    px_last = w.BLPSubscribe('AAL LN Equity','PX_LAST')
    
    print px_last


if __name__ == "__main__":
    upload_file('C:\st_sim\projects\FixedIncomeReferential\Datas.xlsx')
    #extract_unique('INDUSTRY_SECTOR')
    upload_mapping_table('C:\st_sim\projects\FixedIncomeReferential\mapping_file.txt', 'INDUSTRY_SECTOR')

