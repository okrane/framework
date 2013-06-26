# -*- coding: utf-8 -*-

import pandas as pd
from pandas.io.parsers import *
from lib.data.ui.Explorer import *
from lib.dbtools.connections import Connections
from lib.data.pyData import convertStr
import numpy as np

def upload_file(filename = 'C:\st_sim\projects\FixedIncomeReferential\Datas.xlsx'):
    xls = pd.ExcelFile(filename)
    data = xls.parse('Paris', header = 0 )
    client = Connections.getClient('HPP')
    # get mapping
    mapping = client['FixedIncome']['FieldMapping']    
    mapping_dictionary = {}
    for m in mapping.find():
        mapping_dictionary[m['field']] = m
        
    print mapping_dictionary
    
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
    for doc in collection.find():
        print doc
        print " "
        
def extract_unique(field):
    client = Connections.getClient('HPP')
    collection = client['FixedIncome']['Referential']
    
    result = collection.aggregate([{'$project': {field: 1, '_id': 0}}])
    a = result['result']
    l = np.unique(np.array([str(x[field]) for x in result['result']]))
    print l
    
def upload_mapping_table(filename, field):
    csv = pd.read_csv(filename, ';')        
    client = Connections.getClient('HPP')
    collection = client['FixedIncome']['FieldMapping']
    collection.remove()
    
    d = {'field': field}
    for i in range(len(csv.index)):   
        print csv['Value'][i], csv['Mapped Value'][i]
        #print np.isnan(str(csv['Mapped Value'][i])
        d[str(csv['Value'][i])] = str(csv['Mapped Value'][i]) if str(csv['Mapped Value'][i]) != 'nan'  else ''
    
    collection.insert(d)

def download_to_csv(filename):
    pass

def bloom_code():
    import win32com.client

    # Get Dispatch interface from blpdatax.dll
    w = win32com.client.Dispatch('Bloomberg.Data.1')
    
    # Use BlpSubscribe method to request price
    px_last =  w.BLPSubscribe('AAL LN Equity','PX_LAST')

    print px_last


if __name__ == "__main__":
    upload_file('C:\st_sim\projects\FixedIncomeReferential\Datas.xlsx')
    #extract_unique('INDUSTRY_SECTOR')
    #upload_mapping_table('C:\st_sim\projects\FixedIncomeReferential\mapping_file.txt', 'INDUSTRY_SECTOR')