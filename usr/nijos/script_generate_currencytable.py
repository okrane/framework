# -*- coding: utf-8 -*-
"""
Created on Wed Jun 19 11:35:53 2013

@author: njoseph
"""

import numpy as np
import scipy 
from datetime import *
import pandas as pd
import lib.dbtools.read_dataset as read_dataset
import lib.data.matlabutils as matlabutils
from lib.dbtools.connections import Connections


path_export='H:/TQM/mapping_table/'

################################
## Currency Table
################################

Connections.change_connections('production')

date='20131024'
data=read_dataset.histocurrencypair(date = date)


myfile = open(path_export+'currency_table_'+date+'.txt','w')
myfile.write('  <currencymanager normalise="EUR" useBaseCCY="no">\n')
for i in range(0,data.shape[0]):
    myfile.write('      <ccy symbol="'+data.ix[i]['ccy']+'" multiplier="1" default-rate="'+str(data.ix[i]['rate'])+'" />\n')

myfile.write('  </currencymanager>\n')
myfile.close()


################################
## Place Table
################################
Connections.change_connections('production')

pref_ = "LUIDBC01_" if Connections.connections == "dev" else  ""
req=("SELECT  flexexch.SUFFIX,exch.EXCHGNAME "
    " FROM %sKGR..FlextradeExchangeMapping flexexch "
    " LEFT JOIN %sKGR..EXCHANGEMAPPING exchmap on ( "
    " flexexch.EXCHANGE=exchmap.EXCHANGE ) "
    " LEFT JOIN %sKGR..EXCHANGE exch on ( "
    " exchmap.EXCHGID=exch.EXCHGID ) ")  % (pref_,pref_,pref_)
vals=Connections.exec_sql('KGR',req)
data=pd.DataFrame.from_records(vals,columns=['suffix','name'])
data=pd.DataFrame(matlabutils.uniqueext(data.values,rows=True),columns=['suffix','name'])

myfile = open(path_export+'place_table_'+datetime.strftime(datetime.today(),'%Y%m%d')+'.txt','w')
myfile.write('  <user-defined name="Place Name (active)" numeric="no">\n')
myfile.write('      <mapping property="TAG_100 (active)">\n')
for i in range(0,data.shape[0]):
    if not data.ix[i]['suffix']=='AG':
        myfile.write('          <rule predicate="EQUALS" source="'+data.ix[i]['suffix']+'" constant="'+data.ix[i]['name']+'"  />\n')

myfile.write('      </mapping>\n')
myfile.write('  </user-defined>\n')
myfile.close()


################################
## Destination Table
################################
Connections.change_connections('production')

pref_ = "LUIDBC01_" if Connections.connections == "dev" else  ""
req=("SELECT  MIC,PLATFORM "
    " FROM %sKGR..EXCHANGEREFCOMPL ")  % (pref_)
vals=Connections.exec_sql('KGR',req)
data=pd.DataFrame.from_records(vals,columns=['MIC','name'])
data=pd.DataFrame(matlabutils.uniqueext(data.values,rows=True),columns=['MIC','name'])

myfile = open(path_export+'destination_table_'+datetime.strftime(datetime.today(),'%Y%m%d')+'.txt','w')
myfile.write('  <user-defined name="Exec Venue Name" numeric="no">\n')
myfile.write('      <mapping property="Exec Venue">\n')
for i in range(0,data.shape[0]):
    myfile.write('          <rule predicate="EQUALS" source="'+data.ix[i]['MIC']+'" constant="'+data.ix[i]['name']+'"  />\n')

myfile.write('      </mapping>\n')
myfile.write('  </user-defined>\n')
myfile.close()








