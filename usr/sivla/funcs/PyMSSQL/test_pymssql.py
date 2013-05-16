# -*- coding: utf-8 -*-
"""
Created on Wed May 08 09:34:59 2013

@author: svlasceanu
"""
"""
import pymssql
#conn = pymssql.connect(host='LUIDBC01', user='KEPLERCM\svlasceanu', password='asa', database='KGR')
#conn = pymssql.connect(host='172.29.100.64', user='python_ro', password='python4ever!!', database='KGR')
conn = pymssql.connect(host='172.29.0.24', user='greg', password='greggreg', database='master')
cur = conn.cursor()
cur.callproc('SP_WHO', ())
#cur.execute('select * from sysobjects')
print cur.fetchall()
#conn.close()
"""
"""
import pyodbc
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=172.29.100.64;DATABASE=KGR;UID=python_ro;PWD=python4ever!!')
cursor = cnxn.cursor()
cursor.execute('select * from KGR..security_market where security_id = 276')
print cursor.fetchall()
"""

from lib.dbtools.connections import Connections
Connections.change_connections('dev')
a = Connections.exec_sql('KGR', 'select top 10 * from SECURITY', as_dataframe = True)
print a

