# -*- coding: utf-8 -*-

import pymssql

def export_security(filename = "security.kcdb"):
    conn = pymssql.connect(host='LUIDBC01', user='python_ro', password='python4ever!!', database='KGR', as_dict = True)
    cur = conn.cursor()
    cur.execute("""select SECID, 
                SECNAME, 
                EXCHGID, 
                SYMBOL1, 
                SYMBOL2, 
                SYMBOL3, 
                SYMBOL4,
                SYMBOL5, 
                SYMBOL6,
                SYMBOL7,
                SYMBOL8,
                SYMBOL9,
                SYMBOL10,
                CCY,
                MARKET,
                SUBMARKET,
                SOURCE,
                MKTGROUP,
                PLC
                from SECURITY where STATUS='A' """)
    
    f = open(filename, "w")
    row = cur.fetchone()    
    f.write( ";".join(row.keys()) + '\n')
    while row :        
        f.writelines( ";".join([str(x) if x else '' for x in row.values()])  + '\n')
        row = cur.fetchone()
    conn.close()        
    f.close()

def export_security_market(filename = "security_market.kcdb"):
    conn = pymssql.connect(host='LUIDBC01', user='python_ro', password='python4ever!!', database='KGR', as_dict = True)
    cur = conn.cursor()
    cur.execute("""select * from security_market""")
    
    f = open(filename, "w")
    row = cur.fetchone()    
    f.write( ";".join(row.keys()) + '\n')
    while row :        
        f.writelines( ";".join([str(x) if x else '' for x in row.values()])  + '\n')
        row = cur.fetchone()
    conn.close()
    f.close()

def export_quotation_group(filename = "quotation_group.kcdb"):
    conn = pymssql.connect(host='LUIDBC01', user='python_ro', password='python4ever!!', database='KGR', as_dict = True)
    cur = conn.cursor()
    cur.execute("""select * from QUOTATION_GROUP""")
    
    f = open(filename, "w")
    row = cur.fetchone()    
    f.write( ";".join(row.keys()) + '\n')
    while row :        
        f.writelines( ";".join([str(x) if x else '' for x in row.values()])  + '\n')
        row = cur.fetchone()
    conn.close()
    f.close()

def export_trading_hours(filename = "trading_hours.kcdb"):
    conn = pymssql.connect(host='LUIDBC01', user='python_ro', password='python4ever!!', database='KGR', as_dict = True)
    cur = conn.cursor()
    cur.execute("""select * from trading_hours""")
    
    f = open(filename, "w")
    row = cur.fetchone()    
    f.write( ";".join(row.keys()) + '\n')
    while row :        
        f.writelines( ";".join([str(x) if x else '' for x in row.values()])  + '\n')
        row = cur.fetchone()
    conn.close()
    f.close()

export_quotation_group()