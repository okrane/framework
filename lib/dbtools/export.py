# -*- coding: utf-8 -*-
import pymssql

def export_to_csv(host, user, password, database, filename, query, delimiter = ';'):
    conn = pymssql.connect(host=host, user=user, password=password, database=database, as_dict = True)
    cur = conn.cursor()
    cur.execute(query)
    
    f = open(filename, "w")
    row = cur.fetchone()    
    f.write( delimiter.join(row.keys()) + '\n')
    while row :        
        f.writelines( delimiter.join([str(x) if x else '' for x in row.values()])  + '\n')
        row = cur.fetchone()