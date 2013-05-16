import pyodbc

cnxn = pyodbc.connect('DSN=PARSRV21;UID=python_ro;PWD=python4ever!!')
cursor = cnxn.cursor()


cursor.execute("select top 10 * from SECURITY")
row = cursor.fetchall()
for r in row:
    print r
    
cnxn.close()