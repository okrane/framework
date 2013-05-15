'''
Created on 23 mars 2010

@author: syarc
'''

import sys
if sys.version_info[0] == 2 and sys.version_info[1] >= 6:
    import pyodbc

import traceback

class ODBC(object):

    def __init__(self, connectstring, autocommit=True):
        self._conn = pyodbc.connect(connectstring, autocommit=autocommit)
        self._cursor = self._conn.cursor()

    def close(self):
        self._cursor.close()
        self._conn.commit()
        self._conn.close()

    def select(self, query, *args):
        ''' Run a simple query statement and return it results
            @param query: select statement (accept '?', in that case specify value
                          using extra arguments
            @param *args: arguments to select statement
        '''
        try:
            return self._cursor.execute(query, args).fetchall()
        except:
            print query
            traceback.print_exc()
            sys.exit()

    def selectOne(self, query, *args):
        ''' Run a simple query statement and return the first line
            @param query: select statement (accept '?', in that case specify value
                          using extra arguments
            @param *args: arguments to select statement
        '''
        return self._cursor.execute(query, args).fetchone()

    def selectBig(self, query, *args):
        ''' Run a simple query statement and return it results
            @param query: select statement (accept '?', in that case specify value
                          using extra arguments
            @param *args: arguments to select statement
        '''
        return self._cursor.execute(query, args)

    def run(self, query, *args):
        ''' Run a simple query statement, it returns the rowcount affected
            @param query: select statement (accept '?', in that case specify value
                          using extra arguments
            @param *args: arguments to select statement
        '''
        self._cursor.execute(query, args)
        return self._cursor.rowcount

    def executemany(self, query, *args):
        self._cursor.executemany(query, *args)
        return self._cursor.rowcount

    def execDirect(self, query):
        ''' Run a simple query statement and returns nothing

            The main difference with the 'select' method is that the underlying
            library will use the ODBC SQLExecDirect. The pyodbc current API uses
            SQLExecDirect if there is not binding params, to this method doesn't
            allow extra arguments.

            You NEED this to create temporary table

            @param query: sql statement
        '''
        self._cursor.execute(query)

    def dropTable(self, table_name):
        ''' Drop a table by its name '''
        return self._cursor.execute('drop table ' + table_name)

    def dropIndex(self, table_name, index_name):
        ''' Drop a index '''
        return self._cursor.execute('drop index ' + index_name + ' on ' + table_name)

    def truncateTable(self, table_name):
        ''' Truncate (delete all records) a table '''
        return self._cursor.execute('delete from ' + table_name)

    def isTableExist(self, table_name):
        ''' Returns true if the table exists '''
        query = ''' select case when exists
                                    (select 1 from information_schema.tables
                                            where table_name = '%s')
                                    then 1 else 0 end ''' % table_name
        ans = self._cursor.execute(query).fetchone()
        return ans[0] == 1

