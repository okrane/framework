'''
Created on Mar 6, 2012

@author: syarc
'''

import psycopg2
import traceback
import sys

class PostgreSQL(object):

    def __init__(self, host, dbname, port, user,password):
        self._conn = psycopg2.connect(host=host, dbname=dbname, port=port, user=user, password=password)
        self._conn.autocommit=False
        self._cursor = self._conn.cursor()

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
        self._cursor.execute(query, args)
        return self._cursor.fetchone()

    def run(self, query, *args):
        ''' Run a simple query statement, it returns the rowcount affected
            @param query: select statement (accept '?', in that case specify value
                          using extra arguments
            @param *args: arguments to select statement
        '''
        self._cursor.execute(query, args)
        return self._cursor.rowcount

    def close(self):
        self._cursor.close()
        self._conn.commit()
        self._conn.close()

    def executemany(self, query, *args):
        self._cursor.executemany(query, *args)
        return self._cursor.rowcount

    def commit(self):
        self._conn.commit()
