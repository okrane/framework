'''
Created on Feb 18, 2010

@author: syarc
'''
import sys
import csv
import gzip

try:
    import sqlite3
except:
    # sqlite3 is not available (probably a old python version)
    # try with pysqlite2
    from pysqlite2 import dbapi2 as sqlite3

import types

class SQLiteBase(object):
    ''' Base class for all object dealing with SQLite base
        It provides some basic methods for processing select and insert queries.
    '''
    def __init__(self, filename):
        self._db = filename
        self._conn = sqlite3.connect(self._db)
        self._conn.text_factory = str # No unicode support
        self._conn.row_factory = sqlite3.Row

    def select(self, query, *args):
        ''' Execute a simple select statement and returns its results
            @param Row SQL query
        '''
        cursor = self._conn.cursor()
        cursor.execute(query, args)
        res = cursor.fetchall()
        cursor.close()

        return res

    def selectOne(self, query, *args):
        ''' Execute a simple select statement and returns first line
            @param Row SQL query
        '''
        cursor = self._conn.cursor()
        cursor.execute(query, args)
        res = cursor.fetchone()
        cursor.close()

        return res

    def executeMany(self, query, generator):
        cursor = self._conn.cursor()
        try:
            cursor.executemany(query, generator)
        finally:
            cursor.close()
            self._conn.commit()

    def execute(self, query, *args):
        ''' Simply execute a query, returns nothing
            @param SQL query
            @param *args extra binded parameters
        '''
        cursor = self._conn.cursor()
        try:
            cursor.execute(query, args)
        finally:
            cursor.close()
            self._conn.commit()

    def column_names (self, table_name):
        cols = None
        cursor = self._conn.cursor()
        try:
            cursor.execute('SELECT * FROM %s WHERE 1=2' % table_name)
            cols = cursor.description
        finally:
            cursor.close()

        return [col[0] for col in cols]

    #  ----------------------------------------------
    #            Insert Statement
    #  ----------------------------------------------

    def prepareInsertStmt(self, tablename, keys):
        ''' Returns a query as a string
        '''
        return  ('INSERT INTO "' + tablename + '" ('
                 + ','.join(keys) + ') VALUES ('
                 + ','.join(map(lambda x: ':' + x, keys)) + ')'
                 )

    def execPreparedInsertStmt(self, stmt, data):
        ''' Execute a prepared statement
            @param stmt String returns from prepareInsertStmt
            @return Nothing
        '''
        cursor = self._conn.cursor()
        try:
            cursor.execute(stmt, data)
        finally:
            cursor.close()
            self._conn.commit()

    def execManyPreparedInsertStmt(self, stmt, generator):
        ''' ExecuteMany a prepared statement
            @param stmt String returns from prepareInsertStmt
            @param generator
            @return Nothing
        '''
        cursor = self._conn.cursor()
        try:
            cursor.executemany(stmt, generator)
        finally:
            cursor.close()
            self._conn.commit()

    def insertStmt(self, tablename, data):
        ''' Execute a insert statement
            @param tablename: table name
            @param data. dictionary containing data
        '''

        query = ('INSERT INTO "' + tablename + '" ('
                 + ','.join(data.keys()) + ') VALUES ('
                 + ','.join(map(lambda x: ':' + x, data.keys())) + ')'
                 )

        print query
        self.execute(query, data)

    #  ----------------------------------------------
    #            Utilities method
    #  ----------------------------------------------

    def dropTable(self, name):
        """ Drop table 'name' """
        self._conn.execute('DROP TABLE "%s"' % name)

    def isTableExist(self, tablename):
        query = "select 1 from sqlite_master where type = 'table' and name = '%s'" % tablename
        c = self._conn.cursor()
        c.execute(query)
        row = c.fetchone()
        return row is not None

    def createTable(self, name, columns, dropIfExist=False):
        """ Create a table called 'name'
            @param name: Table name
            @param columns: List of columns with their type (e.g "id INTEGER, value TEXT")
            @param dropIfExist: If True, the table is dropped
        """
        if dropIfExist and self.isTableExist(name):
            self.dropTable(name)

        if type(columns) is not type('') and hasattr(columns, '__len__') and len(columns) > 0:
            columns = ','.join(columns)

        self.execute('CREATE TABLE "%s" (%s)' % (name, columns))

    def createIndex(self, name, table, columns, unique = False):
        ''' Create a index called 'name' on table 'table'

            @param name: Index name
            @param table: Table
            @param columns: Table columns to be indexed
            @param unique: Is the index unique
        '''

        unique_str = ''
        if unique:
            unique_str = ' UNIQUE '

        self.execute('CREATE %s INDEX "%s" ON "%s" (%s)' % (unique_str, name, table, columns))

    def close(self):
        self._conn.close()

class sqliteWriter(SQLiteBase):
    """ Wrapper for accessing SQLite database """
    def __init__(self, filename):
        SQLiteBase.__init__(self, filename)

    def write(self, table, generator, dropIfExist=False):
        header = generator.next()

        # Generic insert statement
        query = ('insert into ' + table + ' ('
                 + ','.join(map(str, header))
                 + ') values (' + ','.join(map(lambda x: ':' + x, header)) + ')'
                 )


        self.execute(query, header)

        # Table does not exist
        if not self.isTableExist(table):
            first_line = generator.next() # the first line is used to deduced type
            self.createTableFromData(table, first_line, dropIfExist)
            self._conn.execute(query, first_line) # Insert line pop

        # Process the rest
        self._conn.executemany(query, generator)

    def createTableFromData(self, name, line, dropIfExist=False):
        """ Create a table called 'name', the column are deduced from the dictionary 'line'
            @param name: Table name
            @param line: Dictionary of sample value to insert into the table (e.g {'id':1, 'value': 'toto')
            @param dropIfExist: If True, the table is dropped
        """
        if dropIfExist and self.isTableExist(name):
            self.dropTable(name)

        cursor = self._conn.cursor()
        fields = ', '.join(f for f in self._field_creation(line))
        cursor.execute("CREATE TABLE %s (%s)" % (name, fields))
        self._conn.commit()
        cursor.close()

    def _field_creation(self, dict):
        """ Generator used for generating table from a
            dictionary
        """
        for i in dict:
            yield ' '.join([i, self._typemap(type(dict[i]))])


    def _typemap(self, type):
        """ Map python type with sqllite type """
        if type == types.IntType:
            return 'INTEGER'
        elif type == types.FloatType:
            return 'REAL'
        elif type == types.StringType:
            return 'TEXT'
        else:
            raise ValueError('SQLLite : ' + str(type) + ' is unknown !')

def execDeleteStmt(database, query):
    conn = sqlite3.connect(database)
    try:
        cursor = conn.execute(query)
        rowcount = cursor.rowcount
        cursor.close()
    finally:
        conn.close()

    return rowcount

def execSelectStmt(database, query):
    conn = sqlite3.connect(database)
    try:
        cursor = conn.execute(query)
        result = cursor.fetchall()
        cursor.close()
    finally:
        conn.close()

    return result


def create_table_from_csv(db, filename, table_name, force=False, delimiter=',',  excluded_cols=[]):

    if filename.endswith('.gz'):
        fd = gzip.open(filename, 'rb')
    else:
        fd = open(filename, 'r')

    reader = csv.DictReader(fd, delimiter = delimiter)
    fieldnames = [f for f in reader.fieldnames if f not in excluded_cols]

    db.createTable(table_name, [f + ' TEXT' for f in fieldnames],force)

    stmt = db.prepareInsertStmt(table_name, fieldnames)
    db.execManyPreparedInsertStmt(stmt, reader)

    fd.close()
