'''
Created on 25 mars 2010

@author: syarc

Module to handle temporaty table

'''


class TempTable(object):
    '''
        Utility class to handle temporary table
    '''

    def __init__ (self, backend, name, fields):
        '''
            Constructor
            @param backend database backend object
            @param Temporary table name
            @param fields list of fields of temporary table (sql form)
        '''
        self._backend = backend
        self.name = name
        self.fields = fields
        self.index = []

        if self._backend.isTableExist(self.name):
            self.drop()
        self.create()

    @property
    def backend(self):
        return self._backend

    def create(self):
        ''' Create the temporary table '''
        query = 'create table %s ( %s )' % (self.name, self.fields)
        self._backend.execDirect(query)

    def drop(self):
        ''' Drop the temporary table '''
        self._backend.dropTable(self.name)

    def reset(self):
        ''' Truncate data in the table '''
        self.drop()
        self.create()

    def addIndex(self, index_name, columns, clustered=False):
        if clustered:
            clus = 'clustered'
        else:
            clus = ''
        query = 'create %s index %s on %s (%s)' % (clus, index_name,
                                                   self.name, columns)

        self._backend.execDirect(query)
        self.index.append(index_name)
