'''
Created on Sep 27, 2010

@author: syarc
'''
import numpy

class Matrix:
    def __init__(self, nrow, ncol):
        self._data = numpy.zeros(shape=(nrow,ncol))
        self._rows = dict()
        self._cols = dict()

    def _get_position(self, value, list):
        if type(value) is type(''):
            if value in list:
                position = list[value]
            else:
                position = len(list)
                list[value] = position
        else:
            position = value

        return position

    def setitem(self, row, col, value):

        # Deduce row number
        irow = self._get_position(row, self._rows)

        # Deduce col number
        icol = self._get_position(col, self._cols)

        try:
            self._data[irow][icol]= value
        except IndexError as ie:
            print "Index Error: (%s,%s)" % (row, col)
            raise ie


    def __getitem__(self, row):
        irow = self._get_position(row, self._rows)
        return self._data[irow]

    def getitem(self, row, col):
        irow, icol = self._get_position(row, self._rows), self._get_position(col, self._cols)

        return self._data[irow][icol]

    def rowname(self, row):
        for code, position in self._rows.iteritems():
            if position == row:
                return code

    def colname(self, col):
        for code, position in self._cols.iteritems():
            if position == col:
                return code

    def row_iterator(self):
        return self._rows.__iter__()

    def col_iterator(self):
        return self._cols.__iter__()


