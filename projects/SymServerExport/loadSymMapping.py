#! /usr/bin/python

import sys, os
import symbolMapping

import unittest

class extractSymbols:

    def __init__ ( self, source ) :
        self.source = open( source )
        self.symbols = []

    def extract( self ):
        line = self.source.readline()
        while line :
            data = line.split(';') 
            if len ( data ) >= 3 :
                ID = data[0]
                sym = data[1]
                AGR = data[2].replace('\r', '')
                AGR = AGR.replace('\n','')
                if AGR == '':
                    AGR = None
                self.symbols.append( symbolMapping.symbol( sym, AGR, None, None, None, None, ID, None, None ) )
            line = self.source.readline()


    def locateSymbol ( self, ID ) :
        for i in self.symbols :
            if i.securityID == str(ID) :
                return [ i.sym, i.symAGR ]
        #return False

class testExtractSymbol( unittest.TestCase):

    def setUp( self ):
        self.es = extractSymbols( 'testData/symMap_sample' )

    def testInit( self) :
        self.assertRaises( TypeError, extractSymbols, None  )
        self.assertRaises( IOError, extractSymbols, 'symMap_sample' )
    def testExtract( self ) :
        self.es.extract()
        self.assertNotEqual( [], self.es.symbols )
         
    def testLocateSymbol( self ):
        self.es.extract()
        self.assertEqual( None, self.es.locateSymbol( 234215423532 ) )
        self.assertNotEqual( None, self.es.locateSymbol( 157 ) )
        self.assertEqual( 'OR.PA', self.es.locateSymbol( 157 )[0] )
    #def tearDown( self ) :
        
def main():
    print "Starting symbol extraction"
    if len (sys.argv) < 2 :
        print "Not enough argument"
        return 0

    source = sys.argv[1]
    map = extractSymbols( source )
    map.extract()
    print "searching for a symbol" 
    print map.locateSymbol( 270120 )


if __name__ == "__main__" :
    sys.exit( main () )







