#! /usr/bin/python

import sys, os
import symbolMapping
import loadCurves
import loadSymMapping
import unittest

class exchange:
    def __init__( self, name, destination, ranking, cotationGroup ):
        self.name = name
        self.destination = destination
        self.ranking = ranking
        self.cotationGroup = cotationGroup

    def __eq__ ( self, other ):
        return ( self.name == other.name ) and ( self.destination == other.destination ) and ( self.ranking ==
        other.ranking ) and ( self.cotationGroup == other.cotationGroup )


    def __ne__ ( self, other):
        return not( self == other )

    def toList ( self ):
        return [ self.name, self.destination, self.ranking, self.cotationGroup ]

class testExchange( unittest.TestCase ):
       
    def testOverloadEQ( self ):
        self.assertTrue( exchange( "FTE.PA", 'PA', 1, 43 ) != exchange( "FTE.PA", 'LN', 1, 43 ) )
        self.assertTrue( exchange( "FTE.PA", 'PA', 1, 43 ) == exchange( "FTE.PA", 'PA', 1, 43 ) )

    def testOverloadNEQ( self ):
        self.assertFalse( exchange( "FTE.PA", 'PA', 1, 43 ) == exchange( "FTE.PA", 'LN', 1, 43 ) )
        self.assertFalse( exchange( "FTE.PA", 'PA', 1, 43 ) != exchange( "FTE.PA", 'PA', 1, 43 ) )

    def testToList ( self ) :
        list = exchange( "FTE.PA", 'PA', 1, 43 ).toList()
        self.assertTrue( list[0] == 'FTE.PA' )
        self.assertTrue( list[1] == 'PA' )
        self.assertTrue( list[2] == 1 )
        self.assertTrue( list[3] == 43 )

class extractExchange:
    def __init__( self, map ):
        self.map = map
        self.exchanges = []

    def extract( self ):
        for i in self.map:
            name  = i.sym.split('.')[len ( i.sym.split('.') ) - 1 ]
            destination = i.destination
            ranking = i.ranking
            cotationGroup = i.exchange
            
#            exist = -1
#            for j in range ( len(self.exchanges) ) :
#                if self.exchanges[j].destination == destination and self.exchanges[j].cotationGroup ==cotationGroup :
#                    if self.exchanges[j].ranking > int(ranking) :
#                        exist = j 
            temp = exchange( name, destination, ranking, cotationGroup )
            if name != "" :
                if len ( self.exchanges ) == 0 :
                    self.exchanges.append( temp )
                else :
                    exist = False
                    for j in self.exchanges:
                        if j == temp :
                            exist = True
                    if exist == False :
                        self.exchanges.append( temp )
#            else:
#                self.exchanges[exist] = temp    

class testExtractExchange( unittest.TestCase ):

    def setUp( self ):
        self.map = [ symbolMapping.symbol( "FTE.PA", None, None, None, None, None, None, None, None ),
symbolMapping.symbol( "ASSA B.SX", None, None, None, None, None, None, None, None ) ]

    def testNoMap( self ):
        ee =extractExchange( None )
        self.assertRaises( TypeError, ee.extract )

    def testExtract( self ):
        ee = extractExchange( self.map ) 
        ee.extract()
        self.assertEqual( 2, len(ee.exchanges) )

# for direct use :

def main():
    flexapp = os.getenv( 'FLEXAPP' )
    if len(sys.argv) < 2 :
        print "not enough arguments"
        return 0
    sourceCheuvreux = sys.argv[1]
    sourceSec = sys.argv[2]

    print "extract map"
    mapping = loadSymMapping.extractSymbols( sourceSec )
    mapping.extract()
    print "extract symbol mapping"
    mapping = symbolMapping.symbolMapping( mapping.symbols, sourceCheuvreux )
    mapping.extract()
    print "reconciliate the data"
    map = mapping.reconciliate()
    for i in map:
        print i.toList()

    print "extract exchanges data"
    exch = extractExchange( map )
    exch.extract()
    for i in exch.exchanges:
        print i.toList()
    print len( exch.exchanges )
    print( "exiting" )

if __name__ == "__main__":
    sys.exit( main() )

