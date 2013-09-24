#! /usr/bin/python

import sys, os
import exchangeMapping
import unittest

class extractExchangeFromFile:
    def __init__ ( self, refExchange ):
        self.refExchange = open( refExchange )
        self.exchanges= []

    def extract( self ):
        line = self.refExchange.readline()
        while line:
            line.replace( '\n', '')
            data = line.split( ';' )
            if len ( data ) == 4 :
                self.exchanges.append( exchangeMapping.exchange( data[3].replace('\n', ''), data[0], None, None ) )
                line = self.refExchange.readline()

        self.refExchange.close()

#class testExtractExchangeFromFile

# for direct use :

def main():
    flexapp = os.getenv( 'FLEXAPP' )
    if len(sys.argv) < 2 :
        print "not enough arguments"
        return 0
    sourceFile = sys.argv[1]

    print "extract exchanges data"
    exch = extractExchangeFromFile( sourceFile )
    exch.extract()
   
    print len( exch.exchanges ) 
    print( "exiting" )

if __name__ == "__main__":
    sys.exit( main() )

