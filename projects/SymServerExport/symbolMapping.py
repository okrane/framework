#! /usr/bin/python

import sys, os


class symbol:

    def __init__ ( self, sym, symAGR, sedol, isin, reuter, exchange, securityID, destination, ranking ):
        #self.sym = sym
        #self.symAGR = symAGR
        self.sedol = sedol
        self.isin = isin
        self.reuter = reuter
        self.exchange = exchange
        self.securityID = securityID
        self.destination = destination
        self.ranking = ranking        
        
        if sym != None :
            self.sym = sym.replace( ' ', '|' )
        else :
            self.sym = sym

        if symAGR !=None :
            self.symAGR = symAGR.replace( ' ', '|' )
        else:
            self.symAGR = symAGR

    def toList ( self ):
        return [self.sym, self.symAGR, self.sedol, self.isin, self.reuter, self.exchange, self.securityID, self.destination, self.ranking ]

class symbolMapping :

    def __init__ ( self, mapCheuvreux, sourceFileSecurity ) :
        self.sourceFileSecurity = sourceFileSecurity
        self.mapCheuvreux = mapCheuvreux
        self.mapSec = []
    def extract(self):

        # Second pass: completing the missing fields ( Sedol ) from Cheuvreux's security base 
        f = open( self.sourceFileSecurity )
        line = f.readline()
        while line:
            line = line.replace( '\n', '' )
            data = line.split(';')
            if len(data) >6 :
                self.mapSec.append( symbol ( None, None, data[6], data[4], data[5], data[3], data[0], data[1], data[2] ))
            line = f.readline()
        f.close()
    
    def reconciliate ( self ):
        result = []
        for ticker in self.mapSec:
            security = self.locateSecurity( ticker.securityID, None )
            if security != None :
                result.append( symbol( security.sym, security.symAGR, ticker.sedol, ticker.isin, ticker.reuter, ticker.exchange, ticker.securityID, ticker.destination, ticker.ranking ) )
            else:
                print "Couldn't find : " + str(ticker.toList())
            #print ticker.toList()
        return result
    
    def locateSecurity( self, ID, destination ):        
        for i in self.mapCheuvreux:
            if i.securityID == ID : #and i.destination == destination:
                return i
        return None 

    def locateSymbol ( self, isin, exchange ):
        for i in self.mapCheuvreux :
            if i.exchange == exchange :
                if i.isin== isin :
                    return i.sym
                #elif i.sedol == id :
                #    return i.sym 

        return "not found"

    def locateReuter( self, isin, exchange ):
        for i in self.mapSec :
            if i.exchange == exchange :
                if i.isin== isin :
                    return i.reuter
                #elif i.sedol == id :
                #    return i.reuter

        return "not found"

    def locateIsin ( self, sym ):
        for i in self.mapCheuvreux :
            if i.sym == sym :    
                return i.isin
        return "not found"     

    def locateSedol ( self, sym ):
        reuter = self.locateReuter( self.locateIsin( sym ), self.locateSectorCode( sym ) )
        for i in self.mapSec :
            if i.reuter == reuter :
                return i.sedol
        return "not found"

    def locateSectorCode ( self, sym ):
        for i in self.mapCheuvreux :
            if i.sym == sym :
                return i.exchange
        return "not found" 

# for direct use :

def main () :
    print ( "starting symbol mapping" )
    
    if len( sys.argv) < 3 : 
        print "not enough arguments"
        return 0

    sourceFileSecurity = sys.argv[2]
    
    #map = symbolMapping ( sourceCheuvreux, sourceFileSecurity )
    #map.extract() 
   
#    print "Real symbol" 
#    print map.locateSymbol( "FR0000120404", "F1" )
#    print map.locateReuter( "FR0000120404", "F1" )
#    print map.locateIsin("AC.PA")
#    print map.locateSedol("AC.PA")
#    print map.locateSectorCode("AC.PA")

#    print "\nFake symbol"
#    print map.locateSymbol( "FR0000120404PA", "F1" )
#    print map.locateIsin("AC.PAPA")
#    print map.locateSedol("AC.PAPA") 
#    print map.locateSectorCode("AC.PAPA")
    
    print "reconcilating the date"
    #data = map.reconciliate()
    print "done"
    #for i in data:
    #    print i.toList()

    print ( "exiting symbol mapping" )
    return 0

if __name__ == "__main__":
    sys.exit( main () )
