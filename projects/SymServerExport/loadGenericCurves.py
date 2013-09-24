#! /usr/bin/python

import sys, os
import symbolMapping
import loadCurves
import exchangeMapping
import loadSymMapping
import loadExchange
import unittest

class extractGenericProfile:
    def __init__ ( self, sourceFile, resultFile, exchanges ):
        self.source = open( sourceFile )
        self.exchanges = exchanges
        self.result = open ( resultFile, 'w' )
        self.curves = []
    def extract(self):
        line = self.source.readline()
        while line :
            data = line.split(';')
            if len ( data ) > 100 :
                destination = data[1]
                context = data[2]
                cotationGroup = data[3]
                rank = data[4]
                active = data[5]
                open = data[11]
                close = data[12]
                intraday = data[13]
		bucketSum = float( open ) + float( intraday ) + float( close )
		for  j in data[14:116]:
			if j != '' :
				bucketSum += float(j) 
                if intraday == "" :
                    intraday = 0
                if active == '1' :
                    name = ''
                    for i in self.exchanges:
                        if i.destination == destination : # and i.cotationGroup == cotationGroup :
                            name = i.name
                            if cotationGroup == "null":
                                cotationGroup = "AG" 
                    if name != "" :
		    	if bucketSum >0.95 and bucketSum < 1.05 : 	
                        	name = name + "_" + cotationGroup   
        	                curve = name + ";0:0:0:0:103:0:" + str(open) + ":" + str(close) + " "
                	        for j in data[14:116]:
                        	    curve += j + ":"
	                        curve += intraday + ": 0"  
                            
                            	exist = False
                            	for i in range(len(self.curves)) :
                                	localName = self.curves[i][1].split(';')[0]
                                	if localName == name :
                                		exist = True
                                    		if self.curves[i][0] > rank :
                                        		self.curves[i] = [ rank, curve + '\n']

                            	if exist == False :
        	                	self.curves.append( [ rank, curve + '\n'] )   
                            #self.result.write( curve + '\n' )
                        else:
                            print 'ERROR: This Curves sum is ' + str( bucketSum ) + ')for ' + str( destination ) + ' + ' + str( context ) 
                    else :
                        print "No name : " + line
#            else:    
#                print "Not enough data : " + line
            line = self.source.readline()

        for i in self.curves :
            self.result.write( i[1] )
        
        self.source.close()
        self.result.close()

class testExtractGenericProfile( unittest.TestCase):
    def setUp ( self ):
        self.sourceFileName = 'testData/generic_curves'
        self.resultFileName = 'result'
        self.exchanges = [ exchangeMapping.exchange( 'PAR', '4', 1, 43 ) ]

    def testNoArgument( self ):
        self.assertRaises( TypeError, extractGenericProfile, None, None, [] ) 

    def testIncorrectInput( self ):
        self.assertRaises( IOError, extractGenericProfile, 'testesd', None, [] )

    def testExtraction( self ) :
        egp = extractGenericProfile( self.sourceFileName, self.resultFileName, self.exchanges ) 
        egp.extract()


# for direct use :

def main():
    flexapp = os.getenv( 'FLEXAPP' )
    if len(sys.argv) < 3 :
        print "not enough arguments"
        return 0
    sourceFile = sys.argv[1]
    sourceExchange = sys.argv[2]
    exportFile = sys.argv[3]

    print "extract exchanges data"
    exch = loadExchange.extractExchangeFromFile( sourceExchange )
    exch.extract()
    for i in exch.exchanges:
        print i.toList()
    
    print "extract generic profile"
    profiles = extractGenericProfile( sourceFile, exportFile, exch.exchanges )
    profiles.extract()

    print( "exiting" )

if __name__ == "__main__":
    sys.exit( main() )




