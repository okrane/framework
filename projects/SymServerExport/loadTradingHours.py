#! /usr/bin/python

import sys, os
import unittest
import loadExchange

class extractExchangeParameters:
    def __init__( self, sourceFile, exchange, exportFile ):
        self.source = open(sourceFile )
	self.exchange = exchange
        self.result = open ( exportFile, 'w' )

    def HHmm ( self, input ):
	data = input.split(':')
	if len ( data ) == 3  :
		return data[0] + ':' + data[1]
	else:
		return '00:00'

    def extract( self ):
        line = self.source.readline()

        while line :
            data = line.split( ';' )
	    if len ( data ) > 10 :
		destination = data[0]
		cotationGroup = data[1]

		openingAuc = self.HHmm( data[2])
		openingFix = self.HHmm( data[3])

		opening = self.HHmm( data[4])
		closing = self.HHmm( data[12])

		intradayAuc = self.HHmm( data[5])
		intradayFix = self.HHmm( data[6])
		intradayStop = self.HHmm( data[7])
		intradayResumptionAuc = self.HHmm( data[8])
		intradayResumption = self.HHmm( data[9])
	
		closingAuc = self.HHmm( data[11])
		closingFix = self.HHmm( data[12])

		postOpening = self.HHmm( data[13])
		postClosing = self.HHmm( data[14])

		tradingAtLastOpening = self.HHmm( data[15])
		tradingAtLAstClosing = self.HHmm( data[16])

		tradingAfterHoursOpening = self.HHmm( data[17])
		tradingAfterHoursClosing = self.HHmm( data[18])

		contextID = data[19]
	
		name = ''	
		output = None
		for i in self.exchange:
			if i.destination == destination:
				name = i.name
		if cotationGroup == '':
			output = 'Country=' + name + ',Exch:Sector=' + name + ',Currency=--,Multiplier=1.00,Broker=TEST,Open=' + opening + ',Close=' + closing + ',Auc Open Start=' + openingAuc + ',Auc Open End=' + openingFix + ',Auc Intra Start=' + intradayAuc + ',Auc Intra End=' + intradayFix + ',Auc Close Start=' + closingAuc + ',Auc Close End=' + closingFix + ',Time Diff=0,Electronic=YES,Holiday=NO,'
		else:
			output = 'Country=' + name + ',Exch:Sector=' + name + ':' + cotationGroup + ',Currency=--,Multiplier=1.00,Broker=TEST,Open=' + opening + ',Close=' + closing + ',Auc Open Start=' + openingAuc + ',Auc Open End=' + openingFix + ',Auc Intra Start=' + intradayAuc + ',Auc Intra End=' + intradayFix + ',Auc Close Start=' + closingAuc + ',Auc Close End=' + closingFix + ',Time Diff=0,Electronic=YES,Holiday=NO,'
		
		if name != '' and output != None:
			self.result.write( output + '\n' )

            line = self.source.readline()

	# adding default AGR
	self.result.write( 'Country=AGGR,Exch:Sector=AG,Currency=--,Multiplier=1.00,Broker=TEST,Open=07:00,Close=17:30,Auc Open Start=07:00,Auc Open End=09:00,Auc Intra Start=00:00,Auc Intra End=00:00,Auc Close Start=17:30,Auc Close End=17:35,Time Diff=0,Electronic=YES,Holiday=NO,\n' )

        self.source.close()
        self.result.close()


class testExtractExchangeParameters( unittest.TestCase):
    def setup( self ):
        self.inputFile = "file"

    def testNoFile( self ):
        self.assertRaises( TypeError, extractExchangeParameters, None, 'file' )

    def testInvalidFile ( self ):
        self.assertRaises( IOError, extractExchangeParameters, 'someFile', None)

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

    print "extracting exchange parameteres"
    trd_time = extractExchangeParameters( sourceFile, exch.exchanges, exportFile )
    trd_time.extract()

if __name__ == "__main__":
    sys.exit( main() )

