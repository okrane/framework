#! /usr/bin/python

import sys, os
import symbolMapping



class curve :
    def __init__ ( self, destination, securityID, symbol, ranking, active, open, intraday, close, buckets ):
        self.destination = destination
        self.securityID = securityID
        self.symbol = str(symbol)
        self.ranking = ranking
        self.active = active
        self.open = open
        self.intraday = intraday
        self.close = close
        self.buckets = buckets        
    
    def toString( self ,separator):
        separator = str( separator )
        result =  str(self.symbol) + separator + str(self.open) + separator + str(self.close) + separator 
        for i in self.buckets:
            result += str(i) + separator
        result += self.intraday
        return result 

class profile :
    def __init__ ( self, id, destination, name, rank, dir ):
        self.id = id
        self.destination = destination
        self.name = name 
        self.rank = rank
        self.dir = dir
        self.file = open( dir + "/VWAP_Profile_" + str(id) , 'w')
        self.dataContainer = []

    def addSym( self, line ):
        data = line.split(';')
        symbol = data[0]
        destination = data[1]
        rank = data[4]
        bucketSum =  float(data[11])+ float(data[13])+ float(data[12])
        if data[5] == '1' : # Filter active profiles only
    	    buckets = []	
            for i in data[14:116]:
	    		if len(i) > 1:
                    		buckets.append( float(i) )
                    		bucketSum += float(i) 
                	else:
				buckets.append( 0.0 )
            if bucketSum >0.95 and bucketSum < 1.05 : 
                #Check that the symbol isn't already loaded with a better 
                exist = False
                for j in range( len(self.dataContainer) ) :
                    if self.dataContainer[j].securityID == data[0] and self.dataContainer[j].destination == destination:
                        exist = True
                        if self.dataContainer[j].ranking > self.rank : 
                        	self.dataContainer[j] = curve( destination, data[0], None, data[4], data[5], data[11], data[13], data[12], buckets )
                if exist == False :
                    self.dataContainer.append ( curve( destination, data[0], None, data[4], data[5], data[11], data[13], data[12], buckets ) )
            else :
                print 'ERROR: This Curves sum is ' + str( bucketSum ) + ')for ' + str( data[0] ) + ' + ' + str(data[4] )

    def reconciliate( self, symMap ) :
        for i in self.dataContainer:
            for j in symMap :
                if (j.securityID == i.securityID) :
                    if str(i.destination) == "all":
                        i.symbol = j.symAGR
                    else:
                        i.symbol = j.sym

    def toFile( self, separator):
        for i in self.dataContainer :
            self.file.write( i.toString( separator ) + '\n' )  
    
	def __del__ ( self ):
		self.file.close()

class extractProfiles :

	def __init__ (self, source, resultDir, header ):
		self.source = source
		self.resultDir = resultDir
		self.headerFile =header
		self.profiles = [] 
		# need to check that the file/directory exist
		
	def extract(self):
		f = open( self.source )
		
        	p = None
        	self.profiles.append( profile( 0, None, 'default', None, self.resultDir ) )
        	p =  self.profiles[0]
        	pdefault =  self.profiles[0]

		line = f.readline()
		while line:
			if len(line.split(';'))>3 :
				context = line.split(';')[2]	
                		ranking = line.split(';')[4]
				#if line.split(';')[1] == "all" :
				#	# Aggregated symbol	
				#	context +="_AGR" 
			
				# check if the profile already exist and create it if it doesn't
				exist = False
				for i in self.profiles:
					if i.name == context : #and i.rank == ranking :
						exist = True
						p = i
				if exist == False :
					self.profiles.append( profile( len(self.profiles), line.split(';')[1], context, line.split(';')[4], self.resultDir ) )		
					p = self.profiles [ len( self.profiles ) -1 ]
					# load the next line
				p.addSym(line)
				pdefault.addSym(line)                
			line = f.readline()
		f.close()
    
        def reconciliate ( self, map ) :
            for i in self.profiles:
                i.reconciliate( map )

	def toFile (self, separator):
            for i in self.profiles :
                i.toFile( separator )        

	def __del__ ( self ):
		self.header = open( self.headerFile, 'w' )

		n= len(self.profiles) + 1
		message = "const int nProfiles =" + str(n) +";\n" 
		message +="const char [32] ={ "
		for i in self.profiles:
			message += "\"" + i.name + "\","

		message += "\"None\" };\n"
		self.header.write( message )

		self.header.close()		


# for direct use :
		
def main():
	flexapp = os.getenv( 'FLEXAPP' )
	if len(sys.argv) < 4 :
		print "not enough arguments"
		return 0 
	sourceFile = sys.argv[1]
	exportDir = sys.argv[2]
	header = sys.argv[3]
    
	profiles = extractProfiles( sourceFile, exportDir, header )
	profiles.extract()
        profiles.toFile( '\t')
	print( "exiting" )

if __name__ == "__main__":
	sys.exit( main() )



