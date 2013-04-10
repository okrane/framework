"""
@author: Silviu
"""
import xml.sax.handler
#from Sybase import *
#from Sybase import Sybase
import re, os, sys
import Sybase
try:
    from Sybase import Sybase
except:
    pass

db_connection_file = "%s/st_work.xml" % os.path.dirname(__file__)



class Connections:
    """ Connections Handler and DBAccess Class
        Public use:
            Connections.change_connections(...)
            Connections.exec_sql(...)
    """
    
    
    connections = "production"     # The current active Connection    
    cursors     = {}        # Cursor list pointing to different servers (to avoid successive conections)
    bases       = {"quant": "quant", "quant_data":"quant_data", "repository":"repository"}
    
    @staticmethod
    def change_connections(con_name):
        """ 
            @param con_name: Connection Name (same syntax as Matlab)
        """
        Connections.connections = con_name
        Connections.cursors = {}
        
        parser = xml.sax.make_parser()
        handler = db_handler()
        parser.setContentHandler(handler)
        parser.parse(db_connection_file)
        
        Connections.bases = handler.bases 
        
    @staticmethod
    def getCursor(server):
        """ 
            @param server: the name of a server
            @return: a cursor pointing to the server
        """
        parser = xml.sax.make_parser()
        handler = db_handler()
        parser.setContentHandler(handler)
        parser.parse(db_connection_file)
        
        #print handler.mapping
        #print server.upper()
        #print handler.mapping[server.upper()][2]
        #print handler.mapping[server.upper()][3]
        #print handler.mapping[server.upper()][1]            
        db = Sybase.connect(server.upper(), handler.mapping[server.upper()][2],
                                handler.mapping[server.upper()][3],
                                handler.mapping[server.upper()][1])
        curs = db.cursor()
        return curs
    
    @staticmethod
    def exec_sql(server, query):
        """ Same functionality as its Matlab counterpart
            @param server: the server
            @param query: SQL query to execute
            @return: the result of the query (row list format)
        """
        curs = Connections.getCursor(server.upper())
        curs.execute(query)
        return curs.fetchall()
    
    @staticmethod
    def exec_sql_schema(server, query):
        """ Same functionality as its Matlab counterpart
            @param server: the server
            @param query: SQL query to execute
            @return: the result of the query (row list format)
        """
        curs = Connections.getCursor(server.upper())
        curs.execute(query)
        return (curs.fetchall(), [e[0] for e in curs.description])
        
class db_handler(xml.sax.handler.ContentHandler):
    """ XML parser for st_work.xml
    """
    
    def __init__(self):
        self.mapping = {}        
        self.dsn = {}
        self.bases = {}
        self.conn = ""
        
        self.buffer = []
        
        self.inDsn = 0
        self.connection = 0
        self.inServer = 0
        
    def startElement(self, name, attributes):
        if name == "target":
            self.connection = attributes["name"] == Connections.connections
        elif name == "SERVER":
            self.server = attributes["NAME"]
            self.inServer = 1
        elif name == "DSN":
            self.conn = attributes["connection"]
            self.inDsn = 1
        elif name == "base":
            self.bases[attributes["key"]] = attributes["name"]
    def characters(self, data):
        if self.inServer:
            self.buffer = re.split(r'\|', data)
        if self.inDsn:
            self.buffer = data
    def endElement(self, name):
        if (name == "SERVER") and self.connection:
            self.inServer = 0
            self.mapping[self.server] = self.buffer
        elif(name == "DSN"):
            self.inDsn = 0
            self.dsn[self.conn] = self.buffer
            
                        
