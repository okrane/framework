"""
@author: Silviu
"""
import xml.sax.handler
import pyodbc

import re, os, sys

db_connection_file = "%s\\connections.xml" % os.path.dirname(__file__)

class Connections:
    """ Connections Handler and DBAccess Class
        Public use:
            Connections.change_connections(...)
            Connections.exec_sql(...)
    """
    
    
    connections = "dev"     # The current active Connection        
    bases       = {"quant": "quant", "quant_data":"quant_data", "repository":"repository"}
    connection_objects = {}
    
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
      
        driver = 'SQL Server'if os.name == 'nt' else 'TDS'
        
        driver = handler.mapping[server.upper()][4]
        if driver == 'SQL Server' and os.name != 'nt': driver = 'TDS'
        address = handler.mapping[server.upper()][0].split(':')[0]
        port = handler.mapping[server.upper()][0].split(':')[1]
        database = handler.mapping[server.upper()][1]            
        user = handler.mapping[server.upper()][2]
        password = handler.mapping[server.upper()][3]
        print driver, address, database, user, password, port
        
        cnxn = pyodbc.connect('DRIVER={%s};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s;Port=%s' % (driver, address, database, user, password, port ))        
        curs = cnxn.cursor()        
        return curs, cnxn
    
    @staticmethod
    def exec_sql(server, query, schema = False, as_dict = False, as_dataframe = False):
        """ Same functionality as its Matlab counterpart
            @param server: the server
            @param query: SQL query to execute
            @flag schema: return an additional parameter containing the description of the columns
            @flag as_dict: the return is a list of dictionaries (keys are the column names)
            @flag as_dataframe: the return is a pandas DataFrame (required installed pandas)            
            @return: the result of the query (row list format)
        """
        curs, db = Connections.getCursor(server.upper())
        
        curs.execute(query)
        if query.lstrip("\n ").lower()[:6] == "select":
            result = curs.fetchall()
            keys   = [e[0] for e in curs.description]
            db.close()            
            # iterate over modes and return result            
            if as_dict:
                v = []                
                for row in result:
                    v.append(dict(zip(keys, row)))
                return v
            if as_dataframe:
                import pandas as pd                
                frame = {}
                for i in range(len(keys)):
                    frame[keys[i]] = [a[i] for a in result]                                
                return pd.DataFrame(frame)
            
            return result if not schema else (result, keys)
        else:
            db.commit() 
        
        db.close()
   
        
class db_handler(xml.sax.handler.ContentHandler):
    """ XML parser for st_work.xml
    """
    
    def __init__(self):
        self.mapping = {}        
        self.dsn = {}
        self.bases = {}
        self.conn = ""
        self.driver = ""
        
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
        elif name == "connections":
            self.driver = attributes["driver"]
    def characters(self, data):
        if self.inServer:
            self.buffer = re.split(r'\|', data)
        if self.inDsn:
            self.buffer = data
    def endElement(self, name):
        if (name == "SERVER") and self.connection:
            self.inServer = 0
            self.mapping[self.server] = self.buffer
            self.mapping[self.server].append(self.driver)
        elif(name == "DSN"):
            self.inDsn = 0
            self.dsn[self.conn] = self.buffer
            
                        