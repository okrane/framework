"""
@author: Silviu
"""
import xml.sax.handler
import re
import os
from cheuvreux.dbtools import environment
try:
    from Sybase import Sybase
except:
    import Sybase

if os.path.exists("C:\\Apps\\MATLAB\\work\\st_work.xml"):
    db_connection_file = db_connection_file = "C:\\Apps\\MATLAB\\work\\st_work.xml"
else:
    db_connection_file = "file:" + os.path.join(os.path.dirname(__file__), "st_work.xml")

class Connections:
    """ Connections Handler and DBAccess Class
        Public use:
            Connections.change_connections(...)
            Connections.exec_sql(...)
    """


    connections = environment  # The current active Connection
    cursors     = {}           # Cursor list pointing to different servers (to avoid successive conections)
    bases       = {"quant": "quant", "quant_data":"quant_data", "repository":"repository"}

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
    change_connections = staticmethod (change_connections)

    def getCursor(server):
        """
            @param server: the name of a server
            @return: a cursor pointing to the server
        """
        parser = xml.sax.make_parser()
        handler = db_handler()
        parser.setContentHandler(handler)
        parser.parse(db_connection_file)

        if server.upper() in Connections.cursors:
            return Connections.cursors[server.upper()]
        else:
            #print handler.mapping
            #print server.upper()
            #print handler.mapping[server.upper()][2]
            #print handler.mapping[server.upper()][3]
            #print handler.mapping[server.upper()][1]
            handler.dsn[handler.mapping[server.upper()][0]]
            db = Sybase.connect(handler.dsn[handler.mapping[server.upper()][0]], handler.mapping[server.upper()][2],
                                handler.mapping[server.upper()][3],
                                handler.mapping[server.upper()][1], locking = 0)
            curs = db.cursor()
            Connections.cursors[server.upper()] = curs
            return curs
    getCursor = staticmethod(getCursor)

    def exec_sql(server, query):
        """ Same functionality as its Matlab counterpart
            @param server: the server
            @param query: SQL query to execute
            @return: the result of the query (row list format)
        """
        curs = Connections.getCursor(server.upper())
        curs.execute(query)
        return curs.fetchall()
    exec_sql = staticmethod(exec_sql)

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
