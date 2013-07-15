if __name__ == '__main__':
    import sys
    from datetime import datetime
    from src.import_FIX import DatabasePlug
    from lib.dbtools.connections import Connections
    Connections.change_connections("production")
    
    database_server     = 'PROD'
    database            = 'Mars'
    environment         = 'prod'
    source              = 'CLNT1'
    dateToday           = datetime.strftime(datetime.now(), "%Y%m%d")
    dates               = [dateToday]
    
    DatabasePlug(database_server    = database_server, 
                 database           = database,
                 environment        = environment, 
                 source             = source, 
                 dates              = dates,
                 mode               = "write").fill()