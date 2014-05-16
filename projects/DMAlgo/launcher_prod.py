if __name__ == '__main__':
    from datetime import datetime, timedelta
    from src.import_FIX import DatabasePlug
    from lib.dbtools.connections import Connections
    Connections.change_connections("production")
    
    database_server     = 'MARS'
    database            = 'Mars'
    environment         = 'prod'
    source              = 'CLNT1'
    yesterday           = datetime.now() - timedelta(days = 1)
    date                = datetime.strftime(yesterday, "%Y%m%d")
    dates               = [date]
   
    # data_server: mongodb
    # database: mongodb
    # source: FIX Logs
    DatabasePlug(database_server    = database_server, 
                 database           = database,
                 environment        = environment, 
                 source             = source, 
                 dates              = dates,
                 mode               = "write").fill()