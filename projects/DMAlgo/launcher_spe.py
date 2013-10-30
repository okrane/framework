if __name__ == '__main__':
    from datetime import datetime, timedelta
    from src.import_FIX import DatabasePlug
    from lib.dbtools.connections import Connections
    Connections.change_connections("production")
    
    database_server     = 'MARS'
    database            = 'Mars'
    environment         = 'prod'
    source              = 'CLNT1'
    dates               =  ['20131014', '20131015', '20131016']

    
    dates               = sorted(dates)

    DatabasePlug(database_server    = database_server, 
                 database           = database,
                 environment        = environment, 
                 source             = source, 
                 dates              = dates,
                 mode               = "write").fill(order_deals=True)
                 
    Connections.change_connections("dev")
    dates               = sorted(dates)
    DatabasePlug(database_server    = database_server, 
                 database           = database,
                 environment        = environment, 
                 source             = source, 
                 dates              = dates,
                 mode               = "write").fill(order_deals=True)