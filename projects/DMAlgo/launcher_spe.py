if __name__ == '__main__':
    from datetime import datetime, timedelta
    from src.import_FIX import DatabasePlug
    from lib.dbtools.connections import Connections
    Connections.change_connections("production")
    
    database_server     = 'MARS'
    database            = 'Mars'
    environment         = 'prod'
    source              = 'CLNT1'
    dates               =  ['20130812', '20130813', '20130814', '20130815', '20130816',
                            '20130805', '20130806', '20130807', '20130809', '20130810',
                            '20130729', '20130730', '20130731', '20130801', '20130802',
]

    
    dates               = sorted(dates)
    DatabasePlug(database_server    = database_server, 
                 database           = database,
                 environment        = environment, 
                 source             = source, 
                 dates              = dates,
                 mode               = "write").fill(order_deals=True)