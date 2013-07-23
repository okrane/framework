if __name__ == '__main__':
    from datetime import datetime, timedelta
    from src.import_FIX import DatabasePlug
#     from lib.dbtools.connections import Connections
#     Connections.change_connections("production")
    
    database_server     = 'MARS'
    database            = 'Mars'
    environment         = 'prod'
    source              = 'CLNT1'
    dates               =  ['20130708','20130709','20130710','20130711','20130712',
                            '20130701','20130702','20130703','20130704','20130705',
                            '20130624','20130625','20130626','20130627', '20130628',
                            '20130617','20130618','20130619','20130620','20130621',
                            '20130610','20130611','20130612','20130613','20130614',
                            '20130603','20130604','20130605','20130606','20130607']
    
    DatabasePlug(database_server    = database_server, 
                 database           = database,
                 environment        = environment, 
                 source             = source, 
                 dates              = dates,
                 mode               = "write").fill(order_deals=False)