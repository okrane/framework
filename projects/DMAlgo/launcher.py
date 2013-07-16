if __name__ == '__main__':
    from datetime import datetime, timedelta
    from src.import_FIX import DatabasePlug
    
    
    database_server     = 'HPP'
    database            = 'Mars'
    environment         = 'prod'
    source              = 'CLNT1'
    yesterday           = datetime.now() - timedelta(days = 1)
    date                = datetime.strftime(yesterday, "%Y%m%d")
    dates               = [date]
    
    DatabasePlug(database_server    = database_server, 
                 database           = database,
                 environment        = environment, 
                 source             = source, 
                 dates              = dates,
                 mode               = "write").fill()