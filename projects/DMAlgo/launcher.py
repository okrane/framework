if __name__ == '__main__':
    import sys
    from datetime import datetime
    from src.import_FIX import DatabasePlug
    
    dates       = ["20130701"]
    
    database_server     = 'TEST'
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