import logging

class Logger:
    __done = False
    logging = None
    def __init__(self):
        if not Logger.__done:
            level = 1
            to_file = False
            
            import sys
            for arg in sys.argv:
                if "loglevel=" in arg.lower():
                    level =  int(arg[arg.find("loglevel=") + 9])
                if "to_file=true" in arg.lower():
                    to_file = True
            
            Logger.__done = True
            
            self.set(level, to_file)
            
    def set(self, level = 0, to_file = False):
        if level == 2:
            lev = logging.INFO
        elif level == 3:
            lev = logging.WARNING
        elif level == 4:
            lev = logging.ERROR
        elif level == 5:
            lev = logging.CRITICAL
        else:
            lev = logging.DEBUG
        
        print "The chosen logging level is: " + str(logging._levelNames[lev])

        Logger.__done = True
        import socket
        import datetime
        import pytz
        
        # Remove all the handlers 
        logging.getLogger('').handlers = []
        # Default to file
        if to_file:
            file = datetime.datetime.now(tz = pytz.timezone('UTC')).strftime("%Y%m%d-%H%M-") + socket.gethostname() + '.log'
            logging.basicConfig(level   =lev,
                                format  ='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                                datefmt ='%m-%d %H:%M',
                                filename= file,
                                filemode='a')
            print "Log at:" + file
        else:
            logging.getLogger('').setLevel(lev)
        # Console
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(filename)s:%(lineno)d \t %(message)s')
        console.setFormatter(formatter)
        logging.getLogger('').addHandler(console)
        Logger.logging = logging

Logger()