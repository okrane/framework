import logging
"""
    Available options :
        --loglevel=1        -> Display only all log => Critical
        --loglevel=2        -> Display only all log => Error
        --loglevel=3        -> Display only all log => Warning
        --loglevel=4        -> Display only all log => Info
        --loglevel=5        -> Display only all log => Debug
        --to_file           -> Log also to file
        --capture_stdout    -> redirect stdout to logging
        --capture_stderr    -> redirect stderr to logging
"""
class Logger:
    __done = False
    logging = None
    def __init__(self):
        if not Logger.__done:
            level           = 5
            to_file         = False
            capture_stdout  = False
            capture_stderr  = True
            import sys
            for arg in sys.argv:
                if "loglevel=" in arg.lower():
                    level =  int(arg[arg.find("loglevel=") + 9])
                if "to_file" in arg.lower():
                    to_file = True
                if "capture_stdout" in arg.lower():
                    capture_stdout = True
                if "capture_stderr" in arg.lower():
                    capture_stderr = True
            
            Logger.__done = True
            
            self.set(level, to_file, capture_stdout, capture_stderr)
            
    def set(self, level, to_file = False, capture_stdout = False, capture_stderr = False):
        if level == 4:
            lev = logging.INFO
        elif level == 3:
            lev = logging.WARNING
        elif level == 2:
            lev = logging.ERROR
        elif level == 1:
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
        
        
        
     
        class StreamToLogger(object):
            """
            Fake file-like stream object that redirects writes to a logger instance.
            """
            def __init__(self, logger, log_level=logging.INFO):
                self.logger = logger
                self.log_level = log_level
                self.linebuf = ''
     
            def write(self, buf):
                for line in buf.rstrip().splitlines():
                    self.logger.log(self.log_level, line.rstrip())
        import sys     
        stdout_logger = logging.getLogger('STDOUT')
        sl = StreamToLogger(stdout_logger, logging.INFO)
        if capture_stdout:
            sys.stdout = sl
         
        stderr_logger = logging.getLogger('STDERR')
        sl = StreamToLogger(stderr_logger, logging.ERROR)
        if capture_stderr:
            sys.stderr = sl

#Logger()

