import sys
import quickfix
'''
Created on 4 juil. 2012

@author: vilec
'''


class Application():
    def onCreate(self, sessionID): return
    def onLogon(self, sessionID): return
    def onLogout(self, sessionID): return
    def toAdmin(self, message, sessionID): return
    def toApp(self, message, sessionID): return
    def fromAdmin(self, message, sessionID): return
    def fromApp(self, message, sessionID): return



if __name__ == '__main__':
  
    fileName = "C:\st_project\PythonFix\conf\FIX42.xml"
    
    try:
        settings = quickfix.SessionSettings(fileName)
        application = Application()
        storeFactory = quickfix.FileStoreFactory(settings)
        logFactory = quickfix.FileLogFactory(settings)
        acceptor = quickfix.SocketAcceptor(application, storeFactory, settings, logFactory)
        acceptor.start()
        # while condition == true: do something
        acceptor.stop()
    except quickfix.ConfigError, e:
        print e
