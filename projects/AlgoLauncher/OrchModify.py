import quickfix as fix
import threading
import time
import datetime
import xml.etree.ElementTree as ET
from lib.io.ConfigParser_NL import ConfigParser_NL
import calendar

'''
Created on 4 juil. 2012

@author: gupon
'''

INIT = 'INIT'
CONNECT = 'CONNECTED'

STATUS = 'FIRST'
LASTORDERID = ''
LASTClORDERID = ''

def log(logfile, log_str):
    
    log_file = open(logfile, 'a')
    hour = str(datetime.datetime.now())[11:-3]
    
    log_file.write("%s : %s\n" %(hour, log_str))
    log_file.close()
    
class Application(fix.Application):
    
    state = INIT
    
    def onCreate(self, sessionID):
        self.session_id = sessionID
        self.state = INIT
        print "Creating New FIX Session with Session_id : %s" %self.session_id
        log(LOG_FILE, "Creating New FIX Session with Session_id : %s" %self.session_id)
        return
    
    def onLogon(self, sessionID):
        self.state = CONNECT
        print "Log on complete for FIX session : %s " %self.session_id
        log(LOG_FILE, "Log on complete for FIX session : %s " %self.session_id)
        return
    
    def onLogout(self, sessionID):
        self.state = INIT
        print "Session : %s, logged out " %self.session_id
        log(LOG_FILE, "Session : %s, logged out " %self.session_id)
        return
    
    def toAdmin(self, message, sessionID): return
    def toApp(self, message, sessionID): return
    def fromAdmin(self, message, sessionID): return
    
    
    def fromApp(self, message, sessionID):
        
        global STATUS
        global LASTClORDERID
        global LASTORDERID
        
        msgType = fix.MsgType()
        ExecType = fix.ExecType()
        OrdStatus = fix.OrdStatus()
        OrigClOrderID = fix.OrigClOrdID()
        
        tag_valueType = message.getHeader().getField(msgType).getString()
        
        #print message
        
        if tag_valueType == '8':
            tag_valueExec = message.getField(ExecType).getString()
            tag_valueOrdStatus = message.getField(OrdStatus).getString()
            ClOrderID = message.getField(11)
            OrderId = message.getField(37)
            
            if tag_valueExec == '0':
                STATUS = 'GO'
                LASTORDERID = OrderId
                LASTClORDERID = ClOrderID
                
                print "ACK received for [%s] : %s" %(OrderId, message)
                log(LOG_FILE, "ACK received for [%s] : %s" %(ClOrderID, message))
                
            elif tag_valueExec == '5':
                ValueOrigOrderID = message.getField( OrigClOrderID ).getString()
                print "Replace received for [%s] : %s" %(OrderId, message)
                log(LOG_FILE, "Replace received for [%s] : %s" %(ClOrderID, message))
                
                if ValueOrigOrderID == LASTClORDERID:
                    STATUS = 'GO'
                    LASTORDERID = OrderId
                    LASTClORDERID = ClOrderID
                    
            elif tag_valueExec == '4':
                ValueOrigOrderID = message.getField( OrigClOrderID ).getString()
                print "Cancel received for [%s] : %s" %(ClOrderID, message)
                log(LOG_FILE, "Cancel received for [%s] : %s" %(ClOrderID, message))
                
                STATUS = 'GO'
                
            elif tag_valueExec == '1':
                LastShares = fix.LastShares()
                LastPrice = fix.LastPx()
                
                ValueLastShares = message.getField( LastShares ).getString()
                ValueLastPrice = message.getField( LastPrice ).getString()
                print "Partial Fill for [%s] : %s @ %s" %(ClOrderID, ValueLastShares, ValueLastPrice)
                log(LOG_FILE, "Partial Fill for [%s] : %s @ %s" %(ClOrderID, ValueLastShares,ValueLastPrice))
                
            elif tag_valueExec == '2':
                LastShares = fix.LastShares()
                LastPrice = fix.LastPx()
                ValueLastShares = message.getField( LastShares ).getString()
                ValueLastPrice = message.getField( LastPrice ).getString()
                print "Full Fill for [%s] : %s @ %s" %(ClOrderID, ValueLastShares, ValueLastPrice)
                log(LOG_FILE, "Full Fill for [%s] : %s @ %s" %(ClOrderID, ValueLastShares, ValueLastPrice))
                
            elif tag_valueExec == '6':
                print "Pending Cancel for [%s]" %ClOrderID
                
            elif tag_valueExec == 'E':
                print "Pending Replace for [%s]" %ClOrderID
            
            else:
                print 'Uncatched message : %s' %message
                log(LOG_FILE, 'Uncatched message : %s' %message)
        else:
            print 'Uncatched message : %s' %message
            log(LOG_FILE, 'Uncatched message : %s' %message)
            STATUS = 'GO'
        return
    
    def getState(self):
        return self.state
    
class FIXSession():
    
    def __init__(self, settingFile, dicoFile, application, mode, duration, log_file):
        
        self.settings_file = settingFile
        self.file = settingFile
        self.mode = None
        self.settings = None
        self.application = None
        self.storeFactory = None
        self.logFactory = None #fix.ScreenLogFactory( settings )
        self.is_connected = False
        self.nb_envoi = 0
        self.dicoFile = dicoFile
        self.logfile = log_file
        
        self.mode = mode
        self.settings = fix.SessionSettings(self.settings_file)
        self.application = application
        self.storeFactory = fix.FileStoreFactory(self.settings)
        self.logFactory = fix.FileLogFactory(self.settings)
        
        if mode == "acceptor":
            self.connector = fix.SocketAcceptor( self.application, self.storeFactory, self.settings, self.logFactory )
            self.connector.setting
            print "Socket Acceptor OK ! "
            log(LOG_FILE, 'Socket Acceptor OK ! ')
            
        else :
            self.connector = fix.SocketInitiator(self.application, self.storeFactory, self.settings, self.logFactory)
            self.connector.setting
            print "Socket Initiator OK ! "
            log(LOG_FILE, 'Socket Initiator OK ! ')
        
        self.connector.start()
        
        while not self.connector.isLoggedOn():
            continue
        
        self.is_connected = True
        time.sleep(duration)
        self.connector.stop()
        
    
class Orchestrator(Application):
    
    def __init__(self, StoreFile, OrchesFile, settingsFile, dicoFile):
        self.storeFile = StoreFile
        self.orchesFile = OrchesFile
        self.l_OrderId = []
        self.settings_file = settingsFile
        self.dicoFile = dicoFile
    
    def setLastOrder(self, OrderID):
        self.lastOrderId = OrderID
        
    def getLastOrder(self):
        return self.lastOrderId
    
    def getConfig(self):
        config = ConfigParser_NL.RawConfigParser()
        config.read(self.settings_file)
        return config
    
    def getFIXtag(self,name):
        struct = ET.parse(self.dicoFile)
        raw_data = struct.getroot()
        
        gen_conf = raw_data.find('messages')
        for elt in gen_conf.findall('message'):
            if elt.get('name') == 'NewOrderSingle':
                NOS_dico = elt
        
        for field in NOS_dico.findall('field'):
            if field.get('name') == name:
                return field.get('number')
        
        return
        
    def SendAlgo(self, param_line, day, user):
        
        global STATUS
        global LASTClORDERID
        
        ClOrderID = self.getLastOrderID()
        message = fix.Message();
        header = message.getHeader();
        
        #Message Headeer
        header.setField(8, self.getConfig()._defaults['BeginString'])
        header.setField(49,self.getConfig()._sections['SESSION']['SenderCompID'])
        header.setField(56,self.getConfig()._sections['SESSION']['TargetCompID'])
        header.setField(fix.MsgType("D"))
        
        #permanent config
        message.setField(fix.ClOrdID("AAA%03d"%long(ClOrderID)))
        message.setField(21, '2')
        message.setField(22, '4')
        
        if param_line['Price'] != "" and param_line['Price'] != "none":
            message.setField(40, '2')
        else:
            message.setField(40, '1')
        
#         message.setField(50, 'pws08fctuser006')
        message.setField(115, 'PWARE')
        message.setField(fix.TransactTime(calendar.timegm(time.gmtime())))
        message.setField(fix.TimeInForce(fix.TimeInForce_DAY))
        message.setField(9049, user)
        
        #param
        for u, v in param_line.items():
            if str(v) != 'none':
                if u == 'EndTime' or u == 'StartTime':
                    if str(v) == ' ':
                        message.setField(long(self.getFIXtag(u)), ' ')
                    else:
                        message.setField(long(self.getFIXtag(u)), day + '-' + str(v) + ':00')
                else:
                    message.setField(long(self.getFIXtag(u)), str(v))
        
        #iter ClOrderID in store
        self.recordLastOrderID(ClOrderID +1)
        self.l_OrderId.append(ClOrderID)
        
        STATUS = 'WAIT'
        
        fix.Session.sendToTarget(message)
        print "[Order Sent : AAA%03d] : %s" %(long(ClOrderID), message.__str__())
        log(LOG_FILE, "[Order Sent : AAA%03d] : %s" %(long(ClOrderID), message.__str__()))
    
    def ModifyAlgo(self, new_params, day, user):
        
        global STATUS
        global LASTClORDERID
        
        message = fix.Message();
        header = message.getHeader();
        
        #Message Header  
        header.setField(8, self.getConfig()._defaults['BeginString'])
        header.setField(49,self.getConfig()._sections['SESSION']['SenderCompID'])
        header.setField(56,self.getConfig()._sections['SESSION']['TargetCompID'])
        
        header.setField(fix.MsgType("G"))
        
        ClOrderID = LASTClORDERID
        OrderID = LASTORDERID
        
        message.setField(41, ClOrderID)
        
        newOrderID = self.getLastOrderID()
        message.setField(11, "AAA%03d"%long(newOrderID))
        self.recordLastOrderID(newOrderID +1)
        
        message.setField(21, '2')
        message.setField(22, '4')
        if new_params['Price'] != "" and new_params['Price'] != "none":
            message.setField(40, '2')
        else:
            message.setField(40, '1')
            
        message.setField(47, 'A')
#         message.setField(50, 'pws08fctuser006')
        message.setField(115, 'PWARE')
        message.setField(9049, user)
        
        #soumission de la nouvelle valeur du parametre de l'ordre
        for u, v in new_params.items():
            if str(v) != 'none':
                if u == 'StartTime' or u == 'EndTime':
                    if str(v) == ' ':
                        message.setField(long(self.getFIXtag(u)), ' ')
                    else:
                        message.setField(long(self.getFIXtag(u)), day + '-' + str(v) + ':00')
                else:
                    message.setField(long(self.getFIXtag(u)), str(v))
        
        message.setField(fix.TransactTime(calendar.timegm(time.gmtime())))
        
        #sending
        fix.Session.sendToTarget(message)
        
        print "[Order Modify: " + ClOrderID + "] : " + message.__str__()
        STATUS = 'WAIT'
    
    def CancelAlgo(self, param_line, day, user):
        
        global STATUS
        global LASTClORDERID
        
        message = fix.Message();
        header = message.getHeader();
        
        #Message Header  
        header.setField(8, self.getConfig()._defaults['BeginString'])
        header.setField(49,self.getConfig()._sections['SESSION']['SenderCompID'])
        header.setField(56,self.getConfig()._sections['SESSION']['TargetCompID'])
        
        header.setField(fix.MsgType("F"))
        
        ClOrderID = LASTClORDERID
        OrderID = LASTORDERID
        
        message.setField(41, ClOrderID)
        
        newOrderID = self.getLastOrderID()
        message.setField(11, "AAA%03d"%long(newOrderID))
        self.recordLastOrderID(newOrderID +1)
        
        message.setField(21, '2')
        message.setField(22, '4')
        message.setField(40, '2')
        message.setField(47, 'A')
#         message.setField(50, 'pws08fctuser006')
        message.setField(115, 'PWARE')
        message.setField(9049, user)
        
        #soumission de la nouvelle valeur du parametre de l'ordre
        for u, v in param_line.items():
            if str(v) != 'none':
                if u == 'StartTime' or u == 'EndTime':
                    if v != ' ':
                        message.setField(long(self.getFIXtag(u)), ' ')
                    else:
                        message.setField(long(self.getFIXtag(u)), day + '-' + str(v) + ':00')
                elif u == 'StrategyName':
                    continue
                else:
                    message.setField(long(self.getFIXtag(u)), str(v))
        
        message.setField(fix.TransactTime(calendar.timegm(time.gmtime())))
        
        #sending
        fix.Session.sendToTarget(message)
        
        print "[Order Cancel : " + ClOrderID + "] : " + message.__str__()
        log(LOG_FILE, "[Order Cancel : " + ClOrderID + "] : " + message.__str__())
        
        STATUS = 'WAIT'
        
    def getLastOrderID(self):
        store_file = open(self.storeFile,'r')
        lastOrderID = store_file.readlines()
        store_file.close()
        return int(lastOrderID[0])
    
    def recordLastOrderID(self, ClOrderID):
        store_file = open(self.storeFile,'w')
        store_file.write(str(ClOrderID))
        store_file.close()
        return
    
    def iter_param_file(self):
        
        data = open(self.orchesFile,'r')
        
        ListParams ={}
        
        names = None
        idx = 1
        for line in data:
            temp = line.rsplit("|")
            elt = {}
            if names == None:
                names = temp[0:len(temp)-1]
            else:
                for i in range(0,len(names)):
                    elt[names[i]] = temp[i]
                    
                ListParams[idx] = elt
                idx = idx + 1
        
        return ListParams
    
    def OrchBasket(self, listParams, day, mode, log_file, user):
        
        first_order = listParams[1]
        
        first = 1
        for u, v in listParams.items():
            if first == 1:
                print "Sending next order : ..."
                #time.sleep(10)
                self.SendAlgo(v, day, user)
                
                if mode != 'submit':
                    first = 0
                    
            elif first == 0 and STATUS == 'GO':
                if v['StrategyName'] == '':
                    
                    if mode == 'switch':
                        print "waiting for the modified strategy to begin ..."
                        log(log_file, "waiting for the modified strategy to begin ...")
                        time.sleep(120)
                    
                    self.CancelAlgo(first_order, day, user)
                    first  = 1
                else:
                    if mode == 'switch':
                        print "waiting for the initial strategy to begin ..."
                        log(log_file, "waiting for the modified strategy to begin ...")
                        time.sleep(120)
                    
                    self.ModifyAlgo(v, day, user)
                    
            count = 0
            while STATUS == 'WAIT' and count < 10:
                print "Waiting for ack ..."
                time.sleep(1)
                count += 1
                
            if count > 10 :
                print "No ACK received => continue test process anyway"
                
            
        return
    
if __name__ == '__main__':
    
    LOG_FILE = ''
    global LOG_FILE

    today = str(datetime.datetime.today())
    
    today = today.rsplit(' ')
    today = today[0].rsplit('-')
    day = '%s%s%s' %(today[0],today[1],today[2])
    
    cfg_file = "./cfg/fixcfg_Kepler1.cfg" 
    dico_file = "./cfg/K_CSAM.xml" 
    storeFile = './cfg/store.txt'
    
    user = 'ON2'
    OrchesFile = './inputs/basket-test.txt'
    
    logfile_name = './logs/basket_test_%s.log' %day
    LOG_FILE = logfile_name
    
    mode = 'submit'
#     mode = 'switch'
    
    duration = 1000
    
    new_app = Application()
    
    thread1 = threading.Thread(None, FIXSession, None, (cfg_file, dico_file, new_app, 'initiator', duration, logfile_name))
    
    #lancement de la session
    thread1.start()
    
    log(logfile_name, '====== start New FIX connexion : ======')
    
    while new_app.getState() != 'CONNECTED':
        continue
    
    OrchesModif = Orchestrator(storeFile, OrchesFile, cfg_file, dico_file)
    
    l_params = OrchesModif.iter_param_file()
    
    OrchesModif.OrchBasket(l_params, day, mode, logfile_name, user)
    