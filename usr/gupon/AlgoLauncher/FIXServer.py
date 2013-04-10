import quickfix as fix
import threading
import time
import datetime
import os
import xml.etree.ElementTree as ET


'''
Created on 4 juil. 2012

@author: gupon
'''

INIT = 'INIT'
CONNECT = 'CONNECTED'

class Application(fix.Application):
    
    state = INIT
        
    def onCreate(self, sessionID):
        self.session_id = sessionID
        self.state = INIT
        print("########  Creating New FIX Session with Session_id : %s ########")%self.session_id
        return
    
    def onLogon(self, sessionID):
        self.state = CONNECT
        print "Log on complete for FIX session : %s " %self.session_id 
        return
    
    def onLogout(self, sessionID):
        self.state = INIT
        print "Session : %s, logged out " %self.session_id
        return
    
    def toAdmin(self, message, sessionID): return
    def toApp(self, message, sessionID): return
    def fromAdmin(self, message, sessionID): return
    def fromApp(self, message, sessionID):
        print "test02" 
        return
    
class FIXServer():
    
    #initialisation logs
    today = str(datetime.datetime.now())
    jour = today[8:10]
    mois = today[5:7]
    annee = today[0:4]
    
    file = open("C:/workspace/dev_gupon/usr/dev/gupon/AlgoLauncher/logs/BasketSender_" + jour + mois + annee + ".log", 'w')
    
    def __init__(self,settingFile,dicoFile):
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
        
        #initialisation du dico
        struct = ET.parse(dicoFile)
        raw_data = struct.getroot()
        
        custom ={}
        for conf in raw_data.findall('field'):
            custom[conf.get('fixtag')] = conf.get('name')
        
        self.custom = custom
        
        
    def log(self, message):
        hour = str(datetime.datetime.now())[9:]
        FIXServer.file.write("%s : %s"%(hour,message))
        return
    
    def Connect(self,mode):
        
        self.mode = mode
        self.settings = fix.SessionSettings( self.settings_file)
        self.application = Application()
        self.storeFactory = fix.FileStoreFactory( self.settings )
        self.logFactory = fix.FileLogFactory( self.settings )
        
        if mode == "acceptor":
            self.connector = fix.SocketAcceptor( self.application, self.storeFactory, self.settings, self.logFactory )
            self.connector.setting
            print "Socket Acceptor OK ! "
            self.log('Socket Acceptor OK ! ')
            self.is_connected = True
            
            
        else :
            self.connector = fix.SocketInitiator(self.application, self.storeFactory, self.settings, self.logFactory)
            self.connector.setting
            print "Socket Initiator OK ! "
            self.log('Socket Initiator OK ! ')
            self.is_connected = True
        
        self.connector.start()
        
        while not self.connector.isLoggedOn():
            continue
        
        time.sleep(100)
        
    
    def SendMsg(self):
        message = fix.Message();
        header = message.getHeader();
        
        #param connexion
        header.setField(8, "FIX.4.2")
        header.setField(49,"432")
        header.setField(fix.TargetCompID("AlgoCore_gupon1"))
        
        header.setField(fix.MsgType("D"))
        
        message.setField(fix.OrderQty(1000))
        message.setField(fix.Price(20))
        
        #param order
        id = 25
        message.setField(fix.ClOrdID("%03d"%id))
        
        message.setField(fix.Symbol("FR0000133308"))
        message.setField(48, "FR0000133308")
        message.setField(22, "8")

        message.setField(fix.OrdType(fix.OrdType_LIMIT))
        
        message.setField(fix.Side(fix.Side_BUY))
        message.setField(fix.HandlInst(fix.HandlInst_MANUAL))
        message.setField(100,"PA")
        message.setField(15,"EUR")
        message.setField(47, "A")
        
        message.setField(58, "TARGET CLOSE MICHELIN")
        message.setField(109, "6253")
        message.setField(5031, "6253")
        message.setField(847, "TargetClose")
        message.setField(5036, "O")
        message.setField(5037, "10944493")
        message.setField(6065, "10")
        message.setField(6066, "N")
        message.setField(7740, "N")
#        message.setField(9625, "")
        message.setField(9739, "Y")
        
        message.setField(30012, "Y")
        
        message.setField(fix.TransactTime((datetime.datetime.now ().strftime ("%Y%m%d-%H:%M:%S.%f"))[:-3]))        
        message.setField(fix.TimeInForce(fix.TimeInForce_DAY))
        
        #sending
#        fix.Session.sendToTarget(message)
        print "[Order Sent] : " + message.__str__()
        self.log("[Order Sent] : %s" %message.__str__())
    
    def iter_param_file(self, param_file):
        
        data = open(param_file,'r')
        
        ListParams ={}
        
        id = 1
        for line in data:
            temp = line.rsplit("|")
            elt = {}
            if id == 1:
                names = temp
                names = names[0:len(names)-1]
            else:
                for i in range(0,len(names)):
                    if temp[i] != '':
                        elt[names[i]] = temp[i]
                    else:
                        elt[names[i]] = self.get_default(names[i])
                ListParams[id] = elt
            id = id +1
        
        return ListParams

    def get_default(self, param):
        struct = ET.parse(self.dicoFile)
        raw_data = struct.getroot()
        
        res = None
        for conf in raw_data.findall('field'):
            if conf.get('name') == param:
                res = conf.get('default')
        
        if res != None:
            return res
        else:
            print "param : %s not found ! "%param
            return ''

if __name__ == '__main__':
    
    cfg_file = "C:/workspace/dev_gupon/usr/dev/gupon/AlgoLauncher/cfg/fixcfg.cfg" 
    param_file = "C:/basket-test.txt"
    dico_file = "C:/workspace/dev_gupon/usr/dev/gupon/AlgoLauncher/cfg/customtags.xml" 
    
    new_session = FIXServer(cfg_file, dico_file)
#    new_session.connector.start()
    
#    thread1 = threading.Thread(None, new_session.Connect, None,('initiator',))
#    
#    thread1.start()
#    time.sleep(2)
#    
#    new_session.SendMsg()

    liste_algos = new_session.iter_param_file(param_file)
    
    print len(liste_algos)
    print liste_algos[15]
    print new_session.custom['847']
    
    
        