import quickfix as fix
import threading
import time
import datetime
import os
import sys
import xml.etree.ElementTree as ET
import ConfigParser_NL
import calendar

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
        print("Creating New FIX Session with Session_id : %s")%self.session_id
        new_session.log("Creating New FIX Session with Session_id : %s"%self.session_id)
        return
    
    def onLogon(self, sessionID):
        self.state = CONNECT
        print "Log on complete for FIX session : %s " %self.session_id
        new_session.log("Log on complete for FIX session : %s " %self.session_id)
        return
    
    def onLogout(self, sessionID):
        self.state = INIT
        print "Session : %s, logged out " %self.session_id
        new_session.log("========== Session : %s, logged out ==========" %self.session_id)
        new_session.log("========== End of Log ==========")
        return
    
    def toAdmin(self, message, sessionID): return
    def toApp(self, message, sessionID): return
    def fromAdmin(self, message, sessionID): return
    def fromApp(self, message, sessionID):
#        print "Received message from AlgoBox"
#        print message.__str__()
        new_session.ManageER(message)
        
        return


class FIXServer():
    
    def __init__(self,settingFile,dicoFile, store_orders):
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
        self.storeOrders = store_orders
        
        #initialisation logs
        today = str(datetime.datetime.now())
        jour = today[8:10]
        mois = today[5:7]
        annee = today[0:4]
        
        self.logfile = "./logs/BasketSender_" + jour + mois + annee + ".log"
        
        #initialisation du dico
        struct = ET.parse(dicoFile)
        raw_data = struct.getroot()
        
        self.ListAlgoSent = {}
    
    def ManageER(self, message):
        
        msgType = fix.MsgType()
        ExecType = fix.ExecType()
        OrdStatus = fix.OrdStatus()
        
        tag_valueType = message.getHeader().getField(msgType).getString()
        
        print message.__str__()
        
        if tag_valueType == '8':
            tag_valueExec = message.getField( ExecType ).getString()
            tag_valueOrdStatus = message.getField( OrdStatus ).getString()
            OrderID = message.getField(11)
            
            if tag_valueExec == '0':
                
                AB_OrderId = message.getField(37)
                TransacTime = message.getField(60)
                self.ListAlgoSent[OrderID] = AB_OrderId
                self.log('-------------- New Ack from AlgoBox for order %s --------------' %OrderID)
                self.log('Ack message (FIX) Order %s : %s' %(OrderID,message.__str__()))
                print 'ACK for order [%s] : %s' %(OrderID, message.__str__())
                #recording ACK info
                store_orders = open(self.storeOrders, 'a')
                store_orders.write('%s;%s;%s;\n' %(OrderID, AB_OrderId, TransacTime))
                
                
            elif tag_valueOrdStatus == '8':
                self.log('-------------- Reject message from AlgoBox :  --------------')
                self.log('Order Rejected : %s,  FIX : %s '%(OrderID,message.__str__()))
                
            elif tag_valueExec == '1':
                print '-------------- Partiall Fill message from AlgoBox :  --------------'
                ClOrdID = fix.ClOrdID()
                OrderID = fix.OrderID()
                print "Partiall Fill ER for order : %s (%s)" %(message.getField(ClOrdID).getString(), message.getField(OrderID).getString())
                LastShares = fix.LastShares()
                OrderQty = fix.OrderQty()
                CumQty = fix.CumQty()
                exec_qty = message.getField(LastShares).getString()
                ord_qty = message.getField(OrderQty).getString()
                cum_qty = message.getField(CumQty).getString()
                print "Exec Quantity: %s || OrderQuantity: %s || CumQuantity: %s" %(exec_qty, ord_qty, cum_qty)
                
            elif tag_valueExec == '2':
                ClOrdID = fix.ClOrdID()
                OrderID = fix.OrderID()
                LastShares = fix.LastShares()
                OrderQty = fix.OrderQty()
                CumQty = fix.CumQty()
                print '-------------- Full Fill message from AlgoBox :  --------------'
                print "Full Fill ER for order : %s (%s)" %(message.getField(ClOrdID).getString(), message.getField(OrderID).getString())
                exec_qty = message.getField(LastShares).getString()
                ord_qty = message.getField(OrderQty).getString()
                cum_qty = message.getField(CumQty).getString()
                print "Exec Quantity: %s || OrderQuantity: %s || CumQuantity: %s" %(exec_qty, ord_qty, cum_qty)
                
        elif tag_valueType == '3':
            self.log('-------------- Reject message from AlgoBox :  --------------')
            self.log('cause : %s'%message.getField(58))
            #print message.__str__()
            self.log(message.__str__())
        
        return message
                
    def log(self, message):
        hour = str(datetime.datetime.now())[11:-3]
        logfile = open(self.logfile,'a')
        logfile.write("%s : %s\n" %(hour,message))
        logfile.close()
        return
    
    def getConfig(self):
        config = ConfigParser_NL.RawConfigParser()
        config.read(self.settings_file)
        return config
        
    def getLastOrderID(self):
        store_file = open('./cfg/store.txt','r')
        lastOrderID = store_file.readlines()
        store_file.close()
        return int(lastOrderID[0])
    
    def recordLastOrderID(self, ClOrderID):
        temp_file = open('./cfg/store.txt','w')
        temp_file.write(str(ClOrderID))
        #print "record last order ID : " + 
        temp_file.close()
        return
    
    def Connect(self,mode, duration):
        
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
            
        else :
            self.connector = fix.SocketInitiator(self.application, self.storeFactory, self.settings, self.logFactory)
            self.connector.setting
            print "Socket Initiator OK ! "
            self.log('Socket Initiator OK ! ')
        
        self.connector.start()
        
        while not self.connector.isLoggedOn():
            continue
        
        self.is_connected = True
        time.sleep(duration)
        new_session.connector.stop()
    
    def get_OrderID(self, ClOrderID):
        
        storeOrders = open(self.storeOrders,'r')
        
        for line in storeOrders:
            array_line = line.rsplit(';')
            if array_line[0] == ClOrderID:
                storeOrders.close()
                return array_line[1]
        
        storeOrders.close()
        return
        
        
    def ModifyAlgo(self, new_params, day):
        
        message = fix.Message();
        header = message.getHeader();
        
        #Message Header  
        header.setField(8, self.getConfig()._defaults['BeginString'])
        header.setField(49,self.getConfig()._sections['SESSION']['SenderCompID'])
        header.setField(56,self.getConfig()._sections['SESSION']['TargetCompID'])
        
        header.setField(fix.MsgType("G"))
        
        ClOrderID = new_params['ClOrderID']
        OrderID = self.get_OrderID(ClOrderID)
        
        message.setField(41, ClOrderID)
        message.setField(40, '1')
        
        newOrderID = self.getLastOrderID()
        message.setField(11, "AAA%03d"%long(newOrderID))
        self.recordLastOrderID(newOrderID +1)
        
        message.setField(21, '2')
        message.setField(22, '4')
        message.setField(40, '1')
        message.setField(47, 'A')
        message.setField(50, 'pws08fctuser006')
        message.setField(115, 'PWARE')
        message.setField(9249, 'TU4')
        
        #soumission de la nouvelle valeur du parametre de l'ordre
        for u, v in new_params.items():
            if v != '' and u!='ClOrderID':
                message.setField(long(self.getFIXtag(u)), v)
        
        message.setField(fix.TransactTime((datetime.datetime.now ().strftime ("%Y%m%d-%H:%M:%S.%f"))[:-3]))
        
        #sending
        fix.Session.sendToTarget(message)
        
        print "[Order Modified : " + ClOrderID + "] : " + message.__str__()
        self.log("[Order Modified : %s] :" %ClOrderID)
        for u, v in new_params.items():
            self.log("%s : %s" %(u,v))
        self.log("[Order Modified : %s] fix msg : %s" %(ClOrderID, message.__str__()))
        
    
    def CancelAlgo(self, param_line, algo_name):
        message = fix.Message();
        header = message.getHeader();
        
        #Message Header
        header.setField(8, self.getConfig()._defaults['BeginString'])
        header.setField(49,self.getConfig()._sections['SESSION']['SenderCompID'])
        header.setField(56,self.getConfig()._sections['SESSION']['TargetCompID'])
        
        header.setField(fix.MsgType("F"))
        
        message.setField(41, param_line['ClOrderID'])
        message.setField(11, "Cl" + param_line['ClOrderID'])
        message.setField(37, "Cl" + param_line['ClOrderID'])
        message.setField(fix.TransactTime((datetime.datetime.now ().strftime ("%Y%m%d-%H:%M:%S.%f"))[:-3]))
        message.setField(38,param_line['OrderQty'])
        message.setField(54,param_line['Side'])
        message.setField(55,param_line['Symbol'])
        message.setField(48,param_line['Symbol'])
        message.setField(109, "6253")
        message.setField(847, algo_name)
        message.setField(30010, "N")
        
        #sending
        fix.Session.sendToTarget(message)
        print "[Order Cancelled : " + param_line['ClOrderID'] + "] : " + message.__str__()
        self.log("[Order Cancelled : " + param_line['ClOrderID'] + "] : " + message.__str__())
    
    def AlgoSingle(self, param_line, day):
        
        ClOrderID = self.getLastOrderID()
        message = fix.Message();
        header = message.getHeader();
        
        #Message Headeer
        header.setField(8, self.getConfig()._defaults['BeginString'])
        header.setField(49,self.getConfig()._sections['SESSION']['SenderCompID'])
        header.setField(56,self.getConfig()._sections['SESSION']['TargetCompID'])
        header.setField(fix.MsgType("D"))
        
        #permanent config
        message.setField(1, 'A1')
        message.setField(fix.ClOrdID("AAA%03d"%long(ClOrderID)))
        message.setField(21, '2')
        message.setField(22, '4')
        message.setField(40, '2')
        message.setField(50, 'pws08fctuser006')
        message.setField(115, 'PWARE')
        #message.setField(fix.TransactTime((datetime.datetime.now ().strftime ("%Y%m%d-%H:%M:%S.%f"))[:-3]))
        message.setField(fix.TransactTime(calendar.timegm(time.gmtime())))
        message.setField(fix.TimeInForce(fix.TimeInForce_DAY))
        message.setField(9049, 'TU4')
        
        #param
        for u, v in param_line.items():
            if u == 'EndTime' or u == 'StartTime':
                if str(v) == ' ':
                    message.setField(long(self.getFIXtag(u)), ' ')
                else:
                    message.setField(long(self.getFIXtag(u)), day + '-' + str(v) + ':00')
            else:
                message.setField(long(self.getFIXtag(u)), str(v))
        
        #iter ClOrderID in store
        self.recordLastOrderID(ClOrderID +1)
        
        fix.Session.sendToTarget(message)
        print "[Order Sent : " + str(ClOrderID) +" ] : " + message.__str__()
        
        
    def SendAlgo(self, param_line, algo_name, day):
        message = fix.Message();
        header = message.getHeader();
        
        #Message Headeer
        header.setField(8, self.getConfig()._defaults['BeginString'])
        header.setField(49,self.getConfig()._sections['SESSION']['SenderCompID'])
        header.setField(56,self.getConfig()._sections['SESSION']['TargetCompID'])
        
        header.setField(fix.MsgType("D"))

        #param order
        message.setField(fix.ClOrdID("%03d"%long(param_line['ClOrderID'])))
        
        for u, v in param_line.items():
            if v != '' and u != 'RIC' and u != 'ClOrderID':
                if u == 'ExpireTime' or u == 'EffectiveTime':
                    v = day + '-' + v
                if u == 'Symbol':
                    message.setField(long(self.getFIXtag("SecurityID")), v)
                message.setField(long(self.getFIXtag(u)), v)
        
        message.setField(fix.OrdType(fix.OrdType_LIMIT))
        
        message.setField(fix.HandlInst(fix.HandlInst_MANUAL))
        
        #message.setField(47, "A")
        
        #message.setField(109, "6253")
        
        
        message.setField(fix.TransactTime((datetime.datetime.now ().strftime ("%Y%m%d-%H:%M:%S.%f"))[:-3]))        
        message.setField(fix.TimeInForce(fix.TimeInForce_DAY))
        
        #sending
        fix.Session.sendToTarget(message)
        
        print "[Order : %s sent] : %s" %(param_line['ClOrderID'],message.__str__())
        self.log("--------------------------------------------------------------------------------")
        self.log("[Order : %s sent] with parameters : " %param_line['ClOrderID'])
        for u, v in param_line.items():
            self.log(" %s : %s" %(u,v))
        self.log("[Order : %s sent] : %s" %(param_line['ClOrderID'],message.__str__()))
        self.log("--------------------------------------------------------------------------------")
    
    def iter_param_file(self, param_file):
        
        data = open(param_file,'r')
        
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
    
    def iter_modify_file(self, modif_file):
        data = open(modif_file,'r')
        
        ListModify ={}
        
        for line in data:
            parsed_line = line.rsplit('|')
            if parsed_line[0] == 'ClOrderID':
                names = parsed_line[0:len(parsed_line)-1]
            else:
                elt = {}
                for u in range(0,len(names)):
                    elt[names[u]] = parsed_line[u]
                
                ListModify[parsed_line[0]] = elt
        
        return ListModify
    
 
if __name__ == '__main__':

    today = str(datetime.datetime.today())

    today = today.rsplit(' ')
    today = today[0].rsplit('-')
    day = '%s%s%s' %(today[0],today[1],today[2])
    
    duration = 1000
#     param_file = 'basket_test_HUNT'
    param_file = 'basket_REtest'
    modify_file = ''
    
    cfg_file = "./cfg/fixcfg_Kepler1.cfg" 
    dico_file = "./cfg/K_CSAM.xml" 
    
    store_orders = './cfg/store_orders.txt'
    
    param_file = "./inputs/" + param_file + ".txt"
    
    new_session = FIXServer(cfg_file, dico_file, store_orders)
    
    liste_algos = new_session.iter_param_file(param_file)
    
    if modify_file != '':
        modify_file = "./inputs/" + modify_file + ".txt"
        liste_modify = new_session.iter_modify_file(modify_file)
    
    
    new_session.log("==========================================================")
    new_session.log(" New Basket for algo : ")
    new_session.log("==========================================================")
    
    thread1 = threading.Thread(None, new_session.Connect, None,('initiator',duration,))
    
    #lancement de la session
    thread1.start()
    while not new_session.is_connected:
        continue
    
    #envoi des algos
    new_session.log('================ Start of Algo sending ================')
    for u, v in liste_algos.items():
        new_session.AlgoSingle(v, day)
        time.sleep(1)
        
    new_session.log('================ End of Algo sending ================')
    
    if modify_file != '':
        
        listKeys = liste_modify.iterkeys()
        floatKeys = []
        for k in listKeys:
            floatKeys.append(k)
        
        floatKeys.sort()
        for u in floatKeys:
            v = liste_modify[u]
            new_session.log(" ==> waiting for next occurrence in %s secs" %str(2))
            time.sleep(1)
            new_session.ModifyAlgo(v, day)
           
    new_session.log('================ End of BackTest ================')
    
    
    