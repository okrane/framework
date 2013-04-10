'''
Created on 10 august 2012

@author: gupon
'''

import quickfix as fix
from XMLTestReader import ReadTestFile, PrintTestData, GetOrderList
import random
import datetime
import time
import threading
import datetime
from mhlib import isnumeric

sessionSORin = ""
sessionSORout = ""

class MarketDestination :
    MarketDict = {"PARIS_PRIM" : "PA", "CHIX_L" : "XL", "LONDON_PRIM":"L", "BATS_EUR":"BS", "TURQ_L":"TQL"}
    
    @staticmethod   
    def getMarketName( market_code):
        market_name = None 
        for k, v in MarketDestination.MarketDict.iteritems():
            if v == market_code :
                market_name = k
                break
        
        return market_name

class OrderBatch:
    
    book = {}
    count = {}
    
    def __init__(self):
        self.book = {}
        self.book["DAY"] = {}
        self.book["GTD"] = {}
        
        self.count["DAY"] = 0
        self.count["GTD"] = 0
        
    def add_order(self, message):
    
        if message.getField(fix.OrdType()).getString() != "2":
            print "wrong type of order (different from limit) %s" %message.getField(fix.OrdType()).getString()
        else:
            orderID = message.getField(fix.ClOrdID()).getString()
            scrty = message.getField(fix.Symbol()).getString()
            qty = message.getField(fix.OrderQty()).getString()
            price = message.getField(fix.Price()).getString()
            side = message.getField(fix.Side()).getString()
            venue = "L"
            
            if message.getField(fix.TimeInForce()).getString() == "0":
                new_order = order.__init__(self, type, scrty, qty, price, side, venue)
                temp = self.book["DAY"]
                temp[orderID] = new_order
                self.count["DAY"] = self.count["DAY"] + 1
            elif message.getField(fix.TimeInForce()).getString() == "6":
                date = message.getField(fix.ExpireTime()).getString()
                new_order = order.__init__(self, type, scrty, qty, price, side, venue, date)
                temp = self.book["GTD"]
                temp[orderID] = new_order
                self.count["GTD"] = self.count["GTD"] + 1
            else:
                print "unhanddled time in force"
                
        return
    
    def delete_order(self, ID, type):
        
        temp = self.book[type]
        del temp[ID]
        self.count[type] = self.count[type] - 1
        return
    

#mapping venue - security ID for France Telecom 
class SecurityID :
    #MarketDict = {"LONDON_PRIM" : "4753", "BATS_EUR":"BARCl", "TURQ_L":"BARCl","CHIX_L":"BARCl"}
    MarketDict = {"LONDON_PRIM" : "20906", "BATS_EUR":"TCGl", "TURQ_L":"TCGl", "CHIX_L":"TCGl"}
    
    @staticmethod   
    def getSecID( market_name):
        secID = None 
        for k, v in SecurityID.MarketDict.iteritems():
            if v == market_name :
                secID = k
                break
        
        return secID

class order:
    def __init__(self, type, scrty, qty, price, side, venue, date = ""):
        self.type = type
        self.quantity = qty
        self.price = price
        self.side = side
        self.venue = venue
        self.security = scrty
        self.date = date
    
    def getType(self):
        return self.type
    
    def getQuantity(self):
        return self.quantity
    
    def getPrice(self):
        return self.price
    
    def getSide(self):
        return self.side
    
    def getVenue(self):
        return self.venue
    
    def getSecurity(self):
        return self.security
    
    def getDate(self):
        return self.date

class OrderStatus :
    Status = {"0" : "New",
        "1" : "Partially filled",
        "2" : "Filled",
        "3" : "Done for day",
        "4" : "Canceled",
        "5" : "Replaced",
        "6" : "Pending Cancel (e.g. result of Order Cancel Request <F>)" ,
        "7" : "Stopped",
        "8" : "Rejected",
        "9" : "Suspended",
        "A" : "Pending New",
        "B" : "Calculated",
        "C" : "Expired",
        "D" : "Accepted for bidding",
        "E" : "Pending Replace"}
    
    @staticmethod   
    def getOrderStatus(status):
        return OrderStatus.Status(status)

class Application(fix.Application):
    orderID = 0
    execID = 0
    state = "INIT"  
    
    def getSessionId(self):
        return self.session_id
        
    def onCreate(self, sessionID): 
        self.session_id = sessionID
        Application.state = "INIT"
        print("Application Created....... -> Session_id : %s")%self.session_id
        return
    
    def onLogon(self, sessionID): 
        print("LOGGED ON......., Session : %s"%sessionID)
        return
    
    def onLogout(self, sessionID): 
        print("Session logged out, Session :"%sessionID)
        
    def toAdmin(self,  message, sessionID): 
#        print "Received Something sor session %s \n"%sessionID
#        print "--->the Message is : %s"%message
        return
    
    def fromAdmin(self, message, sessionID):
#        print "Sending Something BRIDGE session %s \n"%sessionID
#        print "--->the Message is : %s"%message
        return
    
    def toApp(self, sessionID, message):
        return
    
    def displayER(self, message):
#        ord_status = fix.OrdStatus()
#        ord_status_value = message.getHeader().getField( ord_status).getString()
#        ER_status = OrderStatus.getOrderStatus(ord_status_value)
        return
        
    def fromApp(self, message, sessionID):
        
        sessionID = self.getSessionId()
        
        SOR_Tester.orchestrator(Tester, sessionID, message)
        
        return
    
    def manageER(self, message):
        #print the Execution Report in a clean , readable way
        status = fix.OrdStatus()
#        message.getField(status)
        print "Received an ER with Status : %s"%status.getString()
        
    def genOrderID(self):
        self.orderID = self.orderID+1
        return `self.orderID`
    
    def genExecID(self):
        self.execID = self.execID+1
        return `self.execID`
    
class FIXServer:
    
    state = "INIT"
    def __init__(self,settingFile):
        self.settings_file = settingFile
        self.file = settingFile
        self.mode = None
        self.settings = None
        self.application = None
        self.storeFactory = None
        self.logFactory = None #fix.ScreenLogFactory( settings )
        self.is_connected = False
        
    def getLastOrderID(self):
        temp_file = open("C:/workspace/dev_gupon/usr/dev/gupon/PythonFix/conf/store.txt")
        lastOrderID = temp_file.readlines()
        temp_file.close()
        return int(lastOrderID[0])
    
    def recordLastOrderID(self, id):
        temp_file = open("C:/workspace/dev_gupon/usr/dev/gupon/PythonFix/conf/store.txt",'w')
        temp_file.write(id)
        temp_file.close()
        return
        
    def sendNewOrderSingle(self, order):
        message = fix.Message();
        header = message.getHeader();
        
        #param connexion
        header.setField(8, "FIX.4.2")
        header.setField(49,"GUPONFIX")
        header.setField(fix.TargetCompID("SORTEST"))
        
        
        if order.getType() == "IOC":
            header.setField(fix.TimeInForce(fix.TimeInForce_IMMEDIATEORCANCEL))
        
        header.setField(fix.MsgType("D"))
        
        message.setField(fix.OrderQty(float(order.getQuantity())))
        message.setField(fix.Price(float(order.getPrice())))
        
        #param order
        id = self.getLastOrderID() + 1
        self.recordLastOrderID(str(id))
        
        message.setField(fix.ClOrdID("%03d"%id))
        
        message.setField(fix.Symbol(order.getSecurity()))
        message.setField(48, order.getSecurity())
        message.setField(22, "8")

        if order.getType() == "Limit" or order.getType() == "IOC":
            message.setField(fix.OrdType(fix.OrdType_LIMIT))
        
        message.setField(fix.Side(fix.Side_BUY if order.getSide() == "Buy" else fix.Side_SELL))
        message.setField(fix.HandlInst(fix.HandlInst_MANUAL))
        message.setField(100,"L")
        message.setField(15,"GBX")
        message.setField(fix.TransactTime((datetime.datetime.now ().strftime ("%Y%m%d-%H:%M:%S.%f"))[:-3]))
        message.setField(5001,"433")
        message.setField(5010,"448070")
        message.setField(fix.TimeInForce(fix.TimeInForce_DAY))
        
        Tester.clientMessage = message
            
        #sending
        fix.Session.sendToTarget(message)
        print "[Order Sent] : " + message.__str__()
        
        
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
            self.is_connected = True
            
            
        else :
            self.connector = fix.SocketInitiator(self.application, self.storeFactory, self.settings, self.logFactory)
            self.connector.setting
            print "Socket Initiator OK ! "
            self.is_connected = True
        
    def RunSOR(self, side, mode):
        
        if not self.is_connected :
            self.Connect(mode)
      
        self.connector.start()
        
        while not self.connector.isLoggedOn():
            continue
        
        if mode == "initiator" and side == "NORD":
            print "instanciate NORD connexion"
            
        
        elif mode == "initiator" and side == "SUD":
            print "instanciate SUD connexion"

        
class SOR_Tester():
    
    def FeederRun(self, scenar):
        self.file =  "C:/workspace/dev_gupon/usr/dev/gupon/PythonFix/conf/fixcfg.cfg"
        self.server = FIXServer(self.file)
        self.server.Run("initiator", True, scenar)
        
    def FeederRunNOS(self):
        self.file =  "C:/workspace/dev_gupon/usr/dev/gupon/PythonFix/conf/fixcfg.cfg"
        self.server = FIXServer(self.file)
        self.server.RunNOS("initiator", False)
        
    def OrderID(self):
        self.file =  "C:/workspace/dev_gupon/usr/dev/gupon/PythonFix/conf/fixcfg.cfg"
        self.server = FIXServer(self.file)
        print self.server.getLastOrderID()
        self.server.recordLastOrderID("999")
    
    def SOR_connexion(self, side):
        
        print "launching connexion :" + side
        
        if side == "NORD":
            self.file =  "C:/workspace/dev_gupon/usr/dev/gupon/PythonFix/conf/fix_SOR_IN.cfg"
            self.server = FIXServer(self.file)
            self.server.RunSOR(side, "initiator")
            self.SessionID_SORin = self.server.application.getSessionId()
            
        elif side == "SUD":
            self.file =  "C:/workspace/dev_gupon/usr/dev/gupon/PythonFix/conf/fix_SOR_OUT.cfg"
            self.server = FIXServer(self.file)
            self.server.RunSOR(side, "initiator")
            self.SessionID_SORout = self.server.application.getSessionId()
            
        time.sleep(200)
    
    def NewClOrder(self, order):
        self.server.sendNewOrderSingle(order)
    
    def manageIncomingMessage(self, sessionID, message, resp_type):
        
        if sessionID == self.SessionID_SORin:
            
            msgType = fix.MsgType()
            ExecType = fix.ExecType()
            OrderID = fix.OrderID()
            
            tag_valueType = message.getHeader().getField( msgType ).getString()
            tag_valueExec = message.getField( ExecType ).getString()
            tag_valueOrderID = message.getField(OrderID).getString()
            
            if tag_valueType == "8" and tag_valueExec == "0":
                print "[%s] Receiving New message from SOR to Client for order : %s" %(sessionID,tag_valueOrderID)
                
            elif tag_valueType == "8" and tag_valueExec == "A":
                print "[%s] Receiving Pending New message from SOR to Client for order : %s" %(sessionID,tag_valueOrderID)
                
            elif tag_valueType == "8" and tag_valueExec == "1":
                print "[%s] Receiving Partially Filled Report from SOR to Client for order : %s" %(sessionID,tag_valueOrderID)
                
            elif tag_valueType == "8" and tag_valueExec == "2":
                print "[%s] Receiving Fully Filled Report from SOR to Client for order : %s" %(sessionID,tag_valueOrderID)
                
        elif sessionID == self.SessionID_SORout:
            
            print "[%s] SOR Sending message to market : %s" %(sessionID, message)
            self.manage_response(message, resp_type)
            
        return
    
    def manage_response(self, message, status):
        
        message_er = fix.Message();
        header = message_er.getHeader();
        
        if status == "Full":
            
            #envoi d'un full fill
            message_er = fix.Message();
            header = message_er.getHeader();
            
            #param connexion
            header.setField(8, "FIX.4.2")
            header.setField(49,"MARKET")
            header.setField(56, "SORTESTOUT")
            header.setField(35, "8")
            
            fieldType = fix.ClOrdID()
            message_er.setField(37, message.getField(fieldType).getString())
            message_er.setField(11, message.getField(fieldType).getString())
            fieldType = fix.Symbol()
            message_er.setField(55, message.getField(fieldType).getString())
            message_er.setField(22, "8")
            fieldType = fix.SecurityID()
            message_er.setField(48, message.getField(fieldType).getString())
            message_er.setField(30, "L")
            message_er.setField(20, "0")
            message_er.setField(150, "2")
            message_er.setField(39, "2")
            
            fieldType = fix.OrderQty()
            message_er.setField(38, message.getField(fieldType).getString())
            message_er.setField(17, str(FIXServer.getLastOrderID(self.server)+1))
            FIXServer.recordLastOrderID(self.server, str(FIXServer.getLastOrderID(self.server)+1))
            
            fieldType = fix.OrdType()
            message_er.setField(40, message.getField(fieldType).getString())
            message_er.setField(31, "0")
            message_er.setField(6, "0")
            
            fieldType = fix.OrderQty()
            message_er.setField(32, message.getField(fieldType).getString())
            message_er.setField(14, message.getField(fieldType).getString())
            message_er.setField(151, message.getField(fieldType).getString())
            
            fieldType = fix.TimeInForce()
            message_er.setField(59, message.getField(fieldType).getString())
            
            fix.Session.sendToTarget(message_er)
            print "Envoi du fill du marche : \n%s" %message_er
            
        elif status == "Partial":
            
            req = self.Orchestration[str(self.pos)]
            ExecQty = req["ExecQty"]
            
            #envoi d'un partial fill
            message_er = fix.Message();
            header = message_er.getHeader();
            
            #param connexion
            header.setField(8, "FIX.4.2")
            header.setField(49,"MARKET")
            header.setField(56, "SORTESTOUT")
            header.setField(35, "8")
            
            fieldType = fix.ClOrdID()
            message_er.setField(37, message.getField(fieldType).getString())
            message_er.setField(11, message.getField(fieldType).getString())
            fieldType = fix.Symbol()
            message_er.setField(55, message.getField(fieldType).getString())
            message_er.setField(22, "8")
            fieldType = fix.SecurityID()
            message_er.setField(48, message.getField(fieldType).getString())
            message_er.setField(30, "L")
            message_er.setField(20, "0")
            message_er.setField(150, "1")
            message_er.setField(39, "1")
            
            fieldType = fix.OrderQty()
            message_er.setField(38, message.getField(fieldType).getString())
            message_er.setField(17, str(FIXServer.getLastOrderID(self.server)+1))
            FIXServer.recordLastOrderID(self.server, str(FIXServer.getLastOrderID(self.server)+1))
            
            fieldType = fix.OrdType()
            message_er.setField(40, message.getField(fieldType).getString())
            message_er.setField(31, "0")
            message_er.setField(6, "0")
            
            fieldType = fix.OrderQty()
            
            
            message_er.setField(32, str(ExecQty))
            message_er.setField(14, str(ExecQty))
            message_er.setField(151, str(ExecQty))
            
            fieldType = fix.TimeInForce()
            message_er.setField(59, message.getField(fieldType).getString())
            
            fix.Session.sendToTarget(message_er)
            print "Envoi du partial du marche : \n%s" %message_er
            
        elif status == "Cancel":
            
            #envoi d'un Cancel
            message_er = fix.Message();
            header = message_er.getHeader();
            
            #param connexion
            header.setField(8, "FIX.4.2")
            header.setField(49,"MARKET")
            header.setField(56, "SORTESTOUT")
            header.setField(35, "8")
            
            fieldType = fix.ClOrdID()
            message_er.setField(37, message.getField(fieldType).getString())
            message_er.setField(11, message.getField(fieldType).getString())
            fieldType = fix.Symbol()
            message_er.setField(55, message.getField(fieldType).getString())
            message_er.setField(22, "8")
            fieldType = fix.SecurityID()
            message_er.setField(48, message.getField(fieldType).getString())
            message_er.setField(30, "L")
            message_er.setField(20, "0")
            message_er.setField(150, "4")
            message_er.setField(39, "4")
            
            fieldType = fix.OrderQty()
            message_er.setField(38, message.getField(fieldType).getString())
            message_er.setField(17, str(FIXServer.getLastOrderID(self.server)+1))
            FIXServer.recordLastOrderID(self.server, str(FIXServer.getLastOrderID(self.server)+1))
            
            fieldType = fix.OrdType()
            message_er.setField(40, message.getField(fieldType).getString())
            message_er.setField(31, "0")
            message_er.setField(6, "0")
            
            fieldType = fix.OrderQty()
            message_er.setField(32, "0")
            message_er.setField(14, "0")
            message_er.setField(151, "0")
            
            fieldType = fix.TimeInForce()
            message_er.setField(59, message.getField(fieldType).getString())
            
            fix.Session.sendToTarget(message_er)
            print "Envoi du Cancel du marche : \n%s" %message_er
            
        
        elif status == "ACK":
            #param connexion
            header.setField(8, "FIX.4.2")
            header.setField(49,"MARKET")
            header.setField(56, "SORTESTOUT")
            header.setField(35, "8")
            
            fieldType = fix.ClOrdID()
            message_er.setField(37, message.getField(fieldType).getString())
            message_er.setField(11, message.getField(fieldType).getString())
            
            fieldType = fix.Symbol()
            message_er.setField(55, message.getField(fieldType).getString())
            message_er.setField(22, "8")
            
            fieldType = fix.SecurityID()
            message_er.setField(48, message.getField(fieldType).getString())
            
            message_er.setField(30, "L")
            
            
            
            message_er.setField(17, str(FIXServer.getLastOrderID(self.server)+1))
            FIXServer.recordLastOrderID(self.server, str(FIXServer.getLastOrderID(self.server)+1))
            
            message_er.setField(31, "0")
            message_er.setField(32, "0")
            message_er.setField(6, "0")
            message_er.setField(14, "0")
            
            origHeader = message.getHeader()

            if origHeader.getField(35) == "D":
                fieldType = fix.Price()
                message_er.setField(44, message.getField(fieldType).getString())
                fieldType = fix.OrderQty()
                message_er.setField(38, message.getField(fieldType).getString())
                fieldType = fix.OrdType()
                message_er.setField(40, message.getField(fieldType).getString())
                fieldType = fix.OrderQty()
                message_er.setField(151, message.getField(fieldType).getString())
                fieldType = fix.TimeInForce()
                message_er.setField(59, message.getField(fieldType).getString())
                message_er.setField(20, "0")
                message_er.setField(150, "0")
                message_er.setField(39, "0")
                
            elif origHeader.getField(35) == "F":
                message_er.setField(150, "4")
                message_er.setField(39, "4")
                
            fix.Session.sendToTarget(message_er)
            print "Envoi de l'ACK du marche : \n%s" %message_er
        
        elif status == "Reject":
            #param connexion
            header.setField(8, "FIX.4.2")
            header.setField(49,"MARKET")
            header.setField(56, "SORTESTOUT")
            header.setField(35, "8")
            
            fieldType = fix.ClOrdID()
            message_er.setField(37, message.getField(fieldType).getString())
            message_er.setField(11, message.getField(fieldType).getString())
            fieldType = fix.Symbol()
            message_er.setField(55, message.getField(fieldType).getString())
            message_er.setField(22, "8")
            fieldType = fix.SecurityID()
            message_er.setField(48, message.getField(fieldType).getString())
            message_er.setField(30, "L")
            message_er.setField(20, "0")
            message_er.setField(150, "8")
            message_er.setField(39, "8")
            fieldType = fix.OrderQty()
            message_er.setField(38, message.getField(fieldType).getString())
            message_er.setField(17, str(FIXServer.getLastOrderID(self.server)+1))
            FIXServer.recordLastOrderID(self.server, str(FIXServer.getLastOrderID(self.server)+1))
            
            fieldType = fix.Price()
            message_er.setField(44, message.getField(fieldType).getString())
            
            fieldType = fix.OrdType()
            message_er.setField(40, message.getField(fieldType).getString())
            message_er.setField(31, "0")
            message_er.setField(32, "0")
            message_er.setField(6, "0")
            message_er.setField(14, "0")
            fieldType = fix.OrderQty()
            message_er.setField(151, message.getField(fieldType).getString())
            fieldType = fix.TimeInForce()
            message_er.setField(59, message.getField(fieldType).getString())
            
            fix.Session.sendToTarget(message_er)
            print "Envoi du Reject du marche : \n%s" %message_er
        
        elif status == "OCR":
            
            message = Tester.clientMessage
            #param connexion
            header.setField(8, "FIX.4.2")
            header.setField(49,"GUPONFIX")
            header.setField(56, "SORTEST")
            header.setField(35, "F")
            
            fieldType = fix.ClOrdID()
            message_er.setField(41, message.getField(fieldType).getString())
            
            message_er.setField(11, str(FIXServer.getLastOrderID(self.server)+1))
            FIXServer.recordLastOrderID(self.server, str(FIXServer.getLastOrderID(self.server)+1))
            
            fieldType = fix.Symbol()
            message_er.setField(55, message.getField(fieldType).getString())
            
            fieldType = fix.SecurityID()
            message_er.setField(48, message.getField(fieldType).getString())
            
            fieldType = fix.Currency()
            message_er.setField(15, message.getField(fieldType).getString())
            
            message_er.setField(22, "8")
            
            message_er.setField(5001, "433")
            message_er.setField(9739, "Y")
            message_er.setField(5010,"448070")
            message_er.setField(100,"L")
            
            message.setField(fix.HandlInst(fix.HandlInst_MANUAL))
             
            fieldType = fix.Side()
            message_er.setField(54, message.getField(fieldType).getString())

            fieldType = fix.OrderQty()
            message_er.setField(38, message.getField(fieldType).getString())
            
            fieldType = fix.Price()
            message_er.setField(44, message.getField(fieldType).getString())
            
            fieldType = fix.OrdType()
            message_er.setField(40, message.getField(fieldType).getString())
            
            fieldType = fix.TimeInForce()
            message_er.setField(59, message.getField(fieldType).getString())
            
            fix.Session.sendToTarget(message_er)
            
            print "Envoi de l'OCR vers le SOR : \n%s" %message_er
        
        self.pos = self.pos + 1
        
        return
    
    def orchestrator(self, sessionID, message=""):
        
        if self.pos <= len(self.Orchestration):
            req = self.Orchestration[str(self.pos)]
            self.manageIncomingMessage(sessionID, message, req['action'])
            
            if self.pos > len(self.Orchestration):
                time.sleep(2)
                #Test un Cancel de l'ordre parent
                #print "Cancel a la fin du test : "
                #self.CancelClOrder(self.clientMessage)
                print "============== fin du test ==========="
                return
            
            req_fwd = self.Orchestration[str(self.pos)]
            if req_fwd['type'] == "O":
                self.orchestrator(self.SessionID_SORout, message)
        else:
            print message
        
        return
        
        
def read_orchestration(path):
        
        file = open(path, "r")
        temp = file.readlines()
        file.close()
        res = {}
        
        for item in temp:
            
            if item[len(item)-1] == "\n":
                item = item[:len(item)-1]
            
            rep = item.find(":")
            temp = item[(rep+1):].rsplit("/")
            
            if temp[1] != "Partial":
                res[item[:rep]] = {"type" : temp[0], "action" : temp[1]}
            else:
                res[item[:rep]] = {"type" : temp[0], "action" : temp[1], "ExecQty" : temp[2]}
        
        return res
    
if __name__ == '__main__':
    
    Tester = SOR_Tester()
    
    Tester.pos = 1
    Tester.Orchestration = read_orchestration("C:/workspace/dev_gupon/usr/dev/gupon/PythonFix/orchestration/orchestr1.txt")
    
    sud = threading.Thread(None, Tester.SOR_connexion, None, ("SUD",))
    nord = threading.Thread(None, Tester.SOR_connexion, None, ("NORD",))
    
    print "Initializing SOR Sud connection : "
    sud.start()
    time.sleep(1)
    
    print "Initializing SOR Nord connection : "
    nord.start()
    time.sleep(1)
    
    print "client NOS sending after connexion"
    
    ClientNOS = order("Limit", SecurityID.MarketDict["LONDON_PRIM"], 15000, 14, "Buy", "LONDON_PRIM")
    Tester.NewClOrder(ClientNOS)
    
    
    
    