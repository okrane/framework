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
    def __init__(self, type, scrty, qty, price, side, venue):
        self.type = type
        self.quantity = qty
        self.price = price
        self.side = side
        self.venue = venue
        self.security = scrty
    
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
    
    def toApp(self, sessionID, message): return
    
    def displayER(self, message):
#        ord_status = fix.OrdStatus()
#        ord_status_value = message.getHeader().getField( ord_status).getString()
#        ER_status = OrderStatus.getOrderStatus(ord_status_value)
        return
        
    def fromApp(self, message, sessionID):
        
        beginString = fix.BeginString()
        msgType = fix.MsgType()
        
        message.getHeader().getField( beginString )
        tag_value = message.getHeader().getField( msgType ).getString()
        print "fromApp :: Received message with MsgType : %s "%(tag_value)
        
        # manage Execution Reports
        if tag_value == '8':
            self.displayER(message)
            #self.manageER(message)
        
#        elif tag_value == 'j':
#            #self.displayER(message)
#            self.manageER(message)
            
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
        
    def sendNewOrderSingle(self, order, dark=False):
        message = fix.Message();
        header = message.getHeader();
        
        #param connexion
        header.setField(8, "FIX.4.2")
        header.setField(49,"GUPON2")
        header.setField(fix.TargetCompID("THULB00D"))
        
        
        if order.getType() == "IOC":
            header.setField(fix.TimeInForce(fix.TimeInForce_IMMEDIATEORCANCEL))
        
        header.setField(fix.MsgType("D"))
        
        message.setField(fix.OrderQty(float(order.getQuantity())))
        if order.getType() != "Pegged":
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
        elif order.getType() == "Pegged":
            message.setField(fix.OrdType(fix.OrdType_PEGGED))
        
        message.setField(fix.Side(fix.Side_BUY if order.getSide() == "Buy" else fix.Side_SELL))
        message.setField(fix.HandlInst(fix.HandlInst_MANUAL))
        message.setField(fix.ExDestination(MarketDestination.MarketDict[order.getVenue()]))
        message.setField(fix.TransactTime((datetime.datetime.now ().strftime ("%Y%m%d-%H:%M:%S.%f"))[:-3]))
        
        if dark == True:
            message.setField(fix.MaxFloor(0))
            message.setField(18,"M")
            
            
        #sending
        fix.Session.sendToTarget(message)
        
#        time.sleep(3)
        
#        while not self.application.fromApp(message, self.application.getSessionId()):
#            continue
        
        #log
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
    
    def FlushMarket(self, venue, security):
        # send 2 huge FAK orders in opposite side to consume all liquidity in a specific OB
        orderobj = order("IOC", security, 100000, 10, "Sell", venue)
        self.sendNewOrderSingle(orderobj)
        
        orderobj = order("IOC", security, 100000, 1000, "Buy", venue)
        self.sendNewOrderSingle(orderobj)
        
    def Run(self, mode, loadXMLFile, source):
        
        if not self.is_connected :
            self.Connect(mode)
      
        self.connector.start()
        
        while not self.connector.isLoggedOn():
            continue
        
        if loadXMLFile and mode == "initiator":
            
            TestData = ReadTestFile(source)
            print "Preparing Test ..."
            
            for venueList in TestData['test']['venue']:
                
                venue = venueList['id']
                order_list = GetOrderList(TestData,venue)
                
                print "=========== Feeding Venue : %s ===========" %venue
                
                security = SecurityID.MarketDict[venue]
                
                #clean Simod
                print "========= Flushing Venue ==============="
                self.FlushMarket(venue, security)
                time.sleep(1)
                
                print "========= Feeding with new Limit Orders ============="
                for orders in order_list :
                    orderobj = order("Limit", security, orders["qty"], orders["price"], orders["side"], venue)
                    self.sendNewOrderSingle(orderobj)
                
                time.sleep(1)
                
            print "================= Markets feeding is done =================="
 

    def RunNOS(self, mode, flush):
        if not self.is_connected :
            self.Connect(mode)
      
        self.connector.start()
        
        while not self.connector.isLoggedOn():
            continue
        
        venue = "CHIX_L"
        security = SecurityID.MarketDict[venue]
        
        if flush:
            self.FlushMarket(venue, security)

        NOS = order("Limit", SecurityID.MarketDict["CHIX_L"], 7500, 51, "Sell", "CHIX_L")
        self.sendNewOrderSingle(NOS)
        time.sleep(1)
#        NOS = order("Limit", SecurityID.MarketDict["LONDON_PRIM"], 2000, 50.7, "Sell", "LONDON_PRIM")
#        self.sendNewOrderSingle(NOS)
#        time.sleep(1)
#        NOS = order("Limit", SecurityID.MarketDict["LONDON_PRIM"], 2000, 51, "Sell", "LONDON_PRIM")
#        self.sendNewOrderSingle(NOS)
#        time.sleep(1)
#        NOS = order("Limit", SecurityID.MarketDict["LONDON_PRIM"], 2000, 50.7, "Sell", "LONDON_PRIM")
#        self.sendNewOrderSingle(NOS)
#        time.sleep(1)
#        NOS = order("Limit", SecurityID.MarketDict["LONDON_PRIM"], 5000, 50.8, "Buy", "LONDON_PRIM")
#        self.sendNewOrderSingle(NOS)
#        time.sleep(1)
#        NOS = order("Pegged", SecurityID.MarketDict["CHIX_L"], 5001, 50.8, "Sell", "CHIX_L")
#        self.sendNewOrderSingle(NOS,True)
#        time.sleep(1)
    
if __name__ == '__main__':
    
    
    FIXsession = FIXServer("C:/workspace/dev_gupon/usr/dev/gupon/PythonFix/conf/fixcfg.cfg")
    
    FIXsession.Run("initiator", True, "C:/workspace/dev_gupon/usr/dev/gupon/PythonFix/scenarii/scenar3.xml")
#    FIXsession.RunNOS('initiator',False)est ce qu'il y 
#
#
#    Tester = SOR_Tester()
#    scenar = "C:/workspace/PythonFix/scenarii/scenar9.xml"
#    
#    print "Initializing Simod connection and feeding markets"
#    initiator = threading.Thread(target = Tester.FeederRun(scenar))
#    initiator.start()
#    
#    print "Initializing Simod connection and feeding markets"
#    initiator = threading.Thread(target = Tester.FeederRunNOS())
#    initiator.start()