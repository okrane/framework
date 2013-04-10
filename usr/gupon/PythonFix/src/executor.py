import sys
import time
import thread
import select
import quickfix as fix
from string import lower
import datetime
import quickfix42 as fix42
import uuid
import random
import threading
from XMLTestReader import ReadTestFile, PrintTestData, GetOrderList

class _Getch:
    """Gets a single character from standard input.  Does not echo to the
screen."""
    def __init__(self):
        try:
            self.impl = _GetchWindows()
        except ImportError:
            self.impl = _GetchUnix()

    def __call__(self): return self.impl()


class _GetchUnix:
    def __init__(self):
        import tty, sys

    def __call__(self):
        import sys, select
        select.select([sys.stdin], [], [])
        return sys.stdin.readline()
        
        

class _GetchWindows:
    def __init__(self):
        import msvcrt

    def __call__(self):
        import msvcrt
        return msvcrt.getch()


getch = _Getch()

class MarketDestination :
    MarketDict = {"ENPA" : "PA","CHIX_PARIS" : "XPA", "LONDON"  : "L","CHIX_LONDON" : "XL",  
                  "BATS TRADING" : "BS"}
    
    @staticmethod   
    def getMarketName( market_code):
        market_name = None 
        for k, v in MarketDestination.MarketDict.iteritems():
            if v == market_code :
                market_name = k
                break
        
        return market_name
            
class OrderStatus :
    Status = {"0" : "New",
        "1" : "Partially filled",
        "2" : "Filled",
        "3": "Done for day",
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
    
    
#    def __init__(self):
#        self.__dict__= self._shared_state
    
    
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
#        print "Sending Something sor session %s \n"%sessionID
#        print "--->the Message is : %s"%message
        return
    def toApp(self, sessionID, message): return
    
    def displayER(self, message):
        ord_status = fix.OrdStatus()
        ord_status_value = message.getHeader().getField( ord_status).getString()
        ER_status = OrderStatus.getOrderStatus(ord_status_value)
         
        
        
    def fromApp(self, message, sessionID):
        
        beginString = fix.BeginString()
        msgType = fix.MsgType()
        
        
        message.getHeader().getField( beginString )
        tag_value = message.getHeader().getField( msgType ).getString()
        print "fromApp :: Received Something with MsgType : %s "%(tag_value)
        
        # manage Execution Reports
        if tag_value == '8':
            self.displayER(message)
            self.manageER(message)
        elif tag_value == 'D':
            # manage Child Orders
            self.manageChildOrder(sessionID, message)
            
        return
    
    def manageER(self, message):
        # print the Execution Report in a clean , readable way
        status = fix.OrdStatus()
        message.getField(status)
        print "Received an ER with Status : %s"%status.getString()
    
    def sendER(self, session, order_message, mode = "FULLFILL"):
        ordType = fix.OrdType()
        
        price = fix.Price()
        clOrdID = fix.ClOrdID()
        symbol = fix.Symbol()
        securityId = fix.SecurityID()
        #retirve orginal parameters from the message
        # some of them will be propagater in the ER
        order_message.getField(ordType)
        
        order_message.getField(price)
        order_message.getField(clOrdID)
        order_message.getField(symbol)
        order_message.getField(securityId)
         # fix tags for the ER
        if mode == "FULLFILL":
            status = fix.OrdStatus(fix.OrdStatus_FILLED)
            execType = fix.ExecType(fix.ExecType_FILL)
            orderQty = fix.OrderQty() 
            order_message.getField(orderQty)
        else :
            status = fix.OrdStatus(fix.OrdStatus_PARTIALLY_FILLED)
            execType = fix.ExecType(fix.ExecType_PARTIAL_FILL)
            orderQty = fix.OrderQty() 
            order_message.getField(orderQty)
            qty = float(orderQty.getString())
            orderQty = fix.OrderQty(int(qty *0.5))
            
            
            
        ER = fix42.ExecutionReport()
        ER.setField(securityId)
        ER.setField(symbol)
        ER.setField(orderQty)
        ER.setField(price)
        ER.setField(clOrdID)
        ER.setField(fix.OrderID(self.genOrderID()))
        ER.setField(fix.OrderID(self.genExecID()))
        ER.setField(fix.ExecTransType(fix.ExecTransType_NEW))
        ER.setField(execType)
        ER.setField(securityId)
        ER.setField(fix.LastShares( int(orderQty.getString()) ))
        ER.setField(fix.LastPx( float(price.getString()) ))
        ER.setField(status)
        fix.Session.sendToTarget(ER, session)
        
        
        
        
    def manageChildOrder(self,session,  message):
        destination = fix.ExDestination()
        qty = fix.OrderQty()
        price = fix.Price()
        exDestination =fix.ExDestination() 
        message.getField(destination)
        message.getField(qty)
        message.getField(price)
        message.getField(exDestination)
        
        print "Received an order for : %s, with price %s and Qty : %s , Destination : %s"%(MarketDestination.getMarketName(destination.getString()), qty.getString(), price.getString(), exDestination.getString())
        # send a fullfill ExecReport
        self.sendER(session,message, mode = "PARTIAL FILL")
        
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
    
    def sendSORNOS(self, qty, price, side, venue):
        message = fix.Message();
        header = message.getHeader();
        header.setField(fix.BeginString("FIX.4.2"))
        header.setField(fix.SenderCompID("TESTER_OUT"))
        header.setField(fix.TargetCompID("STTP_SOR_IN"))
        header.setField(fix.MsgType("D"))
        message.setField(fix.OrderQty(float(qty)))
        message.setField(fix.Price(float(price)))
        message.setField(fix.ClOrdID("%03d"%random.randint(1,999)))
        message.setField(fix.Symbol("4753"))
        message.setField(fix.SecurityID("4753"))
        message.setField(fix.OrdType(fix.OrdType_LIMIT))
        message.setField(fix.Side(fix.Side_BUY if side == "Buy" else fix.Side_SELL))
        message.setField(fix.HandlInst(fix.HandlInst_MANUAL))
        message.setField(fix.ExDestination(MarketDestination.MarkerDict[venue]))
        message.setField(fix.TransactTime((datetime.datetime.now ().strftime ("%Y%m%d-%H:%M:%S.%f"))[:-3]))
        fix.Session.sendToTarget(message)
        pass
    
    
    def sendNewOrderSingle(self, qty, price, side, venue):
        message = fix.Message();
        header = message.getHeader();
        header.setField(fix.BeginString("FIX.4.2"))
        header.setField(fix.SenderCompID("GUPON2"))
        header.setField(fix.TargetCompID("THULB00D"))
        header.setField(fix.MsgType("D"))
        message.setField(fix.OrderQty(float(qty)))
        message.setField(fix.Price(float(price)))
        #clOrderId = str(1000 * random.random())
        #message.setField(fix.OrigClOrdID(clOrderId))
        message.setField(fix.ClOrdID("%03d"%random.randint(1,999)))
        message.setField(fix.Symbol("BARC"))
        message.setField(fix.OrdType(fix.OrdType_LIMIT))
        message.setField(fix.Side(fix.Side_BUY if side == "Buy" else fix.Side_SELL))
        message.setField(fix.HandlInst(fix.HandlInst_MANUAL))
        message.setField(fix.ExDestination(MarketDestination.MarketDict[venue]))
        message.setField(fix.TransactTime((datetime.datetime.now ().strftime ("%Y%m%d-%H:%M:%S.%f"))[:-3]))
        fix.Session.sendToTarget(message)
 
    def sendNewOrderSingleIOC(self, qty, price, side, venue):
        message = fix.Message();
        header = message.getHeader();
        header.setField(fix.BeginString("FIX.4.2"))
        header.setField(fix.SenderCompID("TESTER_OUT"))
        header.setField(fix.TargetCompID("Simod"))
        header.setField(fix.TimeInForce(fix.TimeInForce_IMMEDIATEORCANCEL))
        header.setField(fix.MsgType("D"))
        message.setField(fix.OrderQty(float(qty)))
        message.setField(fix.Price(float(price)))
        #clOrderId = str(1000 * random.random())
        #message.setField(fix.OrigClOrdID(clOrderId))
        message.setField(fix.ClOrdID("%03d"%random.randint(1,999)))
        message.setField(fix.Symbol("FR0000131104"))
        message.setField(fix.OrdType(fix.OrdType_LIMIT))
        message.setField(fix.Side(fix.Side_BUY if side == "Buy" else fix.Side_SELL))
        message.setField(fix.HandlInst(fix.HandlInst_MANUAL))
        message.setField(fix.ExDestination(MarketDestination.MarketDict[venue]))
        message.setField(fix.TransactTime((datetime.datetime.now ().strftime ("%Y%m%d-%H:%M:%S.%f"))[:-3]))
        fix.Session.sendToTarget(message)
 
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
            print "Socket Initiator OK ! "
            self.is_connected = True
    def FlushMarket(self, venue):
        # send 2 huge FAK orders in opposite side to consume all liquidity in a specific OB
        self.sendNewOrderSingleIOC(1000000, 100000, "Buy", venue)
        self.sendNewOrderSingleIOC(1000000, 0.1, "Sell", venue) 
            
    def Run(self, mode, loadXMLFile):
        
        if not self.is_connected :
            self.Connect(mode)
      
        self.connector.start()
       
        if loadXMLFile:
            
            #clean Simod 
            self.FlushMarket("ENPA")
            self.FlushMarket("CHIX_PARIS")
            #empty Bid
           
            
            TestData = ReadTestFile("C:/Tools/`PythonFix/conf/test.xml")
            print "Preparing Test ..."
            venue = "ENPA"
            order_list = GetOrderList(TestData,venue )
            
            for order in order_list : 
                self.sendNewOrderSingle(order["qty"], order["price"], order["side"],"LONDON_PRIM")
            
            # send input NOS to the TEsted System
            
            # Set the system in WAIT for test mode
            self.state = "WAIT"
        if self.mode == "acceptor" :
            doLoop = True
            while doLoop :
                continue
#                s = raw_input("Waiting for an incoming connection, press C to Stop , and N to send a SOR NOS \n")
#                if lower(s) == 'c' :
#                    doLoop = False
#                elif s == 'N':
#                    self.sendSORNOS(50000,330,"Buy",'LSE')
#                else : 
#                    print "Unsupported Command, valid choices are c ton cancel and N to send an order"
            #s = raw_input('Press y + Enter when ready to send order, or m for MarketDataRequest, or c to quit !')
#            if lower(s) == 'y' :
#                doLoop = True
#                s = raw_input('Enter the rpice you want : ')
#                price = float(s)
#                self.sendNewOrderSingle(5000, price)
#            elif lower(s) == 'm' :
#                doLoop = True
#                self.getMarketDataRequest()
#            elif lower(s) == 'c' :
#                doLoop = False
#            else :     
#                print "Wrong input ..."
#        
        
#        self.connector.stop()
class AlgoTester():
    def InitiatorRun(self):
        self.file =  "C:/st_project/PythonFix/conf/fixcfg.cfg"  
        self.server = FIXServer(self.file)
        self.server.Run("initiator", True) 
        
    def AcceptorRun(self):
        file =  "C:/st_project/PythonFix/conf/fixcfg.cfg"  
        server = FIXServer(file)
        server.Run("acceptor", False)
        
    def SendNOS2SOR(self, qty, price,side,venue):
        self.server.sendSORNOS(qty, price,side,venue)
        
    def CloseFIXServer(self):
        self.server.connector.stop()
    
    def get_input(self, message):
        print(message)
        c= getch()
        print "use has pressed :%s"%c

        return sys.stdin.readline()
    def RequestInput(self):
        doLoop = True
        print "starting RequestInput"
        s = self.get_input("Waiting for an incoming connection, press C to Stop , and N to send a SOR NOS \n")
        if lower(s) == 'c' :
            doLoop = False
        elif s == 'N':
            self.SendNOS2SOR(50000,330,"Buy",'LSE')
        else : 
            print"Unsupported Command ..."
        
if __name__ == '__main__':
    
     #/home/team13/common13/simod/cfg/lance -n BNP -d 20110216 -S padev847 -s topcac -p 8101 -E 18:00:00 -u 0 -v 1 -P 5015 2>&1 >/tmp/cronjuraz_TOPCAC.log

    try:
        Tester = AlgoTester()
#        file =  "C:/st_project/PythonFix/conf/fixcfg.cfg"  
#        server = FIXServer(file)k
#        server.Run("acceptor", False)
        
        print "Initializing Simod connection"
        initiator = threading.Thread(target = Tester.InitiatorRun)
        initiator.start()
        time.sleep(5)
        
        print "Initializing SOR (Out) connection"
        acceptor = threading.Thread(target = Tester.AcceptorRun)
        acceptor.start()
        
        #time.sleep(5)
        #Tester.RequestInput()
        #inputRequest = threading.Thread(target = Tester.RequestInput)
        #inputRequest.start()
##        Tester.RequestInput()
#        Tester.CloseFIXServer()
        
        #threading.Thread(target =None).start() UNSILENTLY !!!+
#        initiator = threading.Thread(target = server.Run ,args =("acceptor", False) )
#        initiator.start()
        
#        acceptor = threading.Thread(target = AcceptorRun)
#        acceptor.start()
        
        
    except (fix.ConfigError, fix.RuntimeError), e:
        print e