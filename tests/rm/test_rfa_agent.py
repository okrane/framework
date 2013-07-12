import unittest
import os
import time
from threading import Thread

from simep.sched import Scheduler
from simep.sched import Order
from simep.sched import Trade
from simep.rfaagent import RFAAgent

#IP address of the Source server
SOURCE_IP = "10.136.87.194"

# publication constants
LAST_BPRICE_TAG = 22
LAST_APRICE_TAG = 25
LAST_BSIZE_TAG = 30
LAST_ASIZE_TAG = 31
NB_BORDER_TAG1 = 291
NB_AORDER_TAG1 = 292
NB_BORDER_TAG2 = 738
NB_AORDER_TAG2 = 742
START_BPRICE_TAG = 435
START_APRICE_TAG = 440
START_BSIZE_TAG = 729
START_ASIZE_TAG = 734
TRADE_PRICE_TAG = 6
TRADE_DATE_TAG = 16
TRADE_VOLUME_TAG = 178
TRADE_UNITS_TAG = 53
TRADE_TIME_TAG = 5
OPEN_PRICE_TAG = 19
CLOSING_PRICE_TAG = 21
DEAL_TAG = -1140
DEAL_SIZE_TAG = -1110
DEAL_PRICE_TAG = -1100
DEAL_TIME_TAG = -1130
ITEM_STATE_TAG = -1001
STORY_ID_TAG = 715
ALL_VOLUME_TAG = 32
TURNOVER_TAG = 100
NO_BIDMKR2_TAG = 740
NO_ASKMKR2_TAG = 744

#Template for the rfa client config.
#\Connections\Subscription\portNumber        = topcac
RFA_CONFIG_TEMPLATE="""[General]
[RFA]
\Connections\Subscription\connectionType    = SSLED
\Connections\Subscription\userName   	    = juraz
\Connections\Subscription\serverList 	    = %s
\Sessions\Subscription\connectionList 	    = Subscription
\Sessions\Subscription\\recoveryTimeout 	    = -1
"""

class TestRFAProtocol(unittest.TestCase):
###Methods for manipulating order book
    def createTrade(self,id1,id2,price,qty,hour,over):
        trade = Trade()
        trade.orderId1 = id1
        trade.orderId2 = id2
        trade.price = price
        trade.size = qty
        trade.hour = hour
        trade.overAsk = over
        return trade
    
    def createOrder(self,orderId,side,price,qty,type):
        order = Order()
        order.orderId = orderId
        order.side = side
        order.price = price

        if type == Order.Stop or type == Order.StopLimit :    
            order.stopPrice = price
            
        order.initialQty = qty
        order.shownQty = qty
        order.remain = qty
        order.orderType = type
        order.creationTimeMs = 0
        
        order.needExecReport = True #if False, we will not receive execution information
        return order

    def createIcebergOrder(self,orderId,side,price,qty,shownQty,type):
        order = self.createOrder(orderId,side,price,qty,type)
        order.shownQty = shownQty
        order.remain = shownQty
        order.hiddenQty = qty - shownQty
        return order
    
    def createRfaClientConfig(self):
        "Configuring RFA subscriber utility"
        ofile = open("./RFA/RfaSubscriber.ini","w")
        ofile.write(RFA_CONFIG_TEMPLATE%SOURCE_IP)
        ofile.close()
        
    def checkRfaOutput(self, rfa_lines, rfa_ref_frames):
        "Checks correctness of the RFA protocol output"
        try:
            parsed = self.parseRfaOutput(rfa_lines)
        except Exception, msg:
            raise ValueError, "Output file, produced by RFA client, has wrong format:"+str(msg)
        print "Parsed %d RFA frames from Source"%len(parsed)
        #parsed is a list of dictionaries, one dictionary for one frame, read from Source.

        if len(parsed) != len(rfa_ref_frames):
            raise ValueError, "Expected to receive %d frames, but received %d"%(len(rfa_ref_frames), len(parsed))
            
        for frame, ref_frame in zip(parsed, rfa_ref_frames):
            #comparing frame-by-frame
            compareFrames( frame, ref_frame )
            
    def parseRfaOutput(self, rfa_lines):
        "Parse list of lines, produced by reading output of th RFA client"
        rfa_frames = []
        cur_frame = None
        for line in rfa_lines:
            line = line.strip("\n")
            if line == "":
                break #
            if line == "==========================":
                #if frame separator
                if cur_frame != None:
                    rfa_frames.append(cur_frame)
                cur_frame = dict()
                continue
            else:
                #if not separator: parse FIELD-VALUE pair                
                field, value = line.split(' ') 
                cur_frame[int(field)] = value
        if cur_frame != None:
            rfa_frames.append(cur_frame)
        
        return rfa_frames
        
    def publishingTestRunner(self, create_orders_callback, expect_frames):
        """This function performs testing RFA agent. Arguments:
            create_orders_callback: function, that creates orders in the order book.
            This function must receive single parameter: OrderBook instance
            expect_frames: list of dictionaries, representing RFAframes to expect"""
        
        #delay, in seconds, between some operations
        delay = 2

        #creates a RFA agent
        agent = RFAAgent()        
        configureRfaAgent(agent)
        agent.startRFA()

        #scheduler initialization
        sched = Scheduler()
        sched.addOrderBook('normal','FR0000133308')
        
        order_book = sched.getOrderBook('FR0000133308')

        #checks, whether order book is created and returned correctly
        assert(order_book != None)

        #add RFA agent to scheduler
        sched.addAgent(agent)        
        
        #checks the order publishing
        t = Thread(target=sched.run)
        t.start()
        time.sleep(1)

        #using supplied callback function to create orders in the order book.
        #################
        create_orders_callback(order_book)
        #################
              
        #configures the RFA subscriber
        self.createRfaClientConfig()
        ext = os.system(".\\RFA\\rfaclient.exe ./RFA/RfaSubscriber.ini ./RFA/Log/ FTE 30")
        time.sleep(2)

        #closes RFA
        agent.stopRFA()
        #close the agent
        agent.closeRFA()
        # Stop the scheduler (and terminate the thread)
        sched.stop()

        #analyzes the output generated
        try:
            outfile = open("./RFA/Log/out.log","r")
            outdata = outfile.readlines()
            outfile.close()
            os.remove("./RFA/Log/out.log")
            os.remove("./RFA/Log/RFA.log")
        except Exception, msg:
            raise ValueError, "Failed to read output file with error: %s"%msg
        #perform checking
        self.checkRfaOutput(outdata, expect_frames)

    def check_source_server(self):
        #testing availability of a Source server.
        ping_value = os.system("ping -n 1 \"%s\""%SOURCE_IP)
        if ping_value != 0:
            print "Source server is unavailable under IP : %s"%(SOURCE_IP)
            print "Run the server before testing, or change the IP address in the RFA test script"
            return False
        else: return True

#####################
##Test #1
#####################
    def test_1_RFA_connection(self):
        if os.name == "posix":
            print "RFA can't be tested on Linux"
            self.assert_(False)
            return

        #checks if the source server is responding
        if not self.check_source_server(): raise ValueError

        #delay in seconds between actions
        delay = 2

        #create the RFA agent
        agent = RFAAgent() 
        configureRfaAgent(agent)
        time.sleep(delay)
        
        agent.startRFA()
        time.sleep(delay)

        #stops the RFA agent
        agent.stopRFA()

        #closes the agent
        agent.closeRFA()

#####################
##Test #2
#####################
    def test_2_RFA_publishing(self):
        if os.name == "posix":
            print "RFA can't be tested on Linux"
            self.assert_(False)
            return

        #checks if the source server is responding
        if not self.check_source_server(): raise ValueError

        #simple RFA publishing test
        def create_orders(order_book):
            "This function creates a pair of simple orders, to check, how ppublisher will publish them"
            # adds an ask order in the order book
            order = self.createOrder('ASK1',Order.Sell,10,250,Order.Limit)
            evt = order_book.processCreateOrder(order)
            #print "Ask order created and processed"
            
            # tries to make a BID deal on the ASK order
            bid_order = self.createOrder('BID1',Order.Buy,30,100,Order.Limit)
            trade = order_book.processCreateOrder(bid_order).getTrades()[0]
            
        #data to expect from rfa client
        expected_frame1 = {
                    DEAL_SIZE_TAG: "100",
                    DEAL_PRICE_TAG: "10.0000",
                    NB_BORDER_TAG1: "0",
                    NB_AORDER_TAG1: "1",
                    
                    START_BPRICE_TAG+1: "0.0000",
                    START_BPRICE_TAG+2: "0.0000",
                    START_BPRICE_TAG+3: "0.0000",
                    START_BPRICE_TAG+4: "0.0000",
                    START_BPRICE_TAG+5: "0.0000",
                    
                    START_APRICE_TAG+1: "10.0000",
                    START_APRICE_TAG+2: "0.0000",
                    START_APRICE_TAG+3: "0.0000",
                    START_APRICE_TAG+4: "0.0000",
                    START_APRICE_TAG+5: "0.0000",
                    
                    START_BSIZE_TAG+1: "0",
                    START_BSIZE_TAG+2: "0",
                    START_BSIZE_TAG+3: "0",
                    START_BSIZE_TAG+4: "0",
                    START_BSIZE_TAG+5: "0",
                    
                    START_ASIZE_TAG+1: "150",
                    START_ASIZE_TAG+2: "0",
                    START_ASIZE_TAG+3: "0",
                    START_ASIZE_TAG+4: "0",
                    START_ASIZE_TAG+5: "0",
                    
                    NO_BIDMKR2_TAG+0: "0",
                    NO_BIDMKR2_TAG+1: "0",
                    NO_BIDMKR2_TAG+2: "0",
                    NO_BIDMKR2_TAG+3: "0",
                    
                    NO_ASKMKR2_TAG+0: "0",
                    NO_ASKMKR2_TAG+1: "0",
                    NO_ASKMKR2_TAG+2: "0",
                    NO_ASKMKR2_TAG+3: "0"}
        #calls the testing procedure
        self.publishingTestRunner(create_orders, [expected_frame1])

def configureRfaAgent(agent):   
    # setting the default parameters for all sessions ([DEFAULT] section of config)
    agent.addSecurity(
        RIC="FR0000133308",
        RFA="FTE",
        securityId=110,
        tradingDestinationId=4,
        inputFileName="")
    try:
        #Arguments are:
        #  trace_path, service_name, source_host_name
        agent.initRFA(r"./RFA/Log/slcPublisher.log","topcac",SOURCE_IP) #Source server on the local machine
    except ValueError:
        raise ValueError, "Check the SOURCE server IP in the script"

def compareFrames(rfa_frame, rfa_frame_reference):
    "Compare two RFA frames. Each frame is a dictionary, mapping field ID (integer) to the field value (float). Raises ValueError, if not equal"
    for field, value in rfa_frame_reference.items():
        try:
            if rfa_frame[field] != value:
                raise ValueError, "Field %d has value %s, but %s was expected"%(field, rfa_frame[field], value)
        except KeyError:
            raise ValueError, "Field %d was not found in the received frame"%(field)
    for field, value in rfa_frame.items():
        if field not in rfa_frame_reference:
            print "Warning: RFA frame contains unexpected field %d, which is not present in the reference frame"%(field)
            
        
