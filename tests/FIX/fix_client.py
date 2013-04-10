import sys,os,glob
import subprocess

FIX_VERSION = "FIX.4.2"
FIX_SENDER = "CLIENT1"
FIX_TARGET = "Simep"
FIX_SRV_IP = "127.0.0.1"
FIX_SRV_PORT = "5003"
FIX_TIME = "20091006-00:00:00"
ORDERBOOK_RIC = "FR0000120404"
FFIX_OUT_FILE = "gmon.out"


# ----- MAIN FUNCTIONS TO USE IN THE FIX SCRIPT TEST -----

def test_limitOrder01():
    ## create an order without execution and cancel this one
    ## - the order is canceled without have been executed
    ## ------------------------------------------------------
    
    f1 = createConnectOnFrame()
    # create the order = ORDER TYPE : LIMIT, SIDE : BUY, TIF : NO
    f2 = createLimitOrderFrame("3","00001000","1","EUR",ORDERBOOK_RIC,"500","12.00","0","CT LO 01.01")
    # cancel the order
    f3 = createCancelOrderFrame("4","00001001","00001000","1",ORDERBOOK_RIC,"500","CT LO 01.02")
    f4 = createConnectOffFrame("5")

    cmd_file = "./FIX/cmd.txt"
    createCmdFile(cmd_file,(f1+f2+f3+f4))

    fix_log_file = "./FIX/out.log"
    startFIX(cmd_file,fix_log_file)
    log_file = FIX_VERSION + '-' + FIX_TARGET + '-' + FIX_SENDER + ".messages.log"
    cleaned_file = buildLogFile("./FIX/Log/",log_file,cmd_file)
    
    results = []
    results = analyzeLog(cleaned_file)
    result = analyzeReturnsTest1(results)
    
    deleteFiles(cmd_file,fix_log_file,cleaned_file)

    return result

def test_limitOrder02():
    ## create an order and execute this one completely
    ## - the second order is completely executed, the first one is partially executed
    ## ------------------------------------------------------------------------------

    f1 = createConnectOnFrame()
    # create the order = ORDER TYPE : LIMIT, SIDE : SELL, TIF : NO
    f2 = createLimitOrderFrame("3","00001012","2","EUR",ORDERBOOK_RIC,"1000","12.00","0","CT LO 02.01")
    # create the order = ORDER TYPE : LIMIT, BUY : SELL, TIF : NO
    f3 = createLimitOrderFrame("4","00001013","1","EUR",ORDERBOOK_RIC,"600","12.00","0","CT LO 02.02")
    f4 = createConnectOffFrame("5")

    cmd_file = "./FIX/cmd.txt"
    createCmdFile(cmd_file,(f1+f2+f3+f4))

    fix_log_file = "./FIX/out.log"
    startFIX(cmd_file,fix_log_file)

    log_file = FIX_VERSION + '-' + FIX_TARGET + '-' + FIX_SENDER + ".messages.log"
    cleaned_file = buildLogFile("./FIX/Log/",log_file,cmd_file)

    # a trade has to be generated for the average price 12 and a quantity of 600
    # one of the order should have a remaining quantity of 400
    results = []
    results = analyzeLog(cleaned_file)
    result = analyzeReturnsTest2(results)
    
    deleteFiles(cmd_file,fix_log_file,cleaned_file)

    return result

def test_limitOrder03():
    ## create and execute partially an order, cancel this one
    ## - the last order should be removed after the execution
    ## ------------------------------------------------------
    
    f1 = createConnectOnFrame()
    # create the order = ORDER TYPE : LIMIT, SIDE : SELL, TIF : NO
    f2 = createLimitOrderFrame("3","00001020","2","EUR",ORDERBOOK_RIC,"400","12.00","0","CT LO 03.01")
    # create the order = ORDER TYPE : LIMIT, BUY : SELL, TIF : NO
    f3 = createLimitOrderFrame("4","00001021","1","EUR",ORDERBOOK_RIC,"500","12.00","0","CT LO 03.02")
    # cancel the last order
    f4 = createCancelOrderFrame("5","00001022","00001021","1",ORDERBOOK_RIC,"500","CT LO 03.03")
    f5 = createConnectOffFrame("6")

    cmd_file = "./FIX/cmd.txt"
    createCmdFile(cmd_file,(f1+f2+f3+f4+f5))

    fix_log_file = "./FIX/out.log"
    startFIX(cmd_file,fix_log_file)

    log_file = FIX_VERSION + '-' + FIX_TARGET + '-' + FIX_SENDER + ".messages.log"
    cleaned_file = buildLogFile("./FIX/Log/",log_file,cmd_file)
    
    # a trade has to be generated for the average price 12 and a quantity of 400
    # one of the order should have a remaining quantity of 100
    results = []
    results = analyzeLog(cleaned_file)
    result = analyzeReturnsTest3(results)
    
    deleteFiles(cmd_file,fix_log_file,cleaned_file)

    return result

def test_limitOrder04():
    ## create an order with execution and modify its price
    ## - the replace request should be rejected because the order is already executed
    ## ------------------------------------------------------------------------------

    f1 = createConnectOnFrame()
    # create the order = ORDER TYPE : LIMIT, SIDE : SELL, TIF : NO
    f2 = createLimitOrderFrame("3","00001038","2","EUR",ORDERBOOK_RIC,"1000","12.00","0","CT LO 04.01")
    # create the order = ORDER TYPE : LIMIT, BUY : SELL, TIF : NO
    f3 = createLimitOrderFrame("4","00001039","1","EUR",ORDERBOOK_RIC,"600","12.00","0","CT LO 04.02")
    # modify the price the last order
    f4 = createReplaceLimitOrderFrame("5","00001040","00001039","1","EUR",ORDERBOOK_RIC,"600","11.99","0","CT LO 04.03")
    f5 = createConnectOffFrame("6")

    cmd_file = "./FIX/cmd.txt"
    createCmdFile(cmd_file,(f1+f2+f3+f4+f5))

    fix_log_file = "./FIX/out.log"
    startFIX(cmd_file,fix_log_file)

    log_file = FIX_VERSION + '-' + FIX_TARGET + '-' + FIX_SENDER + ".messages.log"
    cleaned_file = buildLogFile("./FIX/Log/",log_file,cmd_file)
    
    # a trade has to be generated for the average price 12 and a quantity of 600
    # one of the order should have a remaining quantity of 400
    results = []
    results = analyzeLog(cleaned_file)
    result = analyzeReturnsTest4(results)
    
    deleteFiles(cmd_file,fix_log_file,cleaned_file)

    return result

def test_limitOrder05():
    ## create an order with execution and modify its price
    ## - after replaced the price of the order, an execution is generated
    ## -------------------------------------------------------------------

    f1 = createConnectOnFrame()
    # create the order = ORDER TYPE : LIMIT, SIDE : SELL, TIF : NO
    f2 = createLimitOrderFrame("3","00001038","2","EUR",ORDERBOOK_RIC,"1000","12.00","0","CT LO 05.01")
    # create the order = ORDER TYPE : LIMIT, BUY : SELL, TIF : NO
    f3 = createLimitOrderFrame("4","00001039","1","EUR",ORDERBOOK_RIC,"500","11.00","0","CT LO 05.02")
    # modify the price the last order
    f4 = createReplaceLimitOrderFrame("5","00001040","00001039","1","EUR",ORDERBOOK_RIC,"500","12.00","0","CT LO 05.03")
    f5 = createConnectOffFrame("6")

    cmd_file = "./FIX/cmd.txt"
    createCmdFile(cmd_file,(f1+f2+f3+f4+f5))

    fix_log_file = "./FIX/out.log"
    startFIX(cmd_file,fix_log_file)

    log_file = FIX_VERSION + '-' + FIX_TARGET + '-' + FIX_SENDER + ".messages.log"
    cleaned_file = buildLogFile("./FIX/Log/",log_file,cmd_file)
    
    # 2 trades has to be generated for the average price 12 and a quantity of 500
    # the partially filled order should have a remaining quantity of 500
    results = []
    results = analyzeLog(cleaned_file)
    result = analyzeReturnsTest5(results)
    
    deleteFiles(cmd_file,fix_log_file,cleaned_file)

    return result

def test_marketOrder01():
    ## create a market order with execution and cancel this one
    ## - no trades should be generated
    ## ---------------------------------------------------------

    f1 = createConnectOnFrame()
    # create the order = ORDER TYPE : MARKET, SIDE : SELL, TIF : NO
    f2 = createMarketOrderFrame("3","00001038","2","EUR",ORDERBOOK_RIC,"1000","0","CT MO 01.01")
    # cancel the order
    f3 = createCancelOrderFrame("4","00001039","00001038","2",ORDERBOOK_RIC,"1000","CT MO 01.02")
    # modify the price the last order
    f4 = createConnectOffFrame("5")

    cmd_file = "./FIX/cmd.txt"
    createCmdFile(cmd_file,(f1+f2+f3+f4))

    fix_log_file = "./FIX/out.log"
    startFIX(cmd_file,fix_log_file)

    log_file = FIX_VERSION + '-' + FIX_TARGET + '-' + FIX_SENDER + ".messages.log"
    cleaned_file = buildLogFile("./FIX/Log/",log_file,cmd_file)
    
    # no trade has to be generated
    results = []
    results = analyzeLog(cleaned_file)
    result = analyzeReturnsTest6(results)
    
    deleteFiles(cmd_file,fix_log_file,cleaned_file)

    return result

def test_marketOrder02():
    ## create a market order with a complete execution
    ## - 1 trade should be generated
    ## ------------------------------------------------

    f1 = createConnectOnFrame()
    # create the order = ORDER TYPE : LIMIT, SIDE : SELL, TIF : NO
    f2 = createLimitOrderFrame("3","00001038","2","EUR",ORDERBOOK_RIC,"1000","12.00","0","CT MO 02.01")
    # create the order = ORDER TYPE : MARKET, SIDE : BUY, TIF : NO
    f3 = createMarketOrderFrame("4","00001039","1","EUR",ORDERBOOK_RIC,"600","0","CT MO 02.02")
    # modify the price the last order
    f4 = createConnectOffFrame("5")

    cmd_file = "./FIX/cmd.txt"
    createCmdFile(cmd_file,(f1+f2+f3+f4))

    fix_log_file = "./FIX/out.log"
    startFIX(cmd_file,fix_log_file)

    log_file = FIX_VERSION + '-' + FIX_TARGET + '-' + FIX_SENDER + ".messages.log"
    cleaned_file = buildLogFile("./FIX/Log/",log_file,cmd_file)
    
    # a trade has to be generated for the average price 12 and a quantity of 600
    # one of the order should have a remaining quantity of 400
    results = []
    results = analyzeLog(cleaned_file)
    result = analyzeReturnsTest7(results)
    
    deleteFiles(cmd_file,fix_log_file,cleaned_file)

    return result

def test_marketOrder03():
    ## create a market order and change its quantity
    ## - no trade should be generated
    ## ------------------------------------------------

    f1 = createConnectOnFrame()
    # create the order = ORDER TYPE : MARKET, SIDE : BUY, TIF : NO
    f2 = createMarketOrderFrame("3","00001039","1","EUR",ORDERBOOK_RIC,"600","0","CT MO 03.01")
    # modify the price the last order
    f3 = createReplaceMarketOrderFrame("4","00001040","00001039","1","EUR",ORDERBOOK_RIC,"1000","0","CT MO 03.02")
    f4 = createConnectOffFrame("5")

    cmd_file = "./FIX/cmd.txt"
    createCmdFile(cmd_file,(f1+f2+f3+f4))

    fix_log_file = "./FIX/out.log"
    startFIX(cmd_file,fix_log_file)

    log_file = FIX_VERSION + '-' + FIX_TARGET + '-' + FIX_SENDER + ".messages.log"
    cleaned_file = buildLogFile("./FIX/Log/",log_file,cmd_file)
    
    # no trade should be generated, but the quantity of the market
    # order should be changed
    results = []
    results = analyzeLog(cleaned_file)
    result = analyzeReturnsTest8(results)
    
    deleteFiles(cmd_file,fix_log_file,cleaned_file)

    return result

def test_stopOrder01():
    ## create a limit and stop orders
    ## - no trade should be generated, the stop order is stored in the order book
    ## ---------------------------------------------------------------------------

    f1 = createConnectOnFrame()
    # create the order = ORDER TYPE : LIMIT, SIDE : BUY, TIF : NO
    f2 = createLimitOrderFrame("3","00001038","1","EUR",ORDERBOOK_RIC,"1000","12.51","0","CT SO 01.01")
    # create a stop order = ORDER TYPE : STOP, SIDE : SELL, TIF : NO
    f3 = createStopOrderFrame("4","00001039","2","EUR",ORDERBOOK_RIC,"500","12.51","0","CT SO 01.02")
    f4 = createConnectOffFrame("5")

    cmd_file = "./FIX/cmd.txt"
    createCmdFile(cmd_file,(f1+f2+f3+f4))

    fix_log_file = "./FIX/out.log"
    startFIX(cmd_file,fix_log_file)

    log_file = FIX_VERSION + '-' + FIX_TARGET + '-' + FIX_SENDER + ".messages.log"
    cleaned_file = buildLogFile("./FIX/Log/",log_file,cmd_file)
    
    # no trade should be generated, the stop order is stored in the order book
    results = []
    results = analyzeLog(cleaned_file)
    result = analyzeReturnsTest9(results)
    
    deleteFiles(cmd_file,fix_log_file,cleaned_file)

    return result

def test_stopOrder02():
    ## create a stop order without execution
    ## - 1 trade should be generated but the stop order should be executed
    ## --------------------------------------------------------------------

    f1 = createConnectOnFrame()
    # create the order = ORDER TYPE : LIMIT, SIDE : BUY, TIF : NO
    f2 = createLimitOrderFrame("3","00001039","1","EUR",ORDERBOOK_RIC,"3000","12.51","0","CT SO 02.01")
    # create a stop order = ORDER TYPE : STOP, SIDE : BUY, TIF : NO
    f3 = createStopLimitOrderFrame("4","00001040","1","EUR",ORDERBOOK_RIC,"10000","12.52","12.51","0","CT SO 02.02")
    # create the order = ORDER TYPE : LIMIT, SIDE : SELL, TIF : NO
    f4 = createLimitOrderFrame("5","00001041","2","EUR",ORDERBOOK_RIC,"3000","12.51","0","CT SO 02.03")
    f5 = createConnectOffFrame("6")

    cmd_file = "./FIX/cmd.txt"
    createCmdFile(cmd_file,(f1+f2+f3+f4+f5))

    fix_log_file = "./FIX/out.log"
    startFIX(cmd_file,fix_log_file)

    log_file = FIX_VERSION + '-' + FIX_TARGET + '-' + FIX_SENDER + ".messages.log"
    cleaned_file = buildLogFile("./FIX/Log/",log_file,cmd_file)
    
    # no trade should be generated, the stop order is stored in the order book
    results = []
    results = analyzeLog(cleaned_file)
    result = analyzeReturnsTest10(results)
    
    deleteFiles(cmd_file,fix_log_file,cleaned_file)

    return result

def test_stopOrder03():
    ## create a stop (market) order with a complete execution
    ## - 1 trade should be generated but the stop order should be executed
    ## --------------------------------------------------------------------

    f1 = createConnectOnFrame()
    # create the order = ORDER TYPE : LIMIT, SIDE : SELL, TIF : NO
    f2 = createLimitOrderFrame("3","00001039","2","EUR",ORDERBOOK_RIC,"1000","12.51","0","CT SO 03.01")
     # create the order = ORDER TYPE : LIMIT, SIDE : SELL, TIF : NO
    f3 = createLimitOrderFrame("4","00001040","2","EUR",ORDERBOOK_RIC,"3000","12.52","0","CT SO 03.02")
     # create the order = ORDER TYPE : LIMIT, SIDE : SELL, TIF : NO
    f4 = createLimitOrderFrame("5","00001041","2","EUR",ORDERBOOK_RIC,"7000","12.53","0","CT SO 03.03")
    # create a stop order = ORDER TYPE : STOP, SIDE : BUY, TIF : NO
    f5 = createStopOrderFrame("6","00001042","1","EUR",ORDERBOOK_RIC,"10000","12.51","0","CT SO 03.04")
    # create the order = ORDER TYPE : LIMIT, SIDE : BUY, TIF : NO
    f6 = createLimitOrderFrame("7","00001043","1","EUR",ORDERBOOK_RIC,"1000","12.51","0","CT SO 03.05")
    f7 = createConnectOffFrame("8")

    cmd_file = "./FIX/cmd.txt"
    createCmdFile(cmd_file,(f1+f2+f3+f4+f5+f6+f7))

    fix_log_file = "./FIX/out.log"
    startFIX(cmd_file,fix_log_file)

    log_file = FIX_VERSION + '-' + FIX_TARGET + '-' + FIX_SENDER + ".messages.log"
    cleaned_file = buildLogFile("./FIX/Log/",log_file,cmd_file)
    
    # no trade should be generated, the stop order is stored in the order book
    results = []
    results = analyzeLog(cleaned_file)
    result = analyzeReturnsTest11(results)
    
    deleteFiles(cmd_file,fix_log_file,cleaned_file)

    return result

# ----- FIX FRAME CREATION FUNCTIONS ----- 

def deleteFiles(cmd_file,fix_log_file,cleaned_file):
    os.remove(os.getcwd() + "\\\\" + cmd_file)
    os.remove(os.getcwd() + "\\\\" + fix_log_file)
    os.remove(os.getcwd() + "\\\\" + cleaned_file)
    os.remove(os.getcwd() + "\\\\" + FFIX_OUT_FILE)
    
def createConnectOnFrame():
    # create connection on and synchronization frames
    logon = buildFIXLogonFrame("1")
    sync = buildFIXSyncFrame("2")
    
    # create the command file with FIX frames
    return logon + '\n' + sync + '\n' 
    
def createConnectOffFrame(msgNum):
    # create the connection off frame
    logout = buildFIXLogoutFrame(msgNum)

    # create the command file with FIX frames
    return logout + '\n' + ":exit" + '\n'

def createLimitOrderFrame(msgNum,orderId,side,currency,ric,qty,price,tif,account):
    # create the limit order frame
    limit = buidFIXLimitOrderFrame(msgNum,orderId,side,currency,ric,qty,price,tif,account)

    # create the command file with FIX frames
    return limit + '\n'

def createCancelOrderFrame(msgSeq,orderId,orderIdToCancel,side,ric,qty,account):
    # create the cancel order frame
    cancel = buidFIXCancelOrderFrame(msgSeq,orderId,orderIdToCancel,side,ric,qty,account)

    # create the command file with FIX frames
    return cancel + '\n'

def createMarketOrderFrame(msgNum,orderId,side,currency,ric,qty,tif,account):
    # create the market order frame
    market = buidFIXMarketOrderFrame(msgNum,orderId,side,currency,ric,qty,tif,account)

    # create the command file with FIX frames
    return market + '\n'

def createStopOrderFrame(msgNum,orderId,side,currency,ric,qty,stop_price,tif,account):
    # create the stop order frame
    stop = buidFIXStopOrderFrame(msgNum,orderId,side,currency,ric,qty,stop_price,tif,account)

    # create the command file with FIX frames
    return stop + '\n'

def createStopLimitOrderFrame(msgNum,orderId,side,currency,ric,qty,price,stop_price,tif,account):
    # create the stop order frame
    stop = buidFIXStopLimitOrderFrame(msgNum,orderId,side,currency,ric,qty,price,stop_price,tif,account)

    # create the command file with FIX frames
    return stop + '\n'

def createReplaceLimitOrderFrame(msgSeq,orderId,orderIdToReplace,side,currency,ric,qty,price,tif,account):
    # create the replace order frame
    replace = buidFIXReplaceLimitOrderFrame(msgSeq,orderId,orderIdToReplace,side,currency,ric,qty,price,tif,account)

    # create the command file with FIX frames
    return replace + '\n'

def createReplaceMarketOrderFrame(msgSeq,orderId,orderIdToReplace,side,currency,ric,qty,tif,account):
    # create the replace order frame
    replace = buidFIXReplaceMarketOrderFrame(msgSeq,orderId,orderIdToReplace,side,currency,ric,qty,tif,account)

    # create the command file with FIX frames
    return replace + '\n'

def createCmdFile(cmd_file, cmd):
    file = open(cmd_file,"w")
    file.write(cmd)
    file.close()

def startFIX(cmd_file,out_file):
    if os.path.exists(out_file):
        os.remove(out_file)
    exe = "./FIX/FFix-V1.0.exe -mode 3 -p fix -a " + FIX_SRV_IP + ":" + FIX_SRV_PORT
    exe = exe + " -s " + cmd_file + " -l " + out_file + " -d 100"
    subprocess.call(exe, close_fds=True)
    
def buildStandardFIXBody(version,msgType,sender,target,msgSeq,time):
    body = "35="+msgType+""
    body = body+"49="+sender+""
    body = body+"56="+target+""
    body = body+"34="+msgSeq+""
    body = body+"52="+time+""
    return body
    
def buildFIXFrame(body):
    frame = "8="+FIX_VERSION+""
    frame = frame +"9=" + str(len(body)) + "" + body
    frame = frame + "10=" + str(buildCheckSum(frame)) + ""
    return frame 
        
def buildFIXLogonFrame(msgSeq):
    body = buildStandardFIXBody(FIX_VERSION,"A",FIX_SENDER,FIX_TARGET,msgSeq,FIX_TIME)
    body = body+"98="+str(0)+""
    body = body+"108="+str(0)+""
    return buildFIXFrame(body)
    
def buildFIXSyncFrame(msgSeq):
    body = buildStandardFIXBody(FIX_VERSION,"1",FIX_SENDER,FIX_TARGET,msgSeq,FIX_TIME)
    body = body+"112=Synchronized?"
    return buildFIXFrame(body)

def buildFIXLogoutFrame(msgSeq):
    body = buildStandardFIXBody(FIX_VERSION,"5",FIX_SENDER,FIX_TARGET,msgSeq,FIX_TIME)
    return buildFIXFrame(body)
 
def buidFIXLimitOrderFrame(msgSeq,orderId,side,currency,ric,qty,price,tif,account):
    body = buildStandardFIXBody(FIX_VERSION,"D",FIX_SENDER,FIX_TARGET,msgSeq,FIX_TIME)
    body = body+"11="+orderId+""
    body = body+"22=4"
    body = body+"54="+side+""
    body = body+"15="+currency+""
    body = body+"48="+ric+""
    body = body+"40=2"
    body = body+"38="+qty+""
    body = body+"44="+price+""
    body = body+"59=0"
    body = body+"21=1"
    body = body+"100=PA"
    body = body+"55="+ric+""
    body = body+"60="+FIX_TIME+""
    body = body+"1="+account+""
    return buildFIXFrame(body)

def buidFIXMarketOrderFrame(msgSeq,orderId,side,currency,ric,qty,tif,account):
    body = buildStandardFIXBody(FIX_VERSION,"D",FIX_SENDER,FIX_TARGET,msgSeq,FIX_TIME)
    body = body+"11="+orderId+""
    body = body+"22=4"
    body = body+"54="+side+""
    body = body+"15="+currency+""
    body = body+"48="+ric+""
    body = body+"40=1"
    body = body+"38="+qty+""
    body = body+"59=0"
    body = body+"21=1"
    body = body+"100=PA"
    body = body+"55="+ric+""
    body = body+"60="+FIX_TIME+""
    body = body+"1="+account+""
    return buildFIXFrame(body)

def buidFIXStopOrderFrame(msgSeq,orderId,side,currency,ric,qty,stop_price,tif,account):
    body = buildStandardFIXBody(FIX_VERSION,"D",FIX_SENDER,FIX_TARGET,msgSeq,FIX_TIME)
    body = body+"11="+orderId+""
    body = body+"22=4"
    body = body+"54="+side+""
    body = body+"15="+currency+""
    body = body+"48="+ric+""
    body = body+"40=3"
    body = body+"38="+qty+""
    body = body+"99="+stop_price+""
    body = body+"59=0"
    body = body+"21=1"
    body = body+"100=PA"
    body = body+"55="+ric+""
    body = body+"60="+FIX_TIME+""
    body = body+"1="+account+""
    return buildFIXFrame(body)

def buidFIXStopLimitOrderFrame(msgSeq,orderId,side,currency,ric,qty,price,stop_price,tif,account):
    body = buildStandardFIXBody(FIX_VERSION,"D",FIX_SENDER,FIX_TARGET,msgSeq,FIX_TIME)
    body = body+"11="+orderId+""
    body = body+"22=4"
    body = body+"54="+side+""
    body = body+"15="+currency+""
    body = body+"48="+ric+""
    body = body+"40=4"
    body = body+"38="+qty+""
    body = body+"44="+price+""
    body = body+"99="+stop_price+""
    body = body+"59=0"
    body = body+"21=1"
    body = body+"100=PA"
    body = body+"55="+ric+""
    body = body+"60="+FIX_TIME+""
    body = body+"1="+account+""
    return buildFIXFrame(body)

def buidFIXCancelOrderFrame(msgSeq,orderId,orderIdToCancel,side,ric,qty,account):
    body = buildStandardFIXBody(FIX_VERSION,"F",FIX_SENDER,FIX_TARGET,msgSeq,FIX_TIME)
    body = body+"11="+orderId+""
    body = body+"41="+orderIdToCancel+""
    body = body+"54="+side+""
    body = body+"38="+qty+""
    body = body+"55="+ric+""
    body = body+"60="+FIX_TIME+""
    body = body+"1="+account+""
    return buildFIXFrame(body)

def buidFIXReplaceLimitOrderFrame(msgSeq,orderId,orderIdToReplace,side,currency,ric,qty,price,tif,account):
    body = buildStandardFIXBody(FIX_VERSION,"G",FIX_SENDER,FIX_TARGET,msgSeq,FIX_TIME)
    body = body+"11="+orderId+""
    body = body+"41="+orderIdToReplace+""
    body = body+"22=4"
    body = body+"54="+side+""
    body = body+"15="+currency+""
    body = body+"48="+ric+""
    body = body+"40=2"
    body = body+"38="+qty+""
    body = body+"44="+price+""
    body = body+"59=0"
    body = body+"21=1"
    body = body+"100=PA"
    body = body+"55="+ric+""
    body = body+"60="+FIX_TIME+""
    body = body+"1="+account+""
    return buildFIXFrame(body)

def buidFIXReplaceMarketOrderFrame(msgSeq,orderId,orderIdToReplace,side,currency,ric,qty,tif,account):
    body = buildStandardFIXBody(FIX_VERSION,"G",FIX_SENDER,FIX_TARGET,msgSeq,FIX_TIME)
    body = body+"11="+orderId+""
    body = body+"41="+orderIdToReplace+""
    body = body+"22=4"
    body = body+"54="+side+""
    body = body+"15="+currency+""
    body = body+"48="+ric+""
    body = body+"40=1"
    body = body+"38="+qty+""
    body = body+"59=0"
    body = body+"21=1"
    body = body+"100=PA"
    body = body+"55="+ric+""
    body = body+"60="+FIX_TIME+""
    body = body+"1="+account+""
    return buildFIXFrame(body)

def buildCheckSum(frame):
    size = len(frame)
    cks = 0
    inc = 0
    while (inc<size):
        cks = cks + ord(frame[inc])
        inc = inc + 1
    return cks%256

def analyzeReturnsTest1(results):
    ret = []
    ret = results[0]

    if len(ret) != 6:
        print "Number of files generated : " + str(len(ret))
        print "Number of files expected : 6"
        print "The test has to be played again"
        return False
    
    for r in ret:
        if r == False: return False
    return True

def analyzeReturnsTest2(results):   
    ret = []
    ret = results[0]

    if len(ret) != 7:
        print "Number of files generated : " + str(len(ret))
        print "Number of files expected : 7"
        print "The test has to be played again"
        return False
    
    for r in ret:
        if r == False: return False
    
    ret = results[1]
    avg_price = 0
    for r in ret:
        avg_price = avg_price + int(r)
    if avg_price%12 != 0: return False
    
    ret = results[2]
    leav_qty = 0
    for r in ret:
        leav_qty = leav_qty + int(r)
    if leav_qty != 400: return False
    
    ret = results[3]
    cum_qty = 0
    for r in ret:
        cum_qty = cum_qty + int(r)
    if cum_qty%600 != 0: return False
    
    ret = results[4]
    fill_side = 0
    for r in ret:
        fill_side = fill_side + int(r)
    if fill_side != 1: return False
    
    ret = results[5]
    parfill_side = 0
    for r in ret:
        parfill_side = parfill_side + int(r)
    if parfill_side != 2: return False
    
    return True

def analyzeReturnsTest3(results):   
    ret = []
    ret = results[0]

    if len(ret) != 9:
        print "Number of files generated : " + str(len(ret))
        print "Number of files expected : 9"
        print "The test has to be played again"
        return False
    
    for r in ret:
        if r == False: return False
    
    ret = results[1]
    avg_price = 0
    for r in ret:
        avg_price = avg_price + int(r)
    if avg_price%12 != 0: return False
    
    ret = results[2]
    leav_qty = 0
    for r in ret:
        leav_qty = leav_qty + int(r)
    if leav_qty != 100: return False
    
    ret = results[3]
    cum_qty = 0
    for r in ret:
        cum_qty = cum_qty + int(r)
    if cum_qty%400 != 0: return False
    
    ret = results[4]
    fill_side = 0
    for r in ret:
        fill_side = fill_side + int(r)
    if fill_side != 2: return False
    
    ret = results[5]
    parfill_side = 0
    for r in ret:
        parfill_side = parfill_side + int(r)
    if parfill_side != 1: return False
    
    return True

def analyzeReturnsTest4(results):   
    ret = []
    ret = results[0]

    if len(ret) != 8:
        print "Number of files generated : " + str(len(ret))
        print "Number of files expected : 8"
        print "The test has to be played again"
        return False

    for r in ret:
        if r == False: return False
    
    ret = results[1]
    avg_price = 0
    for r in ret:
        avg_price = avg_price + int(r)
    if avg_price%12 != 0: return False
    
    ret = results[2]
    leav_qty = 0
    for r in ret:
        leav_qty = leav_qty + int(r)
    if leav_qty != 400: return False
    
    ret = results[3]
    cum_qty = 0
    for r in ret:
        cum_qty = cum_qty + int(r)
    if cum_qty%600 != 0: return False
    
    ret = results[4]
    fill_side = 0
    for r in ret:
        fill_side = fill_side + int(r)
    if fill_side != 1: return False
    
    ret = results[5]
    parfill_side = 0
    for r in ret:
        parfill_side = parfill_side + int(r)
    if parfill_side != 2: return False
    
    return True

def analyzeReturnsTest5(results):
    ret = []
    ret = results[0]

    if len(ret) != 9:
        print "Number of files generated : " + str(len(ret))
        print "Number of files expected : 9"
        print "The test has to be played again"
        return False
    
    for r in ret:
        if r == False: return False
    
    ret = results[1]
    avg_price = 0
    for r in ret:
        avg_price = avg_price + int(r)
    if avg_price%12 != 0: return False
 
    ret = results[2]
    leav_qty = 0
    for r in ret:
        leav_qty = leav_qty + int(r)
    if leav_qty%500 != 0: return False
    
    ret = results[3]
    cum_qty = 0
    for r in ret:
        cum_qty = cum_qty + int(r)
    if cum_qty%500 != 0: return False
    
    ret = results[4]
    fill_side = 0
    for r in ret:
        fill_side = fill_side + int(r)
    if fill_side != 1: return False
    
    ret = results[5]
    parfill_side = 0
    for r in ret:
        parfill_side = parfill_side + int(r)
    if parfill_side != 2: return False
    
    return True

def analyzeReturnsTest6(results):
    return analyzeReturnsTest1(results)

def analyzeReturnsTest7(results):
    return analyzeReturnsTest2(results)

def analyzeReturnsTest8(results):   
    ret = []
    ret = results[0]

    if len(ret) != 6:
        print "Number of files generated : " + str(len(ret))
        print "Number of files expected : 6"
        print "The test has to be played again"
        return False
    
    for r in ret:
        if r == False: return False

    ret = results[1]
    avg_price = 0
    for r in ret:
        avg_price = avg_price + int(r)
    if avg_price != 0: return False

    ret = results[2]
    leav_qty = 0
    for r in ret:
        leav_qty = leav_qty + int(r)
    if leav_qty != 0: return False
    
    ret = results[3]
    cum_qty = 0
    for r in ret:
        cum_qty = cum_qty + int(r)
    if cum_qty != 0: return False
    
    ret = results[4]
    fill_side = 0
    for r in ret:
        fill_side = fill_side + int(r)
    if fill_side != 0: return False
    
    ret = results[5]
    parfill_side = 0
    for r in ret:
        parfill_side = parfill_side + int(r)
    if parfill_side != 0: return False
    
    return True

def analyzeReturnsTest9(results):
    ret = []
    ret = results[0]

    if len(ret) != 5:
        print "Number of files generated : " + str(len(ret))
        print "Number of files expected : 5"
        print "The test has to be played again"
        return False
    
    for r in ret:
        if r == False: return False

    ret = results[1]
    avg_price = 0
    for r in ret:
        avg_price = avg_price + int(r)
    if avg_price != 0: return False

    ret = results[2]
    leav_qty = 0
    for r in ret:
        leav_qty = leav_qty + int(r)
    if leav_qty != 0: return False
    
    ret = results[3]
    cum_qty = 0
    for r in ret:
        cum_qty = cum_qty + int(r)
    if cum_qty != 0: return False
    
    ret = results[4]
    fill_side = 0
    for r in ret:
        fill_side = fill_side + int(r)
    if fill_side != 0: return False
    
    ret = results[5]
    parfill_side = 0
    for r in ret:
        parfill_side = parfill_side + int(r)
    if parfill_side != 0: return False
    
    return True

def analyzeReturnsTest10(results):   
    ret = []
    ret = results[0]

    if len(ret) != 8:
        print "Number of files generated : " + str(len(ret))
        print "Number of files expected : 8"
        print "The test has to be played again"
        return False
    
    for r in ret:
        if r == False: return False
    
    ret = results[1]
    avg_price = 0
    for r in ret:
        avg_price = avg_price + float(r)
    if avg_price%12.51 != 0: return False
    
    ret = results[2]
    leav_qty = 0
    for r in ret:
        leav_qty = leav_qty + int(r)
    if leav_qty != 0: return False
    
    ret = results[3]
    cum_qty = 0
    for r in ret:
        cum_qty = cum_qty + int(r)
    if cum_qty%3000 != 0: return False
    
    ret = results[4]
    fill_side = 0
    for r in ret:
        fill_side = fill_side + int(r)
    if fill_side != 3: return False
    
    ret = results[5]
    parfill_side = 0
    for r in ret:
        parfill_side = parfill_side + int(r)
    if parfill_side != 0: return False
    
    return True

def analyzeReturnsTest11(results):   
    ret = []
    ret = results[0]

    if len(ret) != 14:
        print "Number of files generated : " + str(len(ret))
        print "Number of files expected : 14"
        print "The test has to be played again"
        return False
    
    for r in ret:
        if r == False: return False
    
    ret = results[1]
    avg_price = 0
    for r in ret:
        avg_price = avg_price + float(r)
    if avg_price != 75.117: return False
    
    ret = results[2]
    leav_qty = 0
    for r in ret:
        leav_qty = leav_qty + int(r)
    if leav_qty != 7000: return False
    
    ret = results[3]
    cum_qty = 0
    for r in ret:
        cum_qty = cum_qty + int(r)
    if cum_qty != 25000: return False
    
    ret = results[4]
    fill_side = 0
    for r in ret:
        fill_side = fill_side + int(r)
    if fill_side != 8: return False
    
    ret = results[5]
    parfill_side = 0
    for r in ret:
        parfill_side = parfill_side + int(r)
    if parfill_side != 1: return False
    
    return True

def buildLogFile(path,log_file,cmd_file):
    log = open(path+log_file, "r")
    cmd = open(cmd_file, "r")
    
    new_file = "cleaned_" + log_file
    out = open(new_file, "w")

    cmd_ligne = cmd.readline()
    while cmd_ligne !="" and cmd_ligne != "exit:": 
        next_cmd = cmd.readline()
        if next_cmd != "":
            log_ligne = log.readline()
            while log_ligne != cmd_ligne and log_ligne != "":
                log_ligne = log.readline()
            if log_ligne != "":
                out.write("[SEND]|" + cmd_ligne)
                log_ligne = log.readline()
                while log_ligne != next_cmd and log_ligne != "":
                    out.write("[RECV]|" + log_ligne)
                    log_ligne = log.readline()
                log.seek(0)
                cmd_ligne = next_cmd
            else: break
        else: break

    log.close()
    cmd.close()
    out.close()
    return new_file
     
def analyzeLog(cleaned_file):
    ret = []
    avg_price = []
    leav_qty = []
    cum_qty = []
    fill_side = []
    parfill_side = []
    
    f = open(cleaned_file,"r")
    ligne = f.readline()
    if ligne == "": return False
    
    nbRcvMsg = 0
    while ligne != "":
        if str(ligne.split('|')[0]) == "[SEND]":
            order_sen = str(ligne.split('|')[1])
            ligne = f.readline()
            if str(ligne.split('|')[0]) == "[SEND]": 
                ret.append(False)
                avg_price.append(0)
                leav_qty.append(0)
                cum_qty.append(0)
                fill_side.append(0)
                parfill_side.append(0)
                return ret,avg_price,leav_qty,cum_qty,fill_side,parfill_side
            while str(ligne.split('|')[0]) == "[RECV]": 
                nbRcvMsg = nbRcvMsg + 1
                order_rcv = str(ligne.split('|')[1])
                
                returns = analyzeOrder(order_sen,order_rcv)

                ret.append(returns[0])
                avg_price.append(returns[1])
                leav_qty.append(returns[2])
                cum_qty.append(returns[3])
                fill_side.append(returns[4])
                parfill_side.append(returns[5])

                ligne = f.readline()
    f.close()
    return ret,avg_price,leav_qty,cum_qty,fill_side,parfill_side

def analyzeOrder(order_sen,order_rcv):
    avg_price=0
    leav_qty=0
    cum_qty=0
    fill_side=0
    parfill_side=0
    ret = True
    
    msgTypeSen = str(order_sen.split('')[2])
    msgTypeRcv = str(order_rcv.split('')[2])
    
    # succeeded connection on validation (log on)
    if msgTypeSen == "35=A" and msgTypeRcv == "35=A":
        if "58=" in  order_rcv: ret = False
        
    # succeeded synchronization validation (heart beat message)
    elif msgTypeSen == "35=1" and msgTypeRcv == "35=0":
        if "58=" in  order_rcv: ret = False
        
    # succeeded connection off (log out)
    elif msgTypeSen == "35=5" and msgTypeRcv == "35=5":
        if "58=" in  order_rcv: ret = False
        
    # succeeded to create new order or replace an order (receive a message execution report)
    elif msgTypeSen == "35=D" or msgTypeSen == "35=G" and msgTypeRcv == "35=8":
        # get the status of the order
        ord_status = getTagValue(order_rcv,"39=")
        if ord_status == "0": # new order
            # TIF type - 0:DAY, 1:GTC, 2:OPG, 3:IOC, 4:FOK, 5:GTX, 6:GTD
            if getTagValue(order_rcv,"59=") != getTagValue(order_sen,"59="): ret = False
            # Side order - 1: Buy, 2 - Sell (there are other values)
            if getTagValue(order_rcv,"54=") != getTagValue(order_sen,"54="): ret = False
            # RIC of the order book
            if getTagValue(order_rcv,"48=") != getTagValue(order_sen,"48="): ret = False
            # Order quantity
            if getTagValue(order_rcv,"38=") != getTagValue(order_sen,"38="): ret = False
            # Last price
            if getTagValue(order_rcv,"31=") != "0": ret = False
            # Quantity bought/sold on this last fill
            if getTagValue(order_rcv,"32=") != "0": ret = False
            # Average price of all fills on this order
            if getTagValue(order_rcv,"6=") != "0": ret = False
            # Total quantity bought/sold
            if getTagValue(order_rcv,"151=") != getTagValue(order_sen,"38="): ret = False
            # Total executed quantity
            if getTagValue(order_rcv,"14=") != "0": ret = False
            # Execution type : 0:new, 1:Partial fill, 2: fill(there are other values)
            if getTagValue(order_rcv,"150=") != "0": ret = False
            # Additional comments 
            if "58=" in  order_rcv: ret = False
            
        if ord_status == "1": # Partially filled
            if getTagValue(order_rcv,"59=") != getTagValue(order_sen,"59="): ret = False
            if getTagValue(order_rcv,"48=") != getTagValue(order_sen,"48="): ret = False
            if getTagValue(order_rcv,"150=") != "1": ret = False
            if "58=" in  order_rcv: ret = False
            
            # Side
            parfill_side = getTagValue(order_rcv,"54=")
            # Average price
            avg_price = getTagValue(order_rcv,"6=")
            # Leave quantity
            leav_qty = getTagValue(order_rcv,"151=")
            # Quantity executed
            cum_qty = getTagValue(order_rcv,"14=")
        
        if ord_status == "2": # Filled
            if getTagValue(order_rcv,"59=") != getTagValue(order_rcv,"59="): ret = False
            if getTagValue(order_rcv,"48=") != getTagValue(order_rcv,"48="): ret = False
            if getTagValue(order_rcv,"150=") != "2": ret = False
            if "58=" in  order_rcv: ret = False

            # Side
            fill_side = getTagValue(order_rcv,"54=")
            # Average price
            avg_price = getTagValue(order_rcv,"6=")
            # Leave quantity
            leav_qty = getTagValue(order_rcv,"151=")
            # Quantity executed
            cum_qty = getTagValue(order_rcv,"14=")

    # cancel order validation
    elif msgTypeSen == "35=F" and msgTypeRcv == "35=8":
         # get the status of the order
        ord_status = getTagValue(order_rcv,"39=")
        if ord_status == "6" or ord_status == "4": # pending/canceled order
            # Side order - 1: Buy, 2 - Sell (there are other values)
            if getTagValue(order_rcv,"54=") != getTagValue(order_sen,"54="): ret = False
            # RIC of the order book
            if getTagValue(order_rcv,"55=") != getTagValue(order_sen,"55="): ret = False
            # Order quantity
            if getTagValue(order_rcv,"38=") != getTagValue(order_sen,"38="): ret = False
            # Check the order canceled
            if getTagValue(order_rcv,"41=") != getTagValue(order_sen,"41="): ret = False
            # Additional comments 
            if "58=" in  order_rcv: ret = False
            
    # tried to replace an order but rejection
    elif msgTypeSen == "35=G" and msgTypeRcv == "35=9":
        # order id rejected
        if getTagValue(order_rcv,"11=") != getTagValue(order_sen,"11="): ret = False
        # order id not replaced
        if getTagValue(order_rcv,"41=") != getTagValue(order_sen,"41="): ret = False
        
    else: ret = False
    
    return ret,avg_price,leav_qty,cum_qty,fill_side,parfill_side
    
def getTagValue(order,tag):
    return order[order.find(tag)+(len(tag)):order.find(tag)+(order.find("",order.find(tag)+1) - order.find(tag))]
