from simep.agents.baseagent import BaseAgent
from collections import list
from struct import Struct


class basic_lob:
    FORMAT_STRING_FOR_PACKING_LOB    =  '<ddddddddddiiiiiiiiiiii16s'
    def __init__(self, a_lob = None,nb_trades = None) :
        self.lob_converter = Struct(Lob_to_Record.FORMAT_STRING_FOR_PACKING_LOB)
        if a_lob and nb_trades:
            self.bid1=a_lob.getBid(0).price    
            self.bid2=a_lob.getBid(1).price    
            self.bid3=a_lob.getBid(2).price    
            self.bid4=a_lob.getBid(3).price    
            self.bid5=a_lob.getBid(4).price    
            self.ask1=a_lob.getAsk(0).price    
            self.ask2=a_lob.getAsk(1).price    
            self.ask3=a_lob.getAsk(2).price    
            self.ask4=a_lob.getAsk(3).price    
            self.ask5=a_lob.getAsk(4).price
            self.deltaTime=-1
            self.bids1=a_lob.getBid(0).size 
            self.bids2=a_lob.getBid(1).size 
            self.bids3=a_lob.getBid(2).size 
            self.bids4=a_lob.getBid(3).size 
            self.bids5=a_lob.getBid(4).size 
            self.asks1=a_lob.getAsk(0).size
            self.asks2=a_lob.getAsk(1).size
            self.asks3=a_lob.getAsk(2).size
            self.asks4=a_lob.getAsk(3).size
            self.asks5=a_lob.getAsk(4).size
            self.nbTrades=nb_trades        
            self.time[16]=a_lob.evtTime
        else :
            self.bid1=0  
            self.bid2=0    
            self.bid3=0    
            self.bid4=0    
            self.bid5=0    
            self.ask1=0    
            self.ask2=0    
            self.ask3=0    
            self.ask4=0    
            self.ask5=0
            self.deltaTime=-1
            self.bids1=0 
            self.bids2=0 
            self.bids3=0 
            self.bids4=0 
            self.bids5=0 
            self.asks1=0
            self.asks2=0
            self.asks3=0
            self.asks4=0
            self.asks5=0
            self.nbTrades=0        
            self.time[16]=''
            
    def Update_LOB(self, a_lob ,nb_trades) :
            self.bid1=a_lob.getBid(0).price    
            self.bid2=a_lob.getBid(1).price    
            self.bid3=a_lob.getBid(2).price    
            self.bid4=a_lob.getBid(3).price    
            self.bid5=a_lob.getBid(4).price    
            self.ask1=a_lob.getAsk(0).price    
            self.ask2=a_lob.getAsk(1).price    
            self.ask3=a_lob.getAsk(2).price    
            self.ask4=a_lob.getAsk(3).price    
            self.ask5=a_lob.getAsk(4).price
            self.deltaTime=-1
            self.bids1=a_lob.getBid(0).size 
            self.bids2=a_lob.getBid(1).size 
            self.bids3=a_lob.getBid(2).size 
            self.bids4=a_lob.getBid(3).size 
            self.bids5=a_lob.getBid(4).size 
            self.asks1=a_lob.getAsk(0).size
            self.asks2=a_lob.getAsk(1).size
            self.asks3=a_lob.getAsk(2).size
            self.asks4=a_lob.getAsk(3).size
            self.asks5=a_lob.getAsk(4).size
            self.nbTrades=nb_trades        
            self.time[16]=a_lob.evtTime
            
    def WriteToFile(self, file_object):
        file_object.write(self.lob_converter.pack(self.bid1,self.bid.price,self.bid3,self.bid4,self.bid5,                  \
                                                     self.ask1,self.ask2,self.ask3,self.ask4,self.ask5,self.deltaTime, \
                                                     self.bid1s,self.bid2s,self.bid3s,self.bid4s,self.bid5s,           \
                                                     self.ask1s,self.ask2s,self.ask3s,self.ask4s,self.ask5s,self.time))
    
class Lob_to_Record:
    FORMAT_STRING_FOR_PACKING_TRADES =  '<ddi'
   
    def __init__(self, a_lob, some_trades):
        self.trade_list = list()
        self.lob = basic_lob(a_lob, len(some_trades)) 
        self.trades_converter = Struct(Lob_to_Record.FORMAT_STRING_FOR_PACKING_TRADES)
        for trade in some_trades:
            t = (trade.price, trade.size, trade.overAsk)
            self.trade_list.append(t)
    def UpdateLOBData(self, a_lob, some_trades):
        self.lob.Update_LOB(a_lob,len(some_trades))
        self.trade_list = []
        for trade in some_trades:
            t = (trade.price, trade.size, trade.overAsk)
            self.trade_list.append(t)
    def write_lob_to_file(self, file_object):
        self.lob.WriteToFile(file_object)
        for t in self.trade_list:
            file_object.write(self.trades_converter.pack(t(1),t(2),t(3)))
        
        
    
