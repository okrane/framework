from SM import *

class Inter_S(SM):
    class States:
        INIT             = -1
        NOT_SENT         = 0
        AT_MID           = 1
        AGGRESSIVE       = 2        
        FINAL            = 3
    
    def __init__(self, order_name, timer_name, duration_seconds,state_init):
        self.occ_id = order_name
        self.timer_id = timer_name
        self.duration = duration_seconds
        self.state    = state_init
    
    def setOrderName(self, name):
        self.occ_id = name
        
    def onProcessFeed(self, sub_id, marketManager):
        pass    
    
    def onProcessMarket(self, ven_id, occ_id, evt, order, execution, marketManager):        
        if occ_id != self.occ_id:
            return         
                
        if self.state == Inter_S.States.INIT:
            if evt == pytvfo.EVT_ACK_NEW:
                self.state = Inter_S.States.AT_MID
                self.registerTimer(occ_id, self.duration)
                self.log("State: AT_MID. Creating Timer")
            elif evt == pytvfo.EVT_NACK_NEW:
                self.state = Inter_S.States.NOT_SENT
                self.log('State: NOT_SENT, passive order not sent')
        elif self.state == Inter_S.States.AT_MID:
            if evt == pytvfo.EVT_EXEC and order['LeavesQty'] == 0:
                self.log('Passive order fully filled, remove this order from the state machine')
                self.state = Inter_S.States.FINAL   
        elif self.state == Inter_S.States.AGGRESSIVE:
            if evt == pytvfo.EVT_EXEC and order['LeavesQty'] == 0:
                self.log('Aggressive order fully filled, remove this order from the state machine')
                self.state = Inter_S.States.FINAL         
             
    def onProcessTimer(self, timer_id, marketManager):        
        if timer_id != self.timer_id:
            return 
        self.log("Timer is on, State: AGGRESSIVE")
        self.state = Inter_S.States.AGGRESSIVE
        
    def isFinalState(self):
        return self.state == Inter_S.States.FINAL     
        