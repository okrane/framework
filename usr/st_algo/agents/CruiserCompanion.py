from simep.agents.companion import Companion 

class CruiserCompanion(Companion):
    def __init__(self, tactic):
        self.tactic = tactic
        self.indicators = {}
        self.market_turnover = 0
        self.market_volume   = 0
    
    def process_indicators(self, evt):
        self.indicators['OptimalQty'] = self.tactic['OptimalQty']
        self.indicators['TotalQty']   = self.tactic.moneyManager.getTotalQty()
        self.indicators['ExecQty']    = self.tactic.moneyManager.getExecQty()        
        self.indicators['TacticVwap'] = self.tactic.moneyManager.getStatistics()
        
        market = self.tactic.marketManager.getFeedInfo(evt.getVenueId())
        if market['TRADE_EVENT']:
            tr = self.tactic.marketManager.getTrades()
            for t in tr:
                self.market_turnover += t.price * t.size
                self.market_volume   += t.size             
                
    def get_final_indicators(self):
        self.indicators['MarketVwap'] = self.market_turnover / self.market_volume
        return self.indicators
        
        