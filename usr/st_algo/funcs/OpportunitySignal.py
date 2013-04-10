import numpy as np

'''
Constructor
'''
import math
        
class OpportunitySignal:
    '''
    classdocs
    '''
    def __init__(self, window_size=60):
        '''
        Constructor
        '''
        # sqrt(datenum(0, 0, 0, 0, 10, 0)*86400*1e6) 
        # we want the volatility in euro by second
        self.coef_vol = 24495.0
         
        self.pvalue_fwd = 0.5
        self.pvalue_bwd = 0.5
        self.vcum       = 0.0
        self.vcumsigma  = 0.0
        self.vcum2cum   = 0.0
        self.sigman_fwd = 0.0
        self.sigman_bwd = 0.0
        self.last_price = None
        self.s_ref      = 0.0        
        self.current_vol = 0.0
        
        self.window_size = window_size
        self.list_pvalue_fwd = [0.5 for i in range(self.window_size)]
        self.list_pvalue_bwd = [0.5 for i in range(self.window_size)]
        self.pvalue_list_index = 0
        
        
    def update(self, marketManager):
        pass
    
    def updateSignals(self, current_vol_gk_10mn_bp, last_trades, market_vwap, end_of_period_volume, end_of_period_remaining_time):
        if current_vol_gk_10mn_bp is not None and len(last_trades) > 0:
            for t in last_trades:
                self.last_price = t.price
                self.current_vol =  t.price * (current_vol_gk_10mn_bp / 10000.0)/self.coef_vol
                self.vcum +=   t.size
                self.vcumsigma  += self.current_vol * t.size
                self.vcum2cum   += np.square(self.vcumsigma)
                self.sigman_fwd = np.sqrt(self.vcum2cum) / self.vcum
                
            if self.last_price != None and end_of_period_volume > 0 and end_of_period_remaining_time > 0:
                # Forward Signal    
                self.pvalue_fwd = self.normalCDF((self.last_price - market_vwap) /  self.sigman_fwd);
                
                # Backward Signal 
                self.s_ref = (market_vwap * self.vcum + (end_of_period_volume - self.vcum) * self.last_price) / end_of_period_volume             
                self.sigman_bwd = (1.0 - self.vcum/end_of_period_volume ) * np.sqrt(np.square(self.current_vol) * end_of_period_remaining_time / 3.0 )
                self.pvalue_bwd = self.normalCDF((self.last_price - self.s_ref) / self.sigman_bwd) 
            
                self.list_pvalue_fwd[self.pvalue_list_index] = self.pvalue_fwd
                self.list_pvalue_bwd[self.pvalue_list_index] = self.pvalue_bwd
                self.pvalue_list_index += 1
                self.pvalue_list_index = (self.pvalue_list_index % self.window_size)

    def getFwdOpportunitySignal(self):
        return self.pvalue_fwd
    
    def getBwdOpportunitySignal(self):
        return self.pvalue_bwd
    
    def getMeanFwd(self):
        return np.average(self.list_pvalue_fwd)
    
    def getMeanBwd(self):
        return np.average(self.list_pvalue_bwd)
    
    def getStdFwd(self):
        return np.std(self.list_pvalue_fwd)
    
    def getStdBwd(self):
        return np.std(self.list_pvalue_bwd)
    
    def getRangeFwd(self):
        return np.ptp(self.list_pvalue_fwd)
    
    def getRangeBwd(self):
        return np.ptp(self.list_pvalue_bwd)
    
    def normalCDF(self, z):
        if z > 6.0: 
            return 1.0
        if z < -6.0: 
            return 0.0 
        b1 = 0.31938153
        b2 = -0.356563782
        b3 = 1.781477937
        b4 = -1.821255978
        b5 = 1.330274429
        p  = 0.2316419
        c2 = 0.3989423
        a= math.fabs(z)
        t = 1.0/(1.0+a*p)
        b = c2* math.exp((-z)*(z/2.0))
        n = ((((b5*t+b4)*t+b3)*t+b2)*t+b1)*t
        n = 1.0-b*n
        if ( z < 0.0 ):
            n = 1.0 - n
        return n    
