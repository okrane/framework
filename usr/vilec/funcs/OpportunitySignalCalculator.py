from scipy.stats import norm
import numpy as np

'''
Constructor
'''
        
class OpportunityCalculator(object):
    '''
    classdocs
    '''
    def __init__(self, end_of_period_volume):
        '''
        Constructor
        '''
        # sqrt(datenum(0, 0, 0, 0, 10, 0)) 
        # we want the volatility in euro par second
        self.coef_vol =   0.0833 
         
        self.pvalue_fwd = 0.5
        self.pvalue_bwd = 0.5
        self.vcum      = 0.0
        self.vcumsigma = 0.0
        self.vcum2cum  = 0.0
        self.sigman_fwd    = 0.0
        self.sigman_bwd    = 0.0
        self.last_price = None
        self.s_ref      = 0.0
        self.end_of_period_volume = end_of_period_volume
        self.current_vol = 0.0
        
         
    def update_signals(self, current_vol_gk_10mn_bp, last_trades, market_vwap,  end_of_period_remaining_time):
        for t in last_trades:
            self.last_price = t.price
            self.current_vol =  t.price * (current_vol_gk_10mn_bp / 10000.0)/self.coef_vol
            self.vcum +=   t.size
            self.vcumsigma  += self.current_vol * t.size
            self.vcum2cum   += np.square(self.vcumsigma)
            self.sigman_fwd = np.sqrt(self.vcum2cum) / self.vcum
            
        if self.last_price != None:
            # Forward Signal
            self.pvalue_fwd = 100*norm.cdf((self.last_price - market_vwap) /  self.sigman_fwd);
            # Backward Signal
            self.s_ref = (market_vwap * self.vcum + (self.end_of_period_volume - self.vcum) * self.last_price) / self.end_of_period_volume
            # dt in, seconds ? 
            self.sigman_bwd = (1.0 - self.vcum/self.end_of_period_volume )*np.sqrt(np.square(self.current_vol) * end_of_period_remaining_time / 3.0 )
            self.pvalue_bwd = 100.0 * norm.cdf((self.last_price - self.s_ref) / self.sigman_bwd) 
            
             
        

    def get_fwd_opportunity_signal(self):
        return self.pvalue_fwd
    
    def get_bwd_opportunity_signal(self):
        return self.pvalue_bwd
    
    
    
    

    
        
        
        
        
        
        
        