import numpy as np
import math
from numpy.core.function_base import linspace
        
class OnlineOptimization:

    def __init__(self, mode):
        '''
        Constructor
        '''
        additional_params = {}
        # lambda in mean-variance function
        additional_params['lambda'] = 1e0
        
        # mu to penalize large/small rate w.r.t. reference rate
        additional_params['mu_mean_variance'] = 5e-1
        additional_params['mu_max_outperform'] = 1e0
        additional_params['mu_min_underperform'] = 1e0
        
        # const in maximize outperform probability P(slippage > c)
        additional_params['c_max_outperform'] = 10e-4
    
        # const in minimize underperform probability P(slippage < -c)
        additional_params['c_min_outperform'] = 2e-4
        
        
    def updateSignals(self, moneyManager, marketManager, estimateParameter, rates):
        # get current value for market
        V_tilde_M = marketManager.getFeedInfo('AE', 'QTE_CNT1')
        S_tilde_M = self.marketManager.getFeedInfo('AE', 'WTD_AVE1')
        
        # get current value for execution        
        V_tilde_E = moneyManager.getStatistics()
        S_tilde_E = moneyManager.getExecQty()
        
        # get estimate parameters        # market VWAP
        Exp_S_hat_M = estimateParameter['Exp_S_hat_M']        
        Var_S_hat_M = estimateParameter['Var_S_hat_M']
        
        # market volume
        Exp_V_hat_M_E = estimateParameter['Exp_V_hat_M_E']
        Var_V_hat_M_E = estimateParameter['Var_V_hat_M_E']
        
        # slippage
        exp_slippage = estimateParameter['exp_slippage']
        var_slippage = estimateParameter['var_slippage']
        
        # compute the mean and variance of slippage
        self.additional_params['ref_rate'] = rates['ref_rate']
        range_of_rates = linspace(rates['min_rate'], rates['max_rate'], rates['step_rate'])
        value_fct = []
        for r in range_of_rates:
            mean_P = self.mean_P(r, V_tilde_E, V_tilde_M, S_tilde_E, S_tilde_M, Exp_S_hat_M, Exp_V_hat_M_E, exp_slippage)
            mean_Q = self.mean_Q(r, V_tilde_M, Exp_V_hat_M_E)
            cov_P_Q = self.cov_P_Q(r)
            var_P = self.var_P(r, V_tilde_E, V_tilde_M, Var_S_hat_M, Var_V_hat_M_E, var_slippage)
            var_Q = self.var_Q(r, Var_V_hat_M_E)
            
            mean_s = self.meanSlippage(mean_P, mean_Q, cov_P_Q, var_Q)                
            var_s = self.varSlippage(mean_P, mean_Q, cov_P_Q, var_P, var_Q)
        
            value_fct = obj_function(mean_s, var_s, vec_r);
                [val_max idx_max] = max(value_fct);
                min_r = V_tilde_E/(V_tilde_M + Exp_V_hat_M_E);
                
                optimal_r(i_cp,i_cdv) = max(vec_r(idx_max), min_r);
                
        

        
        
    def meanSlippage(self, E_R, E_S, C_R_S, V_S):
        return E_R/E_S - C_R_S/E_S^2 + V_S*E_R/E_S^3
    
    def varSlippage(self, E_R, E_S, C_R_S, V_R, V_S):
        return (E_R/E_S)^2.0*(V_R/E_R^2 - 2.0*C_R_S/(E_R*E_S) + V_S/E_S^2)
    
    def mean_P(self, r, V_tilde_E, V_tilde_M, S_tilde_E, S_tilde_M, Exp_S_hat_M, Exp_V_hat_M_E, Exp_d_hat_E):
        return V_tilde_E*S_tilde_E - r*V_tilde_M*S_tilde_M \
               + (r*V_tilde_M - V_tilde_E)*Exp_S_hat_M \
               + Exp_d_hat_E * Exp_S_hat_M * (((r*V_tilde_M - V_tilde_E) + r* Exp_V_hat_M_E) /(1.0 -r))

    def mean_Q(self, r, V_tilde_M, Exp_V_hat_M_E):
        return r/(1.0-r)*(V_tilde_M + Exp_V_hat_M_E)

    def cov_P_Q(self, r):
        return 0.0
    
    def var_P(self, r, V_tilde_E, V_tilde_M, Var_S_hat_M, Var_V_hat_M_E, Var_d_hat_E):
        return (r*V_tilde_M - V_tilde_E)^2 * Var_S_hat_M + (r/(1.0-r))^2 \
                *Var_S_hat_M*Var_V_hat_M_E*Var_d_hat_E
                
    def var_Q(self, r, Var_V_hat_M_E):
        return (r/(1.0-r))^2*Var_V_hat_M_E
    
    def objectiveFct(self, mode, mean_s, var_s, r, additional_params):
        ref_rate = additional_params['ref_rate']
        if mode == 'mean_variance_function':
            return mean_s - additional_params['lambda']/2.0*var_s - additional_params['mu_mean_variance']*(r-ref_rate)^2
        
        if mode == 'max_outperform_function':
            return self.normalCDF((mean_s - additional_params['c_max_outperform']) / var_s^.5) - additional_params['mu_max_outperform']*(r-ref_rate)^2
        
        if mode == 'min_underperform_function':
            return - self.normalCDF(-(mean_s + additional_params['c_min_outperform']) / var_s^.5) - additional_params['mu_min_underperform']*(r-ref_rate)^2
     
    
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
     
        
   
