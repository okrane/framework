from simep.funcs.data.pyData import pyData 

class approx_1:
    
    def __init__(self, security, day):
        x = pyData('csv', filename = 'C:/st_sim/usr/dev/sivla/funcs/MathTools/context.csv')
        self.A = x['A'][0]
        self.k = x['k'][0]
        self.sigma = x['sigma'][0]
    
    def get_params(self):
        return {'A': self.A, 'k': self.k, 'sigma': self.sigma}
    
    def compute_H(self, context):
        return 0
    
    def gamma(self, n):
        return 0 if n == 0 else 1/n