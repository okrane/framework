'''
Created on 10 mai 2011

@author: elber
'''



import os


alpha      = 0.000008
mu         = 0.000002
sigma      = 100
price_mean = 0.02
price_std  = 1.00
size_std   = 1.00
n_days     = 1000
wkrs       = 3

cmd = "C:/python/release/python C:/st_sim/usr/dev/elber/scenarii/volatility_zi_alpha_scenarii.py %f %f %f %f %f %f %d %d"
cmd = cmd %(alpha, mu, sigma, price_mean, price_std, size_std, n_days, wkrs)
os.system(cmd)