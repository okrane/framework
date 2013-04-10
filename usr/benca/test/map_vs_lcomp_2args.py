'''
Created on 6 janv. 2011

@author: benca
'''

import math
from random import random
from time import time
m = 10000
n = 1000
_getattr = getattr
_map = map
_len = len

class O:
    def __init__(self):
        self.x = random()*255.0

x_= "x"
def get_x1(O): return O.x
def get_x2(O): return getattr(O, x_)
L = [O() for i in xrange(n)]
M = xrange(m)


    
print "#######################################################"
print 'Test 0 : map getattr'
t0 = time()
for i in M:
    map(getattr, L, ['x']*_len(L))
t1 = time()
print 'exec time = %f' %(t1-t0)

print "#######################################################"
print 'Test 1 : _map _getattr'
t0 = time()
for i in M:
    _map(_getattr, L, ['x']*_len(L))
t1 = time()
print 'exec time = %f' %(t1-t0)

print "#######################################################"
print 'Test 2 : list comp'
t0 = time()
for i in M:
    [o.x for o in L]
t1 = time()
print 'exec time = %f' %(t1-t0)