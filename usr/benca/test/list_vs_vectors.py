'''
Created on 21 janv. 2011

@author: benca
'''

import numpy
import math
from random import random
from time import time
m = 10000
n = 1000

x_= "x"
L = [random()*255.0 for i in xrange(n)]
M = xrange(m)


    
print "#######################################################"
print 'Test 0 : list appending'
t0 = time()
for i in M:
    R = []
    for e in L:
        R.append(e)
t1 = time()
print 'exec time = %f' %(t1-t0)

print "#######################################################"
print 'Test 1 : numpy appending'
t0 = time()
for i in M:
    R = numpy.zeros((0, 1), dtype=numpy.float32)
    for e in L:
        R.append(e)
t1 = time()
print 'exec time = %f' %(t1-t0)
