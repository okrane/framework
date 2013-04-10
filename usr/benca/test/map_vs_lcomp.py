'''
Created on 6 janv. 2011

@author: benca
'''

import math
from random import random
from time import time
m = 10000
n = 1000
_chr = round
_map = map
my_list = [int(random()*255) for i in xrange(n)]

print "#######################################################"
print 'Test 0 : _map _chr'
t0 = time()
for i in xrange(m):
    _map(_chr, my_list)
t1 = time()
print 'exec time = %f' %(t1-t0)

print "#######################################################"
print 'Test 1 : map chr'
t0 = time()
for i in xrange(m):
    map(chr, my_list)
t1 = time()
print 'exec time = %f' %(t1-t0)

print "#######################################################"
print 'Test 2 : list comprehension with _chr'
t0 = time()
for i in xrange(m):
    [_chr(j) for j in my_list]
t1 = time()
print 'exec time = %f' %(t1-t0)

print "#######################################################"
print 'Test 3 : list comprehension'
t0 = time()
for i in xrange(m):
    [chr(j) for j in my_list]
t1 = time()
print 'exec time = %f' %(t1-t0)