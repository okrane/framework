'''
Created on 20 dec. 2010

@author: benca
'''

import random
import time

n = 100000
m = 100
t = map(chr, range(97, 123))
my_list = map(t.__getitem__, [int(random.random()*(len(t)-1)) for i in range(n)])

print "#######################################################"
print 'Test 0 : with a naive for loop'
t0 = time.time()
for i in xrange(m):
    minimum = 1.0
    index   = 0
    for k,v in enumerate(my_list):
        if v < minimum:
            minimum = v
            index   = k
t1 = time.time()
print 'exec time = %f' %(t1-t0)


print "#######################################################"
print 'Test 1 : index(min(mylist))'
t0 = time.time()
for i in xrange(m):
    a = min(my_list)
    my_list.index(a)
t1 = time.time()
print 'exec time = %f' %(t1-t0)


print "#######################################################"
print 'Test 2 : min(xrange(len(my_list)), key=my_list.__getitem__)'
t0 = time.time()
for i in xrange(m):
    min(xrange(len(my_list)), key=my_list.__getitem__) 
t1 = time.time()
print 'exec time = %f' %(t1-t0)


print "#######################################################"
print 'Test 3 : min(enumerate(my_list), key=itemgetter(1))[1]'
from operator import itemgetter 
t0 = time.time()
for i in xrange(m):
    min(enumerate(my_list), key=itemgetter(1))[1]
t1 = time.time()
print 'exec time = %f' %(t1-t0)


print "#######################################################"
print 'Test 4 : min(izip(my_list, count()))[1] '
from itertools import count, izip
t0 = time.time()
for i in xrange(m):
    min(izip(my_list, count()))[1] 
t1 = time.time()
print 'exec time = %f' %(t1-t0)


print "#######################################################"
print 'Test 5 : index(min(array(mylist)))'
t0 = time.time()
import array
for i in xrange(m):
    my_array = array.array('c', my_list)
    a = min(my_array)
    my_array.index(a)
t1 = time.time()
print 'exec time = %f' %(t1-t0)


print "#######################################################"
print 'Test 6 : index(min(myarray))'
my_array = array.array('d', my_list)
t0 = time.time()
for i in xrange(m):
    a = min(my_array)
    my_array.index(a)
t1 = time.time()
print 'exec time = %f' %(t1-t0)

print "#######################################################"
print 'Test 7 : reducing'
t0 = time.time()
def f_(x,y):
    if x < y: return x
    else: return y 
my_indexes = xrange(len(my_list))       
for i in xrange(m):
    l=zip(my_list,count())
    reduce(object.__leq__,l)[1]
t1 = time.time()
print 'exec time = %f' %(t1-t0)

