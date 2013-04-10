'''
Created on 5 janv. 2011

@author: benca
'''


from time import time
from random import random



n1 = 100
n2 = 100
m  = 10000
my_list = [[int(random()*255) for j in xrange(int(random()*n2+1))] for i in xrange(n1)]
my_list2 = [bool(random()%2) for i in xrange(n1)]

def basic_func(my_list, my_list2, m):
    for i in xrange(m):
        A = [len(L) for j,L in enumerate(my_list) if my_list2[j]]
        B = [chr(L) for j,L in A]
        p = 1
        for j in my_list:
            p *= len(j)

def decorated_func(my_list, my_list2, m):
    _len = len
    _chr = chr
    _enumerate = enumerate
    for i in xrange(m):
        A = [_len(L) for j,L in _enumerate(my_list) if my_list2[j]]
        B = [_chr(L) for j,L in A]
        p = 1
        for j in my_list:
            p *= _len(j)

print "#######################################################"
print 'Test 0 : using basic builtins'
t0 = time()
basic_func(my_list, my_list2, m)
t1 = time()
print 'exec time = %f' %(t1-t0)

print "#######################################################"
print 'Test 1 : using local builtins'
t0 = time()
decorated_func(my_list, my_list2, m)
t1 = time()
print 'exec time = %f' %(t1-t0)

