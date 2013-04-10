'''
Created on 13 janv. 2011

@author: benca
'''

import math
f = open('C:/stockscomparator_res.txt', 'r')
L = f.readlines()
f.close()


M = 22*[0.0]
V = 22*[0.0]
for l in L:
    A = map(float, l.split('|'))
    for i in range(22):
        M[i] += A[i+1]
        V[i] += A[i+1]**2

n = len(L)
for i in range(22):
    M[i] /= n
    V[i] /= n

for i in range(22):
    V[i] -= M[i]**2
    V[i] = math.sqrt(V[i])
    
    
print M
print V