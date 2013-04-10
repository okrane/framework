'''
Created on 5 avr. 2011

@author: benca
'''


def merge_sort(A):
    n = len(A)
    if n <= 1:
        return A
    left  = []
    right = []
    mid = int(n*0.5)
    for e in range(mid):
        left.append(A[e])
    for e in range(mid, n):
        right.append(A[e])
    left  = merge_sort(left)
    right = merge_sort(right)
    # perform merge and sort
    reslt = []
    while len(left) > 0 or len(right) > 0:
        if len(left) > 0 and len(right) > 0:
            if left[0] <= right[0]:
                reslt.append(left.pop(0))
            else:
                reslt.append(right.pop(0))
        elif len(left) > 0:
            reslt.append(left.pop(0))
        elif len(right) > 0:
            reslt.append(right.pop(0))
    return reslt




if __name__ == '__main__':
    from random import random
    A = [int(1000*random()) for i in range(10)]
    print A
    print merge_sort(A)