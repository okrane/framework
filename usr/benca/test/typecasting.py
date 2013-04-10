'''
Created on 29 dec. 2010

@author: benca
'''

import time



#def measureTimeNamed(name=None):    
#    def f_(*args):
#        print "#######################################################"
#        print name
#        t = time.clock()
#        f(*args)
#        dt= time.clock()- t
#        print 'exec time = %f' %dt
#    #f_.__docstring__ = f.__docstring__
#    f_.doc = f.__doc__
#    return f_

def measureTime(f,name= None):
    def f_(*args):
        print "#######################################################"
        print name
        t = time.clock()
        f(*args)
        dt= time.clock()- t
        print 'exec time = %f' %dt
    #f_.__docstring__ = f.__docstring__
    f_.doc = f.__doc__
    return f_

m = 500000
d = "14567894.00000000000000"
#d = 'zeufhzeifzgioe'

def test_1():
    for i in xrange(m):
        try:
            r = int(d)
        except ValueError:
            r = float(d)
        except: raise            
            
def test_2():
    for i in xrange(m):
        try:
            r = int(d)
        except:
            r = float(d)


if __name__ == '__main__':       
    measureTime(test_1, "test 0 : catching the exception")()
    measureTime(test_2, "test 2 : not catching the exception")()
    measureTime(test_1, "test 0 : catching the exception")()
    measureTime(test_2, "test 2 : not catching the exception")()
    measureTime(test_1, "test 0 : catching the exception")()
    measureTime(test_2, "test 2 : not catching the exception")()


