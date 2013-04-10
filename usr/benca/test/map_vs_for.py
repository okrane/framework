'''
Created on 6 janv. 2011

@author: benca
'''
import time

class obj:
    def __init__(self):
        self.dirty= False
         
l= []
for k in xrange(10000):
    l.append(obj())
for k in xrange(5000):
    l[k].dirty= True
    
l1= []    
t= time.clock()
def f_(x):
    if x.dirty:
        l1.append(x)
map(f_,l)

dt= time.clock()-t 
print dt
   
t= time.clock()
def f__(x):
    return x.dirty    
l1= filter(f__,l)
dt= time.clock()-t 
print dt

l1= []
t= time.clock()
for o in l:
    if o.dirty:
        l1.append(o)
dt= time.clock()-t 
print dt

t= time.clock()
l1= [o for o in l if o.dirty] 

dt= time.clock()-t    
print dt    

t= time.clock()
l1= tuple([o for o in l if o.dirty])
dt= time.clock()-t    
print dt    

#t= time.clock()
#g= (o for o in l if o.dirty) 
#l= [o for o in g ]
#dt= time.clock()-t    
#print dt    
