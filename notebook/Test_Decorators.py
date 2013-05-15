# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

# define an initial class
# in reality replaced by "import A"
class A:
    def __init__(self, x, y):
        self.x = x
        self.y = y
    def sum(self):
        return self.x+self.y
    ## what really happens is:
    ## staticmethod(sum3(x, y))
    @staticmethod
    def sum3(x,y):
        return 2*x+y

# <codecell>

# define external function
def prod(tm):
    return tm.x * tm.y

# <codecell>

# attach function to class
A.prod = prod

# <codecell>

# test
a = A(2, 3)
print a.sum()
print a.prod()

# <codecell>

# define another external function
def sum2(self):
    return (self.x+self.y)**2
   

# <codecell>

# replace function in class
A.sum = sum2

# <codecell>

# tests
b = A(2, 3)
print b.sum()
print b.prod()

# <codecell>


# <codecell>

u = A.sum3(4,6)
print u
v = A.sum3(6,4)
print v
w = A.sum3(y=4, x=6)
print w

# <codecell>

def logvariables(func):
    def decorated(x, y):
        print "I was asked to sum these variables X=%d, Y=%d" % (x, y)
        return func(x, y)
    return decorated

@logvariables
def my_func(x, y):
    return x+y

a = my_func(2, 3)
print a

# <codecell>

A.sum3 =  logvariables(A.sum3)

# <codecell>

print staticmethod(A.sum3(11, 22))

# <codecell>


# <codecell>


# <codecell>


