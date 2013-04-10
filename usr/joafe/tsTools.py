'''
Created on Apr 27, 2010

@author: joafe
'''

from tickdb import TickDB
from security import *
import pylab
import math
import random

def midPoint(p1, p2):
    result = r3point()
    result.set1((p1.get1() + p2.get1()) / 2.0)
    result.set2((p1.get2() + p2.get2()) / 2.0)
    result.set3((p1.get3() + p2.get3()) / 2.0)
    return result

class r3point(object):
    def __init__(self):
        self.a = random.expovariate(10.0)
        self.b = random.expovariate(10.0)
        self.c = random.expovariate(10.0) 
    def set(self, a, b, c):
        self.set1(a)
        self.set2(b)
        self.set3(c)
    def set1(self, a):
        self.a = a
    def set2(self, b):
        self.b = b
    def set3(self, c):
        self.c = c
    def get1(self):
        return self.a
    def get2(self):
        return self.b
    def get3(self):
        return self.c
        
        
class gAlgo(object):
    def __init__(self, k=8):
       self.population = []
       self.values = []
       self.n = 36
       self.k = k
       self.addRandom(self.n)
    def addRandom(self, n=1): 
        for i in range(0, n):
            p = r3point()
            self.addPoint(p)
    def addPoint(self, p):
        n = len(self.population)
        value = self.mainFunction(p)
        if (n == 0):
            self.population.append(p)
            self.values.append(value)
        elif (n == 1):
            if(value >= self.values[0]):
                self.population.insert(0, p)
                self.values.insert(0, value)
            else:
                self.population.append(p)
                self.values.append(value)
        else:
            if(value >= self.values[0]):
                self.population.insert(0, p)
                self.values.insert(0, value)
            elif(value < self.values[n - 1]):
                self.population.append(p)
                self.values.append(value)
            else:
                idx = 1
                while(self.values[idx] > value):
                    idx = idx + 1
                self.population.insert(idx, p)
                self.values.insert(idx, value)
    def getSurvivors(self):
        n = 28
        for i in range(0, n):
            self.population.pop()
            self.values.pop()
    def addNewGeneration(self):
        ng = []
        n = len(self.population)
        for i in range(0, n - 1):
            for j in range(i + 1, n):
                p = self.mutation(midPoint(self.population[i], self.population[j]), 0.01)
                ng.append(p)
        m = len(ng)
        for i in range(0, m):
            self.addPoint(ng[i])
            
    def mutation(self, p, epsilon):
        output = p
        output.set1(max(p.get1() + random.normalvariate(0, epsilon), 0.001))
        output.set2(max(p.get2() + random.normalvariate(0, epsilon), 0.001))
        output.set3(max(p.get3() + random.normalvariate(0, epsilon), 0.001))
        return output
    
    def mainFunction(self, p):
        pass
        
    def run(self):
        a = []
        iter = []
        for i in range(0, 100):
            self.getSurvivors()
            self.addNewGeneration()

        
def realisedVariance(vect):
        output = 0
        N = len(vect)
        for i in range(1, N):
            output = output + pow(vect[i] - vect[i - 1], 2)
        return math.sqrt(output * 252)

class timeSeries(object):
    def __init__(self):
        self.times = []
        self.values = []
    def getSize(self):
        return len(self.values)
    def loadHIST(self, security=110, destination=4):
        sec = Security(security, destination)
        result = TickDB.trade_list(sec, '20100412')
        size = len(result.date)
        for i in range(0, size):
            currentTime = result.date[i].hour * 3600 + result.date[i].minute * 60 + result.date[i].second + result.date[i].microsecond / 1000000
            self.times.append(currentTime)
            self.values.append(result['price'][i]) 
    def makeRegular(self, dt):    
        currentTime = self.times[0];
        size = len(self.times)
        regTs = timeSeries()
        for idx in range(1, size):
            currentUp = self.times[idx]
            while(currentTime < currentUp):
                regTs.times.append(currentTime)
                regTs.values.append(self.values[idx - 1])
                currentTime = currentTime + dt
        return regTs
    def getWindow(self, idx0, windowSize):
        output = []
        for i in range(idx0, idx0 + windowSize):
            output.append(self.values[i])
        return output

def subSampling(ts, idxLag):
    output = timeSeries()
    N = ts.getSize()
    i = 0
    while i < N:
        output.values.append(ts.values[i])
        output.times.append(ts.times[i])
        i = i + idxLag
    return output

def movingWindow(ts, wdSize):
    output = timeSeries()
    N = ts.getSize()
    for i in range(0, N - wdSize):
        output.times.append(i + wdSize)
        output.values.append(realisedVariance(ts.getWindow(i, wdSize)))
    return output

def cutWindow(ts, wdSize, fromIdx):
    output = timeSeries()
    for i in range(wdSize):
        output.times.append(ts.times[fromIdx + i])
        output.values.append(ts.values[fromIdx + i])
    return output

def sumDiffsAtPow(vect, power):
    N = len(vect)
    output = 0
    for i in range(1, N):
        output = output + pow(math.log(vect[i]) - math.log(vect[i - 1]), power)
    return output

def signaturePlot(ts, maxLag):
    output = timeSeries()
    for i in range(1, maxLag + 1):
        toCompute = subSampling(ts, i).values
        output.times.append(i)
        output.values.append(math.sqrt(252 * 24 * 3600 * sumDiffsAtPow(toCompute, 2) / (ts.times[-1] - ts.times[0])))
    return output

def splotTheorique(a, b, m, t):
    A = 2 * float(m) / (1 - (float(a) / float(b)))
    B = math.pow(1 / (1 + (float(a) / float(b))), 2)
    C = 1 - B
    D = (1 - math.exp(-(float(a) + float(b)) * t)) / ((float(a) + float(b)) * float(t))
    return A * (B + C * D)

def regressionError(a, b, m, t, y):
    N = len(t)
    output = 0
    for i in range(0, N):
        output = output + pow(splotTheorique(a, b, m, t[i]) - y[i] , 2)
    return output


class testMin(gAlgo):
    def __init__(self, C):
        self.C = C
        gAlgo.__init__(self)
    def mainFunction(self, p):
        return - regressionError(p.get1(), p.get2(), p.get3(), self.C.times, self.C.values)

class maxLkhood(gAlgo):
    def __init__(self, C):
        self.C = C
        gAlgo.__init__(self)
    def aux(self, k, b):
        output = 0.0
        if(k > 1):
            times = self.C.times
            for j in range(0, k - 1):
                output = output + math.exp(-b * (times[k - 1] - times[j]))
        return output
    def mainFunction(self, p):
        a = p.get1() 
        b = p.get2()
        m = p.get3()
        n = len(self.C.times)
        times = self.C.times
        output = -m * times[n - 1]
        for k in range(0, n):
            output = output + (math.exp(-b * (times[n - 1] - times[k])) - 1.0) * (a / b) + math.log(m + a * self.aux(k + 1, b))
        return output





