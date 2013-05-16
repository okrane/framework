'''
Created on Sep 27, 2010

@author: syarc
'''

from math import modf, floor, sqrt

def quantile(x, q, issorted=False):
    '''
        Args:
            x - input data
            q - quantile
            issorted - is x already sorted

        Compute quantile from list x. For median, use q =0.5.
    '''

    if not issorted:
        y = sorted(x)
    else:
        y = x

    if len(x) == 0:
        return 0

    # Parameters for the Hyndman and Fan algorithm
    abcd = [(0, 0, 1, 0), # inverse empirical distrib.function., R type 1
            (0.5, 0, 1, 0), # similar to type 1, averaged, R type 2
            (0.5, 0, 0, 0), # nearest order statistic,(SAS) R type 3

            (0, 0, 0, 1), # California linear interpolation, R type 4
            (0.5, 0, 0, 1), # hydrologists method, R type 5
            (0, 1, 0, 1), # mean-based estimate(Weibull method), (SPSS,Minitab), type 6
            (1, -1, 0, 1), # mode-based method,(S, S-Plus), R type 7
            (1.0 / 3, 1.0 / 3, 0, 1), # median-unbiased ,  R type 8
            (3 / 8.0, 0.25, 0, 1)   # normal-unbiased, R type 9.
           ]

    a, b, c, d = abcd[7 - 1]
    n = len(x)
    g, j = modf(a + (n + b) * q - 1)
    if j < 0:
        return y[0]
    elif j > n:
        return y[n]

    j = int(floor(j))
    if g == 0:
        return y[j]
    else:
        return y[j] + (y[j + 1] - y[j]) * (c + d * g)


def mean(x):
    ''' Compute the mean of a iterable '''
    return sum(x) / len(x)

def weighted_mean(x, w):
    ''' Compute the weight mean ''' 
    return sum(a * b for a,b in zip(x,w)) / float(sum(w))

def weighted_std(x, w):
    sw = sum(w)
    mean = weighted_mean(x,w)
    x2 =  sum(b * (a - mean) * (a - mean) for a,b in zip(x,w))
    return sqrt(x2 / sum(w))

def variance(x):
    ''' Compute the variance of the timeseries (divisor is n-1) '''

    mean_square = mean(x)**2
    n = len(x)

    if n == 1:
        return 0

    sum_square = sum(a**2 for a in x)
    return sum_square / (n - 1) - (n * mean_square) / (n - 1)


def covariance(x, y):
    ''' Compute the covariance of x and y '''
    if len(x) != len(y):
        raise ValueError('x and y should have same length (x: %d, y %d)' % (len(x), len(y)))

    mean_ts1, mean_ts2 = mean(x), mean(y)
    n = len(x)

    return sum((a - mean_ts1) * (b - mean_ts2) for a, b in zip(x, y)) / (n - 1)

def correlation (x,y):
    return covariance(x,y) / (sqrt(variance(x) * sqrt(variance(y))))


def summary(x, quantiles=[0.1, 0.25, 0.5,0.75,0.99]):
    s = sorted(x)
    return [quantile(s, q, True) for q in quantiles]



if __name__ == '__main__':
    a = range(1,11)
    b = range(5,15)
    
    print a
    print b
    
    print weighted_mean(a, b)
    print weighted_std(a, b)